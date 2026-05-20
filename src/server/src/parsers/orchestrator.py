from __future__ import annotations

import gzip
import io
import json
import logging
import os
import tarfile
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sqlalchemy.orm import Session

from lib.database import SessionLocal
from lib.megabase import (
    SessionLocal as MegabaseSessionLocal,
    create_table as megabase_create_table,
    init_megabase,
    insert_record as megabase_insert_record,
)
from lib.models import Asset, LogGroup, LogFile, LogProcess, LogTable
from lib.storage import download_file
from parsers.contracts import ClassificationResult, ParserPipelineResult
from parsers.normalization import sanitize_db_value
from parsers.preprocessor import FileInput, LogPreprocessorService
from parsers.registry import ParserRegistry

logger = logging.getLogger(__name__)


def _resolve_parse_job_workers() -> int:
    raw_value = os.environ.get("LOG_PARSE_JOB_WORKERS", "4").strip()
    try:
        workers = int(raw_value)
    except ValueError:
        workers = 4
    return max(workers, 1)


PARSE_JOB_EXECUTOR = ThreadPoolExecutor(
    max_workers=_resolve_parse_job_workers(),
    thread_name_prefix="logdog-parse-job",
)


def register_pipelines() -> None:
    ParserRegistry.discover(force=True)
    logger.info("Parser pipelines registered: %s", ", ".join(sorted(ParserRegistry.registered_keys())))


def create_process(
    group_id: str,
    file_inputs: list[FileInput] | None = None,
    file_ids: list[str] | None = None,
    file_id: str | None = None,
) -> str:
    db = SessionLocal()
    try:
        group = db.query(LogGroup).filter_by(id=_uuid_or_raw(group_id)).first()
        if group is None:
            raise ValueError(f"Log group '{group_id}' not found.")

        classification_json: str | None = None
        if file_inputs:
            classification = LogPreprocessorService(
                table_name="logs", profile_name=group.profile_name
            ).classify(file_inputs)
            classification_json = classification.model_dump_json()

        process = LogProcess(
            group_id=_uuid_or_raw(group_id),
            file_id=_uuid_or_raw(file_id) if file_id else None,
            status="queued",
            classification=classification_json,
        )
        db.add(process)
        db.commit()
        db.refresh(process)

        if file_ids:
            logger.info(
                "Created process %s for group %s with %d file id(s).",
                process.id,
                group_id,
                len(file_ids),
            )

        return str(process.id)
    finally:
        db.close()


def enqueue_process(
    process_id: str,
    group_id: str,
    file_inputs_json: str | None = None,
    file_ids_json: str | None = None,
) -> None:
    PARSE_JOB_EXECUTOR.submit(
        run_parse_job,
        process_id,
        group_id,
        file_inputs_json,
        file_ids_json,
    )


def mark_process_failed(process_id: str, group_id: str, message: str) -> None:
    db = SessionLocal()
    try:
        process = (
            db.query(LogProcess)
            .filter_by(id=_uuid_or_raw(process_id), group_id=_uuid_or_raw(group_id))
            .first()
        )
        _fail(db=db, process=process, message=message)
    finally:
        db.close()


def orchestrate_files(
    group_id: str,
    file_inputs: list[FileInput],
    persist: bool = True,
    use_llm: bool = True,
) -> ParserPipelineResult:
    db = SessionLocal()
    megabase_db = MegabaseSessionLocal()
    try:
        register_pipelines()
        init_megabase(megabase_db)

        group = db.query(LogGroup).filter_by(id=_uuid_or_raw(group_id)).first()
        profile_name = group.profile_name if group is not None else "default"
        preprocessor = LogPreprocessorService(
            table_name="logs", use_llm=use_llm, profile_name=profile_name
        )
        classification = preprocessor.classify(file_inputs)

        pipeline_result = _parse_and_merge(
            file_inputs=file_inputs, classification=classification
        )

        if persist:
            _persist_artifacts(
                db=db, megabase_db=megabase_db, group_id=group_id, result=pipeline_result
            )

        return pipeline_result
    finally:
        megabase_db.close()
        db.close()


def run_parse_job(
    process_id: str,
    group_id: str,
    file_inputs_json: str | None = None,
    file_ids_json: str | None = None,
) -> None:
    db = SessionLocal()
    megabase_db = MegabaseSessionLocal()
    init_megabase(megabase_db)

    try:
        register_pipelines()

        process = (
            db.query(LogProcess)
            .filter_by(id=_uuid_or_raw(process_id), group_id=_uuid_or_raw(group_id))
            .first()
        )
        if process is None:
            logger.error("run_parse_job: process %s not found for group %s", process_id, group_id)
            return

        process.status = "processing"
        process.error = None
        db.commit()

        group = db.query(LogGroup).filter_by(id=_uuid_or_raw(group_id)).first()
        profile_name = group.profile_name if group is not None else "default"

        file_inputs = _resolve_file_inputs(
            db=db,
            group_id=group_id,
            file_inputs_json=file_inputs_json,
            file_ids_json=file_ids_json,
        )
        if not file_inputs:
            _fail(db, process, "No file inputs available to parse.")
            return

        preprocessor = LogPreprocessorService(
            table_name="logs", use_llm=True, profile_name=profile_name
        )
        classification = preprocessor.classify(file_inputs)
        process.classification = classification.model_dump_json()
        db.commit()

        pipeline_result = _parse_and_merge(
            file_inputs=file_inputs, classification=classification
        )
        if not pipeline_result.table_definitions:
            _fail(db, process, "; ".join(pipeline_result.warnings) or "No tables were produced.")
            return

        _persist_artifacts(
            db=db, megabase_db=megabase_db, group_id=group_id, result=pipeline_result
        )

        process.result = pipeline_result.model_dump_json()
        process.status = "completed"
        process.error = None
        db.commit()
        logger.info("run_parse_job: process=%s completed", process_id)
    except Exception as error:  # noqa: BLE001
        logger.exception("run_parse_job: unhandled error for process %s", process_id)
        process = db.query(LogProcess).filter_by(id=_uuid_or_raw(process_id)).first()
        _fail(db, process, str(error))
    finally:
        megabase_db.close()
        db.close()


# ── File input resolution ────────────────────────────────────────────────


def _resolve_file_inputs(
    db: Session,
    group_id: str,
    file_inputs_json: str | None,
    file_ids_json: str | None,
) -> list[FileInput]:
    if file_inputs_json:
        raw_inputs: list[dict[str, Any]] = json.loads(file_inputs_json)
        return [FileInput(**item) for item in raw_inputs]

    file_id_filter: set[str] | None = None
    if file_ids_json:
        parsed_ids = json.loads(file_ids_json)
        file_id_filter = {str(value) for value in parsed_ids}

    file_rows = db.query(LogFile).filter_by(group_id=_uuid_or_raw(group_id)).all()
    if file_id_filter:
        file_rows = [row for row in file_rows if str(row.id) in file_id_filter]

    file_inputs: list[FileInput] = []
    for file_row in file_rows:
        asset = db.query(Asset).filter_by(id=file_row.asset_id).first()
        if asset is None:
            logger.warning("Skipping log_file %s because linked asset is missing.", file_row.id)
            continue

        raw_bytes = download_file(file_row.asset_id, db=db)
        if raw_bytes is None:
            logger.warning(
                "Skipping file %s because storage payload could not be downloaded.", asset.name
            )
            continue

        decoded_members = _decode_payload(asset.name, raw_bytes)
        for synthetic_name, content in decoded_members:
            file_inputs.append(
                FileInput(
                    file_id=str(file_row.id),
                    filename=synthetic_name,
                    content=content,
                )
            )

    return file_inputs


# ── Archive / file decode helpers ────────────────────────────────────────


def _decode_bytes(raw_bytes: bytes, filename: str) -> str:
    if _is_hex_dump(raw_bytes):
        return _decode_hex_dump(raw_bytes)
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("latin-1")
        except UnicodeDecodeError:
            return raw_bytes.decode("utf-8", errors="ignore")


def _is_hex_dump(raw_bytes: bytes) -> bool:
    try:
        text = raw_bytes.decode("ascii", errors="ignore")
        lines = text.splitlines()[:10]
        hex_lines = 0
        for line in lines:
            parts = line.split()
            if len(parts) >= 2 and all(
                len(p) == 2 and all(c in "0123456789abcdefABCDEF" for c in p) for p in parts[:8]
            ):
                hex_lines += 1
        return hex_lines >= 3
    except Exception:
        return False


def _decode_hex_dump(raw_bytes: bytes) -> str:
    try:
        text = raw_bytes.decode("ascii", errors="ignore")
        result = []
        for line in text.splitlines():
            parts = line.split()
            hex_part = ""
            for p in parts:
                if len(p) == 2 and all(c in "0123456789abcdefABCDEF" for c in p):
                    hex_part += p
            if hex_part:
                try:
                    decoded = bytes.fromhex(hex_part).decode("utf-8", errors="ignore")
                    if decoded.strip():
                        result.append(decoded)
                except ValueError:
                    pass
        return "\n".join(result) if result else text
    except Exception:
        return raw_bytes.decode("utf-8", errors="ignore")


def _decode_payload(filename: str, raw_bytes: bytes) -> list[tuple[str, str]]:
    archive_members = _extract_archive_members(filename, raw_bytes)
    if archive_members:
        expanded: list[tuple[str, str]] = []
        for member_name, member_bytes in archive_members:
            synthetic_name = f"{filename}:{member_name}"
            expanded.append((synthetic_name, _decode_bytes(member_bytes, synthetic_name)))
        return expanded
    return [(filename, _decode_bytes(raw_bytes, filename))]


def _extract_archive_members(filename: str, raw_bytes: bytes) -> list[tuple[str, bytes]]:
    MAX_MEMBERS = 100
    MAX_MEMBER_SIZE = 10 * 1024 * 1024

    if raw_bytes.startswith(b"PK\x03\x04"):
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            members: list[tuple[str, bytes]] = []
            for info in zf.infolist():
                if len(members) >= MAX_MEMBERS:
                    break
                if info.is_dir():
                    continue
                if info.file_size > MAX_MEMBER_SIZE:
                    continue
                members.append((info.filename, zf.read(info.filename)))
            return members

    if raw_bytes.startswith(b"\x1f\x8b\x08"):
        decompressed = gzip.decompress(raw_bytes)
        if len(decompressed) > MAX_MEMBER_SIZE:
            decompressed = decompressed[:MAX_MEMBER_SIZE]
        base_name = (
            filename[:-3] if filename.lower().endswith(".gz") else f"{filename}.decompressed"
        )
        return [(base_name, decompressed)]

    if len(raw_bytes) > 262 and raw_bytes[257:262] == b"ustar":
        members = []
        with tarfile.open(fileobj=io.BytesIO(raw_bytes), mode="r:*") as tf:
            for member in tf.getmembers():
                if len(members) >= MAX_MEMBERS:
                    break
                if not member.isfile():
                    continue
                if member.size > MAX_MEMBER_SIZE:
                    continue
                extracted = tf.extractfile(member)
                if extracted is None:
                    continue
                members.append((member.name, extracted.read()))
        return members

    return []


# ── Parse and merge with universal AI parser + raw fallback ──────────────


def _parse_and_merge(
    file_inputs: list[FileInput],
    classification: ClassificationResult,
) -> ParserPipelineResult:
    """Parse all files using the universal AI parser, falling back to raw ingest."""
    merged_table_definitions: list[Any] = []
    merged_records: dict[str, list[dict[str, Any]]] = {}
    merged_warnings: list[str] = []
    confidence_values: list[float] = []
    used_parser_keys: list[str] = []
    merged_diagnostics: dict[str, Any] = {
        "parsers": {},
        "fallbacks": [],
        "table_row_counts": {},
    }

    # Try universal AI parser first
    try:
        ai_pipeline = ParserRegistry.route("universal_ai")
        ai_result = ai_pipeline.ingest(file_inputs, classification)

        merged_table_definitions.extend(ai_result.table_definitions)
        merged_records.update(ai_result.records)
        merged_warnings.extend(ai_result.warnings)
        confidence_values.append(ai_result.confidence)
        used_parser_keys.append(ai_result.parser_key)
        merged_diagnostics["parsers"]["universal_ai"] = ai_result.diagnostics or {}
    except Exception as error:  # noqa: BLE001
        logger.exception("Universal AI parser failed")
        merged_warnings.append(f"Universal AI parser failed: {error}")
        merged_diagnostics["fallbacks"].append({
            "from_parser": "universal_ai",
            "to_parser": "raw_ingest",
            "reason": str(error),
        })
        ai_result = None

    # If AI returned no rows or failed, fall back to raw ingest
    if ai_result is None or ai_result.confidence < 0.1 or not merged_records:
        try:
            fallback_pipeline = ParserRegistry.route("raw_ingest")
            fallback_result = fallback_pipeline.ingest(file_inputs, classification)

            # If AI had partial results, merge fallback into them
            if fallback_result.table_definitions:
                merged_table_definitions = fallback_result.table_definitions
            if fallback_result.records:
                merged_records.update(fallback_result.records)
            merged_warnings.extend(fallback_result.warnings)
            confidence_values.append(fallback_result.confidence)
            used_parser_keys.append(fallback_result.parser_key)
            merged_diagnostics["parsers"]["raw_ingest"] = fallback_result.diagnostics or {}
            merged_diagnostics["fallbacks"].append({
                "from_parser": "universal_ai" if ai_result is not None else "(none)",
                "to_parser": "raw_ingest",
                "reason": "AI parser produced insufficient rows" if ai_result else "AI parser failed",
            })
        except Exception as fallback_error:  # noqa: BLE001
            logger.exception("Raw ingest fallback also failed")
            merged_warnings.append(f"Raw ingest fallback also failed: {fallback_error}")

    final_confidence = (
        round(sum(confidence_values) / len(confidence_values), 2) if confidence_values else 0.0
    )
    final_parser_key = "mixed"
    if len(set(used_parser_keys)) == 1 and used_parser_keys:
        final_parser_key = used_parser_keys[0]

    merged_diagnostics["parser_used"] = final_parser_key
    merged_diagnostics["row_counts"] = {
        table_name: len(rows) for table_name, rows in merged_records.items()
    }

    return ParserPipelineResult(
        table_definitions=merged_table_definitions,
        records=merged_records,
        parser_key=final_parser_key,
        warnings=merged_warnings,
        confidence=final_confidence,
        diagnostics=merged_diagnostics,
    )


# ── Megabase persistence ─────────────────────────────────────────────────


def _persist_artifacts(
    db: Session,
    megabase_db: Session,
    group_id: str,
    result: ParserPipelineResult,
) -> None:
    for table_definition in result.table_definitions:
        rows = result.records.get(table_definition.table_name, [])
        if not rows:
            logger.debug("Skipping empty table=%s (0 rows)", table_definition.table_name)
            continue
        _ensure_megabase_table(megabase_db, table_definition)
        inserted = _insert_rows(megabase_db, table_definition, rows)
        logger.debug("Persisted table=%s rows=%d", table_definition.table_name, inserted)
        _sync_log_table(db=db, group_id=group_id, table_definition=table_definition)


def _ensure_megabase_table(megabase_db: Session, table_definition: Any) -> None:
    schema = {
        "columns": [
            {
                "name": column.name,
                "type": _sql_to_megabase_type(column.sql_type),
                "nullable": column.nullable,
                "primary_key": bool(column.primary_key and column.name != "id"),
                "description": column.description or "",
            }
            for column in table_definition.columns
            if column.name != "id"
        ]
    }

    try:
        megabase_create_table(megabase_db, table_definition.table_name, schema)
    except ValueError as error:
        if "already exists" not in str(error).lower():
            raise


def _insert_rows(
    megabase_db: Session,
    table_definition: Any,
    rows: list[dict[str, Any]],
) -> int:
    allowed_columns = {column.name for column in table_definition.columns}
    inserted = 0
    for row in rows:
        payload = {
            key: _normalize_value(value)
            for key, value in row.items()
            if key in allowed_columns
        }
        megabase_insert_record(megabase_db, table_definition.table_name, payload)
        inserted += 1
    return inserted


def _normalize_value(value: Any) -> Any:
    sanitized = sanitize_db_value(value)
    if isinstance(sanitized, (dict, list)):
        return json.dumps(sanitized, ensure_ascii=True)
    return sanitized


def _sql_to_megabase_type(sql_type: str) -> str:
    normalized = sql_type.upper()
    if normalized in {"INTEGER", "INT", "SMALLINT"}:
        return "integer"
    if normalized in {"BIGINT"}:
        return "bigint"
    if normalized in {"REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"}:
        return "float"
    if normalized in {"BOOLEAN", "BOOL"}:
        return "boolean"
    if normalized in {"JSON", "JSONB"}:
        return "json"
    if normalized in {"BYTEA", "BLOB", "BINARY"}:
        return "bytea"
    if normalized in {"DATETIME", "TIMESTAMP", "TIMESTAMPTZ"} or "TIMESTAMP" in normalized:
        return "datetime"
    if normalized in {"VARCHAR", "CHAR", "STRING"}:
        return "string"
    return "text"


def _sync_log_table(db: Session, group_id: str, table_definition: Any) -> None:
    schema_json = json.dumps(
        [
            {
                "name": column.name,
                "type": column.sql_type,
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "description": column.description,
            }
            for column in table_definition.columns
        ],
        ensure_ascii=True,
    )

    table_uuid = uuid.UUID(table_definition.table_name)
    existing = db.query(LogTable).filter_by(table=table_definition.table_name).first()
    if existing:
        existing.name = table_definition.display_name
        existing.table = table_definition.table_name
        existing.schema = schema_json
    else:
        db.add(
            LogTable(
                id=table_uuid,
                group_id=_uuid_or_raw(group_id),
                name=table_definition.display_name,
                table=table_definition.table_name,
                schema=schema_json,
            )
        )

    db.commit()


def _uuid_or_raw(value: str) -> Any:
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return value


def _fail(db: Session, process: LogProcess | None, message: str) -> None:
    if process is None:
        return
    try:
        process.status = "failed"
        process.error = message
        db.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Could not persist failure for process %s", getattr(process, "id", "?"))


def get_pipeline_stats() -> dict[str, Any]:
    return {
        "registered_parsers": sorted(ParserRegistry.registered_keys()),
    }
