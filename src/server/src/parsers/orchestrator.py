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
from parsers.binary import (
    binary_metadata,
    extract_printable_text,
    is_probably_binary,
    preview_hex,
    safe_decode_text,
    sha256_bytes,
)
from parsers.contracts import (
    BINARY_PARSER_KEY,
    ClassificationResult,
    ParserPipelineResult,
)
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
            classification = LogPreprocessorService(table_name="logs", profile_name=group.profile_name).classify(
                file_inputs
            )
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
        process = db.query(LogProcess).filter_by(id=_uuid_or_raw(process_id), group_id=_uuid_or_raw(group_id)).first()
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
        preprocessor = LogPreprocessorService(table_name="logs", use_llm=use_llm, profile_name=profile_name)
        classification = preprocessor.classify(file_inputs)

        pipeline_result = _parse_and_merge(file_inputs=file_inputs, classification=classification)

        if persist:
            _persist_artifacts(db=db, megabase_db=megabase_db, group_id=group_id, result=pipeline_result)

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

        process = db.query(LogProcess).filter_by(id=_uuid_or_raw(process_id), group_id=_uuid_or_raw(group_id)).first()
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

        preprocessor = LogPreprocessorService(table_name="logs", use_llm=True, profile_name=profile_name)
        classification = preprocessor.classify(file_inputs)
        process.classification = classification.model_dump_json()
        db.commit()

        pipeline_result = _parse_and_merge(file_inputs=file_inputs, classification=classification)
        if not pipeline_result.table_definitions:
            _fail(db, process, "; ".join(pipeline_result.warnings) or "No tables were produced.")
            return

        _persist_artifacts(db=db, megabase_db=megabase_db, group_id=group_id, result=pipeline_result)

        safe_result = _make_json_safe_pipeline_result(pipeline_result)
        process.result = json.dumps(safe_result, ensure_ascii=True, default=str)
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
            logger.warning("Skipping file %s because storage payload could not be downloaded.", asset.name)
            continue

        decoded_members = _decode_payload(asset.name, raw_bytes)
        for member_input in decoded_members:
            member_input.file_id = str(file_row.id)
            file_inputs.append(member_input)

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


def _decode_payload_to_file_input(filename: str, raw_bytes: bytes) -> FileInput:
    """Decode *raw_bytes* into a ``FileInput``, preserving binary data.

    For text-like content, produces a normal ``FileInput`` with decoded text.
    For binary content, preserves ``raw_bytes``, extracts printable strings
    as ``content``, and sets binary metadata.
    """
    meta = binary_metadata(raw_bytes, filename)
    is_binary = meta["is_binary"]

    if is_binary:
        extracted_text = extract_printable_text(raw_bytes)
        return FileInput(
            filename=filename,
            content=extracted_text,
            raw_bytes=raw_bytes,
            is_binary=True,
            byte_length=meta["byte_length"],
            sha256=meta["sha256"],
        )

    # Text content — decode normally
    decoded = _decode_bytes(raw_bytes, filename)
    return FileInput(
        filename=filename,
        content=decoded,
        raw_bytes=None,
        is_binary=False,
        byte_length=len(raw_bytes),
        sha256=sha256_bytes(raw_bytes),
    )


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


def _decode_payload(filename: str, raw_bytes: bytes) -> list[FileInput]:
    """Decode *raw_bytes* into one or more ``FileInput`` objects.

    Archives (ZIP, GZIP, tar) are expanded into per-member ``FileInput``
    objects. Plain files produce a single ``FileInput``. Each member is
    independently classified as text or binary.
    """
    archive_members = _extract_archive_members(filename, raw_bytes)
    if archive_members:
        expanded: list[FileInput] = []
        for member_name, member_bytes in archive_members:
            synthetic_name = f"{filename}:{member_name}"
            expanded.append(_decode_payload_to_file_input(synthetic_name, member_bytes))
        return expanded
    return [_decode_payload_to_file_input(filename, raw_bytes)]


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
        base_name = filename[:-3] if filename.lower().endswith(".gz") else f"{filename}.decompressed"
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


def _make_json_safe_pipeline_result(result: ParserPipelineResult) -> dict[str, Any]:
    """Convert a ``ParserPipelineResult`` to a JSON-safe dict.

    Replaces any raw ``bytes`` in records with metadata summaries
    (byte length, SHA-256, truncation flag) to avoid serialization
    failures when storing ``process.result``.
    """
    data = result.model_dump()
    for table_name, records in data.get("records", {}).items():
        for record in records:
            for key, value in list(record.items()):
                if isinstance(value, bytes):
                    record[key] = {
                        "_binary": True,
                        "byte_length": len(value),
                        "sha256": sha256_bytes(value),
                        "truncated": False,
                    }
    return data


def _parse_and_merge(
    file_inputs: list[FileInput],
    classification: ClassificationResult,
) -> ParserPipelineResult:
    """Parse all files using the appropriate parser for each file type.

    - Binary files (``is_binary=True``) are routed to ``binary_file`` parser.
    - Text files are parsed with ``universal_ai``, falling back to ``raw_ingest``.

    Confidence aggregation uses conservative row-weighted scoring:
      - Each parser's confidence is weighted by its row contribution.
      - Fallback usage incurs a penalty.
      - If raw_ingest contributes >=50% of rows the result is capped at 0.55.
      - If raw_ingest contributes >=80% of rows the result is capped at 0.45.
    """
    merged_table_definitions: list[Any] = []
    merged_records: dict[str, list[dict[str, Any]]] = {}
    merged_warnings: list[str] = []
    used_parser_keys: list[str] = []
    # Track per-parser confidence and row counts for weighted aggregation
    parser_confidences: list[float] = []
    parser_row_counts: list[int] = []
    merged_diagnostics: dict[str, Any] = {
        "parsers": {},
        "fallbacks": [],
        "table_row_counts": {},
    }

    # ── Split binary vs. text files ──────────────────────────────────
    binary_inputs: list[FileInput] = []
    text_inputs: list[FileInput] = []
    for fi in file_inputs:
        if fi.is_binary:
            binary_inputs.append(fi)
        else:
            text_inputs.append(fi)

    fallback_used = False  # tracks whether raw_ingest fallback was used

    # ── Parse binary files with BinaryFileParser ─────────────────────
    if binary_inputs:
        try:
            binary_pipeline = ParserRegistry.route(BINARY_PARSER_KEY)
            binary_result = binary_pipeline.ingest(binary_inputs, classification)

            merged_table_definitions.extend(binary_result.table_definitions)
            merged_records.update(binary_result.records)
            merged_warnings.extend(binary_result.warnings)
            used_parser_keys.append(binary_result.parser_key)
            merged_diagnostics["parsers"][BINARY_PARSER_KEY] = binary_result.diagnostics or {}

            binary_row_count = sum(len(rows) for rows in binary_result.records.values())
            parser_confidences.append(binary_result.confidence)
            parser_row_counts.append(binary_row_count)
        except Exception as error:  # noqa: BLE001
            logger.exception("Binary file parser failed")
            merged_warnings.append(f"Binary file parser failed: {error}")
            merged_diagnostics["fallbacks"].append(
                {
                    "from_parser": BINARY_PARSER_KEY,
                    "to_parser": "(none)",
                    "reason": str(error),
                }
            )

    # ── Parse text files with universal AI parser ────────────────────
    if not text_inputs:
        # All files were binary — skip AI/text parser entirely
        pass
    else:
        # Try universal AI parser first
        ai_result = None
        try:
            ai_pipeline = ParserRegistry.route("universal_ai")
            ai_result = ai_pipeline.ingest(text_inputs, classification)

            merged_table_definitions.extend(ai_result.table_definitions)
            merged_records.update(ai_result.records)
            merged_warnings.extend(ai_result.warnings)
            used_parser_keys.append(ai_result.parser_key)
            merged_diagnostics["parsers"]["universal_ai"] = ai_result.diagnostics or {}

            ai_row_count = sum(len(rows) for rows in ai_result.records.values())
            parser_confidences.append(ai_result.confidence)
            parser_row_counts.append(ai_row_count)
        except Exception as error:  # noqa: BLE001
            logger.exception("Universal AI parser failed")
            merged_warnings.append(f"Universal AI parser failed: {error}")
            merged_diagnostics["fallbacks"].append(
                {
                    "from_parser": "universal_ai",
                    "to_parser": "raw_ingest",
                    "reason": str(error),
                }
            )

        # If AI returned no rows or failed, fall back to raw ingest
        ai_has_rows = ai_result is not None and any(ai_result.records.values())
        if not ai_has_rows:
            fallback_used = True
            try:
                fallback_pipeline = ParserRegistry.route("raw_ingest")
                fallback_result = fallback_pipeline.ingest(text_inputs, classification)

                # If AI had partial results, merge fallback into them
                if fallback_result.table_definitions:
                    merged_table_definitions = fallback_result.table_definitions
                if fallback_result.records:
                    merged_records.update(fallback_result.records)
                merged_warnings.extend(fallback_result.warnings)
                used_parser_keys.append(fallback_result.parser_key)
                merged_diagnostics["parsers"]["raw_ingest"] = fallback_result.diagnostics or {}
                merged_diagnostics["fallbacks"].append(
                    {
                        "from_parser": "universal_ai" if ai_result is not None else "(none)",
                        "to_parser": "raw_ingest",
                        "reason": "AI parser produced no rows" if ai_result else "AI parser failed",
                    }
                )

                fallback_row_count = sum(len(rows) for rows in fallback_result.records.values())
                parser_confidences.append(fallback_result.confidence)
                parser_row_counts.append(fallback_row_count)
            except Exception as fallback_error:  # noqa: BLE001
                logger.exception("Raw ingest fallback also failed")
                merged_warnings.append(f"Raw ingest fallback also failed: {fallback_error}")
                fallback_used = False
        else:
            fallback_used = False

    # ── Conservative confidence aggregation ──────────────────────────
    total_rows = sum(parser_row_counts)

    if total_rows == 0:
        final_confidence = 0.0
    elif not parser_confidences:
        final_confidence = 0.0
    else:
        # Row-weighted average of parser confidences
        weighted_confidence = (
            sum(conf * count for conf, count in zip(parser_confidences, parser_row_counts)) / total_rows
        )

        # Determine raw_ingest row ratio (if fallback was used)
        raw_ingest_row_ratio = 0.0
        fallback_penalty = 0.0
        applied_cap = None

        if fallback_used and len(parser_row_counts) > 1:
            # The fallback parser is always the last one in the list
            raw_ingest_row_ratio = parser_row_counts[-1] / total_rows if total_rows > 0 else 0.0
            # Fallback penalty scales with how much of the data came from raw_ingest
            fallback_penalty = 0.15 + 0.25 * raw_ingest_row_ratio
        elif fallback_used and len(parser_row_counts) == 1:
            # Only fallback parser ran (no AI result at all)
            raw_ingest_row_ratio = 1.0
            fallback_penalty = 0.15 + 0.25 * 1.0

        final_confidence = weighted_confidence - fallback_penalty

        # Apply caps for fallback-heavy results
        if raw_ingest_row_ratio >= 0.80:
            final_confidence = min(final_confidence, 0.45)
            applied_cap = 0.45
        elif raw_ingest_row_ratio >= 0.50:
            final_confidence = min(final_confidence, 0.55)
            applied_cap = 0.55

        final_confidence = max(0.0, min(1.0, final_confidence))

        merged_diagnostics["confidence_aggregation"] = {
            "formula_version": "orchestrator-v1",
            "weighted_confidence": round(weighted_confidence, 4),
            "fallback_used": fallback_used,
            "raw_ingest_row_ratio": round(raw_ingest_row_ratio, 4),
            "fallback_penalty": round(fallback_penalty, 4),
            "applied_cap": applied_cap,
            "final_confidence": round(final_confidence, 4),
        }

    final_parser_key = "mixed"
    if len(set(used_parser_keys)) == 1 and used_parser_keys:
        final_parser_key = used_parser_keys[0]

    merged_diagnostics["parser_used"] = final_parser_key
    merged_diagnostics["row_counts"] = {table_name: len(rows) for table_name, rows in merged_records.items()}

    return ParserPipelineResult(
        table_definitions=merged_table_definitions,
        records=merged_records,
        parser_key=final_parser_key,
        warnings=merged_warnings,
        confidence=round(final_confidence, 2),
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
                "primary_key": column.primary_key,
                "description": column.description or "",
            }
            for column in table_definition.columns
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
    ts_columns = {
        column.name
        for column in table_definition.columns
        if column.sql_type.upper() in {"TIMESTAMP", "DATETIME", "TIMESTAMPTZ"}
    }
    inserted = 0
    for row in rows:
        payload = {}
        for key, value in row.items():
            if key not in allowed_columns:
                continue
            if key in ts_columns:
                payload[key] = _normalize_timestamp_value(value)
            else:
                payload[key] = _normalize_value(value)
        megabase_insert_record(megabase_db, table_definition.table_name, payload)
        inserted += 1
    return inserted


def _normalize_value(value: Any) -> Any:
    sanitized = sanitize_db_value(value)
    if isinstance(sanitized, (dict, list)):
        return json.dumps(sanitized, ensure_ascii=True)
    return sanitized


def _normalize_timestamp_value(value: Any) -> Any:
    """Convert a timestamp value to a timezone-aware datetime for Postgres."""
    from parsers.normalization import parse_timestamp

    if value is None or value == "":
        return None
    parsed = parse_timestamp(value)
    if parsed is not None:
        return parsed
    # If parsing fails, return None rather than a broken string
    return None


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
