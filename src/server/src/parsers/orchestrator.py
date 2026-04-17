from __future__ import annotations

import os
import json
import logging
import uuid
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
from lib.models import Asset, LogEntry, LogFile, LogProcess, LogTable
from lib.storage import download_file
from parsers.unified.binary import BinaryHandler
from parsers.contracts import ClassificationResult, ParserPipelineResult
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
    entry_id: str,
    file_inputs: list[FileInput] | None = None,
    file_ids: list[str] | None = None,
    file_id: str | None = None,
) -> str:
    db = SessionLocal()
    try:
        entry = db.query(LogEntry).filter_by(id=_uuid_or_raw(entry_id)).first()
        if entry is None:
            raise ValueError(f"Log entry '{entry_id}' not found.")

        classification_json: str | None = None
        if file_inputs:
            classification = LogPreprocessorService(table_name="logs").classify(file_inputs)
            classification_json = classification.model_dump_json()

        process = LogProcess(
            entry_id=_uuid_or_raw(entry_id),
            file_id=_uuid_or_raw(file_id) if file_id else None,
            status="queued",
            classification=classification_json,
        )
        db.add(process)
        db.commit()
        db.refresh(process)

        if file_ids:
            logger.info(
                "Created process %s for entry %s with %d file id(s).",
                process.id,
                entry_id,
                len(file_ids),
            )

        return str(process.id)
    finally:
        db.close()


def enqueue_process(
    process_id: str,
    entry_id: str,
    file_inputs_json: str | None = None,
    file_ids_json: str | None = None,
) -> None:
    PARSE_JOB_EXECUTOR.submit(
        run_parse_job,
        process_id,
        entry_id,
        file_inputs_json,
        file_ids_json,
    )


def mark_process_failed(process_id: str, entry_id: str, message: str) -> None:
    db = SessionLocal()
    try:
        process = db.query(LogProcess).filter_by(id=_uuid_or_raw(process_id), entry_id=_uuid_or_raw(entry_id)).first()
        _fail(db=db, process=process, message=message)
    finally:
        db.close()


def orchestrate_files(
    entry_id: str,
    file_inputs: list[FileInput],
    persist: bool = True,
    use_llm: bool = True,
) -> ParserPipelineResult:
    db = SessionLocal()
    megabase_db = MegabaseSessionLocal()
    try:
        register_pipelines()
        init_megabase(megabase_db)

        preprocessor = LogPreprocessorService(table_name="logs", use_llm=use_llm)
        if use_llm:
            classification = preprocessor.classify_with_llm(file_inputs)
        else:
            classification = preprocessor.classify(file_inputs)

        pipeline_result = _parse_and_merge(file_inputs=file_inputs, classification=classification)

        if persist:
            _persist_artifacts(db=db, megabase_db=megabase_db, entry_id=entry_id, result=pipeline_result)

        return pipeline_result
    finally:
        megabase_db.close()
        db.close()


def run_parse_job(
    process_id: str,
    entry_id: str,
    file_inputs_json: str | None = None,
    file_ids_json: str | None = None,
) -> None:
    db = SessionLocal()
    megabase_db = MegabaseSessionLocal()
    init_megabase(megabase_db)

    try:
        register_pipelines()

        process = db.query(LogProcess).filter_by(id=_uuid_or_raw(process_id), entry_id=_uuid_or_raw(entry_id)).first()
        if process is None:
            logger.error("run_parse_job: process %s not found for entry %s", process_id, entry_id)
            return

        process.status = "processing"
        process.error = None
        db.commit()

        file_inputs = _resolve_file_inputs(
            db=db,
            entry_id=entry_id,
            file_inputs_json=file_inputs_json,
            file_ids_json=file_ids_json,
        )
        if not file_inputs:
            _fail(db, process, "No file inputs available to parse.")
            return

        preprocessor = LogPreprocessorService(table_name="logs", use_llm=True)
        classification = preprocessor.classify_with_llm(file_inputs)
        process.classification = classification.model_dump_json()
        db.commit()

        pipeline_result = _parse_and_merge(file_inputs=file_inputs, classification=classification)
        if not pipeline_result.table_definitions:
            _fail(db, process, "; ".join(pipeline_result.warnings) or "No tables were produced.")
            return

        _persist_artifacts(db=db, megabase_db=megabase_db, entry_id=entry_id, result=pipeline_result)

        _record_feedback(file_inputs=file_inputs, classification=classification, result=pipeline_result)

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


def _resolve_file_inputs(
    db: Session,
    entry_id: str,
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

    file_rows = db.query(LogFile).filter_by(entry_id=_uuid_or_raw(entry_id)).all()
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

        content = _decode_bytes(raw_bytes)
        file_inputs.append(
            FileInput(
                file_id=str(file_row.id),
                filename=asset.name,
                content=content,
            )
        )

    return file_inputs


def _decode_bytes(raw_bytes: bytes) -> str:
    binary_handler = BinaryHandler()
    if binary_handler.is_binary_extension("file.bin"):
        decode_result = binary_handler.analyze_and_decode(raw_bytes)
        if decode_result.decoded_lines:
            return "\n".join(decode_result.decoded_lines)

    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("latin-1")
        except UnicodeDecodeError:
            return raw_bytes.decode("utf-8", errors="ignore")


def _parse_and_merge(file_inputs: list[FileInput], classification: ClassificationResult) -> ParserPipelineResult:
    parser_key = classification.selected_parser_key or "unified"

    try:
        pipeline = ParserRegistry.route(parser_key)
    except KeyError as error:
        return ParserPipelineResult(
            table_definitions=[],
            records={},
            parser_key="none",
            warnings=[str(error)],
            confidence=0.0,
        )

    try:
        return pipeline.ingest(file_inputs, classification)
    except Exception as error:  # noqa: BLE001
        logger.exception("Parser '%s' failed", parser_key)
        return ParserPipelineResult(
            table_definitions=[],
            records={},
            parser_key=parser_key,
            warnings=[f"Parser '{parser_key}' failed: {error}"],
            confidence=0.0,
        )


def _persist_artifacts(
    db: Session,
    megabase_db: Session,
    entry_id: str,
    result: ParserPipelineResult,
) -> None:
    for table_definition in result.table_definitions:
        rows = result.records.get(table_definition.table_name, [])
        _ensure_megabase_table(megabase_db, table_definition)
        inserted = _insert_rows(megabase_db, table_definition, rows)
        logger.debug("Persisted table=%s rows=%d", table_definition.table_name, inserted)

        _sync_log_table(db=db, entry_id=entry_id, table_definition=table_definition)


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


def _insert_rows(megabase_db: Session, table_definition: Any, rows: list[dict[str, Any]]) -> int:
    allowed_columns = {column.name for column in table_definition.columns if column.name != "id"}
    inserted = 0
    for row in rows:
        payload = {key: _normalize_value(value) for key, value in row.items() if key in allowed_columns}
        megabase_insert_record(megabase_db, table_definition.table_name, payload)
        inserted += 1
    return inserted


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True)
    return value


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


def _sync_log_table(db: Session, entry_id: str, table_definition: Any) -> None:
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
    existing = db.query(LogTable).filter_by(id=table_uuid).first()
    if existing:
        existing.name = table_definition.table_name
        existing.table = table_definition.table_name
        existing.schema = schema_json
    else:
        db.add(
            LogTable(
                id=table_uuid,
                entry_id=_uuid_or_raw(entry_id),
                name=table_definition.table_name,
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


def _record_feedback(
    file_inputs: list[FileInput],
    classification: ClassificationResult,
    result: ParserPipelineResult,
) -> None:
    from parsers.few_shot_store import FewShotStore
    from parsers.schema_cache import SchemaCache

    few_shot_store = FewShotStore()
    schema_cache = SchemaCache()

    for file_input in file_inputs:
        file_classification = next(
            (fc for fc in classification.file_classifications if fc.file_id == file_input.file_id),
            None,
        )
        if not file_classification:
            continue

        lines = file_input.content.splitlines()
        sample_lines = [line for line in lines[:10] if line.strip()]
        if not sample_lines:
            continue

        few_shot_store.record_successful_parse(
            format_name=file_classification.detected_format,
            domain="unknown",
            sample_lines=sample_lines,
            schema={
                "tables": [
                    {
                        "name": td.table_name,
                        "columns": [col.name for col in td.columns],
                    }
                    for td in result.table_definitions
                ],
            },
            confidence=file_classification.format_confidence,
        )

    for table_definition in result.table_definitions:
        schema_cache.put(
            sample_lines=file_inputs[0].content.splitlines()[:10] if file_inputs else [],
            format_name=classification.dominant_format,
            domain="unknown",
            columns=[
                {"name": col.name, "sql_type": col.sql_type, "description": col.description, "nullable": col.nullable}
                for col in table_definition.columns
            ],
            extraction_strategy="per_line",
        )


def get_pipeline_stats() -> dict[str, Any]:
    from parsers.few_shot_store import FewShotStore
    from parsers.schema_cache import SchemaCache

    few_shot_store = FewShotStore()
    schema_cache = SchemaCache()

    return {
        "few_shot_store": few_shot_store.stats(),
        "schema_cache": schema_cache.stats(),
        "registered_parsers": sorted(ParserRegistry.registered_keys()),
    }
