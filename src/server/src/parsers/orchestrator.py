from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.orm import Session

from src.lib.database import SessionLocal
from src.lib.megabase import (
    SessionLocal as MegabaseSessionLocal,
    create_table as megabase_create_table,
    init_megabase,
    insert_record as megabase_insert_record,
)
from src.lib.models import Asset, LogEntry, LogFile, LogParseProcess, LogTable
from src.lib.storage import download_file
from src.parsers.contracts import ClassificationResult, ParserPipelineResult
from src.parsers.preprocessor import FileInput, LogPreprocessorService
from src.parsers.registry import ParserRegistry

logger = logging.getLogger(__name__)


def register_pipelines() -> None:
    ParserRegistry.discover(force=True)
    logger.info("Parser pipelines registered: %s", ", ".join(sorted(ParserRegistry.registered_keys())))


def create_parse_process(
    entry_id: str,
    file_inputs: list[FileInput] | None = None,
    file_ids: list[str] | None = None,
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

        process = LogParseProcess(
            entry_id=_uuid_or_raw(entry_id),
            status="queued",
            classification=classification_json,
        )
        db.add(process)
        db.commit()
        db.refresh(process)

        if file_ids:
            logger.info(
                "Created parse process %s for entry %s with %d file id(s).",
                process.id,
                entry_id,
                len(file_ids),
            )

        return str(process.id)
    finally:
        db.close()


def orchestrate_files(
    entry_id: str,
    file_inputs: list[FileInput],
    persist: bool = True,
) -> ParserPipelineResult:
    db = SessionLocal()
    megabase_db = MegabaseSessionLocal()
    try:
        register_pipelines()
        init_megabase(megabase_db)

        classification = LogPreprocessorService(table_name="logs").classify(file_inputs)
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

        process = (
            db.query(LogParseProcess).filter_by(id=_uuid_or_raw(process_id), entry_id=_uuid_or_raw(entry_id)).first()
        )
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

        classification = LogPreprocessorService(table_name="logs").classify(file_inputs)
        process.classification = classification.model_dump_json()
        db.commit()

        pipeline_result = _parse_and_merge(file_inputs=file_inputs, classification=classification)
        if not pipeline_result.table_definitions:
            _fail(db, process, "; ".join(pipeline_result.warnings) or "No tables were produced.")
            return

        _persist_artifacts(db=db, megabase_db=megabase_db, entry_id=entry_id, result=pipeline_result)

        process.result = pipeline_result.model_dump_json()
        process.status = "completed"
        process.error = None
        db.commit()
        logger.info("run_parse_job: process=%s completed", process_id)
    except Exception as error:  # noqa: BLE001
        logger.exception("run_parse_job: unhandled error for process %s", process_id)
        process = db.query(LogParseProcess).filter_by(id=_uuid_or_raw(process_id)).first()
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
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("latin-1")
        except UnicodeDecodeError:
            return raw_bytes.decode("utf-8", errors="ignore")


def _parse_and_merge(file_inputs: list[FileInput], classification: ClassificationResult) -> ParserPipelineResult:
    preferred_keys: list[str] = []
    if classification.selected_parser_key:
        preferred_keys.append(classification.selected_parser_key)

    grouped_inputs, selections, routing_warnings = ParserRegistry.resolve_for_files(
        file_inputs=file_inputs,
        preferred_keys=preferred_keys,
    )

    if not grouped_inputs:
        return ParserPipelineResult(
            table_definitions=[],
            records={},
            parser_key="none",
            warnings=routing_warnings or ["No parser could be selected for the uploaded files."],
            confidence=0.0,
        )

    table_definitions = []
    merged_records: dict[str, list[dict[str, Any]]] = {}
    merged_warnings = list(routing_warnings)
    parser_keys_used: list[str] = []
    confidence_total = 0.0
    confidence_count = 0

    for parser_key, parser_files in grouped_inputs.items():
        try:
            pipeline = ParserRegistry.route(parser_key)
        except KeyError as error:
            merged_warnings.append(str(error))
            continue

        try:
            group_result = pipeline.ingest(parser_files, classification)
        except Exception as error:  # noqa: BLE001
            logger.exception("Parser '%s' failed", parser_key)
            merged_warnings.append(f"Parser '{parser_key}' failed: {error}")
            continue

        table_definitions.extend(group_result.table_definitions)
        merged_records.update(group_result.records)
        merged_warnings.extend(group_result.warnings)
        parser_keys_used.append(parser_key)
        confidence_total += group_result.confidence
        confidence_count += 1

    if not table_definitions:
        selection_summary = ", ".join(
            f"{selection.filename}->{selection.parser_key} ({selection.score:.2f})" for selection in selections
        )
        if selection_summary:
            merged_warnings.append(f"Routing summary: {selection_summary}")

    return ParserPipelineResult(
        table_definitions=table_definitions,
        records=merged_records,
        parser_key=parser_keys_used[0] if len(parser_keys_used) == 1 else "multi_parser",
        warnings=merged_warnings,
        confidence=round((confidence_total / confidence_count) if confidence_count else 0.0, 2),
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

    existing = db.query(LogTable).filter_by(entry_id=_uuid_or_raw(entry_id), name=table_definition.table_name).first()
    if existing:
        existing.table = table_definition.table_name
        existing.schema = schema_json
    else:
        db.add(
            LogTable(
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


def _fail(db: Session, process: LogParseProcess | None, message: str) -> None:
    if process is None:
        return
    try:
        process.status = "failed"
        process.error = message
        db.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Could not persist failure for process %s", getattr(process, "id", "?"))
