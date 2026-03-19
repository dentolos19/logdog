"""Background ingestion orchestrator.

``run_ingestion_job`` is meant to be enqueued via FastAPI's
``BackgroundTasks``.  It reconstructs the file inputs from the JSON
payload stored on the process record, routes parsing through the
``ParserRegistry``, persists parsed table data via ``LogDatabaseSwarm``,
syncs ``LogGroupTable`` metadata to the app DB, and finally marks the
process as ``completed`` (or ``failed`` on error).

``register_pipelines`` must be called once at startup (e.g. in
``main.py``) to populate the ``ParserRegistry`` with all three built-in
pipeline implementations.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from lib.database import SessionLocal
from lib.database_swarm import LogDatabaseSwarm
from lib.models import LogGroupProcess, LogGroupTable
from lib.parsers.contracts import ClassificationResult, ParserPipelineResult
from lib.parsers.preprocessor import FileInput
from lib.parsers.registry import ParserRegistry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline registration
# ---------------------------------------------------------------------------


def register_pipelines() -> None:
    """Register all built-in parser pipelines.

    Call once at application startup before handling any requests.
    """
    from lib.parsers.semiStructured.adapter import SemiStructuredParserPipeline
    from lib.parsers.structured.pipeline import StructuredPipeline
    from lib.parsers.unstructured.pipeline import UnstructuredPipeline

    ParserRegistry.register(StructuredPipeline())
    ParserRegistry.register(SemiStructuredParserPipeline())
    ParserRegistry.register(UnstructuredPipeline())

    logger.info(
        "Parser pipelines registered: %s",
        ", ".join(sorted(ParserRegistry.registered_keys())),
    )


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------


def run_ingestion_job(
    process_id: str,
    log_group_id: str,
    file_inputs_json: str,
) -> None:
    """Parse uploaded files and persist the results.

    Intended to run as a FastAPI ``BackgroundTasks`` callback.  Opens its
    own DB session so that it is fully decoupled from the request session.

    Parameters
    ----------
    process_id:
        Primary key of the ``LogGroupProcess`` record to update.
    log_group_id:
        Primary key of the parent ``LogGroup`` (used as the swarm DB key).
    file_inputs_json:
        JSON-serialised ``list[dict]`` from which ``FileInput`` objects are
        reconstructed (``[{"file_id": ..., "filename": ..., "content": ...}]``).
    """
    db = SessionLocal()
    swarm = LogDatabaseSwarm()

    try:
        process = db.query(LogGroupProcess).filter_by(id=process_id).first()
        if process is None:
            logger.error("run_ingestion_job: process %s not found", process_id)
            return

        # Mark in-flight so the UI can show a spinner.
        process.status = "processing"  # type: ignore[assignment]
        db.commit()

        # Reconstruct classification stored at classify-time.
        classification_raw: str | None = process.classification  # type: ignore[assignment]
        if not classification_raw:
            _fail(db, process, "No classification payload found on process record")
            return

        classification = ClassificationResult.model_validate_json(classification_raw)

        # Reconstruct file inputs.
        raw_inputs: list[dict[str, Any]] = json.loads(file_inputs_json)
        file_inputs = [FileInput(**fi) for fi in raw_inputs]

        # Route to the correct parser pipeline.
        preferred_keys: list[str] = []
        if classification.selected_parser_key:
            preferred_keys.append(classification.selected_parser_key)

        grouped_inputs, selections, routing_warnings = ParserRegistry.resolve_for_files(
            file_inputs=file_inputs,
            preferred_keys=preferred_keys,
        )

        if not grouped_inputs:
            _fail(db, process, "No parser could be selected for the uploaded files")
            return

        logger.info(
            "run_ingestion_job: process=%s group=%s parsers=%s files=%d",
            process_id,
            log_group_id,
            ",".join(sorted(grouped_inputs.keys())),
            len(file_inputs),
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
            except KeyError as exc:
                merged_warnings.append(str(exc))
                continue

            try:
                group_result = pipeline.ingest(parser_files, classification)
            except Exception as exc:  # noqa: BLE001
                logger.exception("run_ingestion_job: parser '%s' failed", parser_key)
                merged_warnings.append(f"Parser '{parser_key}' failed: {exc}")
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
            merged_warnings.append(f"Routing summary: {selection_summary}")
            _fail(db, process, "; ".join(merged_warnings) or "All parser groups failed")
            return

        pipeline_result = ParserPipelineResult(
            table_definitions=table_definitions,
            records=merged_records,
            parser_key=parser_keys_used[0] if len(parser_keys_used) == 1 else "multi_parser",
            warnings=merged_warnings,
            confidence=round((confidence_total / confidence_count) if confidence_count else 0.0, 2),
        )

        # Persist to swarm DB and sync metadata to app DB.
        for table_def in pipeline_result.table_definitions:
            rows = pipeline_result.records.get(table_def.table_name, [])

            inserted = swarm.apply_schema_and_insert(
                log_group_id=log_group_id,
                ddl=table_def.sqlite_ddl,
                table_name=table_def.table_name,
                rows=rows,
            )

            logger.debug(
                "  table=%s inserted=%d",
                table_def.table_name,
                inserted,
            )

            _sync_log_table(db, log_group_id, table_def.table_name, table_def)

        # Persist pipeline result and mark completed.
        process.result = pipeline_result.model_dump_json()  # type: ignore[assignment]
        process.status = "completed"  # type: ignore[assignment]
        db.commit()

        logger.info("run_ingestion_job: process=%s completed", process_id)

    except Exception as exc:  # noqa: BLE001
        logger.exception("run_ingestion_job: unhandled error for process %s", process_id)
        _fail(db, db.query(LogGroupProcess).filter_by(id=process_id).first(), str(exc))
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fail(db: Any, process: LogGroupProcess | None, message: str) -> None:
    if process is None:
        return
    try:
        process.status = "failed"  # type: ignore[assignment]
        process.error = message  # type: ignore[assignment]
        db.commit()
    except Exception:  # noqa: BLE001
        logger.exception("_fail: could not persist failure for process %s", getattr(process, "id", "?"))


def _sync_log_table(
    db: Any,
    log_group_id: str,
    table_name: str,
    table_def: Any,
) -> None:
    """Upsert a ``LogGroupTable`` row for the given parsed table."""
    columns_json = json.dumps([{"name": col.name, "type": col.sql_type} for col in table_def.columns])

    existing = db.query(LogGroupTable).filter_by(log_id=log_group_id, name=table_name).first()
    if existing:
        existing.columns = columns_json
    else:
        entry = LogGroupTable(
            log_id=log_group_id,
            name=table_name,
            columns=columns_json,
            is_normalized=0,
        )
        db.add(entry)

    db.commit()
