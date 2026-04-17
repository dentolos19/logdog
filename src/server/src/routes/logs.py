from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import sqlparse
from docx import Document
from docx.shared import Pt
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import Response
from openpyxl import Workbook
from pydantic import BaseModel, Field
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from lib.database import get_database
from lib.database import SessionLocal as AppSessionLocal
from lib.megabase import SessionLocal as MegabaseSessionLocal
from lib.megabase import drop_table as megabase_drop_table
from lib.megabase import init_megabase
from lib.megabase import query_records as megabase_query_records
from lib.models import Asset, LogEntry, LogFile, LogMessage, LogProcess, LogTable, User
from lib.storage import delete_file, download_file, upload_file
from parsers.orchestrator import create_process, enqueue_process, mark_process_failed
from routes.auth import get_current_user

router = APIRouter(prefix="/logs", tags=["logs"])
logger = logging.getLogger(__name__)


class MessageResponse(BaseModel):
    message: str


class CreateLogEntryRequest(BaseModel):
    name: str
    profile_name: str | None = "default"


class UpdateLogEntryRequest(BaseModel):
    name: str
    profile_name: str | None = None


class CreateProcessRequest(BaseModel):
    file_ids: list[str] | None = None


class LogEntryResponse(BaseModel):
    id: str
    user_id: str
    name: str
    profile_name: str | None
    created_at: datetime


class LogFileResponse(BaseModel):
    id: str
    entry_id: str
    asset_id: str
    name: str
    size: int
    content_type: str
    created_at: datetime


class LogProcessResponse(BaseModel):
    id: str
    entry_id: str
    file_id: str | None
    status: str
    classification: dict[str, Any] | None
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    updated_at: datetime


class FileProcessOutcomeResponse(BaseModel):
    file_id: str
    filename: str
    process_id: str | None
    status: str
    error: str | None = None


class UploadFilesResponse(BaseModel):
    process_ids: list[str]
    status: str
    files: list[LogFileResponse]
    outcomes: list[FileProcessOutcomeResponse]


class ProcessEnqueuedResponse(BaseModel):
    process_ids: list[str]
    status: str
    errors: list[str] = Field(default_factory=list)


class PersistedMessagesResponse(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesRequest(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesResponse(BaseModel):
    saved_messages: int


class QueryRequest(BaseModel):
    sql: str


class FilteredExportRequest(BaseModel):
    format: str = "csv"
    search: str | None = None
    levels: list[str] = Field(default_factory=list)
    field_filters: dict[str, str] = Field(default_factory=dict)
    timestamp_from: str | None = None
    timestamp_to: str | None = None


class QueryResponse(BaseModel):
    status: str
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0
    message: str = ""


class ReportSectionTable(BaseModel):
    title: str
    columns: list[str]
    rows: list[list[Any]]


class ReportSectionRequest(BaseModel):
    heading: str
    content: str
    tables: list[ReportSectionTable] = Field(default_factory=list)


class ReportRequest(BaseModel):
    title: str
    sections: list[ReportSectionRequest]


FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|GRANT|REVOKE)\s",
    re.IGNORECASE,
)
QUERY_RESULT_LIMIT = 500


def _uuid_or_raw(value: str):
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return value


def _parse_json(value: str | None):
    if not value:
        return None

    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {"raw": value}

    if isinstance(parsed, dict):
        return parsed

    return {"value": parsed}


def _parse_message_payload(payload: str | None, role: str, content: str):
    if payload is None or payload == "":
        return {"role": role, "content": content}

    try:
        parsed = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        return {"role": role, "content": content}

    if isinstance(parsed, dict):
        return parsed

    return {"role": role, "content": content}


def _entry_response(entry: LogEntry):
    return LogEntryResponse(
        id=str(entry.id),
        user_id=str(entry.user_id),
        name=entry.name,
        profile_name=entry.profile_name,
        created_at=entry.created_at,
    )


def _log_file_response(log_file: LogFile, asset: Asset):
    return LogFileResponse(
        id=str(log_file.id),
        entry_id=str(log_file.entry_id),
        asset_id=str(log_file.asset_id),
        name=asset.name,
        size=asset.size,
        content_type=asset.type,
        created_at=log_file.created_at,
    )


def _log_process_response(process: LogProcess):
    return LogProcessResponse(
        id=str(process.id),
        entry_id=str(process.entry_id),
        file_id=str(process.file_id) if process.file_id is not None else None,
        status=process.status,
        classification=_parse_json(process.classification),
        result=_parse_json(process.result),
        error=process.error,
        created_at=process.created_at,
        updated_at=process.updated_at,
    )


def _batched_status(total: int, queued: int) -> str:
    if queued == 0:
        return "failed"
    if queued == total:
        return "queued"
    return "partial"


def _require_owned_entry(database: Session, entry_id: str, user_id: uuid.UUID):
    entry = database.query(LogEntry).filter(LogEntry.id == _uuid_or_raw(entry_id), LogEntry.user_id == user_id).first()
    if entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log entry not found.")
    return entry


def _require_owned_file(database: Session, entry_id: str, file_id: str):
    log_file = (
        database.query(LogFile)
        .filter(LogFile.id == _uuid_or_raw(file_id), LogFile.entry_id == _uuid_or_raw(entry_id))
        .first()
    )
    if log_file is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log file not found.")

    asset = database.query(Asset).filter(Asset.id == log_file.asset_id).first()
    if asset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found.")

    return log_file, asset


def _delete_orphan_assets(asset_ids: list[uuid.UUID]):
    database = AppSessionLocal()
    try:
        for asset_id in asset_ids:
            try:
                remaining_links = database.query(LogFile).filter(LogFile.asset_id == asset_id).count()
                if remaining_links == 0:
                    delete_file(asset_id=asset_id, db=database)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to delete orphan asset %s", asset_id)
    finally:
        database.close()


def _cleanup_generated_tables_for_file(database: Session, entry_id: str, file_id: str) -> None:
    latest_completed_process = (
        database.query(LogProcess)
        .filter(
            LogProcess.entry_id == _uuid_or_raw(entry_id),
            LogProcess.file_id == _uuid_or_raw(file_id),
            LogProcess.status == "completed",
        )
        .order_by(LogProcess.updated_at.desc())
        .first()
    )

    if latest_completed_process is None or not latest_completed_process.result:
        return

    parsed_result = _parse_json(latest_completed_process.result)
    if not isinstance(parsed_result, dict):
        return

    table_definitions = parsed_result.get("table_definitions")
    if not isinstance(table_definitions, list):
        return

    table_names: set[str] = set()
    for table_definition in table_definitions:
        if isinstance(table_definition, dict):
            table_name = table_definition.get("table_name")
            if isinstance(table_name, str) and table_name:
                table_names.add(table_name)

    if not table_names:
        return

    megabase_database = MegabaseSessionLocal()
    try:
        init_megabase(megabase_database)
        for table_name in table_names:
            try:
                megabase_drop_table(megabase_database, table_name)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to drop generated table %s before reprocessing file %s", table_name, file_id)
    finally:
        megabase_database.close()

    (
        database.query(LogTable)
        .filter(LogTable.entry_id == _uuid_or_raw(entry_id), LogTable.table.in_(table_names))
        .delete(synchronize_session=False)
    )
    database.commit()


@router.get("", response_model=list[LogEntryResponse])
def list_log_entries(
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entries = (
        database.query(LogEntry).filter(LogEntry.user_id == current_user.id).order_by(LogEntry.created_at.desc()).all()
    )
    return [_entry_response(entry) for entry in entries]


@router.post("", response_model=LogEntryResponse, status_code=status.HTTP_201_CREATED)
def create_log_entry(
    payload: CreateLogEntryRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Entry name must not be empty.")

    profile_name = (payload.profile_name or "default").strip() or "default"

    entry = LogEntry(user_id=current_user.id, name=name, profile_name=profile_name)
    database.add(entry)
    database.commit()
    database.refresh(entry)
    return _entry_response(entry)


@router.get("/{entry_id}", response_model=LogEntryResponse)
def get_log_entry(
    entry_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    return _entry_response(entry)


@router.patch("/{entry_id}", response_model=LogEntryResponse)
def update_log_entry(
    entry_id: str,
    payload: UpdateLogEntryRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Entry name must not be empty.")

    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    entry.name = name
    if payload.profile_name is not None:
        entry.profile_name = payload.profile_name.strip() or "default"
    database.commit()
    database.refresh(entry)
    return _entry_response(entry)


@router.delete("/{entry_id}", response_model=MessageResponse)
def delete_log_entry(
    entry_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)

    log_tables = database.query(LogTable).filter(LogTable.entry_id == entry.id).all()
    table_names = [table.table for table in log_tables]

    if table_names:
        megabase_database = MegabaseSessionLocal()
        try:
            init_megabase(megabase_database)
            for table_name in table_names:
                try:
                    megabase_drop_table(megabase_database, table_name)
                except Exception:  # noqa: BLE001
                    logger.exception("Failed to drop generated table %s", table_name)
        finally:
            megabase_database.close()

    file_rows = database.query(LogFile).filter(LogFile.entry_id == entry.id).all()
    orphan_asset_ids = {file_row.asset_id for file_row in file_rows}

    for file_row in file_rows:
        database.delete(file_row)

    for message in database.query(LogMessage).filter(LogMessage.entry_id == entry.id).all():
        database.delete(message)

    for process in database.query(LogProcess).filter(LogProcess.entry_id == entry.id).all():
        database.delete(process)

    for table in log_tables:
        database.delete(table)

    database.delete(entry)
    database.commit()

    if orphan_asset_ids:
        background_tasks.add_task(_delete_orphan_assets, list(orphan_asset_ids))

    return MessageResponse(message="Log entry deleted.")


@router.get("/{entry_id}/files", response_model=list[LogFileResponse])
def list_log_files(
    entry_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    rows = database.query(LogFile).filter(LogFile.entry_id == entry.id).order_by(LogFile.created_at.desc()).all()

    responses: list[LogFileResponse] = []
    for row in rows:
        asset = database.query(Asset).filter(Asset.id == row.asset_id).first()
        if asset is None:
            continue
        responses.append(_log_file_response(row, asset))

    return responses


@router.get("/{entry_id}/files/{file_id}", response_model=LogFileResponse)
def get_log_file_metadata(
    entry_id: str,
    file_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    log_file, asset = _require_owned_file(database=database, entry_id=str(entry.id), file_id=file_id)
    return _log_file_response(log_file, asset)


@router.get("/{entry_id}/files/{file_id}/download")
def download_log_file(
    entry_id: str,
    file_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    log_file, asset = _require_owned_file(database=database, entry_id=str(entry.id), file_id=file_id)

    payload = download_file(asset_id=log_file.asset_id, db=database)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File content not found.")

    headers = {"Content-Disposition": f'attachment; filename="{asset.name}"'}
    return Response(content=payload, media_type=asset.type or "application/octet-stream", headers=headers)


def _get_table_records(entry_id: str, table_name: str, current_user: User, database: Session) -> list[dict]:
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    table_record = database.query(LogTable).filter(LogTable.entry_id == entry.id, LogTable.table == table_name).first()
    if table_record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found.")

    megabase_database = MegabaseSessionLocal()
    try:
        init_megabase(megabase_database)
        records = megabase_query_records(megabase_database, table_name, limit=100000)
    finally:
        megabase_database.close()

    return records


def _serialize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True)
    return str(value)


def _normalize_level(value: Any) -> str:
    if value is None:
        return ""
    normalized = str(value).strip().upper()
    if normalized == "WARNING":
        return "WARN"
    return normalized


def _extract_row_level(record: dict[str, Any]) -> str:
    for key in ("log_level", "level", "severity"):
        if key in record:
            return _normalize_level(record.get(key))
    return ""


def _extract_row_timestamp(record: dict[str, Any]) -> datetime | None:
    for key in ("timestamp", "ts", "time"):
        value = record.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue
    return None


def _coerce_datetime(raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip().replace("Z", "+00:00")
    if normalized == "":
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _record_matches_filters(record: dict[str, Any], payload: FilteredExportRequest) -> bool:
    if payload.search:
        search_text = payload.search.strip().lower()
        if search_text:
            haystack = json.dumps(record, ensure_ascii=True, sort_keys=True).lower()
            if search_text not in haystack:
                return False

    if payload.levels:
        allowed_levels = {_normalize_level(level) for level in payload.levels if level.strip()}
        if allowed_levels:
            record_level = _extract_row_level(record)
            if record_level not in allowed_levels:
                return False

    if payload.field_filters:
        for key, expected in payload.field_filters.items():
            expected_text = expected.strip().lower()
            if expected_text == "":
                continue
            actual = record.get(key)
            actual_text = "" if actual is None else str(actual).lower()
            if expected_text not in actual_text:
                return False

    from_dt = _coerce_datetime(payload.timestamp_from)
    to_dt = _coerce_datetime(payload.timestamp_to)
    if from_dt is not None or to_dt is not None:
        row_timestamp = _extract_row_timestamp(record)
        if row_timestamp is None:
            return False
        if from_dt is not None and row_timestamp < from_dt:
            return False
        if to_dt is not None and row_timestamp > to_dt:
            return False

    return True


def _apply_export_filters(records: list[dict[str, Any]], payload: FilteredExportRequest) -> list[dict[str, Any]]:
    return [record for record in records if _record_matches_filters(record, payload)]


@router.get("/{entry_id}/tables/{table_name}/download/csv")
def download_table_csv(
    entry_id: str,
    table_name: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    records = _get_table_records(entry_id, table_name, current_user, database)

    if not records:
        columns: list[str] = []
    else:
        columns = list(records[0].keys())

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for record in records:
        writer.writerow([_serialize_value(record.get(col)) for col in columns])

    content = output.getvalue()
    filename = f"{table_name}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=content, media_type="text/csv", headers=headers)


@router.get("/{entry_id}/tables/{table_name}/download/xlsx")
def download_table_xlsx(
    entry_id: str,
    table_name: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    records = _get_table_records(entry_id, table_name, current_user, database)

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = table_name[:31]

    if not records:
        columns: list[str] = []
    else:
        columns = list(records[0].keys())

    worksheet.append(columns)
    for record in records:
        row_values = []
        for col in columns:
            value = record.get(col)
            if value is None:
                row_values.append("")
            elif isinstance(value, (dict, list)):
                row_values.append(json.dumps(value, ensure_ascii=True))
            else:
                row_values.append(value)
        worksheet.append(row_values)

    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)

    filename = f"{table_name}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/{entry_id}/tables/{table_name}/download/filtered")
def download_table_filtered(
    entry_id: str,
    table_name: str,
    payload: FilteredExportRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    records = _get_table_records(entry_id, table_name, current_user, database)
    filtered_records = _apply_export_filters(records, payload)

    export_format = payload.format.strip().lower()
    if export_format not in {"csv", "json"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="format must be csv or json")

    if export_format == "json":
        content = json.dumps(filtered_records, ensure_ascii=True)
        headers = {"Content-Disposition": f'attachment; filename="{table_name}.filtered.json"'}
        return Response(content=content, media_type="application/json", headers=headers)

    columns: list[str] = []
    if filtered_records:
        seen_columns: dict[str, None] = {}
        for row in filtered_records:
            for key in row.keys():
                seen_columns.setdefault(key, None)
        columns = list(seen_columns.keys())

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for record in filtered_records:
        writer.writerow([_serialize_value(record.get(column)) for column in columns])

    headers = {"Content-Disposition": f'attachment; filename="{table_name}.filtered.csv"'}
    return Response(content=output.getvalue(), media_type="text/csv", headers=headers)


@router.delete("/{entry_id}/files/{file_id}", response_model=MessageResponse)
def delete_log_file_route(
    entry_id: str,
    file_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    log_file, _ = _require_owned_file(database=database, entry_id=str(entry.id), file_id=file_id)

    asset_id = log_file.asset_id
    database.delete(log_file)
    database.commit()

    remaining_links = database.query(LogFile).filter(LogFile.asset_id == asset_id).count()
    if remaining_links == 0:
        delete_file(asset_id=asset_id, db=database)

    return MessageResponse(message="Log file deleted.")


@router.post("/{entry_id}/files/upload", response_model=UploadFilesResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_log_files(
    entry_id: str,
    files: list[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    if not files:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="At least one file is required.")

    uploaded_files: list[LogFileResponse] = []
    process_ids: list[str] = []
    outcomes: list[FileProcessOutcomeResponse] = []

    for file in files:
        filename = (file.filename or "uploaded.log").strip() or "uploaded.log"
        content_type = file.content_type or "application/octet-stream"
        file_data = await file.read()

        asset = upload_file(file_data=file_data, filename=filename, content_type=content_type, db=database)

        log_file = LogFile(
            user_id=current_user.id,
            asset_id=asset.id,
            entry_id=entry.id,
        )
        database.add(log_file)
        database.commit()
        database.refresh(log_file)

        file_id = str(log_file.id)
        uploaded_files.append(_log_file_response(log_file, asset))

        process_id: str | None = None
        try:
            process_id = create_process(
                entry_id=str(entry.id),
                file_ids=[file_id],
                file_id=file_id,
            )
            enqueue_process(
                process_id=process_id,
                entry_id=str(entry.id),
                file_ids_json=json.dumps([file_id], ensure_ascii=True),
            )
            process_ids.append(process_id)
            outcomes.append(
                FileProcessOutcomeResponse(
                    file_id=file_id,
                    filename=asset.name,
                    process_id=process_id,
                    status="queued",
                )
            )
        except Exception as error:  # noqa: BLE001
            logger.exception("Failed to enqueue process for file %s", file_id)
            if process_id is not None:
                mark_process_failed(
                    process_id=process_id,
                    entry_id=str(entry.id),
                    message=f"Queueing failed: {error}",
                )
            outcomes.append(
                FileProcessOutcomeResponse(
                    file_id=file_id,
                    filename=asset.name,
                    process_id=process_id,
                    status="failed",
                    error=str(error),
                )
            )

    return UploadFilesResponse(
        process_ids=process_ids,
        status=_batched_status(total=len(outcomes), queued=len(process_ids)),
        files=uploaded_files,
        outcomes=outcomes,
    )


@router.get("/{entry_id}/processes", response_model=list[LogProcessResponse])
def list_entry_processes(
    entry_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    processes = (
        database.query(LogProcess).filter(LogProcess.entry_id == entry.id).order_by(LogProcess.created_at.desc()).all()
    )
    return [_log_process_response(process) for process in processes]


@router.get("/{entry_id}/processes/{process_id}", response_model=LogProcessResponse)
def get_entry_process(
    entry_id: str,
    process_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    process = (
        database.query(LogProcess)
        .filter(LogProcess.id == _uuid_or_raw(process_id), LogProcess.entry_id == entry.id)
        .first()
    )
    if process is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process not found.")
    return _log_process_response(process)


@router.post("/{entry_id}/processes", response_model=ProcessEnqueuedResponse, status_code=status.HTTP_202_ACCEPTED)
def create_entry_process(
    entry_id: str,
    payload: CreateProcessRequest | None = None,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)

    selected_file_ids: list[str] = []
    if payload and payload.file_ids:
        for file_id in payload.file_ids:
            normalized_id = str(_uuid_or_raw(file_id))
            exists = (
                database.query(LogFile)
                .filter(LogFile.id == _uuid_or_raw(file_id), LogFile.entry_id == entry.id)
                .first()
            )
            if exists is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Log file '{file_id}' not found.")
            selected_file_ids.append(normalized_id)
    else:
        selected_file_ids = [
            str(file_row.id) for file_row in database.query(LogFile).filter(LogFile.entry_id == entry.id)
        ]

    if not selected_file_ids:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="No files available to process.")

    process_ids: list[str] = []
    errors: list[str] = []
    for file_id in selected_file_ids:
        process_id: str | None = None
        try:
            _cleanup_generated_tables_for_file(database=database, entry_id=str(entry.id), file_id=file_id)
            process_id = create_process(entry_id=str(entry.id), file_ids=[file_id], file_id=file_id)
            enqueue_process(
                process_id=process_id,
                entry_id=str(entry.id),
                file_ids_json=json.dumps([file_id], ensure_ascii=True),
            )
            process_ids.append(process_id)
        except Exception as error:  # noqa: BLE001
            logger.exception("Failed to enqueue process for file %s", file_id)
            if process_id is not None:
                mark_process_failed(
                    process_id=process_id,
                    entry_id=str(entry.id),
                    message=f"Queueing failed: {error}",
                )
            errors.append(f"{file_id}: {error}")

    return ProcessEnqueuedResponse(
        process_ids=process_ids,
        status=_batched_status(total=len(selected_file_ids), queued=len(process_ids)),
        errors=errors,
    )


@router.get("/{entry_id}/chat/messages", response_model=PersistedMessagesResponse)
def get_chat_messages(
    entry_id: str,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    messages = (
        database.query(LogMessage).filter(LogMessage.entry_id == entry.id).order_by(LogMessage.created_at.asc()).all()
    )

    return PersistedMessagesResponse(
        messages=[_parse_message_payload(message.payload, message.role, message.content) for message in messages]
    )


@router.put("/{entry_id}/chat/messages", response_model=ReplaceMessagesResponse)
def replace_chat_messages(
    entry_id: str,
    payload: ReplaceMessagesRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)

    for message in database.query(LogMessage).filter(LogMessage.entry_id == entry.id).all():
        database.delete(message)

    saved_messages = 0
    for message in payload.messages:
        role = str(message.get("role", "assistant"))
        if role == "":
            role = "assistant"

        content = message.get("content", "")
        if isinstance(content, str):
            normalized_content = content
        else:
            normalized_content = json.dumps(content, ensure_ascii=True)

        database.add(
            LogMessage(
                entry_id=entry.id,
                role=role,
                content=normalized_content,
                payload=json.dumps(message, ensure_ascii=True),
            )
        )
        saved_messages += 1

    database.commit()

    return ReplaceMessagesResponse(saved_messages=saved_messages)


def _get_entry_table_names(database: Session, entry_id: str) -> set[str]:
    """Return the set of megabase table names registered for a log entry."""
    tables = database.query(LogTable).filter(LogTable.entry_id == _uuid_or_raw(entry_id)).all()
    return {row.table for row in tables}


@router.post("/{entry_id}/query", response_model=QueryResponse)
def execute_entry_query(
    entry_id: str,
    payload: QueryRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    sql_text = payload.sql.strip()

    if not sql_text:
        return QueryResponse(status="error", message="SQL query must not be empty.")

    if FORBIDDEN_SQL_KEYWORDS.search(sql_text):
        return QueryResponse(status="error", message="Only SELECT queries are allowed.")

    parsed = sqlparse.parse(sql_text)
    for statement in parsed:
        if statement.get_type() != "SELECT":
            return QueryResponse(
                status="error",
                message=f"Statement type '{statement.get_type()}' is not allowed. Only SELECT is permitted.",
            )

    allowed_tables = _get_entry_table_names(database, str(entry.id))
    if not allowed_tables:
        return QueryResponse(status="error", message="No tables are available for this log entry.")

    start_time = time.monotonic()
    megabase_database = MegabaseSessionLocal()
    try:
        init_megabase(megabase_database)
        result = megabase_database.execute(sa_text(sql_text))
        raw_columns = list(result.keys()) if result.returns_rows else []
        raw_rows = result.fetchall() if result.returns_rows else []
        elapsed_ms = (time.monotonic() - start_time) * 1000

        columns = [str(col) for col in raw_columns]
        limited_rows = raw_rows[:QUERY_RESULT_LIMIT]

        serializable_rows: list[list[Any]] = []
        for row in limited_rows:
            serializable_row: list[Any] = []
            for value in row:
                if isinstance(value, (dict, list)):
                    serializable_row.append(json.dumps(value, ensure_ascii=True))
                elif hasattr(value, "isoformat"):
                    serializable_row.append(value.isoformat())
                else:
                    serializable_row.append(value)
            serializable_rows.append(serializable_row)

        return QueryResponse(
            status="ok",
            columns=columns,
            rows=serializable_rows,
            row_count=len(raw_rows),
            execution_time_ms=round(elapsed_ms, 2),
            message=f"Returned {len(serializable_rows)} of {len(raw_rows)} rows.",
        )
    except Exception as error:
        elapsed_ms = (time.monotonic() - start_time) * 1000
        logger.exception("SQL query failed for entry %s", entry_id)
        return QueryResponse(
            status="error",
            execution_time_ms=round(elapsed_ms, 2),
            message=f"Query failed: {error}",
        )
    finally:
        megabase_database.close()


@router.post("/{entry_id}/report")
def generate_entry_report(
    entry_id: str,
    payload: ReportRequest,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)

    document = Document()

    title_style = document.styles["Title"]
    title_style.font.size = Pt(24)
    document.add_paragraph(payload.title, style="Title")

    document.add_paragraph(f"Log group: {entry.name}")
    document.add_paragraph(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    document.add_page_break()

    for section in payload.sections:
        document.add_heading(section.heading, level=1)

        for paragraph_text in section.content.split("\n"):
            stripped = paragraph_text.strip()
            if stripped:
                document.add_paragraph(stripped)

        for table_data in section.tables:
            if table_data.title:
                document.add_heading(table_data.title, level=2)

            if not table_data.columns or not table_data.rows:
                continue

            doc_table = document.add_table(rows=1, cols=len(table_data.columns))
            doc_table.style = "Light Grid Accent 1"

            header_cells = doc_table.rows[0].cells
            for index, column_name in enumerate(table_data.columns):
                header_cells[index].text = str(column_name)

            for row_values in table_data.rows:
                row_cells = doc_table.add_row().cells
                for index, value in enumerate(row_values):
                    if index < len(row_cells):
                        row_cells[index].text = str(value) if value is not None else ""

            document.add_paragraph()

    output = io.BytesIO()
    document.save(output)
    output.seek(0)

    safe_title = re.sub(r"[^\w\-]", "_", payload.title)[:50] or "report"
    filename = f"{safe_title}.docx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers=headers,
    )
