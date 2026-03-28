from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.lib.database import get_database
from src.lib.megabase import SessionLocal as MegabaseSessionLocal
from src.lib.megabase import drop_table as megabase_drop_table
from src.lib.megabase import init_megabase
from src.lib.models import Asset, LogEntry, LogFile, LogMessage, LogProcess, LogTable, User
from src.lib.storage import delete_file, download_file, upload_file
from src.parsers.orchestrator import create_parse_process, run_parse_job
from src.parsers.preprocessor import FileInput
from src.routes.auth import get_current_user

router = APIRouter(prefix="/logs", tags=["logs"])


class MessageResponse(BaseModel):
    message: str


class CreateLogEntryRequest(BaseModel):
    name: str


class UpdateLogEntryRequest(BaseModel):
    name: str


class CreateProcessRequest(BaseModel):
    file_ids: list[str] | None = None


class LogEntryResponse(BaseModel):
    id: str
    user_id: str
    name: str
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
    status: str
    classification: dict[str, Any] | None
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    updated_at: datetime


class UploadFilesResponse(BaseModel):
    process_id: str
    status: str
    files: list[LogFileResponse]


class ProcessEnqueuedResponse(BaseModel):
    process_id: str
    status: str


class PersistedMessagesResponse(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesRequest(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesResponse(BaseModel):
    saved_messages: int


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
        status=process.status,
        classification=_parse_json(process.classification),
        result=_parse_json(process.result),
        error=process.error,
        created_at=process.created_at,
        updated_at=process.updated_at,
    )


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

    entry = LogEntry(user_id=current_user.id, name=name)
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
    database.commit()
    database.refresh(entry)
    return _entry_response(entry)


@router.delete("/{entry_id}", response_model=MessageResponse)
def delete_log_entry(
    entry_id: str,
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
                megabase_drop_table(megabase_database, table_name)
        except Exception as error:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to drop generated tables: {error}",
            ) from error
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

    for asset_id in orphan_asset_ids:
        remaining_links = database.query(LogFile).filter(LogFile.asset_id == asset_id).count()
        if remaining_links == 0:
            delete_file(asset_id=asset_id, db=database)

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
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)
    if not files:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="At least one file is required.")

    uploaded_files: list[LogFileResponse] = []
    file_inputs: list[FileInput] = []
    file_ids: list[str] = []

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
        file_ids.append(file_id)
        file_inputs.append(
            FileInput(
                file_id=file_id,
                filename=asset.name,
                content=file_data.decode("utf-8", errors="ignore"),
            )
        )
        uploaded_files.append(_log_file_response(log_file, asset))

    process_id = create_parse_process(entry_id=str(entry.id), file_inputs=file_inputs, file_ids=file_ids)
    background_tasks.add_task(
        run_parse_job,
        process_id,
        str(entry.id),
        json.dumps([file_input.model_dump() for file_input in file_inputs], ensure_ascii=True),
        json.dumps(file_ids, ensure_ascii=True),
    )

    return UploadFilesResponse(
        process_id=process_id,
        status="queued",
        files=uploaded_files,
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
    background_tasks: BackgroundTasks,
    payload: CreateProcessRequest | None = None,
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    entry = _require_owned_entry(database=database, entry_id=entry_id, user_id=current_user.id)

    selected_file_ids: list[str] | None = None
    if payload and payload.file_ids:
        selected_file_ids = []
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

    process_id = create_parse_process(entry_id=str(entry.id), file_ids=selected_file_ids)
    background_tasks.add_task(
        run_parse_job,
        process_id,
        str(entry.id),
        None,
        json.dumps(selected_file_ids, ensure_ascii=True) if selected_file_ids else None,
    )

    return ProcessEnqueuedResponse(process_id=process_id, status="queued")


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
