import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from lib.auth import get_current_user
from lib.database import get_db
from lib.database_swarm import LogDatabaseError, LogDatabaseSwarm, ReadOnlyQueryError
from lib.models import LogGroup, LogGroupFile, LogGroupProcess, LogGroupTable, User
from lib.preprocessor import (
    FileInput,
    LogPreprocessorService,
    PreprocessorResult,
)
from lib.storage import upload_file as store_asset
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

logger = logging.getLogger(__name__)

MAX_FILES_PER_UPLOAD = 20
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB

router = APIRouter(prefix="/logs", tags=["logs"])
swarm = LogDatabaseSwarm()

JsonScalar = str | int | float | bool | None
QueryParameters = dict[str, JsonScalar] | list[JsonScalar] | None


class LogTableColumnResponse(BaseModel):
    name: str
    type: str
    not_null: bool
    default_value: str | None
    primary_key: bool


class LogGroupTableResponse(BaseModel):
    id: str
    name: str
    columns: list[LogTableColumnResponse]
    is_normalized: bool
    created_at: datetime
    updated_at: datetime


class LogGroupListResponse(BaseModel):
    id: str
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class LogGroupResponse(LogGroupListResponse):
    tables: list[LogGroupTableResponse]


class CreateLogGroupRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)


class UpdateLogGroupRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)


class ExploreTableResponse(BaseModel):
    name: str
    columns: list[LogTableColumnResponse]
    row_count: int
    preview_rows: list[dict[str, Any]]


class ExploreLogsResponse(BaseModel):
    tables: list[ExploreTableResponse]


class QueryLogsRequest(BaseModel):
    sql: str = Field(min_length=1)
    parameters: QueryParameters = None
    limit: int = Field(default=200, ge=1, le=500)


class QueryLogsResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool


def _get_owned_log_group(database: Session, user_id: str, log_group_id: str) -> LogGroup:
    log_group = (
        database.query(LogGroup)
        .options(selectinload(LogGroup.tables))
        .filter(LogGroup.id == log_group_id, LogGroup.user_id == user_id)
        .first()
    )

    if log_group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log group not found.")

    return log_group


def _parse_columns(raw_columns: str) -> list[LogTableColumnResponse]:
    try:
        columns = json.loads(raw_columns)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Log table metadata is invalid.",
        ) from exc

    return [LogTableColumnResponse.model_validate(column) for column in columns]


def _serialize_log_group_table(table: LogGroupTable) -> LogGroupTableResponse:
    return LogGroupTableResponse(
        id=str(table.id),
        name=str(table.name),
        columns=_parse_columns(str(table.columns)),
        is_normalized=bool(table.is_normalized),
        created_at=table.created_at,
        updated_at=table.updated_at,
    )


def _serialize_log_group(log_group: LogGroup) -> LogGroupResponse:
    sorted_tables = sorted(log_group.tables, key=lambda table: str(table.name).lower())

    return LogGroupResponse(
        id=str(log_group.id),
        name=str(log_group.name),
        created_at=log_group.created_at,
        updated_at=log_group.updated_at,
        tables=[_serialize_log_group_table(table) for table in sorted_tables],
    )


def _sync_metadata(database: Session, log_group_id: str) -> None:
    try:
        swarm.sync_table_metadata(database, log_group_id)
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to synchronize log metadata.",
        ) from exc
    except LogDatabaseError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to access the log database: {exc}",
        ) from exc


@router.get("/", response_model=list[LogGroupListResponse])
def get_log_groups(database: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    return (
        database.query(LogGroup).filter(LogGroup.user_id == current_user.id).order_by(LogGroup.updated_at.desc()).all()
    )


@router.post("/", response_model=LogGroupResponse, status_code=status.HTTP_201_CREATED)
def create_log_group(
    body: CreateLogGroupRequest,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = LogGroup(user_id=str(current_user.id), name=body.name.strip())
    database.add(log_group)

    try:
        database.flush()
        swarm.ensure_database(str(log_group.id))
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create the log group.",
        ) from exc
    except LogDatabaseError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to provision the log database: {exc}",
        ) from exc

    _sync_metadata(database, str(log_group.id))
    return _serialize_log_group(_get_owned_log_group(database, str(current_user.id), str(log_group.id)))


@router.get("/{id}", response_model=LogGroupResponse)
def get_log_group(id: str, database: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    log_group = _get_owned_log_group(database, str(current_user.id), id)
    _sync_metadata(database, str(log_group.id))
    return _serialize_log_group(_get_owned_log_group(database, str(current_user.id), id))


@router.put("/{id}", response_model=LogGroupResponse)
def update_log_group(
    id: str,
    body: UpdateLogGroupRequest,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)
    log_group.name = body.name.strip()

    try:
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update the log group.",
        ) from exc

    _sync_metadata(database, str(log_group.id))
    return _serialize_log_group(_get_owned_log_group(database, str(current_user.id), id))


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_log_group(
    id: str, database: Session = Depends(get_db), current_user: User = Depends(get_current_user)
) -> None:
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    try:
        database.delete(log_group)
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete the log group.",
        ) from exc

    try:
        swarm.delete_database(id)
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"The log group was deleted, but database cleanup failed: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# Upload + Preprocess Response Models
# ---------------------------------------------------------------------------


class InferredColumnResponse(BaseModel):
    name: str
    sql_type: str
    description: str
    nullable: bool
    kind: str
    example_values: list[str]


class SegmentationResultResponse(BaseModel):
    strategy: str
    confidence: float
    rationale: str


class FileObservationResponse(BaseModel):
    filename: str
    line_count: int
    detected_format: str
    format_confidence: float
    segmentation_hint: str
    sample_size: int
    warnings: list[str]


class SampleRecordResponse(BaseModel):
    source_file: str
    line_start: int
    line_end: int
    fields: dict[str, Any]


class PreprocessResultResponse(BaseModel):
    id: str
    log_id: str
    schema_summary: str
    schema_version: str
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumnResponse]
    segmentation: SegmentationResultResponse
    sample_records: list[SampleRecordResponse]
    file_observations: list[FileObservationResponse]
    warnings: list[str]
    assumptions: list[str]
    confidence: float
    created_at: datetime


class UploadLogFilesResponse(BaseModel):
    uploaded_files: int
    process_result: PreprocessResultResponse


def _serialize_preprocess_result(process: LogGroupProcess) -> PreprocessResultResponse:
    """Deserialize the stored JSON result into the response model."""

    result = json.loads(str(process.result))

    return PreprocessResultResponse(
        id=str(process.id),
        log_id=str(process.log_id),
        schema_summary=result["schema_summary"],
        schema_version=result["schema_version"],
        table_name=result["table_name"],
        sqlite_ddl=result["sqlite_ddl"],
        columns=[InferredColumnResponse(**column) for column in result["columns"]],
        segmentation=SegmentationResultResponse(**result["segmentation"]),
        sample_records=[SampleRecordResponse(**record) for record in result["sample_records"]],
        file_observations=[FileObservationResponse(**observation) for observation in result["file_observations"]],
        warnings=result["warnings"],
        assumptions=result["assumptions"],
        confidence=result["confidence"],
        created_at=process.created_at,
    )


@router.post("/{id}", response_model=UploadLogFilesResponse, status_code=status.HTTP_201_CREATED)
def upload_log_files(
    id: str,
    files: list[UploadFile] = File(...),
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    if len(files) > MAX_FILES_PER_UPLOAD:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Too many files. Maximum is {MAX_FILES_PER_UPLOAD} per upload.",
        )

    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one file is required.",
        )

    # Phase 1: Read, validate, and store each file.
    file_inputs: list[FileInput] = []

    for upload in files:
        raw_data = upload.file.read()

        if len(raw_data) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File '{upload.filename}' exceeds the {MAX_FILE_SIZE_BYTES // (1024 * 1024)} MB limit.",
            )

        filename = upload.filename or "unnamed.log"
        mime_type = upload.content_type or "application/octet-stream"

        # Persist the asset and link it to this log group.
        asset = store_asset(raw_data=raw_data, name=filename, size=len(raw_data), mime_type=mime_type, db=database)
        database.add(
            LogGroupFile(
                user_id=str(current_user.id),
                log_id=str(log_group.id),
                asset_id=str(asset.id),
            )
        )

        # Decode content for the preprocessor (best-effort UTF-8).
        try:
            content = raw_data.decode("utf-8")
        except UnicodeDecodeError:
            content = raw_data.decode("utf-8", errors="replace")

        file_inputs.append(FileInput(filename=filename, content=content))

    try:
        database.flush()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store uploaded files.",
        ) from exc

    # Phase 2: Run the preprocessor.
    process_record = LogGroupProcess(
        log_id=str(log_group.id),
        user_id=str(current_user.id),
        status="processing",
    )
    database.add(process_record)

    try:
        database.flush()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create process record.",
        ) from exc

    try:
        service = LogPreprocessorService(table_name="log_entries")
        result: PreprocessorResult = service.preprocess(file_inputs)

        process_record.status = "completed"
        process_record.result = result.model_dump_json()
        process_record.schema_version = result.schema_version
    except Exception as exc:
        logger.exception("Preprocessor failed for log group %s", id)
        process_record.status = "failed"
        process_record.error = str(exc)
        database.commit()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Preprocessing failed: {exc}",
        ) from exc

    # Phase 3: Apply the schema to the swarm database.
    try:
        swarm.apply_schema(str(log_group.id), result.sqlite_ddl)
    except LogDatabaseError as exc:
        logger.warning("Failed to apply schema DDL to swarm: %s", exc)
        result.warnings.append(f"Schema DDL could not be applied: {exc}")
        process_record.result = result.model_dump_json()

    try:
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist preprocessing result.",
        ) from exc

    _sync_metadata(database, str(log_group.id))

    return UploadLogFilesResponse(
        uploaded_files=len(file_inputs),
        process_result=_serialize_preprocess_result(process_record),
    )


@router.get("/{id}/explore", response_model=ExploreLogsResponse)
def explore_logs(id: str, database: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    log_group = _get_owned_log_group(database, str(current_user.id), id)
    _sync_metadata(database, str(log_group.id))

    try:
        tables = swarm.explore_database(str(log_group.id))
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to explore the log database: {exc}",
        ) from exc

    return ExploreLogsResponse(
        tables=[ExploreTableResponse.model_validate(table) for table in tables],
    )


@router.post("/{id}/query", response_model=QueryLogsResponse)
def query_logs(
    id: str,
    body: QueryLogsRequest,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    try:
        query_result = swarm.execute_read_only_query(
            log_group_id=str(log_group.id),
            sql=body.sql,
            parameters=body.parameters,
            row_limit=body.limit,
        )
    except ReadOnlyQueryError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute the query: {exc}",
        ) from exc

    return QueryLogsResponse.model_validate(query_result)


@router.get("/{id}/processes", response_model=list[PreprocessResultResponse])
def get_log_processes(
    id: str,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    processes = (
        database.query(LogGroupProcess)
        .filter(
            LogGroupProcess.log_id == str(log_group.id),
            LogGroupProcess.user_id == str(current_user.id),
            LogGroupProcess.status == "completed",
        )
        .order_by(LogGroupProcess.created_at.desc())
        .all()
    )

    return [_serialize_preprocess_result(process) for process in processes]
