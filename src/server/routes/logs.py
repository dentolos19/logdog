import io
import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import StreamingResponse
from lib.auth import get_current_user
from lib.database import get_db
from lib.database_swarm import LogDatabaseError, LogDatabaseSwarm, ReadOnlyQueryError
from lib.models import Asset, LogGroup, LogGroupFile, LogGroupProcess, LogGroupTable, User
from lib.parsers.contracts import ClassificationResult as _ClassificationResult  # noqa: F401
from lib.parsers.orchestrator import run_ingestion_job
from lib.parsers.preprocessor import (
    FileInput,
    LogPreprocessorService,
)
from lib.storage import get_file as retrieve_asset
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
    description: str
    not_null: bool
    default_value: str | None
    primary_key: bool


class LogGroupTableResponse(BaseModel):
    id: str
    name: str
    columns: list[LogTableColumnResponse]
    row_count: int
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


class ExploreColumnResponse(BaseModel):
    name: str
    type: str
    not_null: bool
    default_value: str | None
    primary_key: bool


class ExploreTableResponse(BaseModel):
    name: str
    columns: list[ExploreColumnResponse]
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


class TableRowsResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int


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


def _parse_columns(
    raw_columns: str,
    descriptions: dict[str, str] | None = None,
) -> list[LogTableColumnResponse]:
    try:
        columns = json.loads(raw_columns)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Log table metadata is invalid.",
        ) from exc

    result: list[LogTableColumnResponse] = []
    for column in columns:
        description = ""
        if descriptions is not None:
            description = descriptions.get(column["name"], "")
        result.append(
            LogTableColumnResponse(
                name=column["name"],
                type=column["type"],
                description=description,
                not_null=column["not_null"],
                default_value=column["default_value"],
                primary_key=column["primary_key"],
            )
        )
    return result


def _build_column_description_map(
    database: Session,
    log_group_id: str,
    table_name: str,
) -> dict[str, str]:
    """Find the most recent completed process and extract column descriptions for the given table."""

    latest_process = (
        database.query(LogGroupProcess)
        .filter(
            LogGroupProcess.log_id == log_group_id,
            LogGroupProcess.status == "completed",
            LogGroupProcess.result.isnot(None),
        )
        .order_by(LogGroupProcess.created_at.desc())
        .first()
    )

    if latest_process is None or latest_process.result is None:
        return {}

    try:
        result_data = json.loads(str(latest_process.result))
    except json.JSONDecodeError:
        return {}

    for generated_table in _extract_generated_tables(result_data):
        if generated_table.get("table_name") != table_name:
            continue

        return {column["name"]: column.get("description", "") for column in generated_table.get("columns", [])}

    return {}


def _extract_generated_tables(result_data: dict[str, Any]) -> list[dict[str, Any]]:
    generated_tables = result_data.get("generated_tables")
    if isinstance(generated_tables, list):
        return [table for table in generated_tables if isinstance(table, dict)]

    table_name = result_data.get("table_name")
    sqlite_ddl = result_data.get("sqlite_ddl")
    columns = result_data.get("columns")

    if isinstance(table_name, str) and isinstance(sqlite_ddl, str) and isinstance(columns, list):
        return [
            {
                "table_name": table_name,
                "sqlite_ddl": sqlite_ddl,
                "columns": columns,
                "is_normalized": table_name == "logs",
                "file_id": None,
                "file_name": None,
            }
        ]

    return []


def _serialize_generated_table(generated_table: dict[str, Any]) -> "GeneratedTableResponse":
    return GeneratedTableResponse(
        table_name=str(generated_table["table_name"]),
        sqlite_ddl=str(generated_table["sqlite_ddl"]),
        columns=[InferredColumnResponse(**column) for column in generated_table.get("columns", [])],
        is_normalized=bool(generated_table.get("is_normalized")),
        file_id=str(generated_table["file_id"]) if generated_table.get("file_id") is not None else None,
        file_name=str(generated_table["file_name"]) if generated_table.get("file_name") is not None else None,
    )


def _serialize_log_group_table(
    table: LogGroupTable,
    row_count: int = 0,
    descriptions: dict[str, str] | None = None,
) -> LogGroupTableResponse:
    return LogGroupTableResponse(
        id=str(table.id),
        name=str(table.name),
        columns=_parse_columns(str(table.columns), descriptions),
        row_count=row_count,
        is_normalized=bool(table.is_normalized),
        created_at=table.created_at,
        updated_at=table.updated_at,
    )


def _serialize_log_group(
    log_group: LogGroup,
    database: Session,
    table_row_counts: dict[str, int] | None = None,
) -> LogGroupResponse:
    sorted_tables = sorted(log_group.tables, key=lambda table: str(table.name).lower())
    counts = table_row_counts or {}

    serialized_tables: list[LogGroupTableResponse] = []
    for table in sorted_tables:
        descriptions = _build_column_description_map(database, str(log_group.id), str(table.name))
        row_count = counts.get(str(table.name), 0)
        serialized_tables.append(_serialize_log_group_table(table, row_count, descriptions))

    return LogGroupResponse(
        id=str(log_group.id),
        name=str(log_group.name),
        created_at=log_group.created_at,
        updated_at=log_group.updated_at,
        tables=serialized_tables,
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


def _get_table_row_counts(log_group_id: str) -> dict[str, int]:
    """Return a mapping of table name to row count from the swarm database."""

    try:
        summaries = swarm.summarize_tables(log_group_id)
        return {summary["name"]: summary["row_count"] for summary in summaries}
    except LogDatabaseError:
        # Row counts are informational; a swarm failure should not block the response.
        return {}


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
    refreshed = _get_owned_log_group(database, str(current_user.id), str(log_group.id))
    return _serialize_log_group(refreshed, database)


@router.get("/{id}", response_model=LogGroupResponse)
def get_log_group(id: str, database: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    log_group = _get_owned_log_group(database, str(current_user.id), id)
    _sync_metadata(database, str(log_group.id))
    row_counts = _get_table_row_counts(str(log_group.id))
    refreshed = _get_owned_log_group(database, str(current_user.id), id)
    return _serialize_log_group(refreshed, database, row_counts)


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
    row_counts = _get_table_row_counts(str(log_group.id))
    refreshed = _get_owned_log_group(database, str(current_user.id), id)
    return _serialize_log_group(refreshed, database, row_counts)


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


class GeneratedTableResponse(BaseModel):
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumnResponse]
    is_normalized: bool
    file_id: str | None
    file_name: str | None


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


class ProcessResultDetails(BaseModel):
    """Full details for a completed preprocess run, embedded in ProcessResponse."""

    schema_summary: str
    schema_version: str
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumnResponse]
    generated_tables: list[GeneratedTableResponse]
    segmentation: SegmentationResultResponse
    sample_records: list[SampleRecordResponse]
    file_observations: list[FileObservationResponse]
    warnings: list[str]
    assumptions: list[str]
    confidence: float


class ProcessResponse(BaseModel):
    """Status-oriented process record that covers processing, completed, and failed states."""

    id: str
    log_id: str
    status: str
    error: str | None
    result: ProcessResultDetails | None
    created_at: datetime
    updated_at: datetime


# Keep the old response model for the upload endpoint so its contract is unchanged.
class PreprocessResultResponse(BaseModel):
    id: str
    log_id: str
    schema_summary: str
    schema_version: str
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumnResponse]
    generated_tables: list[GeneratedTableResponse]
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


# ---------------------------------------------------------------------------
# New async-upload response models (v2)
# ---------------------------------------------------------------------------


class FileClassificationResponse(BaseModel):
    file_id: str | None
    filename: str
    detected_format: str
    structural_class: str
    format_confidence: float
    line_count: int
    warnings: list[str]


class ClassificationResponse(BaseModel):
    schema_version: str
    dominant_format: str
    structural_class: str
    selected_parser_key: str
    file_classifications: list[FileClassificationResponse]
    warnings: list[str]
    confidence: float


class UploadLogFilesV2Response(BaseModel):
    uploaded_files: int
    process_id: str
    classification: ClassificationResponse


# ---------------------------------------------------------------------------
# File Response Models
# ---------------------------------------------------------------------------


class LogGroupFileResponse(BaseModel):
    id: str
    asset_id: str
    name: str
    size: int
    mime_type: str
    created_at: datetime


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _serialize_process_record(process: LogGroupProcess) -> PreprocessResultResponse:
    """Serialize a completed LogGroupProcess into the upload-response shape."""

    result = json.loads(str(process.result))
    generated_tables = _extract_generated_tables(result)

    return PreprocessResultResponse(
        id=str(process.id),
        log_id=str(process.log_id),
        schema_summary=result["schema_summary"],
        schema_version=result["schema_version"],
        table_name=result["table_name"],
        sqlite_ddl=result["sqlite_ddl"],
        columns=[InferredColumnResponse(**column) for column in result["columns"]],
        generated_tables=[_serialize_generated_table(table) for table in generated_tables],
        segmentation=SegmentationResultResponse(**result["segmentation"]),
        sample_records=[SampleRecordResponse(**record) for record in result["sample_records"]],
        file_observations=[FileObservationResponse(**observation) for observation in result["file_observations"]],
        warnings=result["warnings"],
        assumptions=result["assumptions"],
        confidence=result["confidence"],
        created_at=process.created_at,
    )


def _serialize_process_status(process: LogGroupProcess) -> ProcessResponse:
    """Serialize a LogGroupProcess into the status-oriented response used by the processes tab."""

    details: ProcessResultDetails | None = None

    if process.status == "completed" and process.result is not None:
        try:
            result_data = json.loads(str(process.result))
            # New async pipeline results have 'table_definitions'; old results have 'schema_summary'.
            # Skip building ProcessResultDetails for new-format results.
            if "schema_summary" not in result_data:
                result_data = {}
                raise KeyError("new-format result")
            generated_tables = _extract_generated_tables(result_data)
            details = ProcessResultDetails(
                schema_summary=result_data["schema_summary"],
                schema_version=result_data["schema_version"],
                table_name=result_data["table_name"],
                sqlite_ddl=result_data["sqlite_ddl"],
                columns=[InferredColumnResponse(**column) for column in result_data["columns"]],
                generated_tables=[_serialize_generated_table(table) for table in generated_tables],
                segmentation=SegmentationResultResponse(**result_data["segmentation"]),
                sample_records=[SampleRecordResponse(**record) for record in result_data["sample_records"]],
                file_observations=[
                    FileObservationResponse(**observation) for observation in result_data["file_observations"]
                ],
                warnings=result_data["warnings"],
                assumptions=result_data["assumptions"],
                confidence=result_data["confidence"],
            )
        except (json.JSONDecodeError, KeyError):
            # If the stored JSON is malformed, surface the process without crashing.
            pass

    return ProcessResponse(
        id=str(process.id),
        log_id=str(process.log_id),
        status=str(process.status),
        error=str(process.error) if process.error is not None else None,
        result=details,
        created_at=process.created_at,
        updated_at=process.updated_at,
    )


def _serialize_log_group_file(log_file: LogGroupFile) -> LogGroupFileResponse:
    asset: Asset = log_file.asset
    return LogGroupFileResponse(
        id=str(log_file.id),
        asset_id=str(asset.id),
        name=str(asset.name),
        size=int(asset.size),
        mime_type=str(asset.type),
        created_at=log_file.created_at,
    )


@router.post("/{id}", response_model=UploadLogFilesV2Response, status_code=status.HTTP_201_CREATED)
def upload_log_files(
    id: str,
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
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
    staged_file_inputs: list[tuple[LogGroupFile, str, str]] = []

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
        log_group_file = LogGroupFile(
            user_id=str(current_user.id),
            log_id=str(log_group.id),
            asset_id=str(asset.id),
        )
        database.add(log_group_file)

        # Decode content for the preprocessor.
        # For binary files, run through our decoder pipeline first.
        from lib.parsers.unstructured_parser import preprocess_binary_input

        decoded_lines = preprocess_binary_input(raw_data)
        content = "\n".join(decoded_lines)

        staged_file_inputs.append((log_group_file, filename, content))

    try:
        database.flush()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to store uploaded files.",
        ) from exc

    file_inputs = [
        FileInput(file_id=str(log_group_file.id), filename=filename, content=content)
        for log_group_file, filename, content in staged_file_inputs
    ]

    # Phase 2: Classify uploaded files (fast, no LLM — determines parser key).
    service = LogPreprocessorService(table_name="logs")
    classification = service.classify(file_inputs)

    # Phase 3: Persist the process record and enqueue the background ingestion job.
    process_record = LogGroupProcess(
        log_id=str(log_group.id),
        user_id=str(current_user.id),
        status="classified",
        classification=classification.model_dump_json(),
        schema_version=classification.schema_version,
    )
    database.add(process_record)

    try:
        database.commit()
    except SQLAlchemyError as exc:
        database.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist process record.",
        ) from exc

    file_inputs_json = json.dumps([fi.model_dump() for fi in file_inputs])
    background_tasks.add_task(
        run_ingestion_job,
        str(process_record.id),
        str(log_group.id),
        file_inputs_json,
    )

    return UploadLogFilesV2Response(
        uploaded_files=len(file_inputs),
        process_id=str(process_record.id),
        classification=ClassificationResponse(
            schema_version=classification.schema_version,
            dominant_format=classification.dominant_format,
            structural_class=classification.structural_class.value,
            selected_parser_key=classification.selected_parser_key,
            file_classifications=[
                FileClassificationResponse(
                    file_id=fc.file_id,
                    filename=fc.filename,
                    detected_format=fc.detected_format,
                    structural_class=fc.structural_class.value,
                    format_confidence=fc.format_confidence,
                    line_count=fc.line_count,
                    warnings=fc.warnings,
                )
                for fc in classification.file_classifications
            ],
            warnings=classification.warnings,
            confidence=classification.confidence,
        ),
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


@router.get("/{id}/processes", response_model=list[ProcessResponse])
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
        )
        .order_by(LogGroupProcess.created_at.desc())
        .all()
    )

    return [_serialize_process_status(process) for process in processes]


@router.get("/{id}/files", response_model=list[LogGroupFileResponse])
def get_log_files(
    id: str,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    log_files = (
        database.query(LogGroupFile)
        .filter(
            LogGroupFile.log_id == str(log_group.id),
            LogGroupFile.user_id == str(current_user.id),
        )
        .options(selectinload(LogGroupFile.asset))
        .order_by(LogGroupFile.created_at.desc())
        .all()
    )

    return [_serialize_log_group_file(log_file) for log_file in log_files]


@router.get("/{id}/files/{file_id}/download")
def download_log_file(
    id: str,
    file_id: str,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    log_file = (
        database.query(LogGroupFile)
        .filter(
            LogGroupFile.id == file_id,
            LogGroupFile.log_id == str(log_group.id),
            LogGroupFile.user_id == str(current_user.id),
        )
        .options(selectinload(LogGroupFile.asset))
        .first()
    )

    if log_file is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    asset: Asset = log_file.asset
    raw_data = retrieve_asset(file_id=str(asset.id), db=database)

    return StreamingResponse(
        content=io.BytesIO(raw_data),
        media_type=str(asset.type),
        headers={
            "Content-Disposition": f'attachment; filename="{asset.name}"',
            "Content-Length": str(asset.size),
        },
    )


@router.get("/{id}/tables/{table_name}/rows", response_model=TableRowsResponse)
def get_table_rows(
    id: str,
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if page < 1:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="page must be >= 1.")
    if not (1 <= page_size <= 200):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="page_size must be between 1 and 200.")

    log_group = _get_owned_log_group(database, str(current_user.id), id)

    # Verify the table belongs to this log group (access control via metadata).
    owned_table = (
        database.query(LogGroupTable)
        .filter(LogGroupTable.log_id == str(log_group.id), LogGroupTable.name == table_name)
        .first()
    )
    if owned_table is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found.")

    try:
        result = swarm.fetch_table_rows(str(log_group.id), table_name, page=page, page_size=page_size)
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch rows: {exc}",
        ) from exc

    return TableRowsResponse.model_validate(result)
