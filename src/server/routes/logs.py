import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from lib.auth import get_current_user
from lib.database import get_db
from lib.log_swarm import LogDatabaseError, LogDatabaseSwarm, ReadOnlyQueryError
from lib.models import LogGroup, LogGroupTable, User
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

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


@router.post("/{id}", status_code=status.HTTP_501_NOT_IMPLEMENTED)
def upload_log_files(id: str, database: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    _get_owned_log_group(database, str(current_user.id), id)
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Log file upload is not implemented yet.",
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
