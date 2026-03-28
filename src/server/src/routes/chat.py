from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from lib.auth import get_current_user
from lib.database import get_db
from lib.database_swarm import LogDatabaseError, LogDatabaseSwarm
from lib.models import LogGroup, User
from pydantic import BaseModel
from sqlalchemy.orm import Session

router = APIRouter(prefix="/logs", tags=["chat"])
swarm = LogDatabaseSwarm()


class PersistedMessagesResponse(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesRequest(BaseModel):
    messages: list[dict[str, Any]]


class ReplaceMessagesResponse(BaseModel):
    saved_messages: int


def _get_owned_log_group(database: Session, user_id: str, log_group_id: str) -> LogGroup:
    log_group = database.query(LogGroup).filter(LogGroup.id == log_group_id, LogGroup.user_id == user_id).first()
    if log_group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log group not found.")
    return log_group


@router.get("/{id}/chat/messages", response_model=PersistedMessagesResponse)
def get_chat_messages(
    id: str,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    try:
        messages = swarm.list_messages(str(log_group.id))
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load chat history: {exc}",
        ) from exc

    return PersistedMessagesResponse(messages=messages)


@router.put("/{id}/chat/messages", response_model=ReplaceMessagesResponse)
def replace_chat_messages(
    id: str,
    body: ReplaceMessagesRequest,
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group = _get_owned_log_group(database, str(current_user.id), id)

    try:
        saved_messages = swarm.replace_messages(str(log_group.id), body.messages)
    except LogDatabaseError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save chat history: {exc}",
        ) from exc

    return ReplaceMessagesResponse(saved_messages=saved_messages)
