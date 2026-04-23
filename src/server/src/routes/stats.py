from __future__ import annotations

import json

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from lib.database import get_database
from lib.models import LogGroup, LogFile, LogProcess, User
from routes.auth import get_current_user

router = APIRouter(prefix="/stats", tags=["stats"])


class ProcessStatusCount(BaseModel):
    queued: int
    processing: int
    completed: int
    failed: int


class FormatCount(BaseModel):
    format: str
    count: int


class DashboardStatsResponse(BaseModel):
    log_group_count: int
    total_files: int
    total_rows: int
    processes: ProcessStatusCount
    format_distribution: list[FormatCount]


def _count_rows_from_process_result(result: str | None):
    if not result:
        return 0

    try:
        parsed = json.loads(result)
    except (json.JSONDecodeError, TypeError):
        return 0

    if not isinstance(parsed, dict):
        return 0

    records = parsed.get("records")
    if not isinstance(records, dict):
        return 0

    total_rows = 0
    for rows in records.values():
        if isinstance(rows, list):
            total_rows += len(rows)

    return total_rows


def _extract_parser_key(result: str | None) -> str | None:
    if not result:
        return None

    try:
        parsed = json.loads(result)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(parsed, dict):
        return None

    parser_key = parsed.get("parser_key")
    if isinstance(parser_key, str) and parser_key:
        return parser_key

    classification = parsed.get("classification")
    if isinstance(classification, dict):
        selected = classification.get("selected_parser_key")
        if isinstance(selected, str) and selected:
            return selected

    return None


@router.get("", response_model=DashboardStatsResponse)
def get_dashboard_stats(
    current_user: User = Depends(get_current_user),
    database: Session = Depends(get_database),
):
    log_group_count = database.query(func.count(LogGroup.id)).filter(LogGroup.user_id == current_user.id).scalar() or 0
    total_files = database.query(func.count(LogFile.id)).filter(LogFile.user_id == current_user.id).scalar() or 0

    queued = 0
    processing = 0
    completed = 0
    failed = 0
    total_rows = 0

    process_rows = (
        database.query(LogProcess.status, LogProcess.result)
        .join(LogGroup, LogProcess.group_id == LogGroup.id)
        .filter(LogGroup.user_id == current_user.id)
        .all()
    )

    format_counts: dict[str, int] = {}
    for status, result in process_rows:
        if status == "queued":
            queued += 1
        elif status == "processing":
            processing += 1
        elif status == "completed":
            completed += 1
            total_rows += _count_rows_from_process_result(result)
            parser_key = _extract_parser_key(result)
            if parser_key:
                format_counts[parser_key] = format_counts.get(parser_key, 0) + 1
        elif status == "failed":
            failed += 1

    format_distribution = [
        FormatCount(format=fmt, count=cnt)
        for fmt, cnt in sorted(format_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    return DashboardStatsResponse(
        log_group_count=log_group_count,
        total_files=total_files,
        total_rows=total_rows,
        processes=ProcessStatusCount(
            queued=queued,
            processing=processing,
            completed=completed,
            failed=failed,
        ),
        format_distribution=format_distribution,
    )
