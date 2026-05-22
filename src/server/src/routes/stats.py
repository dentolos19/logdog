from __future__ import annotations

import json
import os

from collections import Counter
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from lib.database import get_database
from lib.models import Asset, LogGroup, LogFile, LogProcess, User
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
    avg_parser_confidence: float | None = None
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
    confidence_sum = 0.0
    confidence_count = 0

    process_rows = (
        database.query(LogProcess.status, LogProcess.result)
        .join(LogGroup, LogProcess.group_id == LogGroup.id)
        .filter(LogGroup.user_id == current_user.id)
        .all()
    )

    for status, result in process_rows:
        if status == "queued":
            queued += 1
        elif status == "processing":
            processing += 1
        elif status == "completed":
            completed += 1
            total_rows += _count_rows_from_process_result(result)
            if result:
                try:
                    parsed = json.loads(result)
                    if isinstance(parsed, dict):
                        confidence = parsed.get("confidence")
                        if isinstance(confidence, (int, float)):
                            confidence_sum += max(0.0, min(float(confidence), 1.0))
                            confidence_count += 1
                except (json.JSONDecodeError, TypeError):
                    pass
        elif status == "failed":
            failed += 1

    avg_parser_confidence = round(confidence_sum / confidence_count, 2) if confidence_count > 0 else None

    file_format_counts: Counter[str] = Counter()
    file_assets = (
        database.query(Asset.name)
        .join(LogFile, LogFile.asset_id == Asset.id)
        .join(LogGroup, LogFile.group_id == LogGroup.id)
        .filter(LogGroup.user_id == current_user.id)
        .all()
    )
    for (name,) in file_assets:
        ext = os.path.splitext(name)[1].lower() or "unknown"
        file_format_counts[ext] += 1

    format_distribution = [
        FormatCount(format=fmt, count=cnt)
        for fmt, cnt in file_format_counts.most_common()
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
        avg_parser_confidence=avg_parser_confidence,
        format_distribution=format_distribution,
    )
