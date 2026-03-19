import logging
from collections import Counter

from fastapi import APIRouter, Depends
from lib.auth import get_current_user
from lib.database import get_db
from lib.database_swarm import LogDatabaseError, LogDatabaseSwarm
from lib.models import LogGroup, LogGroupFile, LogGroupProcess, User
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["stats"])
swarm = LogDatabaseSwarm()


class ProcessStatusCount(BaseModel):
    pending: int
    classified: int
    processing: int
    completed: int
    failed: int


class DashboardStatsResponse(BaseModel):
    log_group_count: int
    total_files: int
    total_rows: int
    processes: ProcessStatusCount


@router.get("", response_model=DashboardStatsResponse)
def get_dashboard_stats(
    database: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    log_group_count = (
        database.query(func.count(LogGroup.id))
        .filter(LogGroup.user_id == current_user.id)
        .scalar()
        or 0
    )

    total_files = (
        database.query(func.count(LogGroupFile.id))
        .filter(LogGroupFile.user_id == current_user.id)
        .scalar()
        or 0
    )

    process_counts = (
        database.query(LogGroupProcess.status, func.count(LogGroupProcess.id))
        .filter(LogGroupProcess.user_id == current_user.id)
        .group_by(LogGroupProcess.status)
        .all()
    )
    status_counter = Counter({status: count for status, count in process_counts})

    processes = ProcessStatusCount(
        pending=status_counter.get("queued", 0) + status_counter.get("pending", 0),
        classified=status_counter.get("classified", 0),
        processing=status_counter.get("processing", 0),
        completed=status_counter.get("completed", 0),
        failed=status_counter.get("failed", 0),
    )

    total_rows = 0
    if log_group_count > 0:
        log_groups = (
            database.query(LogGroup.id)
            .filter(LogGroup.user_id == current_user.id)
            .all()
        )
        for (log_group_id,) in log_groups:
            try:
                summaries = swarm.summarize_tables(str(log_group_id))
                for summary in summaries:
                    total_rows += summary.get("row_count", 0)
            except LogDatabaseError:
                continue

    return DashboardStatsResponse(
        log_group_count=log_group_count,
        total_files=total_files,
        total_rows=total_rows,
        processes=processes,
    )
