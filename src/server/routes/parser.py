import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from lib.auth import get_current_user
from lib.models import User
from lib.parsers.semiStructured.pipeline import SemiStructuredPipeline
from pydantic import BaseModel

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # 2 MB
MAX_FILES = 10

router = APIRouter(prefix="/parser", tags=["parser"])


class ParsedLogRow(BaseModel):
    id: str
    timestamp: Optional[str]
    level: str
    source: str
    message: str
    metadata: dict[str, Any]
    raw_hash: str
    template_id: Optional[str]
    equipment_id: Optional[str]
    lot_id: Optional[str]
    wafer_id: Optional[str]
    recipe_id: Optional[str]
    step_id: Optional[str]
    module_id: Optional[str]


class FileParseResult(BaseModel):
    filename: str
    stages_executed: list[str]
    confidence: float
    format_detected: Optional[str]
    total_latency_ms: float
    ai_fallback_used: bool
    log_row: ParsedLogRow


@router.post("/test", response_model=list[FileParseResult])
async def test_parser(
    files: list[UploadFile] = File(...),
    current_user: User = Depends(get_current_user),
):
    """
    Run uploaded files through the SemiStructured parsing pipeline.
    Returns a parse result for each file — no data is stored.
    """
    if len(files) > MAX_FILES:
        raise HTTPException(status_code=400, detail=f"Maximum {MAX_FILES} files per request.")

    pipeline = SemiStructuredPipeline()
    results: list[FileParseResult] = []

    for upload in files:
        content = await upload.read()

        if len(content) > MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"File '{upload.filename}' exceeds the 2 MB limit.",
            )

        try:
            text = content.decode("utf-8", errors="replace")
            result = pipeline.process(text)
            row = result.log_row

            results.append(FileParseResult(
                filename=upload.filename or "unknown",
                stages_executed=result.stages_executed,
                confidence=result.confidence,
                format_detected=result.format_detected,
                total_latency_ms=result.total_latency_ms,
                ai_fallback_used=result.ai_fallback_used,
                log_row=ParsedLogRow(
                    id=row.id,
                    timestamp=row.timestamp,
                    level=row.level,
                    source=row.source,
                    message=row.message,
                    metadata=row.metadata,
                    raw_hash=row.raw_hash,
                    template_id=row.template_id,
                    equipment_id=row.equipment_id,
                    lot_id=row.lot_id,
                    wafer_id=row.wafer_id,
                    recipe_id=row.recipe_id,
                    step_id=row.step_id,
                    module_id=row.module_id,
                ),
            ))

        except Exception as exc:
            logger.exception("Parser error on file '%s'", upload.filename)
            raise HTTPException(status_code=500, detail=f"Failed to parse '{upload.filename}': {exc}")

    return results
