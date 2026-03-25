import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from lib.auth import get_current_user
from lib.models import User
from lib.parsers.contracts import ParserPipelineResult
from lib.parsers.preprocessor import FileInput, LogPreprocessorService
from lib.parsers.registry import ParserRegistry
from lib.parsers.semiStructured.pipeline import SemiStructuredPipeline
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # 2 MB
MAX_FILES = 10

router = APIRouter(prefix="/parser", tags=["parser"])


class ParsedLogRow(BaseModel):
    """Mirrors the LogRow baseline columns from normalizer.py / preprocessor._build_baseline_columns."""

    # Baseline columns
    id: str
    timestamp: Optional[str]
    timestamp_raw: Optional[str]
    source: str
    source_type: str
    log_level: str
    event_type: str
    message: str
    raw_text: str
    record_group_id: Optional[str]
    line_start: Optional[int]
    line_end: Optional[int]
    parse_confidence: float
    schema_version: str
    additional_data: dict[str, Any]
    # Pipeline-only
    raw_hash: str
    template_id: Optional[str]
    # Semiconductor-extended
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


class SupportCheckFileInput(BaseModel):
    file_id: str | None = None
    filename: str
    content: str
    mime_type: str | None = None


class SupportCheckRequest(BaseModel):
    files: list[SupportCheckFileInput] = Field(default_factory=list)


class FileSupportCandidate(BaseModel):
    parser_key: str
    supported: bool
    score: float
    reasons: list[str]
    detected_format: str | None = None


class FileSupportSelection(BaseModel):
    file_id: str | None = None
    filename: str
    parser_key: str
    score: float
    reasons: list[str]
    candidates: list[FileSupportCandidate]


class SupportCheckResponse(BaseModel):
    selections: list[FileSupportSelection]
    warnings: list[str] = Field(default_factory=list)


class IngestRequest(BaseModel):
    files: list[SupportCheckFileInput] = Field(default_factory=list)
    preferred_parser_key: str | None = None


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

            results.append(
                FileParseResult(
                    filename=upload.filename or "unknown",
                    stages_executed=result.stages_executed,
                    confidence=result.confidence,
                    format_detected=result.format_detected,
                    total_latency_ms=result.total_latency_ms,
                    ai_fallback_used=result.ai_fallback_used,
                    log_row=ParsedLogRow(
                        id=row.id,
                        timestamp=row.timestamp,
                        timestamp_raw=row.timestamp_raw,
                        source=row.source,
                        source_type=row.source_type,
                        log_level=row.log_level,
                        event_type=row.event_type,
                        message=row.message,
                        raw_text=row.raw_text,
                        record_group_id=row.record_group_id,
                        line_start=row.line_start,
                        line_end=row.line_end,
                        parse_confidence=row.parse_confidence,
                        schema_version=row.schema_version,
                        additional_data=row.additional_data,
                        raw_hash=row.raw_hash,
                        template_id=row.template_id,
                        equipment_id=row.equipment_id,
                        lot_id=row.lot_id,
                        wafer_id=row.wafer_id,
                        recipe_id=row.recipe_id,
                        step_id=row.step_id,
                        module_id=row.module_id,
                    ),
                )
            )

        except Exception as exc:
            logger.exception("Parser error on file '%s'", upload.filename)
            raise HTTPException(status_code=500, detail=f"Failed to parse '{upload.filename}': {exc}")

    return results


@router.post("/support-check", response_model=SupportCheckResponse)
def parser_support_check(
    payload: SupportCheckRequest,
    current_user: User = Depends(get_current_user),
):
    if not payload.files:
        raise HTTPException(status_code=400, detail="At least one file is required.")

    file_inputs = [
        FileInput(file_id=item.file_id, filename=item.filename, content=item.content) for item in payload.files
    ]
    mime_map = {item.file_id: item.mime_type for item in payload.files if item.file_id and item.mime_type}

    grouped, selections, warnings = ParserRegistry.resolve_for_files(
        file_inputs=file_inputs, mime_types_by_file_id=mime_map
    )

    _ = grouped  # grouped output is not needed in this endpoint response.

    selection_by_key: dict[tuple[str | None, str], Any] = {
        (selection.file_id, selection.filename): selection for selection in selections
    }

    response_items: list[FileSupportSelection] = []
    for item in payload.files:
        key = (item.file_id, item.filename)
        selected = selection_by_key.get(key)
        if selected is None:
            continue

        ranked = ParserRegistry.support_for_file(
            file_input=FileInput(file_id=item.file_id, filename=item.filename, content=item.content),
            mime_type=item.mime_type,
        )
        candidates = [
            FileSupportCandidate(
                parser_key=candidate.parser_key,
                supported=candidate.supported,
                score=candidate.score,
                reasons=candidate.reasons,
                detected_format=candidate.detected_format,
            )
            for candidate in ranked
        ]
        response_items.append(
            FileSupportSelection(
                file_id=item.file_id,
                filename=item.filename,
                parser_key=selected.parser_key,
                score=selected.score,
                reasons=selected.reasons,
                candidates=candidates,
            )
        )

    return SupportCheckResponse(selections=response_items, warnings=warnings)


@router.post("/ingest", response_model=ParserPipelineResult)
def parser_ingest(
    payload: IngestRequest,
    current_user: User = Depends(get_current_user),
):
    if not payload.files:
        raise HTTPException(status_code=400, detail="At least one file is required.")

    file_inputs = [
        FileInput(file_id=item.file_id, filename=item.filename, content=item.content) for item in payload.files
    ]

    service = LogPreprocessorService(table_name="logs")
    classification = service.classify(file_inputs)

    preferred_keys: list[str] = []
    if payload.preferred_parser_key:
        preferred_keys.append(payload.preferred_parser_key)
    elif classification.selected_parser_key:
        preferred_keys.append(classification.selected_parser_key)

    grouped, selections, warnings = ParserRegistry.resolve_for_files(
        file_inputs=file_inputs,
        preferred_keys=preferred_keys,
    )
    if not grouped:
        raise HTTPException(
            status_code=400,
            detail="No parser could be selected for the provided files.",
        )

    table_definitions = []
    merged_records: dict[str, list[dict[str, Any]]] = {}
    merged_warnings = list(warnings)
    parser_keys_used: list[str] = []
    confidence_total = 0.0
    confidence_count = 0

    for parser_key, parser_files in grouped.items():
        try:
            pipeline = ParserRegistry.route(parser_key)
        except KeyError as exc:
            merged_warnings.append(str(exc))
            continue

        try:
            result = pipeline.ingest(parser_files, classification)
        except Exception as exc:  # noqa: BLE001
            logger.exception("/parser/ingest parser failure for '%s'", parser_key)
            merged_warnings.append(f"Parser '{parser_key}' failed: {exc}")
            continue

        table_definitions.extend(result.table_definitions)
        merged_records.update(result.records)
        merged_warnings.extend(result.warnings)
        parser_keys_used.append(parser_key)
        confidence_total += result.confidence
        confidence_count += 1

    if not table_definitions:
        selection_summary = ", ".join(
            f"{selection.filename}->{selection.parser_key} ({selection.score:.2f})" for selection in selections
        )
        detail = "; ".join(merged_warnings) or "All parser groups failed"
        if selection_summary:
            detail = f"{detail}. Routing summary: {selection_summary}"
        raise HTTPException(status_code=500, detail=detail)

    return ParserPipelineResult(
        table_definitions=table_definitions,
        records=merged_records,
        parser_key=parser_keys_used[0] if len(parser_keys_used) == 1 else "multi_parser",
        warnings=merged_warnings,
        confidence=round((confidence_total / confidence_count) if confidence_count else 0.0, 2),
    )
