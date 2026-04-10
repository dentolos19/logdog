from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserPipelineResult,
    ParserSupportRequest,
    ParserSupportResult,
    StructuralClass,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from parsers.preprocessor import MULTILINE_CONTINUATION_PATTERN, DetectedFormat, FileInput, LogPreprocessorService
from parsers.registry import ParserPipeline
from parsers.semi_structured.pipeline import PipelineConfig, SemiStructuredPipeline

if TYPE_CHECKING:
    from parsers.contracts import ClassificationResult

logger = logging.getLogger(__name__)


def _segment(lines: list[str]) -> list[tuple[int, int, str]]:
    clusters: list[tuple[int, int, str]] = []
    current_start = 0
    current_lines: list[str] = []

    for index, line in enumerate(lines):
        if MULTILINE_CONTINUATION_PATTERN.match(line) and current_lines:
            current_lines.append(line)
        else:
            if current_lines:
                clusters.append((current_start + 1, current_start + len(current_lines), "\n".join(current_lines)))
            current_start = index
            current_lines = [line]

    if current_lines:
        clusters.append((current_start + 1, current_start + len(current_lines), "\n".join(current_lines)))

    return clusters


def _log_row_to_dict(log_row: Any, filename: str) -> dict[str, Any]:
    from dataclasses import asdict

    data = asdict(log_row)

    extra = data.get("extra")
    if isinstance(extra, dict):
        if "source" not in extra:
            extra["source"] = filename
        data["extra"] = json.dumps(extra)

    for drop_key in ("raw_hash", "template_id", "log_group_id"):
        data.pop(drop_key, None)

    data.pop("id", None)
    return data


def _infer_extra_columns(rows: list[dict[str, Any]]) -> list[ColumnDefinition]:
    key_counts: dict[str, int] = {}
    for row in rows[:50]:
        for key in row:
            if key not in BASELINE_COLUMN_NAMES:
                key_counts[key] = key_counts.get(key, 0) + 1

    threshold = max(1, len(rows[:50]) // 10)
    return [ColumnDefinition(name=key, sql_type="TEXT") for key, count in key_counts.items() if count >= threshold]


class SemiStructuredParserPipeline(ParserPipeline):
    parser_key = "semi_structured"

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        lines = [line for line in request.content.splitlines() if line.strip()]
        if not lines:
            return ParserSupportResult(
                parser_key=self.parser_key,
                supported=False,
                score=0.0,
                reasons=["File is empty after trimming."],
                structural_class=StructuralClass.SEMI_STRUCTURED,
            )

        continuation_count = sum(1 for line in lines if MULTILINE_CONTINUATION_PATTERN.match(line))
        continuation_ratio = continuation_count / len(lines)

        detector = LogPreprocessorService(table_name="logs")
        detected_format, confidence = detector._detect_format(lines)

        score = 0.0
        reasons: list[str] = []

        if detected_format == DetectedFormat.KEY_VALUE:
            score = max(score, 0.55 + confidence * 0.35)
            reasons.append(f"Key-value style content detected (confidence {confidence:.2f}).")

        if detected_format in {DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN}:
            score = max(score, 0.4 + confidence * 0.2)
            reasons.append("Plain or unknown text can be handled by semi-structured stages.")

        if continuation_ratio > 0.1:
            score = max(score, min(0.9, 0.5 + continuation_ratio))
            reasons.append(f"Detected multiline continuation patterns ({continuation_count}/{len(lines)} lines).")

        if request.filename.lower().endswith((".log", ".txt")):
            score = max(score, 0.45)
            reasons.append("Text log extension matched (.log/.txt).")

        if not reasons:
            reasons.append("No strong semi-structured signals detected.")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=score >= 0.4,
            score=round(min(score, 1.0), 2),
            reasons=reasons,
            detected_format=detected_format.value,
            structural_class=StructuralClass.SEMI_STRUCTURED,
        )

    def parse(self, file_inputs: list[FileInput], classification: "ClassificationResult") -> ParserPipelineResult:
        table_defs: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []
        total_confidence = 0.0
        processed = 0

        for file_input in file_inputs:
            lines = [line for line in file_input.content.splitlines() if line.strip()]
            if not lines:
                warnings.append(f"'{file_input.filename}': empty after noise filtering, skipped.")
                continue

            config = PipelineConfig(log_group_id=file_input.file_id or "default", ai_fallback_enabled=True)
            inner_pipeline = SemiStructuredPipeline(config=config)
            clusters = _segment(lines)
            file_rows: list[dict[str, Any]] = []

            for start, end, text in clusters:
                try:
                    result = inner_pipeline.process(text)
                    row = _log_row_to_dict(result.log_row, file_input.filename)
                    file_rows.append(row)
                    total_confidence += result.confidence
                    processed += 1
                except Exception as error:  # noqa: BLE001
                    logger.warning(
                        "Semi-structured pipeline error for '%s' cluster [%d-%d]: %s",
                        file_input.filename,
                        start,
                        end,
                        error,
                    )

            if not file_rows:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            extra_cols = _infer_extra_columns(file_rows)
            all_columns = list(BASELINE_COLUMNS) + extra_cols
            table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
            ddl = build_ddl(table_name, all_columns)
            table_defs.append(TableDefinition(table_name=table_name, columns=all_columns, ddl=ddl))
            records[table_name] = file_rows

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0
        return ParserPipelineResult(
            table_definitions=table_defs,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )
