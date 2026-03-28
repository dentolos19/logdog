"""Semi-structured parser pipeline adapter.

Wraps the existing ``SemiStructuredPipeline`` behind the ``ParserPipeline``
interface so the orchestrator can dispatch to it uniformly.

For each file the adapter:
  1. Segments the content into multiline clusters (one cluster = one record).
  2. Runs each cluster through the ``SemiStructuredPipeline``.
  3. Collects all ``LogRow`` outputs and infers extra columns from them.
  4. Generates a CREATE TABLE DDL statement.
  5. Returns a ``ParserPipelineResult`` with table definitions and row data.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from lib.parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ParserSupportRequest,
    ParserSupportResult,
    ParserPipelineResult,
    StructuralClass,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from lib.parsers.preprocessor import MULTILINE_CONTINUATION_PATTERN, FileInput
from lib.parsers.registry import ParserPipeline
from lib.parsers.semiStructured.pipeline import PipelineConfig, SemiStructuredPipeline

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"


def _segment(lines: list[str]) -> list[tuple[int, int, str]]:
    """Group lines into multiline clusters (start, end, text) — 1-based."""
    clusters: list[tuple[int, int, str]] = []
    current_start = 0
    current_lines: list[str] = []

    for idx, line in enumerate(lines):
        if MULTILINE_CONTINUATION_PATTERN.match(line) and current_lines:
            current_lines.append(line)
        else:
            if current_lines:
                clusters.append(
                    (
                        current_start + 1,
                        current_start + len(current_lines),
                        "\n".join(current_lines),
                    )
                )
            current_start = idx
            current_lines = [line]

    if current_lines:
        clusters.append(
            (
                current_start + 1,
                current_start + len(current_lines),
                "\n".join(current_lines),
            )
        )

    return clusters


def _log_row_to_dict(log_row: Any, line_start: int, line_end: int, filename: str) -> dict[str, Any]:
    """Convert a ``LogRow`` dataclass into an insertion-ready dict."""
    from dataclasses import asdict

    d = asdict(log_row)
    # Overwrite positional / source metadata that the normalizer might have missed.
    d["source"] = d.get("source") or filename
    d["source_type"] = d.get("source_type") or "file"
    d["schema_version"] = SCHEMA_VERSION
    d["line_start"] = line_start
    d["line_end"] = line_end

    # Encode additional_data dict as JSON string.
    additional = d.get("additional_data")
    if isinstance(additional, dict):
        d["additional_data"] = json.dumps(additional)

    # Drop pipeline-only fields that must not land in the DB.
    for drop_key in ("raw_hash", "template_id", "log_group_id"):
        d.pop(drop_key, None)

    # Drop baseline key "id" so SQLite uses AUTOINCREMENT.
    d.pop("id", None)

    return d


def _infer_extra_columns(rows: list[dict[str, Any]]) -> list[Any]:
    """Return ColumnDefinitions for keys that appear in rows but are not baseline."""
    from lib.parsers.contracts import ColumnDefinition

    key_counts: dict[str, int] = {}
    for row in rows[:50]:
        for k in row:
            if k not in BASELINE_COLUMN_NAMES:
                key_counts[k] = key_counts.get(k, 0) + 1

    threshold = max(1, len(rows[:50]) // 10)
    return [ColumnDefinition(name=k, sql_type="TEXT") for k, count in key_counts.items() if count >= threshold]


class SemiStructuredParserPipeline(ParserPipeline):
    """Parser pipeline adapter for semi-structured log formats.

    Wraps ``SemiStructuredPipeline`` and the existing stage architecture
    without duplicating logic. AI fallback is propagated through the
    underlying pipeline's configuration.
    """

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

        from lib.parsers.preprocessor import DetectedFormat, LogPreprocessorService

        detector = LogPreprocessorService(table_name="logs")
        detected_format, confidence = detector._detect_format(lines)

        score = 0.0
        reasons: list[str] = []

        if detected_format == DetectedFormat.KEY_VALUE:
            score = max(score, 0.55 + confidence * 0.35)
            reasons.append(f"Key-value style content detected (confidence {confidence:.2f}).")

        if detected_format in (DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN):
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

    def parse(
        self,
        file_inputs: list[FileInput],
        classification: "ClassificationResult",
    ) -> ParserPipelineResult:
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

            config = PipelineConfig(
                log_group_id=file_input.file_id or "default",
                ai_fallback_enabled=True,
            )
            inner_pipeline = SemiStructuredPipeline(config=config)
            clusters = _segment(lines)
            file_rows: list[dict[str, Any]] = []

            for start, end, text in clusters:
                try:
                    result = inner_pipeline.process(text)
                    row = _log_row_to_dict(result.log_row, start, end, file_input.filename)
                    file_rows.append(row)
                    total_confidence += result.confidence
                    processed += 1
                except Exception as exc:
                    logger.warning(
                        "SemiStructured pipeline error for '%s' cluster [%d-%d]: %s",
                        file_input.filename,
                        start,
                        end,
                        exc,
                    )

            if not file_rows:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            extra_cols = _infer_extra_columns(file_rows)
            all_cols = list(BASELINE_COLUMNS) + extra_cols
            table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
            ddl = build_ddl(table_name, all_cols)
            table_defs.append(TableDefinition(table_name=table_name, columns=all_cols, ddl=ddl))
            records[table_name] = file_rows

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0

        return ParserPipelineResult(
            table_definitions=table_defs,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )
