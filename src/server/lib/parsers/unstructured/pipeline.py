"""Unstructured log parser pipeline.

Uses shared helpers from this package behind the ``ParserPipeline`` interface.
Handles files classified as ``StructuralClass.UNSTRUCTURED`` (PLAIN_TEXT,
UNKNOWN, or low-confidence binary-decoded inputs).

For each file the pipeline:
  1. Filters noise lines.
  2. Clusters multiline records (Drain3 + continuation patterns).
  3. Mines Drain3 log templates.
  4. Extracts fields from each cluster via heuristics.
  5. Optionally enriches with LLM-inferred extra columns.
  6. Generates a CREATE TABLE DDL statement.
  7. Returns ``ParserPipelineResult`` with table definitions and row data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from . import core as _up
from lib.parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserSupportRequest,
    ParserSupportResult,
    ParserPipelineResult,
    StructuralClass,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from lib.parsers.preprocessor import FileInput
from lib.parsers.registry import ParserPipeline

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"


def _row_from_fields(
    fields: dict[str, Any],
    filename: str,
    line_start: int,
    line_end: int,
    raw_text: str,
    template: str,
) -> dict[str, Any]:
    """Build an insertion-ready row dict from heuristic-extracted fields."""
    row: dict[str, Any] = {
        "source": filename,
        "source_type": "file",
        "schema_version": SCHEMA_VERSION,
        "line_start": line_start,
        "line_end": line_end,
        "raw_text": raw_text[:4000],
        "template": template,
    }
    # Promote any extracted fields whose names don't clash with baseline metadata.
    for k, v in fields.items():
        if k in BASELINE_COLUMN_NAMES or k in ("template", "template_cluster_id"):
            row[k] = v
        else:
            row[k] = v  # keep as top-level; DDL will include it if frequent enough

    row.setdefault("log_level", "INFO")
    row.setdefault("message", raw_text.strip()[:500])
    row["parse_confidence"] = 0.6  # default for heuristic paths
    return row


class UnstructuredPipeline(ParserPipeline):
    """Parser pipeline for unstructured / plain-text log files.

    Reuses the battle-tested helpers in this package —
    binary normalization, noise filtering, Drain3 template mining, and
    heuristic field extraction — and adapts their output to ``ParserPipelineResult``.
    """

    parser_key = "unstructured"

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        lines = request.content.splitlines()
        non_empty = [line for line in lines if line.strip()]

        if not non_empty:
            return ParserSupportResult(
                parser_key=self.parser_key,
                supported=False,
                score=0.0,
                reasons=["File is empty after trimming."],
                structural_class=StructuralClass.UNSTRUCTURED,
            )

        from lib.parsers.preprocessor import DetectedFormat, LogPreprocessorService

        detector = LogPreprocessorService(table_name="logs")
        detected_format, confidence = detector._detect_format(non_empty)

        score = 0.35
        reasons: list[str] = ["Unstructured parser is the general fallback."]

        if detected_format in (DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN):
            score = max(score, 0.75)
            reasons.append(f"Detected {detected_format.value} content.")
        elif detected_format == DetectedFormat.KEY_VALUE and confidence < 0.6:
            score = max(score, 0.7)
            reasons.append("Low-confidence key-value content handled better by heuristics.")
        else:
            score = max(score, 0.4)
            reasons.append(f"Structured hints detected ({detected_format.value}); available as fallback.")

        hex_count = sum(1 for line in non_empty[:100] if _up.is_hex_dump_line(line))
        if hex_count > 0:
            score = max(score, min(0.92, 0.65 + (hex_count / max(1, len(non_empty[:100])))))
            reasons.append("Hex dump patterns detected and can be decoded.")

        if request.filename.lower().endswith((".bin", ".dat", ".blob")):
            score = max(score, 0.9)
            reasons.append("Binary-like extension matched.")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=True,
            score=round(min(score, 1.0), 2),
            reasons=reasons,
            detected_format=detected_format.value,
            structural_class=StructuralClass.UNSTRUCTURED,
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
            lines = file_input.content.splitlines()
            clean_lines = _up.filter_noise(lines)

            if not clean_lines:
                warnings.append(f"'{file_input.filename}': no content after noise filtering, skipped.")
                continue

            # Hex-dump special handling.
            hex_count = sum(1 for line in clean_lines if _up.is_hex_dump_line(line))
            if hex_count > len(clean_lines) * 0.5:
                ascii_lines = _up.extract_ascii_from_hexdump(clean_lines)
                if ascii_lines:
                    clean_lines = ascii_lines
                else:
                    warnings.append(f"'{file_input.filename}': hex-dump only, no ASCII content extractable.")
                    continue

            # Cluster + mine templates.
            clusters = _up.cluster_multiline(clean_lines)
            _miner, templates = _up.mine_templates(clusters)

            # Extract fields from each cluster.
            all_fields: list[dict[str, Any]] = []
            file_rows: list[dict[str, Any]] = []
            for (start, end, text), template in zip(clusters, templates):
                fields = _up.extract_fields_heuristic(text)
                fields["template"] = template
                import hashlib

                fields["template_cluster_id"] = hashlib.md5(template.encode(), usedforsecurity=False).hexdigest()[:12]
                all_fields.append(fields)
                row = _row_from_fields(fields, file_input.filename, start, end, text, template)
                file_rows.append(row)

            if not file_rows:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            # Infer per-file extra columns from record frequency.
            extra_cols = self._infer_extra_columns(all_fields)

            all_cols = list(BASELINE_COLUMNS) + extra_cols
            table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
            ddl = build_ddl(table_name, all_cols)

            table_defs.append(TableDefinition(table_name=table_name, columns=all_cols, sqlite_ddl=ddl))
            records[table_name] = file_rows
            total_confidence += 0.65  # unstructured confidence baseline
            processed += 1

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0

        return ParserPipelineResult(
            table_definitions=table_defs,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )

    @staticmethod
    def _infer_extra_columns(all_fields: list[dict[str, Any]]) -> list[ColumnDefinition]:
        """Return ColumnDefinitions for fields that appear in enough records."""
        if not all_fields:
            return []

        key_counts: dict[str, int] = {}
        key_examples: dict[str, list[str]] = {}
        for fields in all_fields:
            for k, v in fields.items():
                if k in BASELINE_COLUMN_NAMES:
                    continue
                key_counts[k] = key_counts.get(k, 0) + 1
                exs = key_examples.setdefault(k, [])
                if len(exs) < 3 and v is not None:
                    exs.append(str(v)[:100])

        threshold = max(2, len(all_fields) // 20)
        cols: list[ColumnDefinition] = []
        # Measurement field names that should use REAL type.
        real_fields = {
            "thickness",
            "pressure",
            "temperature",
            "temp",
            "flow",
            "gas_flow",
            "power",
            "voltage",
            "current",
            "rpm",
            "dose",
            "energy",
            "frequency",
            "bias",
            "uniformity",
            "vacuum",
            "rf_power",
            "reflected",
            "delta",
            "duration",
            "rate",
            "threshold",
            "vibration",
            "resistivity",
            "particle_count",
        }
        for k, count in key_counts.items():
            if count < threshold:
                continue
            sql_type = "REAL" if k in real_fields else "TEXT"
            cols.append(ColumnDefinition(name=k, sql_type=sql_type))

        return cols
