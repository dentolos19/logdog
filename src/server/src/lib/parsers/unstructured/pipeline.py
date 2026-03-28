"""Unstructured log parser pipeline.

Uses shared helpers from this package behind the ``ParserPipeline`` interface.
Handles files classified as ``StructuralClass.UNSTRUCTURED`` (PLAIN_TEXT,
UNKNOWN, or low-confidence binary-decoded inputs).

For each file the pipeline:
  1. Filters noise lines.
  2. Clusters multiline records (Drain3 + continuation patterns).
  3. Mines Drain3 log templates.
  4. Extracts fields from each cluster via heuristics.
  5. Infers extra columns via frequency thresholds.
  6. Optionally enriches with LLM-inferred extra columns.
  7. Suppresses high-frequency heartbeat/noise rows (>40 % frequency, no actionable fields).
  8. Generates a CREATE TABLE DDL statement and returns ``ParserPipelineResult``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import TYPE_CHECKING, Any

from . import core as _up
from .core import MEASUREMENT_FIELD_NAMES as _CORE_MEASUREMENT_FIELDS
from lib import ai
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

# ---------------------------------------------------------------------------
# Semiconductor-domain column names that are always promoted to top-level
# columns when detected in at least one record.
# ---------------------------------------------------------------------------

SEMICONDUCTOR_COLUMN_NAMES = frozenset(
    {
        "wafer_id",
        "tool_id",
        "recipe_id",
        "process_step",
    }
)

# Template columns are always included when Drain3 mining runs.
TEMPLATE_COLUMN_NAMES = frozenset(
    {
        "template",
        "template_cluster_id",
    }
)

# Known measurement field names → REAL type.
# Imported from core.py to maintain a single source of truth.
MEASUREMENT_FIELD_NAMES = frozenset(_CORE_MEASUREMENT_FIELDS)

# Short aliases that map to canonical *_id columns — suppress from extra cols.
_ALIAS_NAMES = frozenset({"wafer", "tool", "recipe"})

# Minimum frequency threshold: a field must appear in ≥5 % of records
# (with a floor of 2 occurrences) to become a dedicated column.
_MIN_FREQUENCY_FLOOR = 2
_MIN_FREQUENCY_RATIO = 0.05


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def _compute_row_confidence(fields: dict[str, Any]) -> float:
    """Compute a per-row parse confidence score based on extraction quality.

    Factors:
      - Base score of 0.40 for any parsed record.
      - +0.10 if a timestamp was extracted.
      - +0.05 if a log level was extracted.
      - +0.05 for each semiconductor identifier (wafer, tool, recipe, step), max +0.15.
      - +0.05 for each measurement field extracted, max +0.15.
      - +0.05 if a template was mined (always true when Drain3 runs).
    Capped at 0.95 (LLM enrichment can push higher).
    """
    score = 0.40

    if fields.get("timestamp") or fields.get("timestamp_raw"):
        score += 0.10
    if fields.get("log_level"):
        score += 0.05

    semi_count = sum(1 for k in ("wafer_id", "tool_id", "recipe_id", "process_step") if fields.get(k))
    score += min(semi_count * 0.05, 0.15)

    measurement_count = sum(1 for k in fields if k in MEASUREMENT_FIELD_NAMES)
    score += min(measurement_count * 0.05, 0.15)

    if fields.get("template"):
        score += 0.05

    return round(min(score, 0.95), 2)


def _row_from_fields(
    fields: dict[str, Any],
    filename: str,
    line_start: int,
    line_end: int,
    raw_text: str,
    column_names: frozenset[str],
) -> dict[str, Any]:
    """Build an insertion-ready row dict from heuristic-extracted fields.

    Fields that are not in ``column_names`` (i.e. not promoted to dedicated
    columns) are collected into the ``additional_data`` JSON blob so no
    information is lost.

    ``template`` and ``template_cluster_id`` are expected to be present in
    ``fields`` (set during the extraction loop in ``_parse_single_file``).
    """
    row: dict[str, Any] = {
        "source": filename,
        "source_type": "file",
        "schema_version": SCHEMA_VERSION,
        "line_start": line_start,
        "line_end": line_end,
        "raw_text": raw_text[:4000],
        # template and template_cluster_id come from the fields dict via the loop below.
    }

    overflow: dict[str, Any] = {}

    for k, v in fields.items():
        if k in column_names or k in BASELINE_COLUMN_NAMES:
            row[k] = v
        else:
            # Field exists but has no dedicated column → overflow.
            overflow[k] = v

    row.setdefault("log_level", "INFO")
    row.setdefault("message", raw_text.strip()[:500])
    row["parse_confidence"] = _compute_row_confidence(fields)

    if overflow:
        row["additional_data"] = json.dumps(overflow, default=str)

    return row


# ---------------------------------------------------------------------------
# Fixed-width field detection
# ---------------------------------------------------------------------------


def _detect_fixed_width_fields(lines: list[str]) -> list[tuple[int, int, str]] | None:
    """Detect fixed-width columnar log formats.

    Examines the first N lines for consistent whitespace boundaries that
    suggest fixed-width columns (e.g. mainframe-style logs, ``xxd`` output,
    or equipment status dumps with aligned columns).

    Returns a list of ``(start_col, end_col, inferred_name)`` tuples if a
    fixed-width layout is detected, or ``None`` if the content is free-form.
    """
    if len(lines) < 5:
        return None

    sample = lines[: min(50, len(lines))]
    # Find lines of similar length (within 10 % of median).
    lengths = sorted(len(line) for line in sample if line.strip())
    if not lengths:
        return None
    median_len = lengths[len(lengths) // 2]
    if median_len < 20:
        return None

    similar = [line for line in sample if abs(len(line) - median_len) <= median_len * 0.1]
    if len(similar) < len(sample) * 0.6:
        return None  # Not enough lines with consistent length.

    # Find columns where ALL similar lines have a space.
    space_cols: set[int] = set(range(median_len))
    for line in similar:
        for col in list(space_cols):
            if col < len(line) and line[col] != " ":
                space_cols.discard(col)

    if not space_cols:
        return None

    # Group consecutive space columns into boundary *runs* (gaps).
    sorted_cols = sorted(space_cols)
    # A boundary is a run of ≥2 consecutive space columns (single spaces
    # between words are not field separators).
    runs: list[tuple[int, int]] = []  # (start, end) of each space run
    run_start = sorted_cols[0]
    prev = sorted_cols[0]
    for col in sorted_cols[1:]:
        if col == prev + 1:
            prev = col
        else:
            run_len = prev - run_start + 1
            if run_len >= 2:
                runs.append((run_start, prev))
            run_start = col
            prev = col
    run_len = prev - run_start + 1
    if run_len >= 2:
        runs.append((run_start, prev))

    if len(runs) < 2:
        return None  # Need at least 2 multi-space gaps for 3 fields.

    # Build field ranges from the gaps.
    field_ranges: list[tuple[int, int, str]] = []
    # First field: from 0 to start of first gap.
    if runs[0][0] > 2:
        field_ranges.append((0, runs[0][0], "field_1"))

    # Middle fields: between consecutive gaps.
    for i in range(len(runs) - 1):
        start = runs[i][1] + 1
        end = runs[i + 1][0]
        if end - start >= 2:
            field_ranges.append((start, end, f"field_{len(field_ranges) + 1}"))

    # Last field: from end of last gap to line end.
    last_start = runs[-1][1] + 1
    if median_len - last_start >= 2:
        field_ranges.append((last_start, median_len, f"field_{len(field_ranges) + 1}"))

    if len(field_ranges) < 3:
        return None  # Need at least 3 fields for a meaningful fixed-width layout.

    return field_ranges


def _extract_fixed_width_fields(
    text: str,
    field_ranges: list[tuple[int, int, str]],
) -> dict[str, str]:
    """Extract field values from a single line using fixed-width positions."""
    fields: dict[str, str] = {}
    for start, end, name in field_ranges:
        if start < len(text):
            value = text[start : min(end, len(text))].strip()
            if value:
                fields[name] = value
    return fields


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


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

        # Fixed-width detection bonus.
        fw = _detect_fixed_width_fields(non_empty[:50])
        if fw:
            score = max(score, 0.8)
            reasons.append(f"Fixed-width columnar layout detected ({len(fw)} fields).")

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
            result = self._parse_single_file(file_input)
            if result is None:
                warnings.append(f"'{file_input.filename}': no content after noise filtering, skipped.")
                continue

            table_def, file_rows, file_warnings, file_confidence = result
            table_defs.append(table_def)
            records[table_def.table_name] = file_rows
            warnings.extend(file_warnings)
            total_confidence += file_confidence
            processed += 1

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0

        return ParserPipelineResult(
            table_definitions=table_defs,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )

    def _parse_single_file(
        self,
        file_input: FileInput,
    ) -> tuple[TableDefinition, list[dict[str, Any]], list[str], float] | None:
        """Parse a single file and return (table_def, rows, warnings, confidence).

        Returns ``None`` if the file has no usable content.
        """
        file_warnings: list[str] = []
        lines = file_input.content.splitlines()
        clean_lines = _up.filter_noise(lines)

        if not clean_lines:
            return None

        # Hex-dump special handling.
        hex_count = sum(1 for line in clean_lines if _up.is_hex_dump_line(line))
        if hex_count > len(clean_lines) * 0.5:
            ascii_lines = _up.extract_ascii_from_hexdump(clean_lines)
            if ascii_lines:
                clean_lines = ascii_lines
            else:
                file_warnings.append(f"'{file_input.filename}': hex-dump only, no ASCII content extractable.")
                return None

        # Check for fixed-width columnar layout.
        fw_fields = _detect_fixed_width_fields(clean_lines)

        # Cluster + mine templates.
        clusters = _up.cluster_multiline(clean_lines)
        _miner, templates = _up.mine_templates(clusters)

        # Extract fields from each cluster.
        all_fields: list[dict[str, Any]] = []
        for (start, end, text), template in zip(clusters, templates):
            fields = _up.extract_fields_heuristic(text)

            # Overlay fixed-width fields if detected.
            if fw_fields:
                first_line = text.split("\n", 1)[0]
                fw_extracted = _extract_fixed_width_fields(first_line, fw_fields)
                for k, v in fw_extracted.items():
                    if k not in fields:
                        fields[k] = v

            fields["template"] = template
            fields["template_cluster_id"] = hashlib.md5(template.encode(), usedforsecurity=False).hexdigest()[:12]
            all_fields.append(fields)

        if not all_fields:
            file_warnings.append(f"No records extracted from '{file_input.filename}'.")
            return None

        # Infer extra columns with frequency-based thresholds.
        extra_cols = self._infer_extra_columns(all_fields)

        # LLM enrichment (optional).
        llm_cols = self._llm_enrich_columns(clusters, extra_cols, file_input.filename, file_warnings)
        if llm_cols:
            existing_names = {c.name for c in extra_cols}
            for col in llm_cols:
                if col.name not in existing_names:
                    extra_cols.append(col)
                    existing_names.add(col.name)

        # Build the full column list and DDL.
        all_cols = list(BASELINE_COLUMNS) + extra_cols
        column_names = frozenset(c.name for c in all_cols)

        table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
        ddl = build_ddl(table_name, all_cols)

        # Build rows with overflow into additional_data.
        file_rows: list[dict[str, Any]] = []
        for (start, end, text), fields in zip(clusters, all_fields):
            row = _row_from_fields(
                fields,
                file_input.filename,
                start,
                end,
                text,
                column_names,
            )
            file_rows.append(row)

        # Heartbeat / noise suppression: remove rows whose template
        # accounts for >40 % of all records and carries no actionable
        # fields (no log_level other than INFO, no measurements, no errors).
        file_rows, suppressed = _suppress_heartbeats(file_rows, len(clusters), file_warnings)

        # Compute file-level confidence from row confidences.
        if file_rows:
            avg_confidence = sum(r.get("parse_confidence", 0.6) for r in file_rows) / len(file_rows)
        else:
            avg_confidence = 0.0

        # Boost confidence if LLM enrichment succeeded.
        if llm_cols:
            avg_confidence = min(avg_confidence + 0.10, 1.0)

        table_def = TableDefinition(table_name=table_name, columns=all_cols, ddl=ddl)
        return table_def, file_rows, file_warnings, round(avg_confidence, 2)

    # ------------------------------------------------------------------
    # Column inference
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_extra_columns(
        all_fields: list[dict[str, Any]],
    ) -> list[ColumnDefinition]:
        """Infer extra columns from extracted fields using frequency thresholds.

        A field becomes a dedicated column if:
          - It appears in ≥5 % of records (minimum 2 occurrences), OR
          - It is a semiconductor-domain identifier (wafer_id, tool_id, etc.), OR
          - It is a template column (template, template_cluster_id).

        Fields below the threshold are NOT dropped — they go into the
        ``additional_data`` JSON blob at row-build time.
        """
        if not all_fields:
            return []

        key_counts: dict[str, int] = {}
        key_examples: dict[str, list[str]] = {}

        for fields in all_fields:
            for k, v in fields.items():
                if k in BASELINE_COLUMN_NAMES or k in _ALIAS_NAMES:
                    continue
                key_counts[k] = key_counts.get(k, 0) + 1
                examples = key_examples.setdefault(k, [])
                if len(examples) < 3:
                    examples.append(str(v)[:100])

        threshold = max(
            _MIN_FREQUENCY_FLOOR,
            int(len(all_fields) * _MIN_FREQUENCY_RATIO),
        )

        cols: list[ColumnDefinition] = []
        seen: set[str] = set()

        # Always include semiconductor columns if they appear in any record.
        for name in SEMICONDUCTOR_COLUMN_NAMES:
            if name in key_counts and name not in seen:
                cols.append(
                    ColumnDefinition(
                        name=name,
                        sql_type="TEXT",
                        description="Semiconductor identifier extracted from log.",
                    )
                )
                seen.add(name)

        # Always include template columns (Drain3 always runs in this pipeline).
        for name in TEMPLATE_COLUMN_NAMES:
            if name not in seen:
                cols.append(
                    ColumnDefinition(
                        name=name,
                        sql_type="TEXT",
                        description="Drain3-mined log template."
                        if name == "template"
                        else "Hash identifying the Drain3 template cluster.",
                    )
                )
                seen.add(name)

        # Frequency-based columns.
        for k, count in key_counts.items():
            if k in seen:
                continue
            if count < threshold:
                continue

            sql_type = "TEXT"
            examples = key_examples.get(k, [])
            if k in MEASUREMENT_FIELD_NAMES or _all_numeric(examples):
                sql_type = "REAL"

            cols.append(
                ColumnDefinition(
                    name=k,
                    sql_type=sql_type,
                    description=f"Extracted from unstructured text ({count}/{len(all_fields)} records).",
                )
            )
            seen.add(k)

        return cols

    # ------------------------------------------------------------------
    # LLM enrichment
    # ------------------------------------------------------------------

    @staticmethod
    def _llm_enrich_columns(
        clusters: list[tuple[int, int, str]],
        heuristic_cols: list[ColumnDefinition],
        filename: str,
        warnings: list[str],
    ) -> list[ColumnDefinition]:
        """Optionally call the LLM to discover additional columns.

        Converts the ``ColumnDefinition`` list to the legacy
        ``InferredColumn`` format expected by ``core.call_llm_for_unstructured``,
        then converts the result back.
        """
        if not ai.has_openrouter_api_key():
            return []

        from lib.parsers.preprocessor import InferredColumn, SqlType, ColumnKind

        # Convert ColumnDefinition → InferredColumn for the core LLM helper.
        legacy_cols: list[InferredColumn] = []
        for col in heuristic_cols:
            sql_type = SqlType.TEXT
            if col.sql_type.upper() == "REAL":
                sql_type = SqlType.REAL
            elif col.sql_type.upper() == "INTEGER":
                sql_type = SqlType.INTEGER
            legacy_cols.append(
                InferredColumn(
                    name=col.name,
                    sql_type=sql_type,
                    description=col.description,
                    nullable=col.nullable,
                    kind=ColumnKind.DETECTED,
                )
            )

        sample_lines = [text[: _up.MAX_LINE_LENGTH] for _, _, text in clusters[: _up.MAX_SAMPLE_LINES]]

        try:
            llm_result = _up.call_llm_for_unstructured(sample_lines, legacy_cols)
        except Exception as exc:
            logger.warning("LLM enrichment failed for '%s': %s", filename, exc)
            warnings.append(f"LLM enrichment failed: {exc}")
            return []

        if llm_result.warnings:
            warnings.extend(llm_result.warnings)

        new_cols: list[ColumnDefinition] = []
        existing_names = {c.name for c in heuristic_cols} | BASELINE_COLUMN_NAMES

        for llm_field in llm_result.fields:
            safe_name = re.sub(r"[^a-z0-9_]", "_", llm_field.name.lower()).strip("_")
            if not safe_name or safe_name in existing_names:
                continue

            sql_type = "TEXT"
            if llm_field.sql_type.upper() in ("INTEGER", "INT"):
                sql_type = "INTEGER"
            elif llm_field.sql_type.upper() in ("REAL", "FLOAT", "DOUBLE"):
                sql_type = "REAL"

            new_cols.append(
                ColumnDefinition(
                    name=safe_name,
                    sql_type=sql_type,
                    description=llm_field.description or f"LLM-inferred from {filename}.",
                )
            )
            existing_names.add(safe_name)

        if new_cols:
            logger.info(
                "LLM enrichment added %d columns for '%s': %s",
                len(new_cols),
                filename,
                ", ".join(c.name for c in new_cols),
            )

        return new_cols


# ---------------------------------------------------------------------------
# Heartbeat suppression
# ---------------------------------------------------------------------------

# Minimum frequency ratio for a template to be considered a heartbeat candidate.
_HEARTBEAT_FREQUENCY_RATIO = 0.40

# Log levels that indicate actionable content (not heartbeats).
_ACTIONABLE_LEVELS = frozenset({"WARN", "WARNING", "ERROR", "FATAL", "CRITICAL", "ALERT", "EMERG", "EMERGENCY"})


def _suppress_heartbeats(
    rows: list[dict[str, Any]],
    total_clusters: int,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], int]:
    """Remove rows whose template is a high-frequency heartbeat.

    A template is suppressed if:
      - It accounts for >40 % of all clusters, AND
      - None of its rows have an actionable log level (WARN/ERROR/etc.), AND
      - None of its rows contain measurement fields.

    Returns ``(filtered_rows, suppressed_count)``.
    """
    if not rows or total_clusters < 5:
        return rows, 0

    # Group rows by template.
    template_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        tmpl = row.get("template", "")
        template_groups.setdefault(tmpl, []).append(row)

    suppressed_templates: set[str] = set()
    for tmpl, group in template_groups.items():
        frequency = len(group) / total_clusters
        if frequency < _HEARTBEAT_FREQUENCY_RATIO:
            continue

        # Check if any row in this template group has actionable content.
        has_actionable = False
        for row in group:
            level = (row.get("log_level") or "").upper()
            if level in _ACTIONABLE_LEVELS:
                has_actionable = True
                break
            # Check for measurement fields.
            if any(k in MEASUREMENT_FIELD_NAMES for k in row if k not in BASELINE_COLUMN_NAMES):
                has_actionable = True
                break

        if not has_actionable:
            suppressed_templates.add(tmpl)

    if not suppressed_templates:
        return rows, 0

    filtered = [r for r in rows if r.get("template", "") not in suppressed_templates]
    suppressed_count = len(rows) - len(filtered)

    if suppressed_count > 0:
        warnings.append(
            f"Suppressed {suppressed_count} heartbeat/noise rows "
            f"({len(suppressed_templates)} template(s) exceeding "
            f"{_HEARTBEAT_FREQUENCY_RATIO:.0%} frequency threshold)."
        )

    return filtered, suppressed_count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_numeric(examples: list[str]) -> bool:
    """Return True if all non-empty example values parse as float."""
    if not examples:
        return False
    for val in examples:
        try:
            float(val)
        except (ValueError, TypeError):
            return False
    return True
