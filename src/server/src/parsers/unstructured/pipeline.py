from __future__ import annotations

import hashlib
import json
import logging
import re
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
from parsers.preprocessor import (
    ColumnKind,
    DetectedFormat,
    FileInput,
    InferredColumn,
    LogPreprocessorService,
    SqlType,
)
from parsers.registry import ParserPipeline
from parsers.unstructured import core as _up
from parsers.unstructured.core import MEASUREMENT_FIELD_NAMES as _CORE_MEASUREMENT_FIELDS

if TYPE_CHECKING:
    from parsers.contracts import ClassificationResult

logger = logging.getLogger(__name__)

SEMICONDUCTOR_COLUMN_NAMES = frozenset({"wafer_id", "tool_id", "recipe_id", "process_step"})
TEMPLATE_COLUMN_NAMES = frozenset({"template", "template_cluster_id"})
MEASUREMENT_FIELD_NAMES = frozenset(_CORE_MEASUREMENT_FIELDS)
_ALIAS_NAMES = frozenset({"wafer", "tool", "recipe"})

_MIN_FREQUENCY_FLOOR = 2
_MIN_FREQUENCY_RATIO = 0.05


def _compute_row_confidence(fields: dict[str, Any]) -> float:
    score = 0.40
    if fields.get("timestamp"):
        score += 0.10
    if fields.get("log_level"):
        score += 0.05

    semiconductor_count = sum(1 for key in ("wafer_id", "tool_id", "recipe_id", "process_step") if fields.get(key))
    score += min(semiconductor_count * 0.05, 0.15)

    measurement_count = sum(1 for key in fields if key in MEASUREMENT_FIELD_NAMES)
    score += min(measurement_count * 0.05, 0.15)

    if fields.get("template"):
        score += 0.05

    return round(min(score, 0.95), 2)


def _row_from_fields(
    fields: dict[str, Any],
    filename: str,
    raw_text: str,
    column_names: frozenset[str],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "source": filename,
        "raw": raw_text[:4000],
    }
    overflow: dict[str, Any] = {}

    for key, value in fields.items():
        if key in column_names or key in BASELINE_COLUMN_NAMES:
            row[key] = value
        elif isinstance(value, (dict, list)):
            overflow[key] = value
        else:
            overflow[key] = value

    row.setdefault("log_level", "INFO")
    row.setdefault("message", raw_text.strip()[:500])
    row["parse_confidence"] = _compute_row_confidence(fields)

    if overflow:
        row["extra"] = json.dumps(overflow, default=str)

    return row


def _detect_fixed_width_fields(lines: list[str]) -> list[tuple[int, int, str]] | None:
    if len(lines) < 5:
        return None

    sample = lines[: min(50, len(lines))]
    lengths = sorted(len(line) for line in sample if line.strip())
    if not lengths:
        return None

    median_len = lengths[len(lengths) // 2]
    if median_len < 20:
        return None

    similar = [line for line in sample if abs(len(line) - median_len) <= median_len * 0.1]
    if len(similar) < len(sample) * 0.6:
        return None

    space_columns: set[int] = set(range(median_len))
    for line in similar:
        for column in list(space_columns):
            if column < len(line) and line[column] != " ":
                space_columns.discard(column)

    if not space_columns:
        return None

    sorted_columns = sorted(space_columns)
    runs: list[tuple[int, int]] = []
    run_start = sorted_columns[0]
    previous = sorted_columns[0]

    for column in sorted_columns[1:]:
        if column == previous + 1:
            previous = column
            continue
        run_length = previous - run_start + 1
        if run_length >= 2:
            runs.append((run_start, previous))
        run_start = column
        previous = column

    run_length = previous - run_start + 1
    if run_length >= 2:
        runs.append((run_start, previous))

    if len(runs) < 2:
        return None

    field_ranges: list[tuple[int, int, str]] = []
    if runs[0][0] > 2:
        field_ranges.append((0, runs[0][0], "field_1"))

    for index in range(len(runs) - 1):
        start = runs[index][1] + 1
        end = runs[index + 1][0]
        if end - start >= 2:
            field_ranges.append((start, end, f"field_{len(field_ranges) + 1}"))

    last_start = runs[-1][1] + 1
    if median_len - last_start >= 2:
        field_ranges.append((last_start, median_len, f"field_{len(field_ranges) + 1}"))

    if len(field_ranges) < 3:
        return None

    return field_ranges


def _extract_fixed_width_fields(text: str, field_ranges: list[tuple[int, int, str]]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for start, end, name in field_ranges:
        if start < len(text):
            value = text[start : min(end, len(text))].strip()
            if value:
                fields[name] = value
    return fields


class UnstructuredPipeline(ParserPipeline):
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

        detector = LogPreprocessorService(table_name="logs")
        detected_format, confidence = detector._detect_format(non_empty)

        score = 0.35
        reasons: list[str] = ["Unstructured parser is the general fallback."]

        if detected_format in {DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN}:
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

        fixed_width = _detect_fixed_width_fields(non_empty[:50])
        if fixed_width:
            score = max(score, 0.8)
            reasons.append(f"Fixed-width columnar layout detected ({len(fixed_width)} fields).")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=True,
            score=round(min(score, 1.0), 2),
            reasons=reasons,
            detected_format=detected_format.value,
            structural_class=StructuralClass.UNSTRUCTURED,
        )

    def parse(self, file_inputs: list[FileInput], classification: "ClassificationResult") -> ParserPipelineResult:
        table_definitions: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []
        total_confidence = 0.0
        processed = 0

        for file_input in file_inputs:
            result = self._parse_single_file(file_input)
            if result is None:
                warnings.append(f"'{file_input.filename}': no content after noise filtering, skipped.")
                continue

            table_definition, file_rows, file_warnings, file_confidence = result
            table_definitions.append(table_definition)
            records[table_definition.table_name] = file_rows
            warnings.extend(file_warnings)
            total_confidence += file_confidence
            processed += 1

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0
        return ParserPipelineResult(
            table_definitions=table_definitions,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )

    def _parse_single_file(
        self,
        file_input: FileInput,
    ) -> tuple[TableDefinition, list[dict[str, Any]], list[str], float] | None:
        file_warnings: list[str] = []
        lines = file_input.content.splitlines()
        clean_lines = _up.filter_noise(lines)

        if not clean_lines:
            return None

        hex_count = sum(1 for line in clean_lines if _up.is_hex_dump_line(line))
        if hex_count > len(clean_lines) * 0.5:
            ascii_lines = _up.extract_ascii_from_hexdump(clean_lines)
            if ascii_lines:
                clean_lines = ascii_lines
            else:
                file_warnings.append(f"'{file_input.filename}': hex-dump only, no ASCII content extractable.")
                return None

        fixed_width_fields = _detect_fixed_width_fields(clean_lines)

        clusters = _up.cluster_multiline(clean_lines)
        _, templates = _up.mine_templates(clusters)

        all_fields: list[dict[str, Any]] = []
        for (start, end, text), template in zip(clusters, templates):
            fields = _up.extract_fields_heuristic(text)

            if fixed_width_fields:
                first_line = text.split("\n", 1)[0]
                fixed_width_extracted = _extract_fixed_width_fields(first_line, fixed_width_fields)
                for key, value in fixed_width_extracted.items():
                    if key not in fields:
                        fields[key] = value

            fields["template"] = template
            fields["template_cluster_id"] = hashlib.md5(template.encode(), usedforsecurity=False).hexdigest()[:12]
            all_fields.append(fields)

        if not all_fields:
            file_warnings.append(f"No records extracted from '{file_input.filename}'.")
            return None

        extra_columns = self._infer_extra_columns(all_fields)
        llm_columns = self._llm_enrich_columns(clusters, extra_columns, file_input.filename, file_warnings)
        if llm_columns:
            existing_names = {column.name for column in extra_columns}
            for column in llm_columns:
                if column.name not in existing_names:
                    extra_columns.append(column)
                    existing_names.add(column.name)

        all_columns = list(BASELINE_COLUMNS) + extra_columns
        column_names = frozenset(column.name for column in all_columns)

        table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
        ddl = build_ddl(table_name, all_columns)

        file_rows: list[dict[str, Any]] = []
        for (start, end, text), fields in zip(clusters, all_fields):
            row = _row_from_fields(fields, file_input.filename, text, column_names)
            file_rows.append(row)

        file_rows, _suppressed = _suppress_heartbeats(file_rows, len(clusters), file_warnings)

        if file_rows:
            average_confidence = sum(row.get("parse_confidence", 0.6) for row in file_rows) / len(file_rows)
        else:
            average_confidence = 0.0

        if llm_columns:
            average_confidence = min(average_confidence + 0.10, 1.0)

        table_definition = TableDefinition(table_name=table_name, columns=all_columns, ddl=ddl)
        return table_definition, file_rows, file_warnings, round(average_confidence, 2)

    @staticmethod
    def _infer_extra_columns(all_fields: list[dict[str, Any]]) -> list[ColumnDefinition]:
        if not all_fields:
            return []

        key_counts: dict[str, int] = {}
        key_examples: dict[str, list[str]] = {}

        for fields in all_fields:
            for key, value in fields.items():
                if key in BASELINE_COLUMN_NAMES or key in _ALIAS_NAMES:
                    continue
                key_counts[key] = key_counts.get(key, 0) + 1
                examples = key_examples.setdefault(key, [])
                if len(examples) < 3:
                    examples.append(str(value)[:100])

        threshold = max(_MIN_FREQUENCY_FLOOR, int(len(all_fields) * _MIN_FREQUENCY_RATIO))
        columns: list[ColumnDefinition] = []
        seen: set[str] = set()

        for name in SEMICONDUCTOR_COLUMN_NAMES:
            if name in key_counts and name not in seen:
                columns.append(
                    ColumnDefinition(
                        name=name,
                        sql_type="TEXT",
                        description="Semiconductor identifier extracted from log.",
                    )
                )
                seen.add(name)

        for name in TEMPLATE_COLUMN_NAMES:
            if name not in seen:
                columns.append(
                    ColumnDefinition(
                        name=name,
                        sql_type="TEXT",
                        description="Drain3-mined log template."
                        if name == "template"
                        else "Hash identifying the Drain3 template cluster.",
                    )
                )
                seen.add(name)

        for key, count in key_counts.items():
            if key in seen or count < threshold:
                continue

            sql_type = "TEXT"
            examples = key_examples.get(key, [])
            if key in MEASUREMENT_FIELD_NAMES or _all_numeric(examples):
                sql_type = "REAL"

            columns.append(
                ColumnDefinition(
                    name=key,
                    sql_type=sql_type,
                    description=f"Extracted from unstructured text ({count}/{len(all_fields)} records).",
                )
            )
            seen.add(key)

        return columns

    @staticmethod
    def _llm_enrich_columns(
        clusters: list[tuple[int, int, str]],
        heuristic_columns: list[ColumnDefinition],
        filename: str,
        warnings: list[str],
    ) -> list[ColumnDefinition]:
        legacy_columns: list[InferredColumn] = []
        for column in heuristic_columns:
            sql_type = SqlType.TEXT
            if column.sql_type.upper() == "REAL":
                sql_type = SqlType.REAL
            elif column.sql_type.upper() == "INTEGER":
                sql_type = SqlType.INTEGER

            legacy_columns.append(
                InferredColumn(
                    name=column.name,
                    sql_type=sql_type,
                    description=column.description,
                    nullable=column.nullable,
                    kind=ColumnKind.DETECTED,
                )
            )

        sample_lines = [text[: _up.MAX_LINE_LENGTH] for _, _, text in clusters[: _up.MAX_SAMPLE_LINES]]

        try:
            llm_result = _up.call_llm_for_unstructured(sample_lines, legacy_columns)
        except Exception as error:  # noqa: BLE001
            logger.warning("LLM enrichment failed for '%s': %s", filename, error)
            warnings.append(f"LLM enrichment failed: {error}")
            return []

        if llm_result.warnings:
            warnings.extend(llm_result.warnings)

        new_columns: list[ColumnDefinition] = []
        existing_names = {column.name for column in heuristic_columns} | BASELINE_COLUMN_NAMES

        for llm_field in llm_result.fields:
            safe_name = re.sub(r"[^a-z0-9_]", "_", llm_field.name.lower()).strip("_")
            if not safe_name or safe_name in existing_names:
                continue

            sql_type = "TEXT"
            if llm_field.sql_type.upper() in {"INTEGER", "INT"}:
                sql_type = "INTEGER"
            elif llm_field.sql_type.upper() in {"REAL", "FLOAT", "DOUBLE"}:
                sql_type = "REAL"

            new_columns.append(
                ColumnDefinition(
                    name=safe_name,
                    sql_type=sql_type,
                    description=llm_field.description or f"LLM-inferred from {filename}.",
                )
            )
            existing_names.add(safe_name)

        if new_columns:
            logger.info(
                "LLM enrichment added %d columns for '%s': %s",
                len(new_columns),
                filename,
                ", ".join(column.name for column in new_columns),
            )

        return new_columns


_HEARTBEAT_FREQUENCY_RATIO = 0.40
_ACTIONABLE_LEVELS = frozenset({"WARN", "WARNING", "ERROR", "FATAL", "CRITICAL", "ALERT", "EMERG", "EMERGENCY"})


def _suppress_heartbeats(
    rows: list[dict[str, Any]],
    total_clusters: int,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], int]:
    if not rows or total_clusters < 5:
        return rows, 0

    template_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        template = row.get("template", "")
        template_groups.setdefault(template, []).append(row)

    suppressed_templates: set[str] = set()
    for template, group in template_groups.items():
        frequency = len(group) / total_clusters
        if frequency < _HEARTBEAT_FREQUENCY_RATIO:
            continue

        has_actionable = False
        for row in group:
            level = (row.get("log_level") or "").upper()
            if level in _ACTIONABLE_LEVELS:
                has_actionable = True
                break
            if any(key in MEASUREMENT_FIELD_NAMES for key in row if key not in BASELINE_COLUMN_NAMES):
                has_actionable = True
                break

        if not has_actionable:
            suppressed_templates.add(template)

    if not suppressed_templates:
        return rows, 0

    filtered = [row for row in rows if row.get("template", "") not in suppressed_templates]
    suppressed_count = len(rows) - len(filtered)

    if suppressed_count > 0:
        warnings.append(
            f"Suppressed {suppressed_count} heartbeat/noise rows "
            f"({len(suppressed_templates)} template(s) exceeding "
            f"{_HEARTBEAT_FREQUENCY_RATIO:.0%} frequency threshold)."
        )

    return filtered, suppressed_count


def _all_numeric(examples: list[str]) -> bool:
    if not examples:
        return False
    for value in examples:
        try:
            float(value)
        except (ValueError, TypeError):
            return False
    return True
