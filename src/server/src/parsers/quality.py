from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_NULL_THRESHOLD = 0.3


@dataclass
class TableQualityReport:
    table_name: str
    required_columns: list[str]
    optional_columns: list[str]
    row_count: int
    column_null_ratios: dict[str, float]
    table_null_ratio: float
    warnings: list[str]
    failed: bool


@dataclass
class ParseQualityReport:
    table_reports: dict[str, TableQualityReport]
    validation_warnings: list[str]
    failed_tables: list[str]
    should_fallback: bool
    confidence_penalty: float


def evaluate_structured_parse_quality(
    records_by_table: dict[str, list[dict[str, Any]]],
    required_columns_by_table: dict[str, list[str]],
    optional_columns_by_table: dict[str, list[str]],
    traceability_fields: set[str] | None = None,
) -> ParseQualityReport:
    table_reports: dict[str, TableQualityReport] = {}
    validation_warnings: list[str] = []
    failed_tables: list[str] = []
    cumulative_penalty = 0.0
    trace_fields = traceability_fields or set()

    for table_name, rows in records_by_table.items():
        required_columns = required_columns_by_table.get(table_name, [])
        optional_columns = optional_columns_by_table.get(table_name, [])
        column_null_ratios = _compute_column_null_ratios(rows)
        table_null_ratio = _compute_table_null_ratio(rows, required_columns)

        table_warnings: list[str] = []
        table_failed = False

        if _detect_header_row_as_data(rows, required_columns):
            table_warnings.append("Detected header row emitted as data.")
            table_failed = True

        if _detect_xml_tag_only_rows(rows):
            table_warnings.append("Detected XML tag-only rows in structured output.")
            table_failed = True

        if _detect_declaration_rows(rows):
            table_warnings.append("Detected XML declaration-only rows in structured output.")
            table_failed = True

        duplicate_ratio = _detect_duplicate_raw_message(rows)
        if duplicate_ratio >= 0.8:
            table_warnings.append(
                f"Detected duplicated raw/message content ratio {duplicate_ratio:.2f} in structured output."
            )
            table_failed = True

        if required_columns:
            required_null_rates = [column_null_ratios.get(column, 1.0) for column in required_columns]
            if required_null_rates:
                max_required_null = max(required_null_rates)
                avg_required_null = sum(required_null_rates) / len(required_null_rates)
                if max_required_null > _NULL_THRESHOLD or avg_required_null > _NULL_THRESHOLD:
                    table_warnings.append(
                        "Required fields exceeded null threshold "
                        f"(max={max_required_null:.2f}, avg={avg_required_null:.2f})."
                    )
                    table_failed = True
                    cumulative_penalty += min(0.7, avg_required_null + 0.2)
                else:
                    cumulative_penalty += avg_required_null * 0.4

        structured_null_ratio = _detect_traceability_masking(rows, required_columns, trace_fields)
        if structured_null_ratio > _NULL_THRESHOLD:
            table_warnings.append(
                "Structured columns are mostly null while traceability fields are populated "
                f"(ratio={structured_null_ratio:.2f})."
            )
            table_failed = True
            cumulative_penalty += 0.6

        if table_failed:
            failed_tables.append(table_name)

        validation_warnings.extend(f"{table_name}: {warning}" for warning in table_warnings)
        table_reports[table_name] = TableQualityReport(
            table_name=table_name,
            required_columns=required_columns,
            optional_columns=optional_columns,
            row_count=len(rows),
            column_null_ratios=column_null_ratios,
            table_null_ratio=table_null_ratio,
            warnings=table_warnings,
            failed=table_failed,
        )

    should_fallback = bool(failed_tables)
    confidence_penalty = min(0.95, cumulative_penalty)
    return ParseQualityReport(
        table_reports=table_reports,
        validation_warnings=validation_warnings,
        failed_tables=failed_tables,
        should_fallback=should_fallback,
        confidence_penalty=confidence_penalty,
    )


def _compute_column_null_ratios(rows: list[dict[str, Any]]) -> dict[str, float]:
    if not rows:
        return {}

    all_columns: set[str] = set()
    for row in rows:
        all_columns.update(row.keys())

    ratios: dict[str, float] = {}
    for column in sorted(all_columns):
        null_count = 0
        for row in rows:
            value = row.get(column)
            if value is None:
                null_count += 1
        ratios[column] = round(null_count / len(rows), 3)

    return ratios


def _compute_table_null_ratio(rows: list[dict[str, Any]], required_columns: list[str]) -> float:
    if not rows:
        return 1.0
    if not required_columns:
        return 0.0

    missing = 0
    total = 0
    for row in rows:
        for column in required_columns:
            total += 1
            if row.get(column) is None:
                missing += 1

    if total == 0:
        return 0.0
    return round(missing / total, 3)


def _detect_header_row_as_data(rows: list[dict[str, Any]], required_columns: list[str]) -> bool:
    if not rows or not required_columns:
        return False

    first_row = rows[0]
    comparable_columns = [column for column in required_columns if column in first_row]
    if not comparable_columns:
        return False

    matches = 0
    for column in comparable_columns:
        value = first_row.get(column)
        if isinstance(value, str) and value.strip().lower() == column.lower():
            matches += 1

    return matches / len(comparable_columns) >= 0.7


def _detect_xml_tag_only_rows(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        for value in row.values():
            if not isinstance(value, str):
                continue
            stripped = value.strip()
            if stripped.startswith("</") and stripped.endswith(">"):
                return True
            if stripped in {"</step>", "</recipe>", "<step>", "<recipe>"}:
                return True
    return False


def _detect_declaration_rows(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        for value in row.values():
            if isinstance(value, str) and value.strip().startswith("<?xml"):
                return True
    return False


def _detect_duplicate_raw_message(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0

    duplicate_count = 0
    comparable_count = 0
    for row in rows:
        raw = row.get("raw")
        message = row.get("message")
        if not isinstance(raw, str) or not isinstance(message, str):
            continue
        comparable_count += 1
        if raw.strip() == message.strip():
            duplicate_count += 1

    if comparable_count == 0:
        return 0.0
    return duplicate_count / comparable_count


def _detect_traceability_masking(
    rows: list[dict[str, Any]], required_columns: list[str], traceability_fields: set[str]
) -> float:
    if not rows or not required_columns or not traceability_fields:
        return 0.0

    masked = 0
    for row in rows:
        structured_missing = all(row.get(column) is None for column in required_columns)
        traceability_populated = any(row.get(field) not in {None, ""} for field in traceability_fields)
        if structured_missing and traceability_populated:
            masked += 1

    return masked / len(rows)
