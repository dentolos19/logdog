from __future__ import annotations

import logging
import re
from typing import Any

import src.parsers.ai_wrappers as ai
from src.parsers.contracts import ColumnDefinition
from src.parsers.structured.type_inference import BASELINE_COLUMN_NAMES, SqlType, infer_columns_from_records

logger = logging.getLogger(__name__)

MAX_SAMPLE_LINES = 50
MAX_TOTAL_CHARS = 8000


def has_openrouter_api_key(api_key: str | None = None) -> bool:
    return ai.has_openrouter_api_key(api_key)


def _build_sample_content(lines: list[str], detected_format: str, max_chars: int = MAX_TOTAL_CHARS) -> str:
    if detected_format == "xml":
        return "\n".join(lines[:30])
    if detected_format == "csv":
        return "\n".join(lines[:20])
    return "\n".join(lines[:MAX_SAMPLE_LINES])[:max_chars]


def _call_llm_for_structured_schema(
    detected_format: str,
    sample_lines: list[str],
    heuristic_columns: list[ColumnDefinition],
    heuristic_summary: str,
    api_key: str | None = None,
    model: str | None = None,
) -> ai.LlmStructuredSchemaResponse:
    if not has_openrouter_api_key(api_key):
        return ai.LlmStructuredSchemaResponse(warnings=["OPENROUTER_API_KEY not set; LLM enrichment skipped."])

    sample_text = _build_sample_content(sample_lines, detected_format)
    if not sample_text.strip():
        return ai.LlmStructuredSchemaResponse(warnings=["No sample content available for LLM analysis."])

    invocation = ai.infer_structured_schema(
        detected_format=detected_format,
        sample_text=sample_text,
        sample_line_count=min(len(sample_lines), MAX_SAMPLE_LINES),
        heuristic_summary=heuristic_summary,
        model=model,
        api_key=api_key,
    )
    if invocation.response is not None:
        return invocation.response

    return ai.LlmStructuredSchemaResponse(
        warnings=[invocation.warning] if invocation.warning else ["LLM enrichment returned no response."]
    )


def _reconcile_columns(
    heuristic_columns: list[ColumnDefinition],
    llm_columns: list[ai.LlmStructuredColumn],
) -> list[ColumnDefinition]:
    result = list(heuristic_columns)
    seen_names = {column.name for column in heuristic_columns}

    for llm_column in llm_columns:
        safe_name = _sanitize(llm_column.name)
        if safe_name in seen_names or safe_name in BASELINE_COLUMN_NAMES:
            continue

        sql_type = SqlType.TEXT
        if llm_column.sql_type.upper() in {"INTEGER", "INT"}:
            sql_type = SqlType.INTEGER
        elif llm_column.sql_type.upper() in {"REAL", "FLOAT", "DOUBLE"}:
            sql_type = SqlType.REAL

        result.append(
            ColumnDefinition(
                name=safe_name,
                sql_type=sql_type.value,
                description=llm_column.description or "Column inferred by LLM from structured data.",
                nullable=llm_column.nullable,
            )
        )
        seen_names.add(safe_name)

    return result


def _sanitize(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
    if not sanitized or sanitized[0].isdigit():
        sanitized = "col_" + sanitized
    return sanitized or "unknown"


def enrich_structured_schema(
    records: list[dict[str, Any]],
    detected_format: str,
    sample_lines: list[str],
    use_llm: bool = True,
    api_key: str | None = None,
    model: str | None = None,
) -> tuple[list[ColumnDefinition], list[str]]:
    warnings: list[str] = []
    if not records:
        return [], warnings

    heuristic_results = infer_columns_from_records(records)
    heuristic_columns: list[ColumnDefinition] = []
    for column_name, sql_type, semantic_type, confidence, _examples in heuristic_results:
        heuristic_columns.append(
            ColumnDefinition(
                name=column_name,
                sql_type=sql_type.value,
                description=f"{semantic_type.value}: inferred from {len(records)} records (confidence: {confidence:.2f})",
                nullable=True,
            )
        )

    if not use_llm or not has_openrouter_api_key(api_key):
        return heuristic_columns, warnings

    heuristic_summary = (
        "\n".join(f"  - {column.name} ({column.sql_type}): {column.description}" for column in heuristic_columns)
        or "  (none detected)"
    )

    llm_result = _call_llm_for_structured_schema(
        detected_format=detected_format,
        sample_lines=sample_lines,
        heuristic_columns=heuristic_columns,
        heuristic_summary=heuristic_summary,
        api_key=api_key,
        model=model,
    )

    if llm_result.warnings:
        warnings.extend(llm_result.warnings)

    if not llm_result.columns:
        return heuristic_columns, warnings

    enriched = _reconcile_columns(heuristic_columns, llm_result.columns)
    return enriched, warnings
