"""AI-powered schema enrichment for structured log formats.

Uses LLM to detect and classify columns in structured data (JSON, XML, CSV, etc.)
beyond what heuristic-only inference can achieve.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from lib import ai
from lib.parsers.contracts import ColumnDefinition
from lib.parsers.structured.type_inference import (
    BASELINE_COLUMN_NAMES,
    SqlType,
    infer_type,
)

logger = logging.getLogger(__name__)

MAX_SAMPLE_LINES = 50
MAX_TOTAL_CHARS = 8000


def has_openrouter_api_key(api_key: str | None = None) -> bool:
    """Check if OpenRouter API key is available."""
    return ai.has_openrouter_api_key(api_key)


def _build_sample_content(
    lines: list[str],
    detected_format: str,
    max_chars: int = MAX_TOTAL_CHARS,
) -> str:
    """Build a content sample for LLM analysis."""
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
    """Call the LLM to enrich the schema with semantic understanding for structured data.

    Args:
        detected_format: The detected format (json, xml, csv, etc.)
        sample_lines: Sample lines from the file
        heuristic_columns: Columns already detected by heuristics
        heuristic_summary: String summary of heuristic columns
        api_key: Optional API key override
        model: Optional model override

    Returns:
        LlmStructuredSchemaResponse with LLM-inferred columns
    """
    if not has_openrouter_api_key(api_key):
        return ai.LlmStructuredSchemaResponse(
            warnings=["OPENROUTER_API_KEY not set; LLM enrichment skipped."],
        )

    sample_text = _build_sample_content(sample_lines, detected_format)
    if not sample_text.strip():
        return ai.LlmStructuredSchemaResponse(
            warnings=["No sample content available for LLM analysis."],
        )

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
        warnings=[invocation.warning] if invocation.warning else ["LLM enrichment returned no response."],
    )


def _reconcile_columns(
    heuristic_columns: list[ColumnDefinition],
    llm_columns: list[ai.LlmStructuredColumn],
) -> list[ColumnDefinition]:
    """Reconcile heuristic and LLM columns, preferring LLM where there's overlap.

    Args:
        heuristic_columns: Columns detected by heuristics
        llm_columns: Columns suggested by LLM

    Returns:
        Merged list of ColumnDefinitions
    """
    heuristic_names = {col.name for col in heuristic_columns}
    result = list(heuristic_columns)
    seen_names: set[str] = heuristic_names

    for llm_col in llm_columns:
        safe_name = _sanitize(llm_col.name)
        if safe_name in seen_names:
            continue
        if safe_name in BASELINE_COLUMN_NAMES:
            continue

        sql_type = SqlType.TEXT
        if llm_col.sql_type.upper() in ("INTEGER", "INT"):
            sql_type = SqlType.INTEGER
        elif llm_col.sql_type.upper() in ("REAL", "FLOAT", "DOUBLE"):
            sql_type = SqlType.REAL

        result.append(
            ColumnDefinition(
                name=safe_name,
                sql_type=sql_type.value,
                description=llm_col.description or f"Column inferred by LLM from structured data.",
                nullable=llm_col.nullable,
            )
        )
        seen_names.add(safe_name)

    return result


def _sanitize(name: str) -> str:
    """Sanitize a column name to be a valid SQLite identifier."""
    import re

    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s or s[0].isdigit():
        s = "col_" + s
    return s or "unknown"


def enrich_structured_schema(
    records: list[dict[str, Any]],
    detected_format: str,
    sample_lines: list[str],
    use_llm: bool = True,
    api_key: str | None = None,
    model: str | None = None,
) -> tuple[list[ColumnDefinition], list[str]]:
    """Enrich column schema for structured data using heuristics and optionally LLM.

    Args:
        records: Parsed records from the file
        detected_format: The format type (json, xml, csv, etc.)
        sample_lines: Raw sample lines for LLM analysis
        use_llm: Whether to use LLM enrichment (default True)
        api_key: Optional API key override
        model: Optional model override

    Returns:
        Tuple of (enriched_columns, warnings)
    """
    from lib.parsers.structured.type_inference import (
        infer_columns_from_records,
        SqlType,
    )

    warnings: list[str] = []

    if not records:
        return [], warnings

    heuristic_results = infer_columns_from_records(records)
    heuristic_columns: list[ColumnDefinition] = []
    for col_name, sql_type, semantic_type, confidence, examples in heuristic_results:
        heuristic_columns.append(
            ColumnDefinition(
                name=col_name,
                sql_type=sql_type.value,
                description=f"{semantic_type.value}: inferred from {len(records)} records (confidence: {confidence:.2f})",
                nullable=True,
            )
        )

    if not use_llm or not has_openrouter_api_key(api_key):
        return heuristic_columns, warnings

    heuristic_summary = (
        "\n".join(f"  - {col.name} ({col.sql_type}): {col.description}" for col in heuristic_columns)
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

    if llm_result.event_type_hint:
        for col in enriched:
            if col.name == "event_type" and not col.description:
                col.description = f"Event type hint from LLM: {llm_result.event_type_hint}"
                break

    return enriched, warnings
