from __future__ import annotations

import json
import logging
import re
from typing import Any

from parsers.contracts import AiColumnPlan, AiExtractionBatch, AiSchemaPlan

logger = logging.getLogger(__name__)

# Prompt version for tracking changes in extraction prompts
PROMPT_VERSION = "2.0.0"


class LlmEngine:
    """AI engine for format-agnostic log schema discovery and extraction."""

    def __init__(self) -> None:
        self._model = None
        self._prompt_version = PROMPT_VERSION

    # ── Lazy-load the LLM model ───────────────────────────────────────

    def _get_model(self):
        if self._model is None:
            from lib.ai import get_generative_model

            self._model = get_generative_model()
        return self._model

    # ── Schema discovery ──────────────────────────────────────────────

    def discover_schema(
        self,
        content: str,
        filename: str = "",
    ) -> AiSchemaPlan | None:
        """Ask the LLM to propose a table schema for the given log content.

        Returns an *AiSchemaPlan* describing suggested columns, their types,
        and an overall confidence score, or *None* if the LLM is unavailable.
        """
        model = self._get_model()
        if model is None:
            return None

        sample = content[:6000]
        prompt = (
            "You are a log schema analyzer. Below is a sample from a log file. "
            "Analyze the data and propose a table schema (column names, types, descriptions) "
            "that best represents the structured information contained in these records.\n\n"
            "Rules:\n"
            "- Use descriptive column names (snake_case, no spaces)\n"
            "- Use appropriate SQL types: TEXT, INTEGER, FLOAT, BOOLEAN, DATETIME\n"
            "- Add a brief description for each column explaining what it represents\n"
            "- Set confidence to reflect how reliably you think this schema fits\n"
            "- If you cannot detect any structure, return confidence < 0.3\n\n"
            f"Filename: {filename}\n"
            f"Content sample:\n{sample}"
        )

        try:
            plan = model.generate_structured(prompt, AiSchemaPlan)
            return plan
        except Exception as e:
            logger.warning("AI schema discovery failed: %s", e)
            return None

    # ── Row extraction ────────────────────────────────────────────────

    def extract_rows(
        self,
        chunk_text: str,
        columns: list[AiColumnPlan],
        filename: str = "",
    ) -> AiExtractionBatch | None:
        """Extract rows from a text chunk matching the given column schema.

        Returns an *AiExtractionBatch* with parsed rows, or *None* on failure.
        """
        model = self._get_model()
        if model is None:
            return None

        col_desc = "\n".join(
            f"  - {c.name} ({c.type}): {c.description or '(no description)'}"
            for c in columns
        )

        prompt = (
            "Extract structured records from this log text chunk. "
            "Return a JSON object with keys 'rows' (list of objects) and 'confidence' (0-1).\n\n"
            "Schema (extract these fields if present):\n"
            f"{col_desc}\n\n"
            "Rules:\n"
            "- Every row must be a flat dict with keys matching the column names above\n"
            "- Use null for missing values (do not omit keys)\n"
            "- Coerce values to match the expected SQL type (e.g. numeric strings -> numbers, "
            "'true'/'false' -> booleans, ISO timestamps -> strings)\n"
            "- If the chunk does not contain any records matching the schema, "
            "return an empty rows list with a warning\n"
            "- Set confidence appropriately per batch\n\n"
            f"Source file: {filename}\n"
            f"Log chunk:\n{chunk_text}"
        )

        try:
            result = model.generate_structured(prompt, AiExtractionBatch)
            return result
        except Exception as e:
            logger.warning("AI batch extraction failed: %s", e)
            return None

    # ── Row repair ────────────────────────────────────────────────────

    def repair_extraction(
        self,
        invalid_rows: list[dict[str, Any]],
        columns: list[AiColumnPlan],
    ) -> list[dict[str, Any]]:
        """Ask the LLM to repair malformed rows.

        Returns a list of repaired rows (may be empty, or fewer than input).
        """
        model = self._get_model()
        if model is None:
            return invalid_rows

        col_desc = "\n".join(
            f"  - {c.name} ({c.type}): {c.description or ''}" for c in columns
        )

        prompt = (
            "The following rows failed validation against the expected schema. "
            "Repair them so each row is a valid flat dict with keys matching the column names.\n\n"
            f"Expected schema:\n{col_desc}\n\n"
            f"Invalid rows:\n{json.dumps(invalid_rows, indent=2, default=str)}\n\n"
            "Return ONLY a JSON array of repaired row objects. "
            "If a row cannot be repaired, omit it from the output."
        )

        try:
            response = model.generate(prompt)
            repaired = _extract_json_array(response)
            if repaired is None:
                logger.warning("Could not parse repair output, returning original rows")
                return invalid_rows
            return repaired
        except Exception as e:
            logger.warning("AI row repair failed: %s", e)
            return invalid_rows

    # ── Schema discovery from structured records ─────────────────────

    def discover_schema_from_records(
        self,
        records: list[dict[str, Any]],
        filename: str = "",
    ) -> AiSchemaPlan | None:
        """Ask the LLM to propose a compact schema from structured records.

        Unlike *discover_schema* (which takes raw text), this method sends
        pre-parsed record dictionaries, so the LLM can focus on semantic
        column inference rather than parsing the format.

        Returns an *AiSchemaPlan* or *None*.
        """
        model = self._get_model()
        if model is None:
            return None

        # Serialize a few records for the prompt
        sample_records = records[:8]
        sample_json = json.dumps(sample_records, indent=2, default=str, ensure_ascii=False)[:7000]

        prompt = (
            "You are a log schema analyzer. Below are structured records from a log file. "
            "Analyze the fields and propose a TABLE SCHEMA that is COMPACT and useful for querying.\n\n"
            "Rules:\n"
            "- Use descriptive snake_case column names\n"
            "- Use appropriate SQL types: TEXT, INTEGER, FLOAT, BOOLEAN, DATETIME\n"
            "- Include an 'extra' column (type TEXT) for any fields that appear in "
            "fewer than half of the records or are event-specific details\n"
            "- Do NOT create columns for fields that are mostly null or very sparse\n"
            "- Prioritize columns that are shared across many records (e.g. timestamp, "
            "service, function_name, log_level, request_id, event_type)\n"
            "- Put event-specific details (e.g. bucket, key, rows, cols, table, "
            "exception, stack_trace, duration_ms, memory_size, sheets, notes) "
            "into the 'extra' JSON column\n"
            "- Set confidence to reflect how reliably you think this schema fits\n"
            "- If you cannot detect any structure, return confidence < 0.3\n\n"
            f"Filename: {filename}\n"
            f"Sample records ({len(sample_records)} shown):\n"
            f"{sample_json}"
        )

        try:
            plan = model.generate_structured(prompt, AiSchemaPlan)
            return plan
        except Exception as e:
            logger.warning("AI schema from records failed: %s", e)
            return None

    # ── Row extraction from structured records ───────────────────────

    def extract_rows_from_records(
        self,
        records: list[dict[str, Any]],
        columns: list[AiColumnPlan],
        filename: str = "",
    ) -> AiExtractionBatch | None:
        """Extract rows from structured records, one output row per input record.

        Returns an *AiExtractionBatch* or *None* on failure.
        """
        model = self._get_model()
        if model is None:
            return None

        col_desc = "\n".join(
            f"  - {c.name} ({c.type}): {c.description or '(no description)'}"
            for c in columns
        )

        records_json = json.dumps(records, indent=2, default=str, ensure_ascii=False)[:10000]

        prompt = (
            "Extract one structured row per input record below. "
            "Return a JSON object with keys: 'rows' (list of objects), 'confidence' (0-1).\n\n"
            "Output schema (extract these fields if present; use null for missing values):\n"
            f"{col_desc}\n\n"
            "Rules:\n"
            "- You MUST produce exactly the same number of output rows as input records\n"
            "- Use null for missing values (do not omit keys)\n"
            "- Coerce values to match the expected SQL type\n"
            "- For the 'extra' column: put ALL fields that do not have their own "
            "dedicated column into a JSON object. This includes fields like bucket, key, "
            "rows, cols, table, sheets, notes, header_row, skip_footer, exception, "
            "stack_trace, duration_ms, billed_duration_ms, memory_size_mb, "
            "max_memory_used_mb, segment_id, sampled, dataset_id, exception_name, "
            "json fields, etc.\n"
            "- Preserve source values from the input records exactly\n"
            "- Set confidence appropriately per batch\n\n"
            f"Source file: {filename}\n"
            f"Input records ({len(records)} total):\n{records_json}"
        )

        try:
            result = model.generate_structured(prompt, AiExtractionBatch)
            return result
        except Exception as e:
            logger.warning("AI record extraction failed: %s", e)
            return None

    def detect_format(self, sample_lines: list[str]) -> dict[str, Any] | None:
        """Detect format using LLM. Returns format name and confidence."""
        model = self._get_model()
        if model is None:
            return None

        sample_text = "\n".join(sample_lines[:20])
        prompt = (
            "Analyze these log lines and detect the format. "
            "Return ONLY a JSON object with keys: 'format', 'confidence' (0-1), 'reason'."
            "\n\n"
            f"Log lines:\n{sample_text}"
        )

        try:
            response = model.generate(prompt)
            result = _extract_json_object(response)
            if result and "format" in result:
                return result
            return None
        except Exception as e:
            logger.warning("LLM format detection failed: %s", e)
            return None

    def parse_with_llm(self, content: str, filename: str) -> dict[str, Any] | None:
        """Parse content using LLM. Returns columns and rows."""
        model = self._get_model()
        if model is None:
            return None

        limited_content = content[:10000]
        prompt = (
            "Parse these log lines and extract structured data. "
            "Return ONLY a JSON object with keys: 'columns' (list of {name, type}), "
            "'rows' (list of objects), 'confidence' (0-1). "
            "Types should be: TEXT, INTEGER, FLOAT, BOOLEAN, DATETIME."
            "\n\n"
            f"Log lines:\n{limited_content}"
        )

        try:
            response = model.generate(prompt)
            result = _extract_json_object(response)
            if result and "rows" in result and "columns" in result:
                return result
            return None
        except Exception as e:
            logger.warning("LLM parsing failed: %s", e)
            return None

    def advise_parse_strategy(
        self,
        sample_records: list[dict[str, str]],
        filename: str = "",
    ) -> dict[str, Any] | None:
        """Ask the LLM what columns a group of log records should have."""
        model = self._get_model()
        if model is None:
            return None

        sample_texts = [rec.get("raw", rec.get("message", ""))[:800] for rec in sample_records[:5]]
        combined = "\n\n---\n\n".join(sample_texts)

        prompt = (
            "You are a log schema advisor. Below are event records from a tool machine log.\n"
            "Analyze the fields present and return **only** a JSON object with:\n"
            "- 'columns': list of {name, type, description}\n"
            "- 'confidence': 0-1 for how well the columns describe the data\n"
            "- 'record_type': short label like 'machine_warning' or 'tool_event'\n\n"
            "Use types: TEXT, INTEGER, FLOAT, BOOLEAN, DATETIME\n\n"
            "Example response format:\n"
            '{"columns":[{"name":"machine","type":"TEXT","description":"Machine ID"},{"name":"event_code","type":"TEXT","description":"Event code"}],"confidence":0.9,"record_type":"tool_event"}'
            "\n\n"
            f"Event records:\n{combined}"
        )

        try:
            response = model.generate(prompt)
            result = _extract_json_object(response)
            if result and "columns" in result:
                return result
            return None
        except Exception as e:
            logger.warning("LLM schema advisor failed: %s", e)
            return None


# ── JSON extraction helpers ────────────────────────────────────────────────


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Extract the first JSON object from a string."""
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        try:
            result = json.loads(m.group())
            if isinstance(result, dict):
                return result
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def _extract_json_array(text: str) -> list[Any] | None:
    """Extract the first JSON array from a string."""
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    m = re.search(r"\[[\s\S]*\]", text)
    if m:
        try:
            result = json.loads(m.group())
            if isinstance(result, list):
                return result
        except (json.JSONDecodeError, TypeError):
            pass
    return None
