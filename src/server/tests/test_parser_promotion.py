"""Tests for promoting common fields from the ``extra`` JSON column.

Verifies:
  - Fields in COMMON_COLUMNS are promoted from extra to top-level row keys.
  - Promoted keys are removed from extra to avoid duplication.
  - Non-common fields stay in extra.
  - Rows without extra are unchanged.
  - Malformed extra values are left alone.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.engine import _promote_common_fields_from_extra


# ── Basic promotion ─────────────────────────────────────────────────────────


class TestPromoteCommonFields:
    def test_promote_single_key(self):
        """A single COMMON_COLUMNS key in extra is promoted."""

        row = {
            "id": "r1",
            "message": "done",
            "extra": '{"cold_start": false, "bucket": "my-bucket"}',
        }
        _promote_common_fields_from_extra(row)

        assert row.get("cold_start") is False
        extra = json.loads(row["extra"])
        assert "cold_start" not in extra
        assert extra.get("bucket") == "my-bucket"

    def test_promote_multiple_keys(self):
        """Multiple common keys are promoted in one pass."""

        row = {
            "id": "r2",
            "extra": json.dumps(
                {
                    "cold_start": True,
                    "function_memory_size": "1024",
                    "function_arn": "arn:aws:lambda:us-east-1:123:function:my-func",
                    "xray_trace_id": "1-abc123",
                    "bucket": "data-bucket",
                    "key": "file.csv",
                }
            ),
        }
        _promote_common_fields_from_extra(row)

        assert row.get("cold_start") is True
        assert row.get("function_memory_size") == "1024"
        assert row.get("function_arn") == "arn:aws:lambda:us-east-1:123:function:my-func"
        assert row.get("xray_trace_id") == "1-abc123"

        extra = json.loads(row["extra"])
        assert "cold_start" not in extra
        assert "function_memory_size" not in extra
        assert "function_arn" not in extra
        assert "xray_trace_id" not in extra
        assert extra.get("bucket") == "data-bucket"
        assert extra.get("key") == "file.csv"

    def test_does_not_overwrite_existing_value(self):
        """If the row already has a value for a key, extra is not promoted."""

        row = {
            "id": "r3",
            "cold_start": "existing_value",
            "extra": '{"cold_start": false, "bucket": "b"}',
        }
        _promote_common_fields_from_extra(row)

        assert row["cold_start"] == "existing_value"  # unchanged
        extra = json.loads(row["extra"])
        # The key should still be in extra since it wasn't promoted
        assert "cold_start" in extra
        assert extra["cold_start"] is False

    def test_extra_becomes_empty_dict(self):
        """When all keys are promoted, extra becomes '{}'."""

        row = {
            "id": "r4",
            "extra": '{"cold_start": false}',
        }
        _promote_common_fields_from_extra(row)

        assert row.get("cold_start") is False
        assert row["extra"] == "{}"

    def test_no_extra_field(self):
        """Row without 'extra' is unchanged."""

        row = {"id": "r5", "message": "hello"}
        _promote_common_fields_from_extra(row)
        assert row == {"id": "r5", "message": "hello"}

    def test_extra_is_none(self):
        """Row with extra=None is unchanged."""

        row = {"id": "r6", "extra": None, "message": "hi"}
        _promote_common_fields_from_extra(row)
        assert row["extra"] is None

    def test_extra_malformed_string(self):
        """Malformed JSON in extra is left untouched."""

        row = {"id": "r7", "extra": "not-json-at-all"}
        _promote_common_fields_from_extra(row)
        assert row["extra"] == "not-json-at-all"

    def test_extra_already_dict(self):
        """When extra is already a dict, promotion still works."""

        row = {
            "id": "r8",
            "extra": {"cold_start": True, "bucket": "b"},
        }
        _promote_common_fields_from_extra(row)

        assert row.get("cold_start") is True
        extra = row["extra"]
        assert isinstance(extra, str)  # should have been re-serialized
        extra_d = json.loads(extra)
        assert "cold_start" not in extra_d
        assert extra_d.get("bucket") == "b"

    def test_promote_from_extra_with_non_common_keys_present(self):
        """Non-common fields remain undisturbed after promotion."""

        row = {
            "id": "r9",
            "extra": json.dumps(
                {
                    "cold_start": False,
                    "function_memory_size": "512",
                    "function_arn": "arn:aws:lambda:us-east-1:1:function:fn",
                    "function_request_id": "req-001",
                    "xray_trace_id": "1-xxx",
                    "service": "my-service",
                    "function_name": "my-func",
                    "level": "INFO",
                    "location": "handler:10",
                    "bucket": "bkt",
                    "key": "obj.csv",
                    "rows": 100,
                    "cols": 10,
                    "exception": "error!",
                    "stack_trace": "Traceback...",
                    "datasetId": "uuid",
                }
            ),
        }
        _promote_common_fields_from_extra(row)

        # Common fields promoted
        assert row.get("cold_start") is False
        assert row.get("function_memory_size") == "512"
        assert row.get("function_arn") == "arn:aws:lambda:us-east-1:1:function:fn"
        assert row.get("function_request_id") == "req-001"
        assert row.get("xray_trace_id") == "1-xxx"
        assert row.get("service") == "my-service"
        assert row.get("function_name") == "my-func"

        extra = json.loads(row["extra"])
        # Event-specific fields remain
        assert extra.get("bucket") == "bkt"
        assert extra.get("key") == "obj.csv"
        assert extra.get("rows") == 100
        assert extra.get("cols") == 10
        assert extra.get("exception") == "error!"
        assert extra.get("stack_trace") == "Traceback..."
        assert extra.get("datasetId") == "uuid"

        # Promoted fields removed from extra
        assert "cold_start" not in extra
        assert "function_memory_size" not in extra
        assert "function_arn" not in extra
        assert "function_request_id" not in extra
        assert "xray_trace_id" not in extra
        assert "service" not in extra
        assert "function_name" not in extra

    def test_extra_empty_dict(self):
        """Empty extra dict is unchanged."""

        row = {"id": "r10", "extra": "{}"}
        _promote_common_fields_from_extra(row)
        assert row["extra"] == "{}"

    def test_extra_non_dict_json(self):
        """JSON array in extra is left untouched."""

        row = {"id": "r11", "extra": '["a", "b"]'}
        _promote_common_fields_from_extra(row)
        assert row["extra"] == '["a", "b"]'


# ── Integration test: lambda.csv sample ─────────────────────────────────────

PROMOTED_LAMBDA_COLUMNS = frozenset(
    {
        "cold_start",
        "function_memory_size",
        "function_arn",
        "function_request_id",
        "xray_trace_id",
    }
)


def test_lambda_sample_promotes_common_fields():
    """Parsing lambda.csv should produce dedicated columns for common Lambda
    runtime metadata instead of burying them in ``extra``."""
    import json as _json

    samples_dir = Path(__file__).resolve().parent.parent.parent.parent / "samples"
    sample_path = samples_dir / "lambda.csv"
    if not sample_path.exists():
        pytest.skip("samples/lambda.csv not found — skipping integration test")

    from parsers.preprocessor import FileInput, LogPreprocessorService
    from parsers.registry import ParserRegistry

    raw_bytes = sample_path.read_bytes()
    content = raw_bytes.decode("utf-8")

    file_input = FileInput(
        filename="lambda.csv",
        content=content,
        raw_bytes=None,
        is_binary=False,
        byte_length=len(raw_bytes),
    )

    preprocessor = LogPreprocessorService(table_name="logs", use_llm=False)
    classification = preprocessor.classify([file_input])

    ParserRegistry.discover()
    pipeline = ParserRegistry.route(classification.selected_parser_key)
    result = pipeline.ingest([file_input], classification)

    # Collect all column names
    all_columns: set[str] = set()
    for td in result.table_definitions:
        all_columns.update(c.name for c in td.columns)

    # Verify promoted columns exist
    for col in sorted(PROMOTED_LAMBDA_COLUMNS):
        assert col in all_columns, (
            f"Expected column {col!r} to be present in parser output but it was not. "
            f"Available columns: {sorted(all_columns)}"
        )

    # Verify promoted fields are NOT in extra JSON of any row
    table_name = list(result.records.keys())[0]
    rows = result.records[table_name]

    for row in rows:
        extra_raw = row.get("extra")
        if not extra_raw or extra_raw == "{}":
            continue
        if isinstance(extra_raw, str):
            try:
                extra_dict = _json.loads(extra_raw)
            except (_json.JSONDecodeError, TypeError):
                continue
        elif isinstance(extra_raw, dict):
            extra_dict = extra_raw
        else:
            continue

        for col in PROMOTED_LAMBDA_COLUMNS:
            assert col not in extra_dict, (
                f"Column {col!r} should be promoted to top-level, "
                f"but found in extra of row {row.get('id', '?')}: {extra_dict}"
            )

    # Verify at least one row populated each promoted column
    populated: dict[str, int] = {col: 0 for col in PROMOTED_LAMBDA_COLUMNS}
    for row in rows:
        for col in PROMOTED_LAMBDA_COLUMNS:
            val = row.get(col)
            if val is not None and val != "" and val != "null":
                populated[col] += 1

    for col, count in populated.items():
        assert count > 0, (
            f"Column {col!r} was never populated with a non-null value "
            f"in any of the {len(rows)} parsed rows"
        )
