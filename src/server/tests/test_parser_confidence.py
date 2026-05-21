"""Tests for parser confidence correctness.

Verifies that confidence reflects real output quality:
  - better inputs produce higher confidence
  - heavy fallback lowers confidence
  - empty/garbled input yields low/zero confidence
  - confidence components are emitted
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure the server src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.contracts import ColumnDefinition
from parsers.engine import (
    CONFIDENCE_FORMULA_VERSION,
    _clamp_confidence,
    _compute_parser_confidence,
    _compute_raw_fallback_confidence,
    _compute_row_completeness,
    _compute_timestamp_success,
    _compute_type_conformity,
    _value_matches_sql_type,
)
from parsers.preprocessor import (
    FileInput,
    LogPreprocessorService,
    _compute_file_format_confidence,
    _is_json_line,
    _key_value_detectability,
    _malformed_line_ratio,
    _non_empty_ratio,
    _structured_line_ratio,
    _timestamp_hit_rate,
)

# ── Classification helpers ─────────────────────────────────────────────


class TestClassificationHelpers:
    """Test the individual signals used in classification confidence."""

    def test_non_empty_ratio(self):
        assert _non_empty_ratio([]) == 0.0
        assert _non_empty_ratio(["", ""]) == 0.0
        assert _non_empty_ratio(["a", "", "b"]) == 2 / 3
        assert _non_empty_ratio(["a", "b", "c"]) == 1.0

    def test_timestamp_hit_rate(self):
        lines = [
            "2024-01-01T12:00:00 INFO starting",
            "no timestamp here",
            "",  # empty
        ]
        rate = _timestamp_hit_rate(lines)
        # Non-empty lines: lines[0] (hit), lines[1] (miss) → 0.5
        assert rate == 0.5

        assert _timestamp_hit_rate([]) == 0.0
        assert _timestamp_hit_rate([""]) == 0.0

    def test_json_parseability(self):
        assert _is_json_line('{"key": "value"}')
        assert _is_json_line("[1, 2, 3]")
        assert not _is_json_line("not json")
        assert not _is_json_line("")
        assert not _is_json_line("{invalid json}")

    def test_key_value_detectability(self):
        lines = [
            "key=value foo=bar",
            "plain text line",
            "",
        ]
        rate = _key_value_detectability(lines)
        assert rate == 0.5  # 1 hit out of 2 non-empty

    def test_structured_line_ratio(self):
        lines = [
            "2024-01-01T12:00:00 INFO starting",  # timestamp + log level
            "key=value",  # key=value
            '{"json": true}',  # json
            "plain text",  # nothing
            "",  # empty
        ]
        ratio = _structured_line_ratio(lines)
        assert ratio == 0.75  # 3 out of 4 non-empty

    def test_malformed_line_ratio(self):
        lines = [
            "normal line with some content",
            "@@@@####$$$$%%%%^^^^",  # high special char density
            '{"json": "with_brackets"}',  # JSON with structural chars (not malformed)
            "a" * 600,  # very long unbroken token
            "",
        ]
        ratio = _malformed_line_ratio(lines)
        # 2 malformed out of 4 non-empty (@@@@ and long token; JSON is fine)
        assert ratio == 2 / 4

    def test_compute_file_format_confidence_structured(self):
        """Well-structured log input should score high."""
        lines = [
            "2024-01-01T12:00:00 INFO service=web request_id=abc123 method=GET status=200",
            "2024-01-01T12:00:01 ERROR service=worker request_id=def456 method=POST status=500",
            "2024-01-01T12:00:02 WARN service=db request_id=ghi789 method=SELECT status=200",
        ]
        confidence, components = _compute_file_format_confidence(lines)
        assert 0.0 <= confidence <= 1.0
        assert confidence > 0.6  # structured input should score well
        assert "structured_line_ratio" in components
        assert "timestamp_hit_rate" in components

    def test_compute_file_format_confidence_garbled(self):
        """Garbled input should score low."""
        lines = ["@@@###$$$%%%", "!!!!????!!!!", "aaa" * 200]
        confidence, components = _compute_file_format_confidence(lines)
        assert 0.0 <= confidence <= 1.0
        assert confidence < 0.4  # garbled input should score low

    def test_compute_file_format_confidence_empty(self):
        """Empty input should score 0."""
        confidence, _ = _compute_file_format_confidence([])
        assert confidence == 0.0

    def test_classification_monotonic(self):
        """Better input should produce higher classification confidence."""
        preprocessor = LogPreprocessorService(use_llm=False)

        garbled = FileInput(filename="garbled.log", content="@@@###$$$%%%\n!!!!????!!!!\n" + "x" * 600)
        structured = FileInput(
            filename="structured.log",
            content="2024-01-01T12:00:00 INFO user=alice action=login\n"
            "2024-01-01T12:00:01 INFO user=bob action=logout",
        )

        result_garbled = preprocessor.classify([garbled])
        result_structured = preprocessor.classify([structured])

        assert result_structured.confidence > result_garbled.confidence


# ── Engine confidence helpers ──────────────────────────────────────────


class TestEngineConfidenceHelpers:
    """Test the individual signals used in parser confidence."""

    def test_clamp_confidence(self):
        assert _clamp_confidence(0.5) == 0.5
        assert _clamp_confidence(-0.1) == 0.0
        assert _clamp_confidence(1.5) == 1.0

    def test_value_matches_sql_type(self):
        assert _value_matches_sql_type("hello", "TEXT")
        assert _value_matches_sql_type(42, "INTEGER")
        assert not _value_matches_sql_type(True, "INTEGER")  # bool is not int
        assert _value_matches_sql_type(True, "BOOLEAN")
        assert _value_matches_sql_type(3.14, "FLOAT")
        assert _value_matches_sql_type("2024-01-01T00:00:00", "TIMESTAMP")
        assert _value_matches_sql_type(None, "TEXT")  # null is always valid

    def test_compute_row_completeness(self):
        columns = [
            ColumnDefinition(name="timestamp", sql_type="TEXT"),
            ColumnDefinition(name="message", sql_type="TEXT"),
            ColumnDefinition(name="user", sql_type="TEXT"),
            ColumnDefinition(name="action", sql_type="TEXT"),
        ]
        rows = [
            {"timestamp": "2024-01-01", "message": "hello", "user": "alice", "action": "login"},
            {"timestamp": "2024-01-02", "message": "world", "user": "bob", "action": "logout"},
            {"timestamp": "2024-01-03", "message": "foo", "user": None, "action": "create"},
        ]
        # Baseline columns: id, timestamp, raw, extra
        # Non-baseline: message, user, action → 3 cols × 3 rows = 9 cells
        # Filled: message=3, user=2, action=3 → 8/9 ≈ 0.888
        completeness = _compute_row_completeness(rows, columns)
        assert 0.0 <= completeness <= 1.0
        assert completeness == 8 / 9

    def test_compute_row_completeness_no_extra_columns(self):
        """If only baseline columns, completeness should be 1.0."""
        rows = [{"timestamp": "2024-01-01", "message": "hello", "id": "1", "raw": "raw1"}]
        columns = [
            ColumnDefinition(name="id", sql_type="TEXT"),
            ColumnDefinition(name="timestamp", sql_type="TEXT"),
            ColumnDefinition(name="raw", sql_type="TEXT"),
            ColumnDefinition(name="extra", sql_type="TEXT"),
        ]
        assert _compute_row_completeness(rows, columns) == 1.0

    def test_compute_timestamp_success(self):
        rows = [
            {"timestamp": "2024-01-01T12:00:00Z"},
            {"timestamp": "2024-01-01T12:00:01Z"},
            {"timestamp": ""},
            {},  # no timestamp
        ]
        rate = _compute_timestamp_success(rows)
        assert rate == 0.5  # 2 out of 4 parseable

    def test_compute_type_conformity(self):
        columns = [
            ColumnDefinition(name="count", sql_type="INTEGER"),
            ColumnDefinition(name="name", sql_type="TEXT"),
            ColumnDefinition(name="active", sql_type="BOOLEAN"),
        ]
        rows = [
            {"count": 42, "name": "alice", "active": True},
            {"count": "not_a_number", "name": "bob", "active": False},  # count is type error
            {"count": 100, "name": None, "active": True},  # null is skipped
        ]
        # Non-null values: count(42), count("not_a_number"), count(100), name("alice"), name("bob"), active(True), active(False), active(True)
        # = 8 non-null. Type errors: count("not_a_number") because str vs INTEGER
        # Conforming: 7/8 = 0.875
        conformity = _compute_type_conformity(rows, columns)
        assert conformity == 7 / 8

    def test_compute_type_conformity_no_extra_cols(self):
        rows = [{"foo": "bar"}]
        columns = [ColumnDefinition(name="id", sql_type="TEXT")]
        assert _compute_type_conformity(rows, columns) == 1.0


class TestComputeParserConfidence:
    """Test the composite parser confidence formula."""

    def test_empty_rows(self):
        confidence, components = _compute_parser_confidence(
            total_rows=0,
            successful_batch_count=0,
            failed_batch_count=0,
            rows_from_ai=0,
            rows_from_fallback=0,
            rows=[],  # noqa
            columns=[],  # noqa
        )
        assert confidence == 0.0
        assert components == {}

    def test_high_quality(self):
        """High batch success, low fallback, good row quality → high confidence."""
        rows = [
            {"timestamp": "2024-01-01T12:00:00Z", "user": "alice", "count": 42},
            {"timestamp": "2024-01-01T12:00:01Z", "user": "bob", "count": 43},
        ]
        columns = [
            ColumnDefinition(name="user", sql_type="TEXT"),
            ColumnDefinition(name="count", sql_type="INTEGER"),
        ]
        confidence, components = _compute_parser_confidence(
            total_rows=10,
            successful_batch_count=5,
            failed_batch_count=0,
            rows_from_ai=10,
            rows_from_fallback=0,
            rows=rows,
            columns=columns,
            llm_average_confidence=0.9,
        )
        assert 0.0 <= confidence <= 1.0
        assert confidence > 0.7  # high quality should score well
        assert "batch_success_rate" in components
        assert "fallback_rate" in components

    def test_heavy_fallback_lowers_confidence(self):
        """High fallback rate should lower confidence vs. no fallback."""
        rows = [
            {"timestamp": "2024-01-01T12:00:00Z", "user": "alice", "count": 42},
        ]
        columns = [
            ColumnDefinition(name="user", sql_type="TEXT"),
            ColumnDefinition(name="count", sql_type="INTEGER"),
        ]

        no_fallback, _ = _compute_parser_confidence(
            total_rows=10,
            successful_batch_count=5,
            failed_batch_count=0,
            rows_from_ai=10,
            rows_from_fallback=0,
            rows=rows,
            columns=columns,
        )

        heavy_fallback, _ = _compute_parser_confidence(
            total_rows=10,
            successful_batch_count=1,
            failed_batch_count=4,
            rows_from_ai=2,
            rows_from_fallback=8,
            rows=rows,
            columns=columns,
        )

        assert no_fallback > heavy_fallback

    def test_low_batch_success_lowers_confidence(self):
        """Low batch success rate should lower confidence."""
        rows = [
            {"timestamp": "2024-01-01T12:00:00Z", "user": "alice"},
        ]
        columns = [ColumnDefinition(name="user", sql_type="TEXT")]

        high_bsr, _ = _compute_parser_confidence(
            total_rows=10,
            successful_batch_count=10,
            failed_batch_count=0,
            rows_from_ai=10,
            rows_from_fallback=0,
            rows=rows,
            columns=columns,
        )

        low_bsr, _ = _compute_parser_confidence(
            total_rows=10,
            successful_batch_count=1,
            failed_batch_count=9,
            rows_from_ai=10,
            rows_from_fallback=0,
            rows=rows,
            columns=columns,
        )

        assert high_bsr > low_bsr


class TestRawFallbackConfidence:
    """Test raw fallback confidence computation."""

    def test_empty_rows(self):
        assert _compute_raw_fallback_confidence([]) == 0.0

    def test_basic_lines(self):
        """Basic text lines should get moderate confidence."""
        rows = [
            {"source": "test.log", "raw": "hello world", "message": "hello world", "timestamp": ""},
            {"source": "test.log", "raw": "foo bar", "message": "foo bar", "timestamp": ""},
        ]
        confidence = _compute_raw_fallback_confidence(rows)
        assert 0.0 <= confidence <= 1.0
        # Without timestamps or enrichment, should be moderate-low
        assert confidence < 0.5

    def test_enriched_lines(self):
        """Lines with timestamps and kv pairs should score higher."""
        rows = [
            {
                "source": "test.log",
                "raw": "2024-01-01T12:00:00 INFO user=alice",
                "message": "2024-01-01T12:00:00 INFO user=alice",
                "timestamp": "2024-01-01T12:00:00Z",
                "user": "alice",
                "log_level": "INFO",
            },
            {
                "source": "test.log",
                "raw": "2024-01-01T12:00:01 WARN user=bob",
                "message": "2024-01-01T12:00:01 WARN user=bob",
                "timestamp": "2024-01-01T12:00:01Z",
                "user": "bob",
                "log_level": "WARN",
            },
        ]
        confidence = _compute_raw_fallback_confidence(rows)
        assert 0.0 <= confidence <= 1.0
        assert confidence > 0.3  # enriched should have decent score

    def test_monotonic_raw(self):
        """More structured lines should score higher than plain lines."""
        plain = [
            {"source": "test.log", "raw": "hello", "message": "hello"},
            {"source": "test.log", "raw": "world", "message": "world"},
        ]
        # Drop .get("timestamp") safety
        for r in plain:
            r.setdefault("timestamp", "")

        enriched = [
            {
                "source": "test.log",
                "raw": "2024-01-01T12:00:00 INFO user=alice",
                "message": "2024-01-01T12:00:00 INFO user=alice",
                "timestamp": "2024-01-01T12:00:00Z",
                "user": "alice",
                "log_level": "INFO",
            },
            {
                "source": "test.log",
                "raw": "2024-01-01T12:00:01 ERROR user=bob",
                "message": "2024-01-01T12:00:01 ERROR user=bob",
                "timestamp": "2024-01-01T12:00:01Z",
                "user": "bob",
                "log_level": "ERROR",
            },
        ]

        assert _compute_raw_fallback_confidence(enriched) > _compute_raw_fallback_confidence(plain)


class TestOrchestratorAggregation:
    """Test conservative confidence aggregation in orchestrator."""

    def test_confidence_formula_version_present(self):
        """The formula version should be a non-empty string."""
        from parsers.engine import CONFIDENCE_FORMULA_VERSION
        assert CONFIDENCE_FORMULA_VERSION == "parser-v1"

    def test_classification_formula_version_present(self):
        from parsers.preprocessor import CONFIDENCE_FORMULA_VERSION as CFV
        assert CFV == "classification-v1"


class TestEndToEnd:
    """End-to-end tests through the full classification pipeline."""

    def test_empty_file(self):
        """Empty file should give zero classification confidence."""
        preprocessor = LogPreprocessorService(use_llm=False)
        file_input = FileInput(filename="empty.log", content="")
        result = preprocessor.classify([file_input])
        assert result.confidence == 0.0

    def test_classification_components_in_diagnostics(self):
        """Diagnostics should include confidence_components per file."""
        preprocessor = LogPreprocessorService(use_llm=False)
        file_input = FileInput(
            filename="test.log",
            content="2024-01-01T12:00:00 INFO user=alice action=login\n"
            "2024-01-01T12:00:01 INFO user=bob action=logout",
        )
        result = preprocessor.classify([file_input])
        assert result.diagnostics is not None
        for file_diag in result.diagnostics.get("files", []):
            if "confidence_components" in file_diag:
                components = file_diag["confidence_components"]
                assert isinstance(components, dict)
                assert "structured_line_ratio" in components
                assert "non_empty_ratio" in components
                break
        else:
            pytest.fail("No file diagnostics with confidence_components found")

    def test_parser_diagnostics_include_components(self):
        """Parser diagnostics should include confidence_components when rows exist."""
        from parsers.engine import UniversalAIParser
        from parsers.contracts import ClassificationResult, StructuralClass, FileClassification

        # We can't easily test UniversalAIParser without mocking LLM,
        # but we can verify the diagnostics structure.
        columns = [
            ColumnDefinition(name="user", sql_type="TEXT"),
            ColumnDefinition(name="count", sql_type="INTEGER"),
        ]
        rows = [
            {"timestamp": "2024-01-01T12:00:00Z", "user": "alice", "count": 42, "raw": "raw1", "message": "msg1"},
            {"timestamp": "2024-01-01T12:00:01Z", "user": "bob", "count": 43, "raw": "raw2", "message": "msg2"},
        ]

        confidence, components = _compute_parser_confidence(
            total_rows=2,
            successful_batch_count=1,
            failed_batch_count=0,
            rows_from_ai=2,
            rows_from_fallback=0,
            rows=rows,
            columns=columns,
        )

        assert isinstance(components, dict)
        assert "batch_success_rate" in components
        assert "fallback_rate" in components
        assert "row_completeness" in components
        assert "type_conformity" in components
        assert "timestamp_parse_success" in components



