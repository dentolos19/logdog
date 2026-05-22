"""Tests for deterministic extra-column row grouping.

Verifies that:
  - Identical extra values are clustered together.
  - Similar (normalized) values are adjacent.
  - Row data outside ``extra`` stays aligned with its row.
  - Missing/empty/null extra rows remain at the end.
  - The original row count is unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.extra_grouping import (
    extract_extra_tokens,
    group_rows_by_extra,
    parse_extra_value,
)


# ── parse_extra_value ─────────────────────────────────────────────────────


class TestParseExtraValue:
    def test_none(self):
        assert parse_extra_value(None) is None

    def test_empty_string(self):
        assert parse_extra_value("") is None
        assert parse_extra_value("   ") is None

    def test_null_literal(self):
        assert parse_extra_value("null") is None
        assert parse_extra_value(" null ") is None

    def test_json_string_dict(self):
        result = parse_extra_value('{"red": "a", "blue": "b"}')
        assert isinstance(result, dict)

    def test_json_string_list(self):
        result = parse_extra_value('["a", "b"]')
        assert isinstance(result, list)

    def test_plain_string(self):
        result = parse_extra_value("hello")
        assert result == "hello"

    def test_dict_passthrough(self):
        d = {"k": "v"}
        assert parse_extra_value(d) is d

    def test_list_passthrough(self):
        lst = [1, 2, 3]
        assert parse_extra_value(lst) is lst


# ── extract_extra_tokens ──────────────────────────────────────────────────


class TestExtractExtraTokens:
    def test_none_extra(self):
        assert extract_extra_tokens(None) == ()

    def test_empty(self):
        assert extract_extra_tokens("") == ()
        assert extract_extra_tokens("null") == ()

    def test_single_scalar(self):
        assert extract_extra_tokens("red") == ("red",)

    def test_json_dict_values(self):
        tokens = extract_extra_tokens('{"color": "red", "size": "large"}')
        assert set(tokens) == {"red", "large"}

    def test_json_list_values(self):
        tokens = extract_extra_tokens('["red", "blue", "white"]')
        assert tokens == ("red", "blue", "white")

    def test_nested_dict(self):
        tokens = extract_extra_tokens('{"a": {"b": "deep"}}')
        assert tokens == ("deep",)

    def test_mixed_types(self):
        tokens = extract_extra_tokens('{"count": 3, "active": true, "label": "test"}')
        assert set(tokens) == {"3", "true", "test"}

    def test_case_normalization(self):
        tokens = extract_extra_tokens('{"color": "Red"}')
        assert tokens == ("red",)

    def test_whitespace_normalization(self):
        tokens = extract_extra_tokens('{"color": "  RED  "}')
        assert tokens == ("red",)

    def test_inner_whitespace_collapse(self):
        tokens = extract_extra_tokens('{"name": "hello   world"}')
        assert tokens == ("hello world",)

    def test_empty_values_skipped(self):
        tokens = extract_extra_tokens('{"a": "", "b": null, "c": "ok"}')
        assert tokens == ("ok",)

    def test_bool_false(self):
        tokens = extract_extra_tokens('{"flag": false}')
        assert tokens == ("false",)

    def test_int_zero(self):
        tokens = extract_extra_tokens('{"n": 0}')
        assert tokens == ("0",)

    def test_plain_string_fallback(self):
        tokens = extract_extra_tokens("some raw string")
        assert tokens == ("some raw string",)


# ── group_rows_by_extra ───────────────────────────────────────────────────


def _row(extra: object, row_id: int = 0) -> dict:
    """Build a minimal row dict with an ``extra`` column and guard fields."""
    return {
        "id": f"row-{row_id}",
        "timestamp": f"2025-01-01T00:00:0{row_id}",
        "raw": f"raw-{row_id}",
        "message": f"msg-{row_id}",
        "extra": extra,
    }


# Convenience short-hands for test clarity
R = "red"
B = "blue"
W = "white"


class TestGroupRowsByExtra:
    def test_empty_list(self):
        assert group_rows_by_extra([]) == []

    def test_single_row(self):
        rows = [_row('{"color": "red"}', 0)]
        result = group_rows_by_extra(rows)
        assert len(result) == 1
        assert result[0]["id"] == "row-0"

    def test_clusters_identical_values(self):
        """red, blue, white, red, white → red, red, white, white, blue."""
        rows = [
            _row('{"c": "red"}', 0),
            _row('{"c": "blue"}', 1),
            _row('{"c": "white"}', 2),
            _row('{"c": "red"}', 3),
            _row('{"c": "white"}', 4),
        ]
        result = group_rows_by_extra(rows)

        # Should have 5 rows
        assert len(result) == 5

        # Extract the extra tokens in result order
        tokens = [r["extra"] for r in result]

        # The red rows should be adjacent to each other
        red_indices = [i for i, t in enumerate(tokens) if t == '{"c": "red"}']
        assert red_indices[1] == red_indices[0] + 1, "red rows must be adjacent"

        # The white rows should be adjacent to each other
        white_indices = [i for i, t in enumerate(tokens) if t == '{"c": "white"}']
        assert white_indices[1] == white_indices[0] + 1, "white rows must be adjacent"

        # blue should be adjacent to itself (single)
        blue_indices = [i for i, t in enumerate(tokens) if t == '{"c": "blue"}']
        assert len(blue_indices) == 1

    def test_row_alignment_preserved(self):
        """Column data outside extra must stay with its row."""
        rows = [
            {"id": "a", "timestamp": "t1", "message": "msg-a", "extra": '{"x": "red"}'},
            {"id": "b", "timestamp": "t2", "message": "msg-b", "extra": '{"x": "blue"}'},
            {"id": "c", "timestamp": "t3", "message": "msg-c", "extra": '{"x": "red"}'},
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3

        # For each row, message must match id
        for r in result:
            expected_msg = f"msg-{r['id']}"
            assert r["message"] == expected_msg, (
                f"message for id={r['id']} was {r['message']!r}, expected {expected_msg!r}"
            )

    def test_missing_extra_at_end(self):
        """Rows without extra go to the end, preserving their relative order."""
        rows = [
            _row('{"c": "blue"}', 0),
            _row(None, 1),
            _row('{"c": "red"}', 2),
            _row("", 3),
            _row('{"c": "red"}', 4),
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 5

        # The first three rows should have non-empty extra
        for r in result[:3]:
            assert r["extra"] is not None and r["extra"] != "" and r["extra"] != "null"

        # The last two rows must be id=1 and id=3 (relative order preserved)
        assert result[-2]["id"] == "row-1"  # None original index 1
        assert result[-1]["id"] == "row-3"  # empty string original index 3

    def test_null_literal_at_end(self):
        rows = [
            _row('{"c": "red"}', 0),
            _row("null", 1),
            _row('{"c": "red"}', 2),
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3
        # row-1 (null literal) should be last
        assert result[-1]["id"] == "row-1"

    def test_case_insensitive_grouping(self):
        """\"Red\" and \"red\" should cluster together."""
        rows = [
            _row('{"c": "Red"}', 0),
            _row('{"c": "blue"}', 1),
            _row('{"c": "red"}', 2),
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3
        extras = [r["extra"] for r in result]
        ri = [i for i, e in enumerate(extras) if "red" in e.lower()]
        assert ri[1] == ri[0] + 1, "case-insensitive red rows must be adjacent"

    def test_whitespace_insensitive_grouping(self):
        rows = [
            _row('{"c": " red "}', 0),
            _row('{"c": "red"}', 1),
            _row('{"c": "blue"}', 2),
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3
        extras = [r["extra"] for r in result]
        ri = [i for i, e in enumerate(extras) if "red" in e.lower()]
        assert ri[1] == ri[0] + 1, "whitespace-normalized red rows must be adjacent"

    def test_deterministic_output(self):
        """Same input produces same output order."""
        rows = [
            _row('{"c": "red"}', 0),
            _row('{"c": "blue"}', 1),
            _row('{"c": "red"}', 2),
            _row('{"c": "blue"}', 3),
            _row('{"c": "white"}', 4),
        ]
        first = group_rows_by_extra(rows)
        second = group_rows_by_extra(rows)
        ids_first = [r["id"] for r in first]
        ids_second = [r["id"] for r in second]
        assert ids_first == ids_second, "grouping must be deterministic"

    def test_frequency_based_clustering(self):
        """Most frequent token appears first in the output."""
        rows = [
            _row('{"color": "red"}', 0),
            _row('{"color": "green"}', 1),
            _row('{"color": "blue"}', 2),
            _row('{"color": "green"}', 3),
            _row('{"color": "green"}', 4),
        ]
        result = group_rows_by_extra(rows)
        # green is most frequent (3), should appear first
        first_token = result[0]["extra"]
        assert "green" in first_token

    def test_no_row_count_change(self):
        """Row count must never change."""
        rows = [_row('{"a": "x"}', i) for i in range(10)] + [_row(None, i) for i in range(10, 15)]
        result = group_rows_by_extra(rows)
        assert len(result) == len(rows)

    def test_extra_is_dict_already(self):
        rows = [
            {"id": "r1", "extra": {"color": "red"}},
            {"id": "r2", "extra": {"color": "blue"}},
            {"id": "r3", "extra": {"color": "red"}},
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3
        extras = [r["extra"] for r in result]
        ri = [i for i, e in enumerate(extras) if e.get("color") == "red"]
        assert ri[1] == ri[0] + 1

    def test_complex_nested_extra(self):
        rows = [
            _row('{"event": "login", "details": {"ip": "1.2.3.4"}}', 0),
            _row('{"event": "logout", "details": {"ip": "4.3.2.1"}}', 1),
            _row('{"event": "login", "details": {"ip": "5.6.7.8"}}', 2),
        ]
        result = group_rows_by_extra(rows)
        assert len(result) == 3
        extras = [r["extra"] for r in result]
        li = [i for i, e in enumerate(extras) if '"login"' in e]
        assert li[1] == li[0] + 1
