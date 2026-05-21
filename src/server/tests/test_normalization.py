"""Tests for multiline log record normalization.

Verifies that logs with blank-line-separated event groups are correctly
grouped into single records instead of being split per physical line.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the server src is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from parsers.engine import (
    _extract_leading_timestamp,
    _looks_like_json_lines,
    _looks_like_multiline_records,
    _normalize_json_records,
    _split_blank_line_groups,
    normalize_records,
)

# ── Sample content matching the 09_dose_evaluation_warning.log ─────────

DOSE_LOG = (
    "01/01/2025 00:00:14.0478 Machine:MCH0001 (Rel:VER0001, DWME [dc], DWME_analysis.c, ?,?, 680)\n"
    "SYSTEM WARNING: DW-3411 SKIP\n"
    "The dose evaluation 0.0902843 [%] exceeds the dose evaluation warning level 0 [%]\n"
    "de_err=0.0902843 [%] de_warn_lvl=0 [%] ESET:93074.6 [bits]\n"
    "freq=50000 [hz] n_slit=32 mb_enabled=T action_handle=7152 exposure_handle=6847\n"
    "[DWME_analysis_determine_dose_performance_result]\n"
    "\n"
    "01/01/2025 00:00:14.2624 Machine:MCH0001 (Rel:VER0001, DWME [dc], DWME_analysis.c, ?,?, 680)\n"
    "SYSTEM WARNING: DW-3411 SKIP\n"
    "The dose evaluation 0.0862883 [%] exceeds the dose evaluation warning level 0 [%]\n"
    "de_err=0.0862883 [%] de_warn_lvl=0 [%] ESET:93106.8 [bits]\n"
    "freq=50000 [hz] n_slit=32 mb_enabled=T action_handle=7153 exposure_handle=6848\n"
    "[DWME_analysis_determine_dose_performance_result]"
)


# ── Helper function tests ────────────────────────────────────────────


class TestSplitBlankLineGroups:
    def test_no_blank_lines(self):
        """No blank lines → one group containing all lines."""
        content = "line1\nline2\nline3"
        groups = _split_blank_line_groups(content)
        assert len(groups) == 1
        assert groups[0]["raw"] == content
        assert groups[0]["start_line"] == 1
        assert groups[0]["end_line"] == 3

    def test_blank_line_separator(self):
        """Blank line between groups → two groups."""
        content = "group1_line1\ngroup1_line2\n\ngroup2_line1\ngroup2_line2"
        groups = _split_blank_line_groups(content)
        assert len(groups) == 2
        assert groups[0]["raw"] == "group1_line1\ngroup1_line2"
        assert groups[1]["raw"] == "group2_line1\ngroup2_line2"
        assert groups[0]["start_line"] == 1
        assert groups[0]["end_line"] == 2
        assert groups[1]["start_line"] == 4
        assert groups[1]["end_line"] == 5

    def test_trailing_blank_lines(self):
        """Trailing blank lines should be ignored."""
        content = "line1\nline2\n\n\n"
        groups = _split_blank_line_groups(content)
        assert len(groups) == 1
        assert groups[0]["raw"] == "line1\nline2"

    def test_empty_content(self):
        groups = _split_blank_line_groups("")
        assert groups == []

    def test_dose_log_groups(self):
        """The 09_dose_evaluation_warning.log should produce 2 groups."""
        groups = _split_blank_line_groups(DOSE_LOG)
        assert len(groups) == 2
        # First group should have 6 lines
        assert len(groups[0]["lines"]) == 6
        assert groups[0]["lines"][0].startswith("01/01/2025")
        # Second group should have 6 lines
        assert len(groups[1]["lines"]) == 6
        assert groups[1]["lines"][0].startswith("01/01/2025")
        # Start/end line numbers
        assert groups[0]["start_line"] == 1
        assert groups[0]["end_line"] == 6
        assert groups[1]["start_line"] == 8
        assert groups[1]["end_line"] == 13


class TestLooksLikeJsonLines:
    def test_jsonl_content(self):
        lines = ['{"a": 1}', '{"b": 2}', 'not json']
        assert _looks_like_json_lines(lines)

    def test_non_json_content(self):
        lines = ["hello world", "foo bar", "baz qux"]
        assert not _looks_like_json_lines(lines)

    def test_dose_log(self):
        """The dose log should NOT be detected as JSONL."""
        lines = DOSE_LOG.splitlines()
        assert not _looks_like_json_lines(lines)

    def test_empty(self):
        assert not _looks_like_json_lines([])


class TestLooksLikeMultilineRecords:
    def test_dose_log(self):
        """The dose log should be detected as multiline records."""
        groups = _split_blank_line_groups(DOSE_LOG)
        assert _looks_like_multiline_records(groups, content=DOSE_LOG)

    def test_single_line_groups(self):
        """All single-line groups should NOT be detected as multiline."""
        content = "line1\nline2\nline3"
        groups = _split_blank_line_groups(content)
        assert not _looks_like_multiline_records(groups, content=content)

    def test_timestamp_leads_multi_line(self):
        """Multi-line groups whose first lines have timestamps qualify."""
        content = (
            "2025-01-01 00:00:00 INFO start\n"
            "  continuation line\n"
            "  more details\n"
            "\n"
            "2025-01-01 00:00:01 WARN something\n"
            "  another continuation\n"
        )
        groups = _split_blank_line_groups(content)
        assert _looks_like_multiline_records(groups, content=content)

    def test_continuation_lines_not_event_starts(self):
        """Continuation lines that don't look like events still qualify."""
        content = (
            "01/01/2025 00:00:00 Event start\n"
            "some detail\n"
            "more detail\n"
            "\n"
            "01/01/2025 00:00:01 Next event\n"
            "detail line\n"
        )
        groups = _split_blank_line_groups(content)
        assert _looks_like_multiline_records(groups, content=content)

    def test_empty(self):
        assert not _looks_like_multiline_records([])


class TestExtractLeadingTimestamp:
    def test_iso_timestamp(self):
        result = _extract_leading_timestamp("2025-01-01 00:00:00 INFO start\nmore")
        assert "2025-01-01" in result
        assert "00:00:00" in result

    def test_us_date_format(self):
        """US date format used in the dose log."""
        result = _extract_leading_timestamp(
            "01/01/2025 00:00:14.0478 Machine:MCH0001\n"
        )
        # Either normalized ISO or raw captured text
        assert "2025-01-01" in result or "01/01/2025" in result
        assert "00:00:14" in result or "14.0478" in result

    def test_no_timestamp(self):
        result = _extract_leading_timestamp("No timestamp here\nmore")
        assert result == ""

    def test_empty(self):
        assert _extract_leading_timestamp("") == ""


# ── normalize_records tests ──────────────────────────────────────────


class TestNormalizeRecordsMultiline:
    """Test that logs with blank-line-separated events are grouped properly."""

    def test_dose_log_returns_two_records(self):
        """The dose evaluation log should produce exactly 2 records."""
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        assert len(records) == 2, f"Expected 2 records, got {len(records)}"

    def test_first_record_contains_lines_1_to_6(self):
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        first = records[0]
        raw = first["raw"]
        # Should contain content from lines 1-6
        assert "01/01/2025 00:00:14.0478" in raw
        assert "DW-3411 SKIP" in raw
        assert "dose evaluation 0.0902843" in raw
        assert "de_err=0.0902843" in raw
        assert "50000 [hz]" in raw
        assert "DWME_analysis_determine_dose_performance_result" in raw
        # Should NOT contain content from the second event
        assert "14.2624" not in raw
        assert "0.0862883" not in raw

    def test_second_record_contains_lines_8_to_13(self):
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        second = records[1]
        raw = second["raw"]
        assert "01/01/2025 00:00:14.2624" in raw
        assert "dose evaluation 0.0862883" in raw
        assert "de_err=0.0862883" in raw
        assert "action_handle=7153" in raw
        assert "exposure_handle=6848" in raw

    def test_timestamps_extracted_for_each_group(self):
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        assert records[0].get("timestamp", ""), "First record should have timestamp"
        assert records[1].get("timestamp", ""), "Second record should have timestamp"
        # Second event timestamp should be later
        assert records[1]["timestamp"] > records[0]["timestamp"]

    def test_source_line_info(self):
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        assert records[0].get("source_line") == 1
        assert records[0].get("end_line") == 6
        assert records[1].get("source_line") == 8
        assert records[1].get("end_line") == 13

    def test_message_field(self):
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        assert "01/01/2025 00:00:14.0478" in records[0].get("message", "")
        assert "01/01/2025 00:00:14.2624" in records[1].get("message", "")

    def test_source_preserved(self):
        records = normalize_records(DOSE_LOG, filename="test.log")
        assert all(r.get("source") == "test.log" for r in records)


# ── JSONL should stay per-line ──────────────────────────────────────


class TestNormalizeRecordsJsonl:
    """JSONL content should NOT be grouped by blank lines."""

    JSONL = '{"ts": "2025-01-01", "event": "start"}\n{"ts": "2025-01-01", "event": "end"}'

    def test_jsonl_stays_per_line(self):
        records = normalize_records(self.JSONL, filename="test.jsonl")
        assert len(records) == 2

    def test_jsonl_with_blank_lines(self):
        """Even if blank lines exist, JSONL should stay per-line."""
        content = '{"a": 1}\n\n{"b": 2}'
        records = normalize_records(content, filename="test.jsonl")
        assert len(records) == 2


# ── Single-line logs should stay unchanged ───────────────────────────


class TestNormalizeRecordsSingleLine:
    def test_one_line(self):
        records = normalize_records("hello world", filename="test.log")
        assert len(records) == 1
        assert records[0]["raw"] == "hello world"

    def test_multiple_lines_no_blanks(self):
        """Multiple lines without blank separators should each be a record."""
        content = "line1\nline2\nline3"
        records = normalize_records(content, filename="test.log")
        assert len(records) == 3
        assert records[0]["raw"] == "line1"
        assert records[1]["raw"] == "line2"
        assert records[2]["raw"] == "line3"

    def test_mixed_blank_and_single_lines(self):
        """Single-line groups between blank lines should still be records."""
        content = "event1\n\nevent2\n\nevent3"
        records = normalize_records(content, filename="test.log")
        # Without timestamps, these should stay as single-line records
        assert len(records) == 3


# ── Empty content ────────────────────────────────────────────────────


class TestNormalizeRecordsEmpty:
    def test_empty(self):
        assert normalize_records("", "empty.log") == []
        assert normalize_records("   \n  \n", "blank.log") == []


# ── CSV and XML should remain unchanged ──────────────────────────────


class TestNormalizeRecordsCsv:
    """CSV content should still go through the CSV path and remain unchanged."""

    CSV_CONTENT = "name,value\nalice,42\nbob,43"

    def test_csv_path(self):
        records = normalize_records(self.CSV_CONTENT, filename="test.csv")
        # CSV sniffing should detect header + 2 rows
        assert len(records) >= 1


class TestNormalizeRecordsXml:
    """XML content should still go through the XML path."""

    XML_CONTENT = "<root><item><id>1</id></item><item><id>2</id></item></root>"

    def test_xml_path(self):
        records = normalize_records(self.XML_CONTENT, filename="test.xml")
        # XML with 2 repeated items should produce 2 records
        assert len(records) == 2


# ── JSON document normalization ────────────────────────────────────────

SAMPLE_10_JSON = """\
{
  "Keys": {
    "ModuleID": "",
    "RecipeStepID": "4.0",
    "WaferID": ""
  },
  "Attributes": {
    "Events": {
      "ControlStateEvents": [
        {"Name": "NAME_0003", "DateTime": "2025-12-16T12:11:35.000000Z"},
        {"Name": "NAME_0004", "DateTime": "2025-12-16T12:18:20.000000Z"}
      ]
    },
    "LPID": "LP_0001",
    "SlotID": "SLOT_0002",
    "Recipe": {
      "RecipeID": "RCP_0002",
      "Type": "PROCESS",
      "SetPoints": [
        {"SensorID": "SENSOR_0001", "DataType": "Float", "Value": ""},
        {"SensorID": "SENSOR_0002", "DataType": "Float", "Value": ""},
        {"SensorID": "SENSOR_0003", "DataType": "Float", "Value": ""}
      ]
    }
  }
}"""


class TestNormalizeJsonRecordsDirect:
    """Direct tests of ``_normalize_json_records``."""

    def test_json_object_returns_one_record(self):
        records = _normalize_json_records(SAMPLE_10_JSON, filename="test.json")
        assert records is not None
        assert len(records) == 1

    def test_flattened_fields_present(self):
        records = _normalize_json_records(SAMPLE_10_JSON, filename="test.json")
        assert records is not None
        rec = records[0]
        # Scalar fields from nested flattening
        assert rec.get("Keys_ModuleID") == ""
        assert rec.get("Keys_RecipeStepID") == "4.0"
        assert rec.get("Keys_WaferID") == ""
        assert rec.get("Attributes_LPID") == "LP_0001"
        assert rec.get("Attributes_SlotID") == "SLOT_0002"
        assert rec.get("Attributes_Recipe_RecipeID") == "RCP_0002"
        assert rec.get("Attributes_Recipe_Type") == "PROCESS"

    def test_complex_fields_stored_as_json_strings(self):
        records = _normalize_json_records(SAMPLE_10_JSON, filename="test.json")
        assert records is not None
        rec = records[0]
        # Nested dict stored as JSON string
        assert isinstance(rec.get("Keys"), str)
        assert '"ModuleID"' in rec["Keys"]
        # Nested array stored as JSON string
        assert isinstance(rec.get("Attributes_Events_ControlStateEvents"), str)
        assert "NAME_0003" in rec["Attributes_Events_ControlStateEvents"]
        # Double-nested dict stored as JSON string
        assert isinstance(rec.get("Attributes"), str)
        assert "ControlStateEvents" in rec["Attributes"]

    def test_raw_and_message(self):
        records = _normalize_json_records(SAMPLE_10_JSON, filename="test.json")
        assert records is not None
        rec = records[0]
        assert rec.get("source") == "test.json"
        assert rec.get("record_index") == 0
        assert "Keys" in rec.get("raw", "")
        assert "Keys" in rec.get("message", "")

    def test_non_json_returns_none(self):
        assert _normalize_json_records("hello world", "test.log") is None
        assert _normalize_json_records("not { json", "test.log") is None
        assert _normalize_json_records("", "test.log") is None

    def test_json_array_of_objects(self):
        content = '[{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]'
        records = _normalize_json_records(content, "test.json")
        assert records is not None
        assert len(records) == 2
        assert records[0].get("a") == 1
        assert records[1].get("a") == 2
        assert records[0].get("b") == "x"

    def test_json_array_of_primitives(self):
        content = "[1, 2, 3]"
        records = _normalize_json_records(content, "test.json")
        assert records is not None
        assert len(records) == 3
        # No flattened scalar keys for primitives
        for rec in records:
            assert rec.get("source") == "test.json"
            assert "raw" in rec
            assert "message" in rec

    def test_empty_json_object(self):
        records = _normalize_json_records("{}", "test.json")
        assert records is not None
        assert len(records) == 1
        assert records[0].get("raw") is not None

    def test_empty_json_array(self):
        records = _normalize_json_records("[]", "test.json")
        assert records is not None
        assert len(records) == 0


class TestNormalizeRecordsJsonDocument:
    """Integration tests through ``normalize_records`` for JSON documents."""

    def test_pretty_printed_json_produces_one_record(self):
        """Pretty-printed JSON should produce 1 record, not fragmented per-line."""
        records = normalize_records(SAMPLE_10_JSON, filename="10_sparse_unknown_fragment.txt")
        assert len(records) == 1, f"Expected 1 record, got {len(records)}"

    def test_flattened_fields_correct(self):
        records = normalize_records(SAMPLE_10_JSON, filename="test.json")
        assert len(records) == 1
        rec = records[0]
        assert rec.get("Keys_RecipeStepID") == "4.0"
        assert rec.get("Attributes_LPID") == "LP_0001"
        assert "Keys" in rec  # complex/nested field stored as JSON string

    def test_json_with_commas_not_mistaken_for_csv(self):
        """JSON containing commas inside strings/objects should NOT be CSV-sniffed."""
        content = '{"name": "alice, bob", "scores": [1, 2, 3]}'
        records = normalize_records(content, "test.json")
        assert len(records) == 1
        assert records[0].get("name") == "alice, bob"

    def test_jsonl_still_per_line(self):
        """JSON Lines content should still produce one record per line."""
        content = '{"a": 1}\n{"b": 2}\n{"c": 3}'
        records = normalize_records(content, "test.jsonl")
        assert len(records) == 3

    def test_jsonl_with_blank_lines(self):
        """JSONL with blank lines should still produce one record per line."""
        content = '{"a": 1}\n\n{"b": 2}'
        records = normalize_records(content, "test.jsonl")
        assert len(records) == 2

    def test_dose_log_regression(self):
        """Sample 09 multiline detection must not regress."""
        records = normalize_records(DOSE_LOG, filename="09_dose_evaluation_warning.log")
        assert len(records) == 2

    def test_csv_still_works(self):
        """CSV content should still go through the CSV path."""
        content = "name,value\nalice,42\nbob,43"
        records = normalize_records(content, "test.csv")
        assert len(records) == 2

    def test_xml_still_works(self):
        """XML content should still go through the XML path."""
        content = "<root><item><id>1</id></item><item><id>2</id></item></root>"
        records = normalize_records(content, "test.xml")
        # Single flattened record (no repeated grain found in this simple test)
        assert len(records) >= 1

    def test_single_line_log(self):
        """Plain single-line content unchanged."""
        records = normalize_records("hello world", "test.log")
        assert len(records) == 1
        assert "hello" in records[0].get("raw", "")
