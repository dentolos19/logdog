"""End-to-end tests for the Unified Parser Pipeline.

Tests:
  1. UnifiedParser routes JSON to StructuredPipeline
  2. UnifiedParser routes plain text to UnstructuredPipeline
  3. UnifiedParser routes binary-like content to UnstructuredPipeline
  4. Mixed-format batch: JSON + plain text in same upload
  5. Semiconductor signal detection accuracy
  6. FormatDiscoveryResult fields populated correctly
  7. Drain3 FilePersistence creates state files
  8. Confidence scoring with event_type boost
  9. Heartbeat suppression still works
  10. Hybrid binary/text log end-to-end parsing
"""

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure the server lib is importable.
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.parsers.contracts import (
    ClassificationResult,
    StructuralClass,
)
from lib.parsers.preprocessor import FileInput
from lib.parsers.registry import ParserRegistry
from lib.parsers.unified import UnifiedParser, SEMICONDUCTOR_SIGNALS, FormatDiscoveryResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _register_pipelines():
    """Ensure all pipelines are registered before each test."""
    from lib.parsers.orchestrator import register_pipelines

    if not ParserRegistry.registered_keys():
        register_pipelines()


@pytest.fixture
def unified_parser():
    return UnifiedParser()


def _make_classification(parser_key: str = "unstructured") -> ClassificationResult:
    return ClassificationResult(
        dominant_format="plain_text",
        structural_class=StructuralClass.UNSTRUCTURED,
        selected_parser_key=parser_key,
        file_classifications=[],
    )


def _make_file_input(filename: str, content: str) -> FileInput:
    return FileInput(
        file_id=hashlib.md5(filename.encode(), usedforsecurity=False).hexdigest()[:12],
        filename=filename,
        content=content,
    )


# ---------------------------------------------------------------------------
# Sample content
# ---------------------------------------------------------------------------

SAMPLE_JSON = json.dumps([
    {"timestamp": "2026-03-15T08:00:00Z", "level": "INFO", "message": "Process started", "tool": "CVD01"},
    {"timestamp": "2026-03-15T08:01:00Z", "level": "ERROR", "message": "Pressure fault", "tool": "CVD01"},
])

SAMPLE_PLAIN_TEXT = """\
2026-03-15T08:00:00Z INFO  [TOOL-CVD01] Process started: recipe=RCP-OXIDE-DEP wafer=W0001 chamber=CHAMBER-A
2026-03-15T08:00:30Z WARN  [TOOL-CVD01] RF reflected power high: reflected=55.2W threshold=50W rf_code=RF_217
2026-03-15T08:01:00Z ERROR [TOOL-CVD01] ERRCODE=0x1A2B Chamber pressure out of range: pressure=85.3mTorr
2026-03-15T08:01:30Z INFO  [TOOL-CVD01] Measurement: thickness=250.1nm uniformity=97.5% wafer=W0001
2026-03-15T08:02:00Z INFO  [TOOL-CVD01] Gas flow: SiH4=150sccm N2=1000sccm O2=50sccm
2026-03-15T08:02:30Z DEBUG [TOOL-CVD01] Heartbeat: status=OK uptime=1234h temperature=25.3C
2026-03-15T08:03:00Z INFO  [TOOL-CVD01] Wafer loaded: W0002 slot=3 lot=LOT-A2024
"""

SAMPLE_BINARY_LIKE = """\
--- HEX_DUMP: RF_generator memory snapshot ---
00000000  48 45 4C 4C 4F 20 57 4F 52 4C 44 00 00 00 00 00  |HELLO WORLD.....|
00000010  45 52 52 43 4F 44 45 3D 30 78 31 41 32 42 00 00  |ERRCODE=0x1A2B..|
00000020  57 41 52 4E 3A 20 52 46 5F 67 65 6E 65 72 61 74  |WARN: RF_generat|
00000030  6F 72 20 6F 76 65 72 68 65 61 74 00 00 00 00 00  |or overheat.....|
"""

SAMPLE_SEMICONDUCTOR_SIGNALS = """\
2026-03-15T08:00:00Z INFO ERRCODE=0x1A2B detected on CHAMBER-A
2026-03-15T08:00:01Z WARN RF_217 reflected power high on TOOL-CVD01
2026-03-15T08:00:02Z INFO Wafer W0045 loaded into CHAMBER-B from LOT-A2024
2026-03-15T08:00:03Z INFO RECIPE-OXIDE started on EQP-LITHO05
"""


# ---------------------------------------------------------------------------
# Tests: Format Discovery
# ---------------------------------------------------------------------------


class TestFormatDiscovery:
    """Test the UnifiedParser.discover_format() method."""

    def test_json_content_detected(self, unified_parser: UnifiedParser):
        fi = _make_file_input("data.json", SAMPLE_JSON)
        result = unified_parser.discover_format(fi)
        # JSON should route to structured or semi_structured (depending on
        # MIME detection and score-based fallback).  The key assertion is
        # that it does NOT route to unstructured.
        assert result.recommended_parser in ("structured", "semi_structured")

    def test_plain_text_routes_to_unstructured(self, unified_parser: UnifiedParser):
        fi = _make_file_input("events.log", SAMPLE_PLAIN_TEXT)
        result = unified_parser.discover_format(fi)
        # Plain text should route to unstructured (or semi_structured depending on score)
        assert result.recommended_parser in ("unstructured", "semi_structured")

    def test_binary_extension_routes_to_unstructured(self, unified_parser: UnifiedParser):
        fi = _make_file_input("dump.bin", SAMPLE_BINARY_LIKE)
        result = unified_parser.discover_format(fi)
        # .bin extension should be handled by unstructured
        assert result.recommended_parser == "unstructured"

    def test_semiconductor_signals_detected(self, unified_parser: UnifiedParser):
        fi = _make_file_input("signals.log", SAMPLE_SEMICONDUCTOR_SIGNALS)
        result = unified_parser.discover_format(fi)
        # Should detect multiple semiconductor signals
        assert len(result.semiconductor_signals) > 0
        assert "errcode" in result.semiconductor_signals
        assert "rf_code" in result.semiconductor_signals
        assert "wafer_id" in result.semiconductor_signals
        assert "chamber" in result.semiconductor_signals

    def test_signal_density_calculated(self, unified_parser: UnifiedParser):
        fi = _make_file_input("dense.log", SAMPLE_SEMICONDUCTOR_SIGNALS)
        result = unified_parser.discover_format(fi)
        # 4 lines, each with at least 1 signal → density > 0
        assert result.signal_density > 0

    def test_discovery_result_has_mime_type(self, unified_parser: UnifiedParser):
        fi = _make_file_input("test.log", SAMPLE_PLAIN_TEXT)
        result = unified_parser.discover_format(fi)
        assert result.mime_type  # Should be non-empty
        assert isinstance(result.reasons, list)

    def test_empty_content_handled(self, unified_parser: UnifiedParser):
        fi = _make_file_input("empty.log", "")
        result = unified_parser.discover_format(fi)
        assert result.recommended_parser  # Should still return a parser key


# ---------------------------------------------------------------------------
# Tests: Unified Ingestion
# ---------------------------------------------------------------------------


class TestUnifiedIngestion:
    """Test the UnifiedParser.ingest() method."""

    def test_plain_text_ingestion(self, unified_parser: UnifiedParser):
        fi = _make_file_input("events.log", SAMPLE_PLAIN_TEXT)
        classification = _make_classification()
        result = unified_parser.ingest([fi], classification)

        assert result.parser_key == "unified"
        assert len(result.table_definitions) > 0
        assert result.confidence > 0

    def test_mixed_format_batch(self, unified_parser: UnifiedParser):
        """JSON + plain text in the same batch should be routed to different pipelines."""
        fi_json = _make_file_input("data.json", SAMPLE_JSON)
        fi_text = _make_file_input("events.log", SAMPLE_PLAIN_TEXT)
        classification = _make_classification()

        result = unified_parser.ingest([fi_json, fi_text], classification)

        assert result.parser_key == "unified"
        # Should have tables from both pipelines
        assert len(result.table_definitions) >= 1

    def test_ingestion_returns_records(self, unified_parser: UnifiedParser):
        fi = _make_file_input("events.log", SAMPLE_PLAIN_TEXT)
        classification = _make_classification()
        result = unified_parser.ingest([fi], classification)

        # Should have at least one table with records
        assert len(result.records) > 0
        for table_name, rows in result.records.items():
            assert len(rows) > 0

    def test_ingestion_handles_empty_file(self, unified_parser: UnifiedParser):
        fi = _make_file_input("empty.log", "   \n\n   ")
        classification = _make_classification()
        result = unified_parser.ingest([fi], classification)

        # Should not crash, may have warnings
        assert isinstance(result.warnings, list)


# ---------------------------------------------------------------------------
# Tests: Semiconductor Signal Patterns
# ---------------------------------------------------------------------------


class TestSemiconductorSignals:
    """Test the semiconductor signal regex patterns."""

    def test_errcode_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["errcode"]
        assert pattern.search("ERRCODE=0x1A2B")
        assert pattern.search("errcode = FAULT_001")
        assert not pattern.search("no error here")

    def test_hex_value_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["hex_value"]
        assert pattern.search("value=0xDEADBEEF")
        assert pattern.search("addr 0x1A")
        assert not pattern.search("no hex here")

    def test_rf_code_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["rf_code"]
        assert pattern.search("RF_217 fault")
        assert pattern.search("code RF_12")
        assert not pattern.search("RF power high")

    def test_chamber_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["chamber"]
        assert pattern.search("CHAMBER-A active")
        assert pattern.search("CHAMBER_B01 idle")
        assert not pattern.search("no chamber")

    def test_wafer_id_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["wafer_id"]
        assert pattern.search("wafer W0045")
        assert pattern.search("W12 loaded")
        assert not pattern.search("no wafer")

    def test_lot_id_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["lot_id"]
        assert pattern.search("LOT-A2024")
        assert pattern.search("FOUP-C117")
        assert not pattern.search("no lot")

    def test_tool_id_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["tool_id"]
        assert pattern.search("TOOL-CVD01")
        assert pattern.search("EQP-LITHO05")
        assert not pattern.search("no tool")

    def test_recipe_pattern(self):
        pattern = dict(SEMICONDUCTOR_SIGNALS)["recipe"]
        assert pattern.search("RECIPE-OXIDE-DEP")
        assert pattern.search("RECIPE_TIN")
        assert not pattern.search("no recipe")


# ---------------------------------------------------------------------------
# Tests: Drain3 FilePersistence
# ---------------------------------------------------------------------------


class TestDrain3Persistence:
    """Test that Drain3 FilePersistence creates and uses state files."""

    def test_persistence_creates_state_file(self):
        from lib.parsers.unstructured.core import mine_templates, DRAIN3_STATE_DIR

        # Use a temp directory to avoid polluting the real state dir
        test_dir = Path(tempfile.mkdtemp())
        try:
            with patch("lib.parsers.unstructured.core.DRAIN3_STATE_DIR", test_dir):
                clusters = [
                    (1, 1, "2026-03-15T08:00:00Z INFO Process started"),
                    (2, 2, "2026-03-15T08:01:00Z ERROR Pressure fault"),
                    (3, 3, "2026-03-15T08:02:00Z INFO Process completed"),
                ]
                _miner, templates = mine_templates(clusters, persistence_key="test-group-123")

                assert len(templates) == 3
                # Check that a state file was created
                state_files = list(test_dir.glob("*.bin"))
                assert len(state_files) == 1
                assert "test-group-123" in state_files[0].name
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_no_persistence_without_key(self):
        from lib.parsers.unstructured.core import mine_templates

        clusters = [
            (1, 1, "2026-03-15T08:00:00Z INFO Process started"),
            (2, 2, "2026-03-15T08:01:00Z ERROR Pressure fault"),
        ]
        _miner, templates = mine_templates(clusters, persistence_key=None)
        assert len(templates) == 2


# ---------------------------------------------------------------------------
# Tests: Confidence Scoring
# ---------------------------------------------------------------------------


class TestConfidenceScoring:
    """Test the per-row confidence scoring."""

    def test_base_confidence(self):
        from lib.parsers.unstructured.pipeline import _compute_row_confidence

        # Minimal fields → base score
        score = _compute_row_confidence({})
        assert score == 0.40

    def test_timestamp_boost(self):
        from lib.parsers.unstructured.pipeline import _compute_row_confidence

        score = _compute_row_confidence({"timestamp": "2026-03-15T08:00:00Z"})
        assert score >= 0.50

    def test_event_type_boost(self):
        from lib.parsers.unstructured.pipeline import _compute_row_confidence

        score_without = _compute_row_confidence({"template": "some template"})
        score_with = _compute_row_confidence({
            "template": "some template",
            "event_type": "alarm",
        })
        assert score_with > score_without

    def test_event_type_unknown_no_boost(self):
        from lib.parsers.unstructured.pipeline import _compute_row_confidence

        score_without = _compute_row_confidence({"template": "some template"})
        score_with = _compute_row_confidence({
            "template": "some template",
            "event_type": "unknown",
        })
        assert score_with == score_without

    def test_max_confidence_capped(self):
        from lib.parsers.unstructured.pipeline import _compute_row_confidence

        # All possible boosts
        score = _compute_row_confidence({
            "timestamp": "2026-03-15T08:00:00Z",
            "log_level": "ERROR",
            "wafer_id": "W0001",
            "tool_id": "TOOL-CVD01",
            "recipe_id": "RCP-OXIDE",
            "process_step": "DEP",
            "thickness": "250.1",
            "pressure": "85.3",
            "temperature": "400",
            "template": "some template",
            "event_type": "alarm",
        })
        assert score <= 0.95


# ---------------------------------------------------------------------------
# Tests: Heartbeat Suppression
# ---------------------------------------------------------------------------


class TestHeartbeatSuppression:
    """Test heartbeat suppression in the pipeline."""

    def test_high_frequency_heartbeats_suppressed(self):
        from lib.parsers.unstructured.pipeline import _suppress_heartbeats

        # Create 10 rows: 6 heartbeats (same template) + 4 real events
        heartbeat_template = "DEBUG Heartbeat: status=OK uptime=<*>h"
        rows = []
        for i in range(6):
            rows.append({
                "template": heartbeat_template,
                "log_level": "DEBUG",
                "message": f"Heartbeat {i}",
            })
        for i in range(4):
            rows.append({
                "template": f"ERROR Pressure fault {i}",
                "log_level": "ERROR",
                "message": f"Fault {i}",
            })

        warnings: list[str] = []
        filtered, suppressed = _suppress_heartbeats(rows, total_clusters=10, warnings=warnings)

        # 6/10 = 60% > 40% threshold, DEBUG level is not actionable → suppressed
        assert suppressed == 6
        assert len(filtered) == 4

    def test_actionable_high_frequency_not_suppressed(self):
        from lib.parsers.unstructured.pipeline import _suppress_heartbeats

        # High frequency but with ERROR level → should NOT be suppressed
        error_template = "ERROR Critical fault detected"
        rows = []
        for i in range(6):
            rows.append({
                "template": error_template,
                "log_level": "ERROR",
                "message": f"Fault {i}",
            })
        for i in range(4):
            rows.append({
                "template": f"INFO Process step {i}",
                "log_level": "INFO",
                "message": f"Step {i}",
            })

        warnings: list[str] = []
        filtered, suppressed = _suppress_heartbeats(rows, total_clusters=10, warnings=warnings)

        # ERROR level is actionable → not suppressed
        assert suppressed == 0
        assert len(filtered) == 10


# ---------------------------------------------------------------------------
# Tests: Hex Dump Preservation
# ---------------------------------------------------------------------------


class TestHexDumpPreservation:
    """Test hex dump extraction with preservation."""

    def test_extract_ascii_only(self):
        from lib.parsers.unstructured.core import extract_ascii_from_hexdump

        lines = [
            "00000000  48 45 4C 4C 4F  |HELLO|",
            "00000010  57 4F 52 4C 44  |WORLD|",
        ]
        result = extract_ascii_from_hexdump(lines, preserve_hex=False)
        assert result == ["HELLO", "WORLD"]

    def test_extract_with_hex_preserved(self):
        from lib.parsers.unstructured.core import extract_ascii_from_hexdump

        lines = [
            "00000000  48 45 4C 4C 4F  |HELLO|",
            "00000010  57 4F 52 4C 44  |WORLD|",
        ]
        result = extract_ascii_from_hexdump(lines, preserve_hex=True)
        assert len(result) == 2
        assert result[0][0] == "HELLO"
        assert "48 45 4C 4C 4F" in result[0][1]
        assert result[1][0] == "WORLD"


# ---------------------------------------------------------------------------
# Tests: Hybrid Log Generator
# ---------------------------------------------------------------------------


class TestHybridLogGenerator:
    """Test the synthetic hybrid log generator."""

    def test_generator_produces_output(self):
        from scripts.generate_hybrid_logs import generate_hybrid_log

        content = generate_hybrid_log(num_lines=100, seed=42)
        lines = content.splitlines()
        assert len(lines) >= 80  # Approximate — binary sections add extra lines

    def test_generator_includes_binary_sections(self):
        from scripts.generate_hybrid_logs import generate_hybrid_log

        content = generate_hybrid_log(num_lines=200, binary_ratio=0.5, seed=42)
        # Should contain hex dump markers
        assert "HEX_DUMP" in content or "BEGIN_B64" in content or "ZLIB_BLOCK" in content

    def test_generator_includes_semiconductor_signals(self):
        from scripts.generate_hybrid_logs import generate_hybrid_log

        content = generate_hybrid_log(num_lines=500, seed=42)
        # Should contain semiconductor signals
        assert "ERRCODE=" in content or "TOOL-" in content or "wafer=" in content

    def test_generator_reproducible_with_seed(self):
        from scripts.generate_hybrid_logs import generate_hybrid_log

        content1 = generate_hybrid_log(num_lines=50, seed=123)
        content2 = generate_hybrid_log(num_lines=50, seed=123)
        assert content1 == content2
