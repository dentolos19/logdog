"""Tests for the unstructured log parser (lib/unstructured_parser.py).

Covers:
  - Encoding detection
  - Noise filtering
  - Multi-line clustering
  - Drain3 template mining
  - Heuristic field extraction (including semiconductor-domain fields)
  - Column inference
  - Integration with the preprocessor for PLAIN_TEXT format
"""

from unittest.mock import patch

import pytest
from lib.parsers.preprocessor import (
    FileInput,
    LogPreprocessorService,
)
from lib.parsers.unstructured_parser import (
    _decode_base64_frames,
    _decode_hex_telemetry,
    _decode_zlib,
    _extract_cleartext_signals,
    cluster_multiline,
    decode_binary_content,
    decode_content,
    detect_encoding,
    extract_ascii_from_hexdump,
    extract_fields_heuristic,
    extract_unstructured_columns,
    extract_unstructured_samples,
    filter_noise,
    infer_columns_from_fields,
    is_binary_content,
    is_hex_dump_line,
    mine_templates,
    preprocess_binary_input,
)

# ---------------------------------------------------------------------------
# Fixtures & sample data
# ---------------------------------------------------------------------------

SEMI_LOG_LINES = [
    "2025-06-15 08:00:01.123 INFO [EQP-CVD-01] Wafer W0045 loaded into chamber. Recipe=RCP-OX-THIN Step=DEPOSITION",
    "2025-06-15 08:00:15.456 INFO [EQP-CVD-01] Gas flow established: N2=200sccm. Stability check PASSED.",
    "2025-06-15 08:01:30.789 WARN [EQP-CVD-01] ALARM: Temperature deviation on W0045. Actual=455°C Expected=450°C delta=5.0°C",
    "2025-06-15 08:03:00.012 INFO [EQP-CVD-01] Step DEPOSITION completed. Duration=120s. Endpoint detected at t=115s",
    "2025-06-15 08:05:00.345 INFO [EQP-CVD-01] Metrology readout for W0045: thickness=102.3nm uniformity=98.5%",
    "2025-06-15 08:06:00.678 INFO [EQP-CVD-01] Wafer W0045 unloaded. Total process time=360s. Result=OK",
]

STACKTRACE_LINES = [
    "2025-01-01 00:00:00.123 ERROR [main] com.example.App - Unhandled exception",
    "java.lang.NullPointerException: null",
    "\tat com.example.App.process(App.java:42)",
    "\tat com.example.App.main(App.java:10)",
    "Caused by: java.io.IOException: Connection refused",
    "\tat java.net.Socket.connect(Socket.java:591)",
    "2025-01-01 00:00:01.456 INFO [main] com.example.App - Retrying in 5s",
]

PLAIN_TEXT_LINES = [
    "Application startup complete.",
    "Listening on port 8080.",
    "Ready to accept connections.",
]

NOISY_LINES = [
    "   ",
    "---",
    "===",
    "###",
    "Hello world",
    "",
    "Another line",
]


@pytest.fixture()
def service() -> LogPreprocessorService:
    return LogPreprocessorService(table_name="test_entries")


# ---------------------------------------------------------------------------
# Encoding detection
# ---------------------------------------------------------------------------


class TestEncoding:
    def test_detect_utf8(self) -> None:
        data = "Hello world".encode("utf-8")
        assert detect_encoding(data) in ("utf-8", "ascii", "utf_8")

    def test_decode_empty(self) -> None:
        assert decode_content(b"") == ""

    def test_decode_utf8(self) -> None:
        text = "Héllo wörld"
        assert decode_content(text.encode("utf-8")) == text

    def test_decode_bad_bytes(self) -> None:
        # Invalid UTF-8 sequence — should not raise.
        result = decode_content(b"\xff\xfe\x00\x01")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Noise filter
# ---------------------------------------------------------------------------


class TestNoiseFilter:
    def test_removes_blank_and_separator_lines(self) -> None:
        filtered = filter_noise(NOISY_LINES)
        assert "Hello world" in filtered
        assert "Another line" in filtered
        assert len(filtered) == 2

    def test_keeps_content_lines(self) -> None:
        filtered = filter_noise(SEMI_LOG_LINES)
        assert len(filtered) == len(SEMI_LOG_LINES)

    def test_removes_binary_lines(self) -> None:
        lines = ["Hello world", "\xff\xfe\x00\x01\x02\x03\x04\x05\x06", "Good line"]
        filtered = filter_noise(lines)
        assert len(filtered) == 2
        assert "Hello world" in filtered


# ---------------------------------------------------------------------------
# Multiline clustering
# ---------------------------------------------------------------------------


class TestMultilineClustering:
    def test_stacktrace_cluster(self) -> None:
        clusters = cluster_multiline(STACKTRACE_LINES)
        # ERROR line is a standalone entry; exception + stack trace is cluster 2; retry line is cluster 3.
        assert len(clusters) == 3
        # Second cluster contains the exception + stack trace lines.
        assert "NullPointerException" in clusters[1][2]
        assert "com.example.App.process" in clusters[1][2]

    def test_simple_lines_one_per_cluster(self) -> None:
        clusters = cluster_multiline(PLAIN_TEXT_LINES)
        assert len(clusters) == len(PLAIN_TEXT_LINES)


# ---------------------------------------------------------------------------
# Drain3 template mining
# ---------------------------------------------------------------------------


class TestTemplateMining:
    def test_mines_templates(self) -> None:
        clusters = cluster_multiline(SEMI_LOG_LINES)
        miner, templates = mine_templates(clusters)
        assert len(templates) == len(clusters)
        # Templates should contain <*> wildcards for variable parts.
        assert any("<*>" in t for t in templates)

    def test_similar_lines_merge_templates(self) -> None:
        # Drain3 should cluster similar lines.
        lines = [
            "2025-01-01 INFO Server started on port 8080",
            "2025-01-02 INFO Server started on port 9090",
            "2025-01-03 INFO Server started on port 3000",
        ]
        clusters = cluster_multiline(lines)
        miner, templates = mine_templates(clusters)
        # The last template should be a parameterised pattern.
        assert "<*>" in templates[-1]


# ---------------------------------------------------------------------------
# Heuristic field extraction
# ---------------------------------------------------------------------------


class TestFieldExtraction:
    def test_extracts_timestamp(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        assert "timestamp_raw" in fields

    def test_extracts_log_level(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        assert fields.get("log_level") == "INFO"

    def test_extracts_wafer_id(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        assert fields.get("wafer_id") == "W0045"

    def test_extracts_recipe(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        assert "recipe_id" in fields

    def test_extracts_step(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        assert "process_step" in fields

    def test_extracts_tool_id(self) -> None:
        fields = extract_fields_heuristic("2025-06-15 08:00:00 INFO [EQP-CVD-01] Equipment ready.")
        assert "tool_id" in fields

    def test_field_aliases(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[0])
        # Aliases should match the canonical names.
        assert fields.get("wafer") == fields.get("wafer_id")
        assert fields.get("tool") == fields.get("tool_id")
        assert fields.get("recipe") == fields.get("recipe_id")

    def test_measurement_extraction(self) -> None:
        fields = extract_fields_heuristic("2025-06-15 08:05:00 INFO for W0113: thickness=348.5nm uniformity=98.5%")
        assert fields.get("thickness") == "348.5"
        assert fields.get("uniformity") == "98.5"

    def test_kv_no_clobber_measurements(self) -> None:
        """Generic KV should not swallow measurement values."""
        fields = extract_fields_heuristic("Readout W0113: thickness=102.3nm pressure=5mTorr")
        assert fields.get("thickness") == "102.3"
        # w0113 should NOT have 'thickness=102.3nm' as its value.
        assert fields.get("w0113") is None

    def test_warn_level(self) -> None:
        fields = extract_fields_heuristic(SEMI_LOG_LINES[2])
        assert fields.get("log_level") == "WARN"


# ---------------------------------------------------------------------------
# Column inference
# ---------------------------------------------------------------------------


class TestColumnInference:
    def test_infers_columns_from_fields(self) -> None:
        all_fields = [extract_fields_heuristic(line) for line in SEMI_LOG_LINES]
        columns = infer_columns_from_fields(all_fields)
        col_names = {c.name for c in columns}
        # wafer_id appears in multiple lines.
        assert "wafer_id" in col_names

    def test_empty_fields(self) -> None:
        assert infer_columns_from_fields([]) == []


# ---------------------------------------------------------------------------
# Full pipeline: extract_unstructured_columns
# ---------------------------------------------------------------------------


class TestExtractUnstructuredColumns:
    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_returns_columns_for_semi_logs(self) -> None:
        columns = extract_unstructured_columns(SEMI_LOG_LINES)
        col_names = {c.name for c in columns}
        # Should always include template columns from Drain3.
        assert "template" in col_names
        assert "template_cluster_id" in col_names

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_returns_empty_for_empty_input(self) -> None:
        assert extract_unstructured_columns([]) == []

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_returns_empty_for_all_noise(self) -> None:
        assert extract_unstructured_columns(["---", "===", "   "]) == []


# ---------------------------------------------------------------------------
# Sample extraction
# ---------------------------------------------------------------------------


class TestSampleExtraction:
    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_extracts_samples(self) -> None:
        columns = extract_unstructured_columns(SEMI_LOG_LINES)
        col_names = {c.name for c in columns} | {"raw_text", "source", "message"}
        samples = extract_unstructured_samples(SEMI_LOG_LINES, "test.log", col_names)
        assert len(samples) > 0
        assert samples[0].source_file == "test.log"
        assert "raw_text" in samples[0].fields


# ---------------------------------------------------------------------------
# Integration with LogPreprocessorService
# ---------------------------------------------------------------------------


class TestPreprocessorIntegration:
    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_plain_text_goes_through_unstructured_parser(self, service: LogPreprocessorService) -> None:
        """When the preprocessor detects PLAIN_TEXT, our unstructured parser should provide columns."""
        # Use truly unstructured text that won't match JSON/CSV/syslog/key=value.
        plain_lines = [
            "System initialization complete.",
            "Loading wafer cassette into loadlock.",
            "Pump-down sequence started.",
            "Base pressure reached: 2.5e-6 Torr.",
            "Process chamber ready.",
        ]
        content = "\n".join(plain_lines)
        file_input = FileInput(filename="fab_tool.log", content=content)

        # Force LLM off in the main preprocessor too.
        service._llm_available = False
        result = service.preprocess([file_input])

        col_names = {c.name for c in result.columns}
        # Template columns should always be included from Drain3 mining.
        assert "template" in col_names or "template_cluster_id" in col_names

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_baseline_columns_preserved(self, service: LogPreprocessorService) -> None:
        file_input = FileInput(filename="plain.log", content="\n".join(PLAIN_TEXT_LINES))
        service._llm_available = False
        result = service.preprocess([file_input])

        col_names = {c.name for c in result.columns}
        assert "id" in col_names
        assert "timestamp" in col_names
        assert "message" in col_names
        assert "raw_text" in col_names


# ---------------------------------------------------------------------------
# Binary / hex-dump handling
# ---------------------------------------------------------------------------


class TestBinaryHexHandling:
    def test_is_binary_content_true(self) -> None:
        assert is_binary_content("\xff\xfe\x00\x01\x02\x03\x04\x05\x06") is True

    def test_is_binary_content_false(self) -> None:
        assert is_binary_content("Hello world") is False
        assert is_binary_content("") is False

    def test_is_hex_dump_line(self) -> None:
        assert is_hex_dump_line("00000000  46 52 4D 31 01 06 42 45  |FRM1..BE|") is True
        assert is_hex_dump_line("Hello world") is False

    def test_extract_ascii_from_hexdump(self) -> None:
        lines = [
            "00000000  46 52 4D 31 01 06 42 45 47 49 4E 5F 42 36 34 0A  |FRM1..BEGIN_B64.|",
            "00000010  4C 33 4D 43 4D 6D 44 42 30 78 53 4B 62 64 54 66  |L3MCMmDB0xSKbdTf|",
        ]
        ascii_parts = extract_ascii_from_hexdump(lines)
        assert len(ascii_parts) == 2
        assert "FRM1" in ascii_parts[0]
        assert "L3MCMmDB0xSKbdTf" in ascii_parts[1]

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_hex_dump_content_processed(self) -> None:
        """Hex dump content should be converted to ASCII before pipeline."""
        hex_lines = [
            "00000000  48 45 4C 4C 4F 20 57 4F 52 4C 44 20 4C 4F 47 53  |HELLO WORLD LOGS|",
            "00000010  53 45 52 56 45 52 20 53 54 41 52 54 45 44 20 4F  |SERVER STARTED O|",
            "00000020  4E 20 50 4F 52 54 20 38 30 38 30 20 4F 4B 20 2E  |N PORT 8080 OK .|",
        ]
        columns = extract_unstructured_columns(hex_lines)
        # Should not crash and should return some columns (at least template).
        col_names = {c.name for c in columns}
        assert "template" in col_names or "template_cluster_id" in col_names

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_pure_binary_returns_empty(self) -> None:
        """Lines that are entirely binary noise should produce no columns."""
        binary_lines = ["\xff\xfe\x00\x01" * 10, "\x80\x81\x82\x83" * 10]
        columns = extract_unstructured_columns(binary_lines)
        assert columns == []


# ---------------------------------------------------------------------------
# Binary decoder tests (Chaos Tier)
# ---------------------------------------------------------------------------


class TestZlibDecoder:
    def test_decompress_valid_zlib(self) -> None:
        """Detect and decompress zlib payload embedded in binary data."""
        import zlib as _zlib

        payload = b'{"error_code": "RF_217", "message": "Reflected power high"}'
        compressed = _zlib.compress(payload)
        # Embed in binary noise with ZLIB marker.
        raw = b"\xde\xad\xbe\xef" + b"ZLIB\x00\x00\x00" + compressed + b"\xff" * 10
        lines = _decode_zlib(raw)
        assert len(lines) >= 1
        assert "RF_217" in lines[0]

    def test_no_zlib_returns_empty(self) -> None:
        raw = b"Hello this is plain text without any zlib"
        assert _decode_zlib(raw) == []

    def test_real_zlib_file(self) -> None:
        """Test with the actual zlib_embedded_02.bin structure."""
        import zlib as _zlib

        inner = b'{"timestamp": "2026-03-05T09:41:12Z", "tool": "Vendor3", "error_code": "RF_217"}'
        compressed = _zlib.compress(inner)
        raw = b"\xde\xad" * 20 + b"ZLIB\x00\x00\x00\xd2" + compressed
        lines = _decode_zlib(raw)
        assert any("RF_217" in line for line in lines)


class TestBase64FrameDecoder:
    def test_decode_framed_base64(self) -> None:
        """Extract and decode Base64 content between markers."""
        import base64 as _b64

        payload = _b64.b64encode(b"Decoded sensor data payload").decode()
        raw = f"FRM1\x01\x06BEGIN_B64\n{payload}\nEND_B64\nCA159CCE".encode()
        lines = _decode_base64_frames(raw)
        assert any("payload_encoding=base64" in l for l in lines)
        assert any("Decoded sensor data payload" in l for l in lines)
        assert any("checksum=CA159CCE" in l for l in lines)

    def test_no_markers_returns_empty(self) -> None:
        raw = b"No base64 framing here"
        assert _decode_base64_frames(raw) == []

    def test_binary_payload_hex_preview(self) -> None:
        """Non-UTF-8 decoded payload should produce hex preview."""
        import base64 as _b64

        payload = _b64.b64encode(b"\xff\xfe\x00\x01\x02\x03").decode()
        raw = f"BEGIN_B64\n{payload}\nEND_B64\n".encode()
        lines = _decode_base64_frames(raw)
        assert any("decoded_hex_preview=" in l or "decoded_size=" in l for l in lines)


class TestHexTelemetryDecoder:
    def test_decode_hex_stream(self) -> None:
        """Parse continuous hex-as-text into sensor records."""
        # Build a valid telemetry stream: HEX_DUMP header + hex chars.
        import struct as _struct

        records = []
        for i in range(4):
            sensor = bytes([0x69, 0xA9, 0x1E, 0x4A + i])
            reading = _struct.pack(">f", 83.5 + i)
            padding = bytes([0x00, i])
            records.append(sensor + reading + padding)
        hex_stream = "".join(r.hex().upper() for r in records)
        raw = f"HEX_DUMP\n{hex_stream}".encode()
        lines = _decode_hex_telemetry(raw)
        assert any("data_format=hex_telemetry" in l for l in lines)
        assert any("sensor=" in l for l in lines)

    def test_no_hex_stream_returns_empty(self) -> None:
        raw = b"Just plain text, nothing hex here"
        assert _decode_hex_telemetry(raw) == []


class TestCleartextSignalExtractor:
    def test_extract_errcode(self) -> None:
        """Extract ERRCODE signals from binary noise."""
        raw = b"\xa6\xc1\x86\xab" + b"\x00\x00ERRCODE=TMP_901\x00" + b"\xcd\xb7" * 10
        lines = _extract_cleartext_signals(raw)
        assert any("TMP_901" in l for l in lines)

    def test_extract_warn(self) -> None:
        """Extract WARN messages from binary noise."""
        raw = b"\xff" * 20 + b"\x00WARN: pressure drift detected\x00" + b"\x83" * 10
        lines = _extract_cleartext_signals(raw)
        assert any("pressure drift" in l for l in lines)

    def test_extract_evt_patterns(self) -> None:
        """Extract EVT code=X msg=Y patterns from binary blobs."""
        raw = b"\x00" * 5 + b"EVT0 code=E100 msg=Valve OPEN\x00" + b"\xff" * 5
        lines = _extract_cleartext_signals(raw)
        assert any("code=E100" in l for l in lines)

    def test_no_signals_in_pure_binary(self) -> None:
        raw = bytes(range(256))  # All byte values, few long printable runs.
        lines = _extract_cleartext_signals(raw)
        # Should not produce meaningful lines from random bytes.
        meaningful = [l for l in lines if "ERRCODE" in l or "WARN" in l or "code=" in l]
        assert meaningful == []


class TestMasterBinaryDecoder:
    def test_decode_binary_content_combines_outputs(self) -> None:
        """Master decoder should run all sub-decoders."""
        raw = b"\xff" * 20 + b"\x00ERRCODE=GAS_12\x00" + b"\xff" * 10
        lines = decode_binary_content(raw)
        assert any("GAS_12" in l for l in lines)

    def test_preprocess_binary_input_text(self) -> None:
        """Regular text content should pass through unchanged."""
        raw = b"2025-06-15 INFO Process started\n2025-06-15 WARN Temp high\n"
        lines = preprocess_binary_input(raw)
        assert len(lines) == 2
        assert "Process started" in lines[0]

    def test_preprocess_binary_input_binary(self) -> None:
        """Binary content should go through decoder pipeline."""
        raw = b"\xff" * 30 + b"\x00ERRCODE=VAC_07\x00Vacuum loss\x00" + b"\xab" * 30
        lines = preprocess_binary_input(raw)
        assert any("VAC_07" in l for l in lines)

    @patch("lib.ai.OPENROUTER_API_KEY", "")
    def test_tool_event_blob_drain3(self) -> None:
        """Valve OPEN/CLOSE events should be extractable and templatable."""
        # Simulate tool_event_blob_01.bin structure.
        events = [
            b"\x00" * 5 + b"EVT0 code=E100 msg=Valve OPEN\x00",
            b"\x00" * 5 + b"EVT1 code=E101 msg=Valve CLOSE\x00",
            b"\x00" * 5 + b"EVT2 code=E102 msg=Valve OPEN\x00",
            b"\x00" * 5 + b"EVT3 code=E103 msg=Valve CLOSE\x00",
        ]
        raw = b"ULP0\x01TOOLX" + b"".join(events)
        lines = preprocess_binary_input(raw)
        assert any("Valve" in l for l in lines)

        # Run through Drain3 to verify template mining.
        from lib.parsers.unstructured_parser import cluster_multiline, mine_templates

        valve_lines = [l for l in lines if "Valve" in l]
        if len(valve_lines) >= 2:
            clusters = cluster_multiline(valve_lines)
            _, templates = mine_templates(clusters)
            # Drain3 should create a template with <*> for the variable parts.
            assert any("<*>" in t for t in templates)
