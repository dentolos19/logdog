"""Unstructured / plain-text log parser.

Handles log files detected as PLAIN_TEXT or UNKNOWN by the preprocessor.
Uses a multi-stage pipeline:
  1. Encoding detection & normalization  (chardet)
  2. Noise filtering                     (regex-based)
  3. Multi-line record clustering         (indentation / continuation)
  4. Log template mining                  (Drain3)
  5. Semantic field extraction            (LLM via langchain-openrouter)

All public helpers are consumed by ``LogPreprocessorService`` in
``lib/preprocessor.py`` and follow the same column / sample conventions.
"""

import base64
import hashlib
import logging
import os
import re
import struct
import zlib
from typing import Any

import chardet
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from langchain_openrouter import ChatOpenRouter
from lib.parsers.preprocessor import (
    ColumnKind,
    InferredColumn,
    SampleRecord,
    SqlType,
)
from pydantic import BaseModel, Field, SecretStr

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment / constants
# ---------------------------------------------------------------------------

APP_NAME = os.getenv("APP_NAME", "Logdog")
APP_URL = os.getenv("APP_URL")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "inception/mercury-2")

MAX_SAMPLE_LINES = 30
MAX_LINE_LENGTH = 2000

# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Lines that are pure noise (empty, only whitespace / dashes / equals).
NOISE_LINE_RE = re.compile(r"^[\s\-=*#~]{0,3}$")

# Continuation: indented, "at ...", "Caused by:", "... N more"
CONTINUATION_RE = re.compile(r"^(?:\s+at\s|Caused by:|\.{3}\s*\d+\s*more|\s{4,}\S|\t\S)")

# Semiconductor-domain timestamp patterns (broader than ISO-8601).
TIMESTAMP_RE = re.compile(
    r"(?P<ts>"
    r"\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
    r"|\d{2}[-/]\w{3}[-/]\d{4}[: ]\d{2}:\d{2}:\d{2}"
    r"|\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}"
    r")"
)

LOG_LEVEL_RE = re.compile(
    r"\b(?P<level>TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|NOTICE|ALERT|EMERG(?:ENCY)?)\b",
    re.IGNORECASE,
)

# Common key=value or key: value patterns.
KV_RE = re.compile(r"(?P<key>\w[\w.]*)\s*[=:]\s*(?:\"(?P<qval>[^\"]*)\"|(?P<val>\S+))")

# Words to skip as KV keys — common prepositions, articles, and operator names
# that appear in prose before a colon (e.g., "Operator jchen: ...").
_KV_STOP_WORDS = frozenset(
    {
        "at",
        "by",
        "in",
        "on",
        "to",
        "is",
        "of",
        "for",
        "the",
        "and",
        "or",
        "not",
        "no",
        "vs",
        "was",
        "has",
        "had",
        "per",
        "via",
        "re",
        # Avoid capturing single-letter keys.
        "a",
        "e",
        "i",
        "o",
        "u",
        "x",
    }
)

# Semiconductor-specific patterns.
WAFER_ID_RE = re.compile(r"\b(?P<wafer>W\d{2,4}|LOT[_-]?\w+|FOUP[_-]?\w+)\b", re.IGNORECASE)
TOOL_ID_RE = re.compile(r"\b(?P<tool>(?:TOOL|EQP|CHAMBER)[_-]?\w+)\b", re.IGNORECASE)
RECIPE_RE = re.compile(r"\b(?P<recipe>(?:RECIPE|RCP)[_-]?\w+)\b", re.IGNORECASE)
STEP_RE = re.compile(r"\b(?:STEP|PHASE)[_:= ]+(?P<step>(?!SiH4|CF4|N2|O2|Ar|He)\w+)\b", re.IGNORECASE)

# Semiconductor measurement patterns (matched before generic KV).
MEASUREMENT_RE = re.compile(
    r"\b(?P<key>thickness|pressure|temperature|temp|flow|gas_flow|power|"
    r"voltage|current|rpm|dose|energy|frequency|bias|uniformity|vacuum|"
    r"rf_power|reflected|delta|duration|threshold|rate|particle_count|"
    r"vibration|resistivity)\s*[=:]\s*"
    r"(?P<val>[\d.eE+-]+\s*(?:nm|um|\xb5m|mm|cm|mTorr|Torr|Pa|\xb0C|C|K|sccm|slm|W|kW|V|mV|A|mA|rpm|Hz|kHz|MHz|%|s|ms|MOhm[\w/-]*|mm/s|C/min|E-\d+)?)\.?",
    re.IGNORECASE,
)

# Strip trailing unit suffixes from numeric measurement values.
UNIT_STRIP_RE = re.compile(
    r"^(?P<num>[\d.eE+-]+)\s*"
    r"(?:nm|um|\xb5m|mm|cm|m|mTorr|Torr|Pa|\xb0C|C|K|sccm|slm|W|kW|V|mV|A|mA|rpm|Hz|kHz|MHz|%|s|ms|MOhm[\w/-]*|mm/s|C/min)?"
    r"\.*$",
    re.IGNORECASE,
)

# Known measurement field names (for REAL type inference).
MEASUREMENT_FIELD_NAMES = {
    "thickness",
    "pressure",
    "temperature",
    "temp",
    "flow",
    "gas_flow",
    "power",
    "voltage",
    "current",
    "rpm",
    "dose",
    "energy",
    "frequency",
    "bias",
    "uniformity",
    "vacuum",
    "rf_power",
    "reflected",
    "delta",
    "duration",
    "rate",
    "threshold",
    "vibration",
    "resistivity",
    "particle_count",
    "target",
}

# Hex dump line: offset followed by hex bytes.
HEX_DUMP_RE = re.compile(r"^[0-9A-Fa-f]{4,8}\s+(?:[0-9A-Fa-f]{2}\s){4,}")

# ASCII portion of hex dump: |printable chars|
HEX_DUMP_ASCII_RE = re.compile(r"\|([^|]+)\|\s*$")

# Continuous hex-as-text: long runs of hex characters (telemetry dumps).
CONTINUOUS_HEX_RE = re.compile(r"^[0-9A-Fa-f]{16,}$")

# Base64 frame markers.
B64_BEGIN_RE = re.compile(r"BEGIN_B64|BASE64_START|B64_BEGIN", re.IGNORECASE)
B64_END_RE = re.compile(r"END_B64|BASE64_END|B64_END", re.IGNORECASE)

# Cleartext signal in binary noise: printable ASCII runs of 4+ chars.
CLEARTEXT_SIGNAL_RE = re.compile(rb"[\x20-\x7e]{4,}")

# ERRCODE / WARN patterns in binary cleartext.
ERRCODE_RE = re.compile(r"ERRCODE\s*=\s*(?P<errcode>\w+)", re.IGNORECASE)
WARN_MSG_RE = re.compile(r"WARN:\s*(?P<warn_msg>.+?)(?:\x00|$)")

# Zlib magic bytes (78 9C = default compression, 78 01 = no/low, 78 DA = best).
ZLIB_MAGIC = (b"\x78\x9c", b"\x78\x01", b"\x78\xda")


# ---------------------------------------------------------------------------
# LLM response schema (structured output)
# ---------------------------------------------------------------------------


class LlmFieldExtraction(BaseModel):
    """One field extracted by the LLM from unstructured text."""

    name: str
    sql_type: str = "TEXT"
    description: str = ""
    example_values: list[str] = Field(default_factory=list)


class LlmUnstructuredResponse(BaseModel):
    """Structured LLM response for unstructured log analysis."""

    fields: list[LlmFieldExtraction] = Field(default_factory=list)
    summary: str = ""
    event_type_hint: str = ""
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Binary / hex-dump helpers
# ---------------------------------------------------------------------------


def is_binary_content(line: str) -> bool:
    """Return True if line has a high ratio of non-printable characters."""
    if not line:
        return False
    non_printable = sum(1 for c in line if not c.isprintable() and c not in "\n\r\t")
    return non_printable / max(len(line), 1) > 0.3


def is_hex_dump_line(line: str) -> bool:
    """Return True if line matches standard hex dump format."""
    return bool(HEX_DUMP_RE.match(line.strip()))


def extract_ascii_from_hexdump(lines: list[str]) -> list[str]:
    """Extract ASCII representations from hex dump lines.

    Lines like: ``00000000  48 45 58  |HEX|``
    Returns: ``['HEX']``
    """
    ascii_parts: list[str] = []
    for line in lines:
        m = HEX_DUMP_ASCII_RE.search(line)
        if m:
            ascii_text = m.group(1).strip()
            if ascii_text:
                ascii_parts.append(ascii_text)
    return ascii_parts


# ---------------------------------------------------------------------------
# Binary content decoders (Chaos Tier)
# ---------------------------------------------------------------------------


def decode_binary_content(raw_bytes: bytes) -> list[str]:
    """Master decoder: detect binary sub-formats and convert to text lines.

    Applies decoders in priority order:
      1. Zlib compressed payload  (signature: ``ZLIB`` marker or ``78 9C`` magic)
      2. Base64 framed payload    (``BEGIN_B64`` / ``END_B64`` markers)
      3. Continuous hex-as-text   (``HEX_DUMP`` header + hex char stream)
      4. Signal-in-noise          (cleartext islands in binary noise)

    Returns a list of text lines suitable for the main parsing pipeline.
    """
    decoded_lines: list[str] = []

    # --- 1. Zlib decompression ---
    zlib_lines = _decode_zlib(raw_bytes)
    if zlib_lines:
        decoded_lines.extend(zlib_lines)

    # --- 2. Base64 framed extraction ---
    b64_lines = _decode_base64_frames(raw_bytes)
    if b64_lines:
        decoded_lines.extend(b64_lines)

    # --- 3. Continuous hex-as-text telemetry ---
    hex_lines = _decode_hex_telemetry(raw_bytes)
    if hex_lines:
        decoded_lines.extend(hex_lines)

    # --- 4. Signal extraction from binary noise ---
    signal_lines = _extract_cleartext_signals(raw_bytes)
    if signal_lines:
        decoded_lines.extend(signal_lines)

    return decoded_lines


def _decode_zlib(raw_bytes: bytes) -> list[str]:
    """Detect and decompress zlib-compressed payloads embedded in binary data.

    Searches for the ZLIB text marker or the ``78 9C`` magic bytes.
    If decompression succeeds, splits the result into text lines.
    """
    lines: list[str] = []

    for magic in ZLIB_MAGIC:
        offset = raw_bytes.find(magic)
        if offset < 0:
            continue
        try:
            decompressed = zlib.decompress(raw_bytes[offset:])
            text = decompressed.decode("utf-8", errors="replace")
            lines.extend(text.splitlines())
            logger.debug("Zlib decompressed %d bytes from offset %d", len(decompressed), offset)
            return lines
        except (zlib.error, Exception):
            continue

    return lines


def _decode_base64_frames(raw_bytes: bytes) -> list[str]:
    """Extract and decode Base64 payloads between BEGIN/END markers.

    Handles framing patterns like:
      ``FRM1\\x01\\x06BEGIN_B64\\n<payload>\\nEND_B64\\n<checksum>``
    """
    lines: list[str] = []
    text = raw_bytes.decode("ascii", errors="replace")

    begin_match = B64_BEGIN_RE.search(text)
    end_match = B64_END_RE.search(text)

    if not begin_match or not end_match:
        return lines

    payload_start = begin_match.end()
    payload_end = end_match.start()

    if payload_end <= payload_start:
        return lines

    raw_payload = text[payload_start:payload_end].strip()
    # Remove newlines from multi-line Base64.
    b64_clean = raw_payload.replace("\n", "").replace("\r", "")

    # Report frame metadata.
    header = text[: begin_match.start()].strip()
    if header:
        # Extract version/frame info from the header.
        printable_header = "".join(c if c.isprintable() else "" for c in header)
        if printable_header:
            lines.append(f"frame_header={printable_header}")

    lines.append("payload_encoding=base64")
    lines.append(f"payload_length={len(b64_clean)}")

    try:
        decoded = base64.b64decode(b64_clean)
        lines.append(f"decoded_size={len(decoded)}")
        # Try to interpret decoded content as text.
        try:
            decoded_text = decoded.decode("utf-8")
            lines.extend(decoded_text.splitlines())
        except UnicodeDecodeError:
            # Binary payload — report as hex summary.
            lines.append(f"decoded_hex_preview={decoded[:64].hex()}")
    except Exception:
        lines.append(f"base64_payload_preview={b64_clean[:80]}")

    # Check for checksum after END marker.
    after_end = text[end_match.end() :].strip()
    printable_after = "".join(c if c.isprintable() else "" for c in after_end)
    if printable_after:
        lines.append(f"checksum={printable_after[:16]}")

    return lines


def _decode_hex_telemetry(raw_bytes: bytes) -> list[str]:
    """Decode continuous hex-as-text telemetry streams.

    Handles files like ``HEX_DUMP\\n69A91E4A00BA0042...`` where the hex
    characters represent packed binary sensor readings.
    """
    lines: list[str] = []
    text = raw_bytes.decode("ascii", errors="replace")
    text_lines = text.strip().split("\n")

    # Identify continuous hex streams.
    hex_stream: str | None = None
    for line in text_lines:
        stripped = line.strip()
        if CONTINUOUS_HEX_RE.match(stripped):
            hex_stream = stripped
            break

    if not hex_stream:
        return lines

    lines.append("data_format=hex_telemetry")
    lines.append(f"hex_stream_length={len(hex_stream)}")

    try:
        byte_data = bytes.fromhex(hex_stream)
        lines.append(f"decoded_bytes={len(byte_data)}")

        # Try to detect repeating record structure by looking for common
        # sensor ID prefixes. Scan for the most frequent 4-byte prefix.
        if len(byte_data) >= 28:  # Need at least 2 records to detect pattern.
            record_len = _detect_record_length(byte_data)
            if record_len and record_len >= 8:
                lines.append(f"record_length={record_len}")
                record_count = len(byte_data) // record_len
                lines.append(f"record_count={record_count}")
                for i in range(min(record_count, 20)):
                    rec = byte_data[i * record_len : (i + 1) * record_len]
                    sensor_id = rec[:4].hex().upper()
                    # Try big-endian float for the reading value.
                    if len(rec) >= 8:
                        reading = struct.unpack(">f", rec[4:8])[0]
                        hex_repr = rec.hex().upper()
                        lines.append(f"sensor={sensor_id} reading={reading:.4f} raw={hex_repr}")
                    else:
                        lines.append(f"sensor={sensor_id} raw={rec.hex().upper()}")
            else:
                # Can't detect record structure; emit hex summary.
                lines.append(f"hex_preview={byte_data[:64].hex().upper()}")
    except ValueError:
        lines.append("hex_parse_error=invalid_hex_chars")

    return lines


def _detect_record_length(data: bytes) -> int | None:
    """Detect repeating record length in binary telemetry data.

    Tries prefix lengths from 4 down to 2 bytes, looking for a repeating
    pattern at consistent intervals. Falls back to autocorrelation for
    records with varying prefixes (e.g., incrementing sensor IDs sharing
    a common high-byte prefix).
    """
    # Try exact prefix match with decreasing prefix sizes.
    for prefix_len in (4, 3, 2):
        prefix = data[:prefix_len]
        second = data.find(prefix, prefix_len)
        if second > 0 and second <= 64:
            candidate = second
            # Validate with third occurrence.
            third = data.find(prefix, second + candidate)
            if third == second + candidate:
                return candidate

    # Autocorrelation: try common record sizes and check byte similarity.
    for candidate_len in range(8, 33):
        if len(data) < candidate_len * 3:
            continue
        matches = 0
        checks = min(len(data) // candidate_len - 1, 10)
        for i in range(checks):
            rec_a = data[i * candidate_len : (i + 1) * candidate_len]
            rec_b = data[(i + 1) * candidate_len : (i + 2) * candidate_len]
            # Count how many byte positions are similar (within 16).
            similar = sum(1 for a, b in zip(rec_a, rec_b) if abs(a - b) <= 16)
            if similar >= candidate_len * 0.4:
                matches += 1
        if matches >= min(checks, 2):
            return candidate_len

    return None


def _extract_cleartext_signals(raw_bytes: bytes) -> list[str]:
    """Extract cleartext signals from binary noise.

    Scans raw bytes for printable ASCII runs (4+ chars) and promotes
    meaningful ones (error codes, warnings, identifiers) to log lines.
    """
    lines: list[str] = []
    matches = CLEARTEXT_SIGNAL_RE.findall(raw_bytes)

    for raw_match in matches:
        text = raw_match.decode("ascii", errors="replace").strip()
        if len(text) < 4:
            continue

        # Classify the cleartext signal.
        errcode_m = ERRCODE_RE.search(text)
        if errcode_m:
            lines.append(f"ERRCODE={errcode_m.group('errcode')} signal={text}")
            continue

        warn_m = WARN_MSG_RE.search(text)
        if warn_m:
            lines.append(f"WARN: {warn_m.group('warn_msg').strip()}")
            continue

        # Check for known patterns (EVT, code=, msg=, etc.)
        if re.search(r"code=|msg=|EVT\d", text):
            lines.append(text)
            continue

        # Skip short gibberish (common in binary dumps).
        if len(text) >= 8 and any(c.isalpha() for c in text):
            lines.append(f"cleartext_signal={text}")

    return lines


def preprocess_binary_input(raw_bytes: bytes) -> list[str]:
    """Top-level entry: convert raw binary bytes into text lines.

    If the content looks like regular text, just split into lines.
    If it contains binary content, run through the decoder pipeline.
    Returns text lines suitable for the main unstructured parser.
    """
    # Quick check: is this mostly printable text?
    sample = raw_bytes[:2048]
    non_printable = sum(
        1
        for b in sample
        if b < 0x20 and b not in (0x0A, 0x0D, 0x09)  # Not newline/CR/tab
    )
    ratio = non_printable / max(len(sample), 1)

    if ratio < 0.1:
        # Mostly text — decode normally.
        text = decode_content(raw_bytes)
        return text.splitlines()

    # Binary content detected — run decoder pipeline.
    logger.info("Binary content detected (%.0f%% non-printable), running decoders", ratio * 100)
    return decode_binary_content(raw_bytes)


# ---------------------------------------------------------------------------
# Drain3 config helper
# ---------------------------------------------------------------------------


def _build_drain_config() -> TemplateMinerConfig:
    """Return a Drain3 config tuned for unstructured / semiconductor logs."""
    config = TemplateMinerConfig()
    config.drain_sim_th = 0.4
    config.drain_depth = 4
    config.drain_max_children = 100
    config.drain_max_clusters = 1024
    return config


# ---------------------------------------------------------------------------
# Core pipeline functions
# ---------------------------------------------------------------------------


def detect_encoding(raw_bytes: bytes) -> str:
    """Detect encoding of raw bytes, returning a codec name."""
    if not raw_bytes:
        return "utf-8"
    result = chardet.detect(raw_bytes[:8192])
    encoding = result.get("encoding") or "utf-8"
    # Normalise common aliases.
    encoding = encoding.lower().replace("-", "_")
    if encoding in ("ascii", "windows_1252", "iso_8859_1", "latin_1"):
        encoding = "utf-8"
    return encoding


def decode_content(raw_bytes: bytes) -> str:
    """Decode raw bytes to str with best-effort encoding detection."""
    encoding = detect_encoding(raw_bytes)
    try:
        return raw_bytes.decode(encoding)
    except (UnicodeDecodeError, LookupError):
        return raw_bytes.decode("utf-8", errors="replace")


def filter_noise(lines: list[str]) -> list[str]:
    """Remove lines that are pure noise (blank / decorative separators / binary)."""
    return [line for line in lines if not NOISE_LINE_RE.match(line) and not is_binary_content(line)]


def cluster_multiline(lines: list[str]) -> list[tuple[int, int, str]]:
    """Group lines into multiline clusters.

    Returns a list of ``(start_line_1based, end_line_1based, text)`` tuples.
    Continuation lines (indented, stack traces) are merged into the preceding
    cluster.
    """
    clusters: list[tuple[int, int, str]] = []
    current_start = 0
    current_lines: list[str] = []

    for idx, line in enumerate(lines):
        if CONTINUATION_RE.match(line) and current_lines:
            current_lines.append(line)
        else:
            if current_lines:
                clusters.append(
                    (
                        current_start + 1,
                        current_start + len(current_lines),
                        "\n".join(current_lines),
                    )
                )
            current_start = idx
            current_lines = [line]

    if current_lines:
        clusters.append(
            (
                current_start + 1,
                current_start + len(current_lines),
                "\n".join(current_lines),
            )
        )

    return clusters


def mine_templates(clusters: list[tuple[int, int, str]]) -> tuple[TemplateMiner, list[str]]:
    """Run Drain3 template mining over cluster texts.

    Returns the miner and a list of template strings (one per cluster, in order).
    """
    config = _build_drain_config()
    miner = TemplateMiner(config=config)
    templates: list[str] = []

    for _, _, text in clusters:
        first_line = text.split("\n", 1)[0][:MAX_LINE_LENGTH]
        result = miner.add_log_message(first_line)
        templates.append(result["template_mined"])

    return miner, templates


def extract_fields_heuristic(text: str) -> dict[str, Any]:
    """Extract known fields from one cluster/line of unstructured text via regex."""
    fields: dict[str, Any] = {}

    ts_match = TIMESTAMP_RE.search(text)
    if ts_match:
        fields["timestamp_raw"] = ts_match.group("ts")
        fields["timestamp"] = ts_match.group("ts")

    level_match = LOG_LEVEL_RE.search(text)
    if level_match:
        fields["log_level"] = level_match.group("level").upper()

    # Semiconductor-specific fields (with aliases for LLM column compatibility).
    wafer_match = WAFER_ID_RE.search(text)
    if wafer_match:
        fields["wafer_id"] = wafer_match.group("wafer")
        fields["wafer"] = wafer_match.group("wafer")

    tool_match = TOOL_ID_RE.search(text)
    if tool_match:
        fields["tool_id"] = tool_match.group("tool")
        fields["tool"] = tool_match.group("tool")

    recipe_match = RECIPE_RE.search(text)
    if recipe_match:
        fields["recipe_id"] = recipe_match.group("recipe")
        fields["recipe"] = recipe_match.group("recipe")

    step_match = STEP_RE.search(text)
    if step_match:
        fields["process_step"] = step_match.group("step")

    # Measurement patterns (before generic KV to avoid value clobbering).
    for m_match in MEASUREMENT_RE.finditer(text):
        key = m_match.group("key").lower()
        val = m_match.group("val").strip()
        # Strip unit suffixes so the value can be stored as REAL.
        unit_m = UNIT_STRIP_RE.match(val)
        if unit_m:
            val = unit_m.group("num")
        if key not in fields:
            fields[key] = val

    # Generic key=value extraction.
    for kv_match in KV_RE.finditer(text):
        key = kv_match.group("key").lower()
        # Skip purely-numeric keys (e.g., "06" from "06:03:16").
        if key.isdigit():
            continue
        val = kv_match.group("qval") or kv_match.group("val")
        # Skip unquoted values containing '=' (likely swallowed another key=value pair).
        if not kv_match.group("qval") and val and "=" in val:
            continue
        if key not in fields and key not in _KV_STOP_WORDS:
            fields[key] = val

    return fields


def _all_numeric(examples: list[str]) -> bool:
    """Return True if all non-empty example values parse as float."""
    if not examples:
        return False
    for val in examples:
        try:
            float(val)
        except (ValueError, TypeError):
            return False
    return True


def infer_columns_from_fields(
    all_fields: list[dict[str, Any]],
) -> list[InferredColumn]:
    """Aggregate extracted fields across many records into InferredColumn list.

    Only returns columns that appear in >=10 % of records (or at least 1 if
    record count < 10).
    """
    if not all_fields:
        return []

    key_counts: dict[str, int] = {}
    key_examples: dict[str, list[str]] = {}

    for fields in all_fields:
        for key, val in fields.items():
            key_counts[key] = key_counts.get(key, 0) + 1
            examples = key_examples.setdefault(key, [])
            if len(examples) < 3:
                examples.append(str(val)[:100])

    # Use 5% threshold (min 2) so sparse-but-meaningful measurement fields
    # like pressure, power, uniformity aren't discarded in mixed-format logs.
    threshold = max(2, len(all_fields) // 20)

    # Skip keys that are already baseline columns or internal aliases.
    baseline_names = {
        "id",
        "timestamp",
        "timestamp_raw",
        "source",
        "source_type",
        "log_level",
        "event_type",
        "message",
        "raw_text",
        "record_group_id",
        "line_start",
        "line_end",
        "parse_confidence",
        "schema_version",
        "additional_data",
        # Aliases — the canonical *_id columns are added via
        # SEMICONDUCTOR_COLUMNS; suppress the short aliases.
        "wafer",
        "tool",
        "recipe",
    }

    columns: list[InferredColumn] = []
    for key, count in key_counts.items():
        if count < threshold or key in baseline_names:
            continue
        # Infer sql_type: use REAL for measurement fields whose examples
        # look numeric (after unit stripping in extract_fields_heuristic).
        sql_type = SqlType.TEXT
        examples = key_examples.get(key, [])
        if key in MEASUREMENT_FIELD_NAMES or _all_numeric(examples):
            sql_type = SqlType.REAL
        columns.append(
            InferredColumn(
                name=key,
                sql_type=sql_type,
                description=f"Extracted from unstructured text (appeared in {count}/{len(all_fields)} records).",
                nullable=True,
                kind=ColumnKind.DETECTED,
                example_values=examples,
            )
        )

    return columns


# ---------------------------------------------------------------------------
# Semiconductor-domain extra columns
# ---------------------------------------------------------------------------

SEMICONDUCTOR_COLUMNS = [
    InferredColumn(
        name="wafer_id",
        sql_type=SqlType.TEXT,
        description="Wafer or lot identifier extracted from the log.",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
    InferredColumn(
        name="tool_id",
        sql_type=SqlType.TEXT,
        description="Equipment/tool identifier.",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
    InferredColumn(
        name="recipe_id",
        sql_type=SqlType.TEXT,
        description="Process recipe identifier.",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
    InferredColumn(
        name="process_step",
        sql_type=SqlType.TEXT,
        description="Manufacturing process step or phase.",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
    InferredColumn(
        name="template",
        sql_type=SqlType.TEXT,
        description="Drain3-mined log template (parameterised pattern).",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
    InferredColumn(
        name="template_cluster_id",
        sql_type=SqlType.TEXT,
        description="Hash identifying the Drain3 template cluster.",
        nullable=True,
        kind=ColumnKind.DETECTED,
    ),
]


# ---------------------------------------------------------------------------
# LLM enrichment for unstructured logs
# ---------------------------------------------------------------------------


def call_llm_for_unstructured(
    sample_lines: list[str],
    heuristic_columns: list[InferredColumn],
) -> LlmUnstructuredResponse:
    """Ask the LLM to discover additional fields in unstructured text."""
    if not OPENROUTER_API_KEY:
        return LlmUnstructuredResponse(
            warnings=["OPENROUTER_API_KEY not set; LLM enrichment skipped."],
        )

    model = ChatOpenRouter(
        model=OPENROUTER_MODEL,
        api_key=SecretStr(OPENROUTER_API_KEY),
        temperature=0.0,
        max_tokens=4096,
        app_title=APP_NAME,
        app_url=APP_URL,
    )

    structured_model = model.with_structured_output(
        LlmUnstructuredResponse,
        method="json_schema",
        strict=True,
    )

    heuristic_summary = (
        "\n".join(f"  - {col.name} ({col.sql_type.value}): {col.description}" for col in heuristic_columns)
        or "  (none detected)"
    )

    sample_text = "\n".join(line[:MAX_LINE_LENGTH] for line in sample_lines[:MAX_SAMPLE_LINES])

    system_prompt = (
        "You are an expert log analyst specializing in unstructured and semi-structured logs.\n"
        "Analyze the provided raw log samples and identify additional fields that can be\n"
        "extracted into table columns. Focus on:\n"
        "1. Domain-specific identifiers (wafer IDs, tool names, recipe names, etc.)\n"
        "2. Repeated key-value patterns not yet captured.\n"
        "3. Numeric measurements or counters.\n"
        "4. A suggested event_type classification for this log source.\n\n"
        "Rules:\n"
        "- Column names must be lowercase snake_case.\n"
        "- sql_type must be TEXT, INTEGER, or REAL.\n"
        "- Do NOT repeat baseline columns (id, timestamp, timestamp_raw, source,\n"
        "  source_type, log_level, event_type, message, raw_text, record_group_id,\n"
        "  line_start, line_end, parse_confidence, schema_version, additional_data).\n"
        "- Do NOT repeat columns already detected by heuristics.\n"
    )

    user_prompt = (
        f"Already-detected columns:\n{heuristic_summary}\n\n"
        f"Sample unstructured log lines:\n```\n{sample_text}\n```\n\n"
        "Identify any additional extractable fields."
    )

    messages = [("system", system_prompt), ("human", user_prompt)]

    try:
        response: LlmUnstructuredResponse = structured_model.invoke(messages)
        return response
    except Exception as exc:
        logger.warning("LLM unstructured enrichment failed: %s", exc)
        return LlmUnstructuredResponse(
            warnings=[f"LLM enrichment failed ({type(exc).__name__}): {exc}"],
        )


# ---------------------------------------------------------------------------
# Main entry point used by preprocessor
# ---------------------------------------------------------------------------


def extract_unstructured_columns(
    lines: list[str],
) -> list[InferredColumn]:
    """Full unstructured parsing pipeline returning extra columns.

    Called by ``LogPreprocessorService._extract_heuristic_columns`` when the
    detected format is PLAIN_TEXT or UNKNOWN.
    """
    clean_lines = filter_noise(lines)
    if not clean_lines:
        return []

    # Pre-check: if most content is hex dump, extract ASCII portions.
    hex_count = sum(1 for line in clean_lines if is_hex_dump_line(line))
    if hex_count > len(clean_lines) * 0.5:
        ascii_lines = extract_ascii_from_hexdump(clean_lines)
        if ascii_lines:
            clean_lines = ascii_lines
        else:
            return []

    # Step 1: Cluster multiline records.
    clusters = cluster_multiline(clean_lines)

    # Step 2: Mine templates with Drain3.
    miner, templates = mine_templates(clusters)

    # Step 3: Extract fields from each cluster via regex.
    all_fields: list[dict[str, Any]] = []
    for (start, end, text), template in zip(clusters, templates):
        fields = extract_fields_heuristic(text)
        fields["template"] = template
        fields["template_cluster_id"] = hashlib.md5(
            template.encode(),
            usedforsecurity=False,
        ).hexdigest()[:12]
        all_fields.append(fields)

    # Step 4: Determine which fields are frequent enough to be columns.
    heuristic_columns = infer_columns_from_fields(all_fields)

    # Step 5: Add semiconductor-specific columns if any were detected.
    detected_names = {col.name for col in heuristic_columns}
    for semi_col in SEMICONDUCTOR_COLUMNS:
        if semi_col.name not in detected_names:
            # Only add if we actually saw this field in at least one record.
            if any(semi_col.name in f for f in all_fields):
                heuristic_columns.append(semi_col)
            elif semi_col.name in ("template", "template_cluster_id"):
                # Always include template columns when we did Drain3 mining.
                heuristic_columns.append(semi_col)

    # Step 6: LLM enrichment (optional).
    sample_lines = [text[:MAX_LINE_LENGTH] for _, _, text in clusters[:MAX_SAMPLE_LINES]]
    llm_result = call_llm_for_unstructured(sample_lines, heuristic_columns)

    if llm_result.fields:
        existing_names = detected_names | {col.name for col in heuristic_columns}
        for llm_field in llm_result.fields:
            safe_name = re.sub(r"[^a-z0-9_]", "_", llm_field.name.lower()).strip("_")
            if safe_name and safe_name not in existing_names:
                sql_type = SqlType.TEXT
                if llm_field.sql_type.upper() in ("INTEGER", "INT"):
                    sql_type = SqlType.INTEGER
                elif llm_field.sql_type.upper() in ("REAL", "FLOAT", "DOUBLE"):
                    sql_type = SqlType.REAL

                heuristic_columns.append(
                    InferredColumn(
                        name=safe_name,
                        sql_type=sql_type,
                        description=llm_field.description,
                        nullable=True,
                        kind=ColumnKind.LLM_INFERRED,
                        example_values=llm_field.example_values,
                    )
                )
                existing_names.add(safe_name)

    return heuristic_columns


def extract_unstructured_samples(
    lines: list[str],
    filename: str,
    column_names: set[str],
    max_samples: int = 5,
) -> list[SampleRecord]:
    """Extract sample records from unstructured text.

    Called by ``LogPreprocessorService._extract_samples`` for PLAIN_TEXT files.
    """
    clean_lines = filter_noise(lines)
    if not clean_lines:
        return []

    # Pre-check: if most content is hex dump, extract ASCII portions.
    hex_count = sum(1 for line in clean_lines if is_hex_dump_line(line))
    if hex_count > len(clean_lines) * 0.5:
        ascii_lines = extract_ascii_from_hexdump(clean_lines)
        if ascii_lines:
            clean_lines = ascii_lines
        else:
            return []

    clusters = cluster_multiline(clean_lines)
    miner, templates = mine_templates(clusters)

    samples: list[SampleRecord] = []
    for (start, end, text), template in zip(clusters[:max_samples], templates[:max_samples]):
        fields = extract_fields_heuristic(text)
        fields["raw_text"] = text
        fields["source"] = filename
        fields["source_type"] = "file"
        fields["template"] = template
        fields["template_cluster_id"] = hashlib.md5(
            template.encode(),
            usedforsecurity=False,
        ).hexdigest()[:12]

        if "message" not in fields:
            fields["message"] = text.strip()[:500]

        # Only include fields that map to known columns.
        filtered = {k: v for k, v in fields.items() if k in column_names}

        samples.append(
            SampleRecord(
                source_file=filename,
                line_start=start,
                line_end=end,
                fields=filtered,
            )
        )

    return samples
