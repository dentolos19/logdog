from __future__ import annotations

import base64
import hashlib
import logging
import re
import zlib
from typing import Any

import chardet
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

import parsers.ai as ai
from parsers.preprocessor import ColumnKind, InferredColumn, SampleRecord, SqlType

logger = logging.getLogger(__name__)

MAX_SAMPLE_LINES = 30
MAX_LINE_LENGTH = 2000

NOISE_LINE_RE = re.compile(r"^[\s\-=*#~]{0,3}$")
CONTINUATION_RE = re.compile(r"^(?:\s+at\s|Caused by:|\.{3}\s*\d+\s*more|\s{4,}\S|\t\S)")

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
KV_RE = re.compile(r"(?P<key>\w[\w.]*)\s*[=:]\s*(?:\"(?P<qval>[^\"]*)\"|(?P<val>\S+))")

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
        "a",
        "e",
        "i",
        "o",
        "u",
        "x",
    }
)

WAFER_ID_RE = re.compile(r"\b(?P<wafer>W\d{2,4}|LOT[_-]?\w+|FOUP[_-]?\w+)\b", re.IGNORECASE)
TOOL_ID_RE = re.compile(r"\b(?P<tool>(?:TOOL|EQP|CHAMBER)[_-]?\w+)\b", re.IGNORECASE)
RECIPE_RE = re.compile(r"\b(?P<recipe>(?:RECIPE|RCP)[_-]?\w+)\b", re.IGNORECASE)
STEP_RE = re.compile(r"\b(?:STEP|PHASE)[_:= ]+(?P<step>(?!SiH4|CF4|N2|O2|Ar|He)\w+)\b", re.IGNORECASE)

MEASUREMENT_RE = re.compile(
    r"\b(?P<key>thickness|pressure|temperature|temp|flow|gas_flow|power|"
    r"voltage|current|rpm|dose|energy|frequency|bias|uniformity|vacuum|"
    r"rf_power|reflected|delta|duration|threshold|rate|particle_count|"
    r"vibration|resistivity)\s*[=:]\s*"
    r"(?P<val>[\d.eE+-]+\s*(?:nm|um|mm|cm|mTorr|Torr|Pa|C|K|sccm|slm|W|kW|V|mV|A|mA|rpm|Hz|kHz|MHz|%|s|ms|MOhm[\w/-]*|mm/s|C/min|E-\d+)?)\.?",
    re.IGNORECASE,
)
UNIT_STRIP_RE = re.compile(
    r"^(?P<num>[\d.eE+-]+)\s*"
    r"(?:nm|um|mm|cm|m|mTorr|Torr|Pa|C|K|sccm|slm|W|kW|V|mV|A|mA|rpm|Hz|kHz|MHz|%|s|ms|MOhm[\w/-]*|mm/s|C/min)?"
    r"\.*$",
    re.IGNORECASE,
)

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

HEX_DUMP_RE = re.compile(r"^[0-9A-Fa-f]{4,8}\s+(?:[0-9A-Fa-f]{2}\s){4,}")
HEX_DUMP_ASCII_RE = re.compile(r"\|([^|]+)\|\s*$")
CONTINUOUS_HEX_RE = re.compile(r"^[0-9A-Fa-f]{16,}$")
CLEARTEXT_SIGNAL_RE = re.compile(rb"[\x20-\x7e]{4,}")


def is_binary_content(line: str) -> bool:
    if not line:
        return False
    non_printable = sum(1 for c in line if not c.isprintable() and c not in "\n\r\t")
    return non_printable / max(len(line), 1) > 0.3


def is_hex_dump_line(line: str) -> bool:
    return bool(HEX_DUMP_RE.match(line.strip()))


def extract_ascii_from_hexdump(lines: list[str]) -> list[str]:
    ascii_parts: list[str] = []
    for line in lines:
        match = HEX_DUMP_ASCII_RE.search(line)
        if not match:
            continue
        ascii_text = match.group(1).strip()
        if ascii_text:
            ascii_parts.append(ascii_text)
    return ascii_parts


def decode_binary_content(raw_bytes: bytes) -> list[str]:
    decoded_lines: list[str] = []
    decoded_lines.extend(_decode_zlib(raw_bytes))
    decoded_lines.extend(_decode_base64_frames(raw_bytes))
    decoded_lines.extend(_decode_hex_telemetry(raw_bytes))
    decoded_lines.extend(_extract_cleartext_signals(raw_bytes))
    return [line for line in decoded_lines if line.strip()]


def _decode_zlib(raw_bytes: bytes) -> list[str]:
    lines: list[str] = []
    for magic in (b"\x78\x9c", b"\x78\x01", b"\x78\xda"):
        index = raw_bytes.find(magic)
        if index < 0:
            continue
        try:
            decompressed = zlib.decompress(raw_bytes[index:])
            decoded = _decode_bytes_to_text(decompressed)
            lines.extend(decoded.splitlines())
            break
        except Exception:
            continue
    return lines


def _decode_base64_frames(raw_bytes: bytes) -> list[str]:
    text = _decode_bytes_to_text(raw_bytes)
    lines = text.splitlines()
    in_frame = False
    chunks: list[str] = []
    decoded_lines: list[str] = []

    for line in lines:
        upper = line.strip().upper()
        if upper in {"BEGIN_B64", "BASE64_START", "B64_BEGIN"}:
            in_frame = True
            chunks = []
            continue
        if upper in {"END_B64", "BASE64_END", "B64_END"}:
            in_frame = False
            payload = "".join(chunks).strip()
            if payload:
                try:
                    decoded = base64.b64decode(payload, validate=False)
                    decoded_lines.extend(_decode_bytes_to_text(decoded).splitlines())
                except Exception:
                    pass
            continue
        if in_frame:
            chunks.append(line.strip())

    return decoded_lines


def _decode_hex_telemetry(raw_bytes: bytes) -> list[str]:
    text = _decode_bytes_to_text(raw_bytes)
    lines: list[str] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not CONTINUOUS_HEX_RE.match(stripped):
            continue
        if len(stripped) % 2 != 0:
            stripped = stripped[:-1]
        try:
            decoded = bytes.fromhex(stripped)
            decoded_line = _decode_bytes_to_text(decoded).strip()
            if decoded_line:
                lines.append(decoded_line)
        except Exception:
            continue
    return lines


def _extract_cleartext_signals(raw_bytes: bytes) -> list[str]:
    lines: list[str] = []
    for match in CLEARTEXT_SIGNAL_RE.finditer(raw_bytes):
        candidate = match.group(0)
        try:
            text = candidate.decode("utf-8", errors="ignore").strip()
        except Exception:
            continue
        if text and not NOISE_LINE_RE.match(text):
            lines.append(text)
    return lines


def _decode_bytes_to_text(raw_bytes: bytes) -> str:
    detected = chardet.detect(raw_bytes)
    encoding = detected.get("encoding") or "utf-8"
    try:
        return raw_bytes.decode(encoding, errors="ignore")
    except Exception:
        return raw_bytes.decode("utf-8", errors="ignore")


def normalize_input(content: str | bytes) -> list[str]:
    if isinstance(content, str):
        return content.splitlines()

    decoded = _decode_bytes_to_text(content)
    if not decoded:
        decoded_lines = decode_binary_content(content)
        return decoded_lines

    lines = decoded.splitlines()
    binary_like = sum(1 for line in lines[:100] if is_binary_content(line))
    if binary_like > max(1, len(lines[:100]) // 2):
        decoded_lines = decode_binary_content(content)
        if decoded_lines:
            return decoded_lines
    return lines


def filter_noise(lines: list[str]) -> list[str]:
    return [line for line in lines if line.strip() and not NOISE_LINE_RE.match(line)]


def cluster_multiline(lines: list[str]) -> list[tuple[int, int, str]]:
    clusters: list[tuple[int, int, str]] = []
    current_start = 0
    current_lines: list[str] = []

    for index, line in enumerate(lines):
        if CONTINUATION_RE.match(line) and current_lines:
            current_lines.append(line)
            continue

        if current_lines:
            clusters.append((current_start + 1, current_start + len(current_lines), "\n".join(current_lines)))

        current_start = index
        current_lines = [line]

    if current_lines:
        clusters.append((current_start + 1, current_start + len(current_lines), "\n".join(current_lines)))

    return clusters


def _build_drain_config() -> TemplateMinerConfig:
    config = TemplateMinerConfig()
    config.drain_sim_th = 0.4
    config.drain_depth = 4
    config.drain_max_children = 100
    config.masking_instructions = []
    return config


def mine_templates(clusters: list[tuple[int, int, str]]) -> tuple[TemplateMiner, list[str]]:
    miner = TemplateMiner(config=_build_drain_config())
    templates: list[str] = []

    for _, _, text in clusters:
        first_line = text.split("\n", 1)[0].strip()
        try:
            result = miner.add_log_message(first_line)
            if result and isinstance(result, dict):
                template = result.get("template_mined") or first_line
            else:
                template = first_line
        except Exception:
            template = first_line
        templates.append(template[:MAX_LINE_LENGTH])

    return miner, templates


def _safe_key(raw_key: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9_]", "_", raw_key.strip()).lower()
    key = re.sub(r"_+", "_", key).strip("_")
    if not key or key[0].isdigit():
        key = "col_" + key
    return key


def _coerce_numeric(value: str) -> Any:
    stripped = value.strip()
    numeric_match = UNIT_STRIP_RE.match(stripped)
    if numeric_match:
        number = numeric_match.group("num")
        try:
            if "." in number or "e" in number.lower():
                return float(number)
            return int(number)
        except ValueError:
            return stripped
    return stripped


def extract_fields_heuristic(text: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}

    timestamp_match = TIMESTAMP_RE.search(text)
    if timestamp_match:
        ts = timestamp_match.group("ts")
        fields["timestamp_raw"] = ts
        fields["timestamp"] = ts

    level_match = LOG_LEVEL_RE.search(text)
    if level_match:
        fields["log_level"] = level_match.group("level").upper()

    wafer_match = WAFER_ID_RE.search(text)
    if wafer_match:
        fields["wafer_id"] = wafer_match.group("wafer")

    tool_match = TOOL_ID_RE.search(text)
    if tool_match:
        fields["tool_id"] = tool_match.group("tool")

    recipe_match = RECIPE_RE.search(text)
    if recipe_match:
        fields["recipe_id"] = recipe_match.group("recipe")

    step_match = STEP_RE.search(text)
    if step_match:
        fields["process_step"] = step_match.group("step")

    for measurement_match in MEASUREMENT_RE.finditer(text):
        key = _safe_key(measurement_match.group("key"))
        value = _coerce_numeric(measurement_match.group("val"))
        fields[key] = value

    for kv_match in KV_RE.finditer(text):
        raw_key = kv_match.group("key")
        key = _safe_key(raw_key)
        if key in _KV_STOP_WORDS:
            continue
        value = kv_match.group("qval") if kv_match.group("qval") is not None else kv_match.group("val")
        if value is None:
            continue
        if key in fields:
            continue
        fields[key] = _coerce_numeric(value)

    if "message" not in fields:
        fields["message"] = text.strip()[:500]

    return fields


def _all_numeric(examples: list[str]) -> bool:
    if not examples:
        return False
    for value in examples:
        try:
            float(value)
        except (ValueError, TypeError):
            return False
    return True


def infer_columns_from_fields(all_fields: list[dict[str, Any]]) -> list[InferredColumn]:
    if not all_fields:
        return []

    key_counts: dict[str, int] = {}
    key_examples: dict[str, list[str]] = {}

    for fields in all_fields:
        for key, value in fields.items():
            if key in {"timestamp", "timestamp_raw", "log_level", "message"}:
                continue
            key_counts[key] = key_counts.get(key, 0) + 1
            examples = key_examples.setdefault(key, [])
            if len(examples) < 3:
                examples.append(str(value)[:100])

    threshold = max(2, int(len(all_fields) * 0.05))
    columns: list[InferredColumn] = []

    for key, count in key_counts.items():
        if count < threshold:
            continue

        examples = key_examples.get(key, [])
        sql_type = SqlType.REAL if key in MEASUREMENT_FIELD_NAMES or _all_numeric(examples) else SqlType.TEXT
        columns.append(
            InferredColumn(
                name=key,
                sql_type=sql_type,
                description=f"Extracted from unstructured text ({count}/{len(all_fields)} records).",
                nullable=True,
                kind=ColumnKind.DETECTED,
                example_values=examples,
            )
        )

    return columns


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
        description="Drain3-mined log template (parameterized pattern).",
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


def call_llm_for_unstructured(
    sample_lines: list[str], heuristic_columns: list[InferredColumn]
) -> ai.LlmUnstructuredResponse:
    heuristic_summary = (
        "\n".join(f"  - {column.name} ({column.sql_type.value}): {column.description}" for column in heuristic_columns)
        or "  (none detected)"
    )
    sample_text = "\n".join(line[:MAX_LINE_LENGTH] for line in sample_lines[:MAX_SAMPLE_LINES])

    invocation = ai.infer_unstructured_fields(sample_text=sample_text, heuristic_summary=heuristic_summary)
    if invocation.response is not None:
        return invocation.response

    return ai.LlmUnstructuredResponse(warnings=[invocation.warning] if invocation.warning else [])


def extract_unstructured_columns(lines: list[str]) -> list[InferredColumn]:
    clean_lines = filter_noise(lines)
    if not clean_lines:
        return []

    hex_count = sum(1 for line in clean_lines if is_hex_dump_line(line))
    if hex_count > len(clean_lines) * 0.5:
        ascii_lines = extract_ascii_from_hexdump(clean_lines)
        if ascii_lines:
            clean_lines = ascii_lines
        else:
            return []

    clusters = cluster_multiline(clean_lines)
    _, templates = mine_templates(clusters)

    all_fields: list[dict[str, Any]] = []
    for (_, _, text), template in zip(clusters, templates):
        fields = extract_fields_heuristic(text)
        fields["template"] = template
        fields["template_cluster_id"] = hashlib.md5(template.encode(), usedforsecurity=False).hexdigest()[:12]
        all_fields.append(fields)

    heuristic_columns = infer_columns_from_fields(all_fields)

    detected_names = {column.name for column in heuristic_columns}
    for semiconductor_column in SEMICONDUCTOR_COLUMNS:
        if semiconductor_column.name in detected_names:
            continue
        if any(semiconductor_column.name in field_map for field_map in all_fields):
            heuristic_columns.append(semiconductor_column)
        elif semiconductor_column.name in {"template", "template_cluster_id"}:
            heuristic_columns.append(semiconductor_column)

    sample_lines = [text[:MAX_LINE_LENGTH] for _, _, text in clusters[:MAX_SAMPLE_LINES]]
    llm_result = call_llm_for_unstructured(sample_lines, heuristic_columns)

    if llm_result.fields:
        existing_names = {column.name for column in heuristic_columns}
        for llm_field in llm_result.fields:
            safe_name = re.sub(r"[^a-z0-9_]", "_", llm_field.name.lower()).strip("_")
            if not safe_name or safe_name in existing_names:
                continue

            sql_type = SqlType.TEXT
            if llm_field.sql_type.upper() in {"INTEGER", "INT"}:
                sql_type = SqlType.INTEGER
            elif llm_field.sql_type.upper() in {"REAL", "FLOAT", "DOUBLE"}:
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
    clean_lines = filter_noise(lines)
    if not clean_lines:
        return []

    hex_count = sum(1 for line in clean_lines if is_hex_dump_line(line))
    if hex_count > len(clean_lines) * 0.5:
        ascii_lines = extract_ascii_from_hexdump(clean_lines)
        if ascii_lines:
            clean_lines = ascii_lines
        else:
            return []

    clusters = cluster_multiline(clean_lines)
    _, templates = mine_templates(clusters)

    samples: list[SampleRecord] = []
    for (start, end, text), template in zip(clusters[:max_samples], templates[:max_samples]):
        fields = extract_fields_heuristic(text)
        fields["raw_text"] = text
        fields["source"] = filename
        fields["source_type"] = "file"
        fields["template"] = template
        fields["template_cluster_id"] = hashlib.md5(template.encode(), usedforsecurity=False).hexdigest()[:12]

        if "message" not in fields:
            fields["message"] = text.strip()[:500]

        filtered = {key: value for key, value in fields.items() if key in column_names}
        samples.append(SampleRecord(source_file=filename, line_start=start, line_end=end, fields=filtered))

    return samples
