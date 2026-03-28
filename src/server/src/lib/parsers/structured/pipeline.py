"""Structured log parser pipeline.

Handles files classified as ``StructuralClass.STRUCTURED``:
  JSON Lines, CSV, Syslog (RFC 3164), Apache/Nginx CLF, Logfmt, Key-Value, XML.

For each file the pipeline:
  1. Extracts records using format-specific logic.
  2. Infers extra (non-baseline) columns using AI-enriched heuristic inference.
  3. Generates a CREATE TABLE DDL statement.
  4. Returns all table definitions and row data via ``ParserPipelineResult``.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import xml.etree.ElementTree as ET
from io import StringIO
from typing import TYPE_CHECKING, Any

from lib.parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserSupportRequest,
    ParserSupportResult,
    ParserPipelineResult,
    StructuralClass,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from lib.parsers.preprocessor import (
    ISO_TIMESTAMP_PATTERN,
    LOG_LEVEL_PATTERN,
    DetectedFormat,
    FileInput,
)
from lib.parsers.registry import ParserPipeline
from lib.parsers.structured.ai_enricher import (
    enrich_structured_schema,
    has_openrouter_api_key,
)
from lib.parsers.structured.type_inference import (
    SqlType,
    infer_type,
    infer_columns_from_records,
)

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"

MAX_NESTING_DEPTH = 3
MAX_ARRAY_LENGTH_STORED = 10

SYSLOG_FACILITIES = [
    "kern",
    "user",
    "mail",
    "daemon",
    "auth",
    "syslog",
    "lpr",
    "news",
    "uucp",
    "cron",
    "authpriv",
    "ftp",
    "ntp",
    "security",
    "console",
    "cron2",
    "local0",
    "local1",
    "local2",
    "local3",
    "local4",
    "local5",
    "local6",
    "local7",
]
SYSLOG_SEVERITIES = [
    "emerg",
    "alert",
    "crit",
    "err",
    "warning",
    "notice",
    "info",
    "debug",
]

SYSLOG_RE = re.compile(
    r"^(?:<(?P<priority>\d{1,3})>)?"
    r"(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(?P<day>\d{1,2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<hostname>\S+)\s+(?P<process>\S+?)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<message>.*)",
    re.IGNORECASE,
)

CLF_RE = re.compile(
    r"^(?P<remote_host>\S+)\s+(?P<ident>\S+)\s+(?P<auth_user>\S+)\s+"
    r"\[(?P<time_local>[^\]]+)\]\s+"
    r'"(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<protocol>[^"]+)"\s+'
    r"(?P<status_code>\d{3})\s+(?P<response_size>\S+)"
    r'(?:\s+"(?P<referer>[^"]*)"\s+"(?P<user_agent>[^"]*)")?',
)


def _sanitize(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s or s[0].isdigit():
        s = "col_" + s
    return s


def _base_row(filename: str, line_start: int, line_end: int, raw_text: str) -> dict[str, Any]:
    return {
        "source": filename,
        "source_type": "file",
        "schema_version": SCHEMA_VERSION,
        "line_start": line_start,
        "line_end": line_end,
        "raw_text": raw_text[:4000],
    }


def _flatten_json_object(
    obj: Any,
    prefix: str = "",
    depth: int = 0,
    max_depth: int = MAX_NESTING_DEPTH,
) -> dict[str, Any]:
    """Flatten a nested JSON object into dot-notation keys.

    Args:
        obj: The JSON object (dict, list, or primitive)
        prefix: Key prefix for dot notation
        depth: Current nesting depth
        max_depth: Maximum depth to recurse

    Returns:
        Dictionary with flattened keys and values
    """
    result: dict[str, Any] = {}

    if depth >= max_depth:
        if isinstance(obj, (dict, list)):
            result[prefix] = json.dumps(obj) if not isinstance(obj, str) else obj
        else:
            result[prefix] = obj
        return result

    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            if v is None:
                result[key] = None
            elif isinstance(v, dict):
                result.update(_flatten_json_object(v, key, depth + 1, max_depth))
            elif isinstance(v, list):
                result.update(_flatten_json_list(v, key, depth + 1, max_depth))
            else:
                result[key] = v
    elif isinstance(obj, list):
        result.update(_flatten_json_list(obj, prefix, depth, max_depth))
    else:
        result[prefix] = obj

    return result


def _flatten_json_list(
    lst: list[Any],
    prefix: str,
    depth: int,
    max_depth: int,
) -> dict[str, Any]:
    """Handle JSON arrays during flattening."""
    result: dict[str, Any] = {}

    if not lst:
        result[f"{prefix}_length"] = 0
        return result

    result[f"{prefix}_length"] = len(lst)

    if len(lst) <= MAX_ARRAY_LENGTH_STORED:
        for i, item in enumerate(lst):
            key = f"{prefix}_{i}"
            if item is None:
                result[key] = None
            elif isinstance(item, dict):
                result.update(_flatten_json_object(item, key, depth + 1, max_depth))
            elif isinstance(item, list):
                result.update(_flatten_json_list(item, key, depth + 1, max_depth))
            else:
                result[key] = item

    if lst and isinstance(lst[0], (dict, list)):
        first_type = type(lst[0]).__name__
        result[f"{prefix}_first_type"] = first_type.lower()

    return result


def _infer_columns_from_records(
    records: list[dict[str, Any]],
    max_sample: int = 100,
) -> list[ColumnDefinition]:
    """Build ColumnDefinitions using semantic type inference for keys not in baseline."""
    key_examples: dict[str, list[str]] = {}
    key_counts: dict[str, int] = {}

    for row in records[:max_sample]:
        for k, v in row.items():
            if k in BASELINE_COLUMN_NAMES:
                continue
            key_counts[k] = key_counts.get(k, 0) + 1
            examples = key_examples.setdefault(k, [])
            if len(examples) < 5 and v is not None:
                examples.append(str(v)[:100])

    threshold = max(1, len(records[:max_sample]) // 10)

    extra: list[ColumnDefinition] = []
    for k, count in key_counts.items():
        if count >= threshold:
            examples = key_examples.get(k, [])
            type_result = infer_type(k, examples)
            extra.append(
                ColumnDefinition(
                    name=k,
                    sql_type=type_result.sql_type.value,
                    description=type_result.description,
                    nullable=True,
                )
            )

    return extra


def _build_table_definition(
    parser_key: str,
    file_id: str | None,
    filename: str,
    records: list[dict[str, Any]],
    detected_format: str,
    sample_lines: list[str],
) -> tuple[TableDefinition, list[str]]:
    """Build a TableDefinition using AI-enriched column inference."""
    table_name = make_table_name(parser_key, file_id, filename)

    use_llm = has_openrouter_api_key()
    enriched_cols, warnings = enrich_structured_schema(
        records=records,
        detected_format=detected_format,
        sample_lines=sample_lines,
        use_llm=use_llm,
    )

    extra_cols = _infer_columns_from_records(records)

    existing_names = {col.name for col in enriched_cols}
    for col in extra_cols:
        if col.name not in existing_names:
            enriched_cols.append(col)
            existing_names.add(col.name)

    all_cols = list(BASELINE_COLUMNS) + enriched_cols
    ddl = build_ddl(table_name, all_cols)
    return TableDefinition(table_name=table_name, columns=all_cols, ddl=ddl), warnings


def _extract_json_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    """Extract records from JSON Lines format with enhanced nested object handling."""
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue

        row = _base_row(filename, i + 1, i + 1, stripped)

        if isinstance(obj, dict):
            flat = _flatten_json_object(obj)
            for k, v in flat.items():
                safe_k = _sanitize(k)
                if safe_k not in BASELINE_COLUMN_NAMES:
                    if isinstance(v, (dict, list)):
                        row[safe_k] = json.dumps(v)
                    else:
                        row[safe_k] = v
                elif safe_k in (
                    "timestamp",
                    "timestamp_raw",
                    "log_level",
                    "message",
                    "event_type",
                    "source",
                ):
                    row[safe_k] = str(v) if v is not None else None

            if "timestamp_raw" not in row:
                ts_m = ISO_TIMESTAMP_PATTERN.search(stripped)
                if ts_m:
                    row["timestamp_raw"] = ts_m.group(0)
                    row["timestamp"] = ts_m.group(0)

            level_m = LOG_LEVEL_PATTERN.search(stripped)
            if level_m and "log_level" not in row:
                row["log_level"] = level_m.group(1).upper()

            if "message" not in row:
                msg_val = obj.get("message") or obj.get("msg") or obj.get("text") or stripped[:500]
                row["message"] = str(msg_val) if msg_val else stripped[:500]

            if "event_type" not in row:
                event_val = obj.get("event") or obj.get("type") or obj.get("action")
                if event_val:
                    row["event_type"] = str(event_val)

        elif isinstance(obj, list):
            row["_json_array_length"] = len(obj)
            for idx, item in enumerate(obj[:MAX_ARRAY_LENGTH_STORED]):
                if isinstance(item, (dict, list)):
                    row[f"_array_item_{idx}"] = json.dumps(item)
                else:
                    row[f"_array_item_{idx}"] = item
            row["message"] = stripped[:500]
        else:
            row["_json_value"] = str(obj)
            row["message"] = stripped[:500]

        row["parse_confidence"] = 0.95
        records.append(row)

    return records


def _extract_csv_records(content: str, filename: str) -> list[dict[str, Any]]:
    """Extract records from CSV format with improved type inference."""
    records: list[dict[str, Any]] = []
    try:
        reader = csv.DictReader(StringIO(content))
        headers = reader.fieldnames or []
        type_hints: dict[str, str] = {}

        sample_rows = []
        for i, raw_row in enumerate(reader):
            if i >= 100:
                break
            sample_rows.append(raw_row)

        for col in headers:
            if not col:
                continue
            examples = [str(row.get(col, "")) for row in sample_rows if row.get(col)]
            if examples:
                type_result = infer_type(col, examples)
                type_hints[col] = type_result.sql_type.value

        reader = csv.DictReader(StringIO(content))
        for i, raw_row in enumerate(reader):
            line_num = i + 2
            row_values: dict[str, Any] = {}
            for k, v in raw_row.items():
                if not k:
                    continue
                safe_k = _sanitize(k)
                if v is not None and v != "":
                    sql_type = type_hints.get(k, "TEXT")
                    if sql_type == "INTEGER":
                        try:
                            row_values[safe_k] = int(v)
                            continue
                        except ValueError:
                            pass
                    elif sql_type == "REAL":
                        try:
                            row_values[safe_k] = float(v)
                            continue
                        except ValueError:
                            pass
                    row_values[safe_k] = v
                else:
                    row_values[safe_k] = None

            row = _base_row(filename, line_num, line_num, ",".join(str(v) for v in raw_row.values()))
            row.update(row_values)

            raw_text = ",".join(str(v) for v in raw_row.values())
            ts_m = ISO_TIMESTAMP_PATTERN.search(raw_text)
            if ts_m:
                row["timestamp_raw"] = ts_m.group(0)
                row["timestamp"] = ts_m.group(0)

            if "message" not in row:
                row["message"] = raw_text[:500]

            level_m = LOG_LEVEL_PATTERN.search(raw_text)
            if level_m:
                row["log_level"] = level_m.group(1).upper()

            row["parse_confidence"] = 0.9
            records.append(row)
    except Exception as exc:
        logger.warning("CSV extraction error for '%s': %s", filename, exc)
    return records


def _extract_syslog_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    """Extract records from Syslog format."""
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = SYSLOG_RE.match(stripped)
        if not m:
            row = _base_row(filename, i + 1, i + 1, stripped)
            level_m = LOG_LEVEL_PATTERN.search(stripped)
            row["log_level"] = level_m.group(1).upper() if level_m else "INFO"
            row["message"] = stripped[:500]
            row["parse_confidence"] = 0.4
            records.append(row)
            continue

        priority = m.group("priority")
        facility = None
        severity = None
        if priority:
            p = int(priority)
            facility_idx = p >> 3
            severity_idx = p & 0x07
            facility = SYSLOG_FACILITIES[facility_idx] if facility_idx < len(SYSLOG_FACILITIES) else str(facility_idx)
            severity = SYSLOG_SEVERITIES[severity_idx] if severity_idx < len(SYSLOG_SEVERITIES) else str(severity_idx)

        ts_raw = f"{m.group('month')} {m.group('day')} {m.group('time')}"
        row = _base_row(filename, i + 1, i + 1, stripped)
        row.update(
            {
                "timestamp_raw": ts_raw,
                "timestamp": ts_raw,
                "source": m.group("hostname"),
                "log_level": (severity or "INFO").upper(),
                "message": m.group("message"),
                "parse_confidence": 0.9,
                "priority": int(priority) if priority else None,
                "facility": facility,
                "severity": severity,
                "hostname": m.group("hostname"),
                "process_name": m.group("process"),
                "pid": int(m.group("pid")) if m.group("pid") else None,
            }
        )
        records.append(row)
    return records


def _extract_clf_records(lines: list[str], filename: str, format_hint: str) -> list[dict[str, Any]]:
    """Extract records from Apache/Nginx CLF format."""
    records: list[dict[str, Any]] = []
    event_type = "nginx_request" if format_hint == DetectedFormat.NGINX_ACCESS.value else "http_request"

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = CLF_RE.match(stripped)
        if not m:
            row = _base_row(filename, i + 1, i + 1, stripped)
            row["message"] = stripped[:500]
            row["parse_confidence"] = 0.3
            records.append(row)
            continue

        row = _base_row(filename, i + 1, i + 1, stripped)
        row.update(
            {
                "timestamp_raw": m.group("time_local"),
                "timestamp": m.group("time_local"),
                "source": m.group("remote_host"),
                "event_type": event_type,
                "message": f"{m.group('method')} {m.group('path')} → {m.group('status_code')}",
                "parse_confidence": 0.9,
                "remote_host": m.group("remote_host"),
                "ident": m.group("ident"),
                "auth_user": m.group("auth_user"),
                "request_method": m.group("method"),
                "request_path": m.group("path"),
                "request_protocol": m.group("protocol"),
                "status_code": int(m.group("status_code")),
                "response_size": m.group("response_size"),
                "referer": m.group("referer"),
                "user_agent": m.group("user_agent"),
            }
        )
        records.append(row)
    return records


def _extract_logfmt_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    """Extract records from logfmt/key=value format."""
    kv_re = re.compile(r'(\w[\w.\-]*)=(?:"([^"]*)"|(\S+))')
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        row = _base_row(filename, i + 1, i + 1, stripped)
        key_examples: dict[str, list[str]] = {}

        for m in kv_re.finditer(stripped):
            key = _sanitize(m.group(1))
            val = m.group(2) if m.group(2) is not None else m.group(3)
            if key not in BASELINE_COLUMN_NAMES:
                row[key] = val
            elif key in ("timestamp", "timestamp_raw", "log_level", "message"):
                row[key] = val
            examples = key_examples.setdefault(key, [])
            if len(examples) < 3 and val is not None:
                examples.append(val)

        for key, examples in key_examples.items():
            if key not in BASELINE_COLUMN_NAMES and key not in row:
                type_result = infer_type(key, examples)
                if type_result.sql_type == SqlType.INTEGER and row.get(key) is not None:
                    try:
                        row[key] = int(row[key])
                    except (ValueError, TypeError):
                        pass
                elif type_result.sql_type == SqlType.REAL and row.get(key) is not None:
                    try:
                        row[key] = float(row[key])
                    except (ValueError, TypeError):
                        pass

        if "timestamp_raw" not in row:
            ts_m = ISO_TIMESTAMP_PATTERN.search(stripped)
            if ts_m:
                row["timestamp_raw"] = ts_m.group(0)
                row["timestamp"] = ts_m.group(0)

        level_m = LOG_LEVEL_PATTERN.search(stripped)
        if level_m and "log_level" not in row:
            row["log_level"] = level_m.group(1).upper()

        row["message"] = row.get("message") or stripped[:500]
        row["parse_confidence"] = 0.85
        records.append(row)
    return records


def _extract_xml_records(content: str, filename: str) -> list[dict[str, Any]]:
    """Extract records from XML format with enhanced namespace and attribute handling."""
    records: list[dict[str, Any]] = []

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return records

    namespace_map: dict[str, str] = {}
    for elem in root.iter():
        for prefix, uri in elem.tag.items() if isinstance(elem.tag, dict) else []:
            pass
        if isinstance(elem.tag, str) and elem.tag.startswith("{"):
            ns_uri = elem.tag.split("}")[0].lstrip("{")
            for child in elem:
                if isinstance(child.tag, str) and child.tag.startswith("{"):
                    child_ns = child.tag.split("}")[0].lstrip("{")
                    if child_ns not in namespace_map.values():
                        pass

    def _get_line_number(element: ET.Element) -> int:
        return 1

    def _element_to_row(element: ET.Element, root_tag: str = "") -> dict[str, Any]:
        tag_name = element.tag
        if "}" in tag_name:
            ns, local_name = tag_name.split("}", 1)
            tag_name = local_name

        row = _base_row(
            filename,
            _get_line_number(element),
            _get_line_number(element),
            ET.tostring(element, encoding="unicode")[:4000],
        )

        row["event_type"] = f"xml_{_sanitize(tag_name)}"

        for attr_key, attr_value in element.attrib.items():
            if attr_key.startswith("{") and "}" in attr_key:
                _, attr_local = attr_key.split("}", 1)
                safe_key = _sanitize(f"attr_{attr_local}")
            else:
                safe_key = _sanitize(f"attr_{attr_key}")
            if safe_key not in BASELINE_COLUMN_NAMES:
                row[safe_key] = attr_value

        for child in element:
            if len(child) > 0:
                continue
            text = (child.text or "").strip()
            if not text:
                continue

            child_tag = child.tag
            if "}" in child_tag:
                _, child_local = child_tag.split("}", 1)
                child_tag = child_local

            safe_key = _sanitize(child_tag)
            if safe_key not in BASELINE_COLUMN_NAMES:
                if safe_key in row:
                    counter = 2
                    while f"{safe_key}_{counter}" in row:
                        counter += 1
                    safe_key = f"{safe_key}_{counter}"
                row[safe_key] = text

        if namespace_map:
            ns_json = json.dumps(namespace_map, ensure_ascii=True)
            row["_xml_namespaces"] = ns_json[:500]

        row["parse_confidence"] = 0.9
        row["message"] = f"XML record: {tag_name}"
        return row

    children = list(root)
    if not children:
        records.append(_element_to_row(root))
        return records

    root_tag = root.tag
    if "}" in root_tag:
        root_tag = root_tag.split("}", 1)[1]
    records.extend(_element_to_row(child, root_tag) for child in children if isinstance(child.tag, str))

    return records

    namespace_map: dict[str, str] = {}
    for prefix, uri in ET.iter_namespaces(root):
        if prefix:
            namespace_map[prefix] = uri

    def _element_to_row(element: ET.Element, root_tag: str = "") -> dict[str, Any]:
        tag_name = element.tag
        if "}" in tag_name:
            ns, local_name = tag_name.split("}", 1)
            ns_prefix = ns.lstrip("{")
            if ns_prefix in namespace_map:
                prefix = [k for k, v in namespace_map.items() if v == ns_prefix][0]
                tag_name = f"{prefix}_{local_name}"
            else:
                tag_name = local_name

        row = _base_row(
            filename,
            element.sourceline or 1,
            element.sourceline or 1,
            ET.tostring(element, encoding="unicode")[:4000],
        )

        row["event_type"] = f"xml_{_sanitize(tag_name)}"

        for attr_key, attr_value in element.attrib.items():
            if attr_key.startswith("{") and "}" in attr_key:
                _, attr_local = attr_key.split("}", 1)
                safe_key = _sanitize(f"attr_{attr_local}")
            else:
                safe_key = _sanitize(f"attr_{attr_key}")
            if safe_key not in BASELINE_COLUMN_NAMES:
                row[safe_key] = attr_value

        for child in element:
            if len(child) > 0:
                continue
            text = (child.text or "").strip()
            if not text:
                continue

            child_tag = child.tag
            if "}" in child_tag:
                _, child_local = child_tag.split("}", 1)
                child_tag = child_local

            safe_key = _sanitize(child_tag)
            if safe_key not in BASELINE_COLUMN_NAMES:
                if safe_key in row:
                    counter = 2
                    while f"{safe_key}_{counter}" in row:
                        counter += 1
                    safe_key = f"{safe_key}_{counter}"
                row[safe_key] = text

        if namespace_map:
            ns_json = json.dumps(namespace_map, ensure_ascii=True)
            row["_xml_namespaces"] = ns_json[:500]

        row["parse_confidence"] = 0.9
        row["message"] = f"XML record: {tag_name}"
        return row

    children = list(root)
    if not children:
        records.append(_element_to_row(root))
        return records

    root_tag = root.tag
    if "}" in root_tag:
        root_tag = root_tag.split("}", 1)[1]
    records.extend(_element_to_row(child, root_tag) for child in children if isinstance(child.tag, str))

    return records


class StructuredPipeline(ParserPipeline):
    """Parser pipeline for structured log formats.

    Routes each file to the appropriate format-specific extractor based on
    the per-file ``detected_format`` stored in the classification result.
    Uses AI-enriched column inference for improved schema detection.
    """

    parser_key = "structured"

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        filename_lower = request.filename.lower()
        lines = request.content.splitlines()

        score = 0.0
        reasons: list[str] = []
        detected_format: str | None = None

        if filename_lower.endswith(".xml"):
            score = max(score, 0.8)
            reasons.append("XML extension matched.")
            detected_format = "xml"
            try:
                ET.fromstring(request.content)
                score = max(score, 0.92)
                reasons.append("Valid XML payload detected.")
            except ET.ParseError:
                reasons.append("XML extension matched but content could not be parsed as XML.")

        from lib.parsers.preprocessor import LogPreprocessorService

        detector = LogPreprocessorService(table_name="logs")
        fmt, confidence = detector._detect_format(lines) if lines else (DetectedFormat.UNKNOWN, 0.0)
        if fmt in {
            DetectedFormat.JSON_LINES,
            DetectedFormat.CSV,
            DetectedFormat.SYSLOG,
            DetectedFormat.APACHE_ACCESS,
            DetectedFormat.NGINX_ACCESS,
            DetectedFormat.LOGFMT,
            DetectedFormat.KEY_VALUE,
        }:
            score = max(score, confidence)
            detected_format = detected_format or fmt.value
            reasons.append(f"Structured format '{fmt.value}' detected with confidence {confidence:.2f}.")

        if filename_lower.endswith((".json", ".csv")):
            score = max(score, 0.75)
            reasons.append("Structured extension matched (.json/.csv).")

        supported = score >= 0.45
        if not reasons:
            reasons.append("No strong structured signals detected.")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=supported,
            score=round(min(score, 1.0), 2),
            reasons=reasons,
            detected_format=detected_format,
            structural_class=StructuralClass.STRUCTURED,
        )

    def parse(
        self,
        file_inputs: list[FileInput],
        classification: "ClassificationResult",
    ) -> ParserPipelineResult:
        table_defs: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []
        total_confidence = 0.0
        processed = 0

        fc_by_id = {fc.file_id: fc for fc in classification.file_classifications if fc.file_id}
        fc_by_name = {fc.filename: fc for fc in classification.file_classifications}

        for file_input in file_inputs:
            fc = (fc_by_id.get(file_input.file_id) if file_input.file_id else None) or fc_by_name.get(
                file_input.filename
            )
            fmt = fc.detected_format if fc else classification.dominant_format
            lines = file_input.content.splitlines()

            try:
                file_records = self._extract(lines, file_input.content, file_input.filename, fmt)
            except Exception as exc:
                logger.warning(
                    "Structured extraction failed for '%s': %s",
                    file_input.filename,
                    exc,
                )
                warnings.append(f"Extraction failed for '{file_input.filename}': {exc}")
                continue

            if not file_records:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            table_def, enrich_warnings = _build_table_definition(
                self.parser_key,
                file_input.file_id,
                file_input.filename,
                file_records,
                fmt,
                lines[:100],
            )
            table_defs.append(table_def)
            records[table_def.table_name] = file_records
            total_confidence += fc.format_confidence if fc else 0.8
            processed += 1
            warnings.extend(enrich_warnings)

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0
        if not table_defs:
            warnings.append("No tables produced; all files failed extraction.")

        return ParserPipelineResult(
            table_definitions=table_defs,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(min(overall_confidence, 1.0), 2),
        )

    def _extract(self, lines: list[str], content: str, filename: str, detected_format: str) -> list[dict[str, Any]]:
        if detected_format == "xml":
            return _extract_xml_records(content, filename)
        if detected_format == DetectedFormat.JSON_LINES.value:
            return _extract_json_records(lines, filename)
        if detected_format == DetectedFormat.CSV.value:
            return _extract_csv_records(content, filename)
        if detected_format == DetectedFormat.SYSLOG.value:
            return _extract_syslog_records(lines, filename)
        if detected_format in (
            DetectedFormat.APACHE_ACCESS.value,
            DetectedFormat.NGINX_ACCESS.value,
        ):
            return _extract_clf_records(lines, filename, detected_format)
        if detected_format in (
            DetectedFormat.LOGFMT.value,
            DetectedFormat.KEY_VALUE.value,
        ):
            return _extract_logfmt_records(lines, filename)
        if filename.lower().endswith(".xml"):
            return _extract_xml_records(content, filename)
        logger.warning(
            "Unexpected format '%s' in structured pipeline, falling back to logfmt",
            detected_format,
        )
        return _extract_logfmt_records(lines, filename)
