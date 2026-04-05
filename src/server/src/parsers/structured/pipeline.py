from __future__ import annotations

import csv
import json
import logging
import re
import xml.etree.ElementTree as ET
from io import StringIO
from typing import TYPE_CHECKING, Any

from parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserPipelineResult,
    ParserSupportRequest,
    ParserSupportResult,
    StructuralClass,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from parsers.preprocessor import (
    ISO_TIMESTAMP_PATTERN,
    LOG_LEVEL_PATTERN,
    DetectedFormat,
    FileInput,
    LogPreprocessorService,
)
from parsers.registry import ParserPipeline
from parsers.structured.enricher import enrich_structured_schema
from parsers.structured.inference import SqlType, infer_type

if TYPE_CHECKING:
    from parsers.contracts import ClassificationResult

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
SYSLOG_SEVERITIES = ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"]

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
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
    if not sanitized or sanitized[0].isdigit():
        sanitized = "col_" + sanitized
    return sanitized


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
    result: dict[str, Any] = {}
    if depth >= max_depth:
        if isinstance(obj, (dict, list)):
            result[prefix] = json.dumps(obj) if not isinstance(obj, str) else obj
        else:
            result[prefix] = obj
        return result

    if isinstance(obj, dict):
        for key, value in obj.items():
            next_key = f"{prefix}.{key}" if prefix else key
            if value is None:
                result[next_key] = None
            elif isinstance(value, dict):
                result.update(_flatten_json_object(value, next_key, depth + 1, max_depth))
            elif isinstance(value, list):
                result.update(_flatten_json_list(value, next_key, depth + 1, max_depth))
            else:
                result[next_key] = value
    elif isinstance(obj, list):
        result.update(_flatten_json_list(obj, prefix, depth, max_depth))
    else:
        result[prefix] = obj

    return result


def _flatten_json_list(
    values: list[Any],
    prefix: str,
    depth: int,
    max_depth: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not values:
        result[f"{prefix}_length"] = 0
        return result

    result[f"{prefix}_length"] = len(values)

    if len(values) <= MAX_ARRAY_LENGTH_STORED:
        for index, item in enumerate(values):
            key = f"{prefix}_{index}"
            if item is None:
                result[key] = None
            elif isinstance(item, dict):
                result.update(_flatten_json_object(item, key, depth + 1, max_depth))
            elif isinstance(item, list):
                result.update(_flatten_json_list(item, key, depth + 1, max_depth))
            else:
                result[key] = item

    if values and isinstance(values[0], (dict, list)):
        result[f"{prefix}_first_type"] = type(values[0]).__name__.lower()

    return result


def _infer_columns_from_records(records: list[dict[str, Any]], max_sample: int = 100) -> list[ColumnDefinition]:
    key_examples: dict[str, list[str]] = {}
    key_counts: dict[str, int] = {}

    for row in records[:max_sample]:
        for key, value in row.items():
            if key in BASELINE_COLUMN_NAMES:
                continue
            key_counts[key] = key_counts.get(key, 0) + 1
            examples = key_examples.setdefault(key, [])
            if len(examples) < 5 and value is not None:
                examples.append(str(value)[:100])

    threshold = max(1, len(records[:max_sample]) // 10)
    extras: list[ColumnDefinition] = []

    for key, count in key_counts.items():
        if count < threshold:
            continue
        examples = key_examples.get(key, [])
        type_result = infer_type(key, examples)
        extras.append(
            ColumnDefinition(
                name=key,
                sql_type=type_result.sql_type.value,
                description=type_result.description,
                nullable=True,
            )
        )

    return extras


def _build_table_definition(
    parser_key: str,
    file_id: str | None,
    filename: str,
    records: list[dict[str, Any]],
    detected_format: str,
    sample_lines: list[str],
) -> tuple[TableDefinition, list[str]]:
    table_name = make_table_name(parser_key, file_id, filename)
    enriched_columns, warnings = enrich_structured_schema(
        records=records,
        detected_format=detected_format,
        sample_lines=sample_lines,
        use_llm=True,
    )

    extra_columns = _infer_columns_from_records(records)
    existing_names = {column.name for column in enriched_columns}
    for column in extra_columns:
        if column.name not in existing_names:
            enriched_columns.append(column)
            existing_names.add(column.name)

    all_columns = list(BASELINE_COLUMNS) + enriched_columns
    ddl = build_ddl(table_name, all_columns)
    return TableDefinition(table_name=table_name, columns=all_columns, ddl=ddl), warnings


def _extract_json_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue

        row = _base_row(filename, index + 1, index + 1, stripped)

        if isinstance(parsed, dict):
            flat = _flatten_json_object(parsed)
            for key, value in flat.items():
                safe_key = _sanitize(key)
                if safe_key not in BASELINE_COLUMN_NAMES:
                    row[safe_key] = json.dumps(value) if isinstance(value, (dict, list)) else value
                elif safe_key in {"timestamp", "timestamp_raw", "log_level", "message", "event_type", "source"}:
                    row[safe_key] = str(value) if value is not None else None

            if "timestamp_raw" not in row:
                timestamp_match = ISO_TIMESTAMP_PATTERN.search(stripped)
                if timestamp_match:
                    row["timestamp_raw"] = timestamp_match.group(0)
                    row["timestamp"] = timestamp_match.group(0)

            level_match = LOG_LEVEL_PATTERN.search(stripped)
            if level_match and "log_level" not in row:
                row["log_level"] = level_match.group(1).upper()

            if "message" not in row:
                message_value = parsed.get("message") or parsed.get("msg") or parsed.get("text") or stripped[:500]
                row["message"] = str(message_value) if message_value else stripped[:500]

            if "event_type" not in row:
                event_value = parsed.get("event") or parsed.get("type") or parsed.get("action")
                if event_value:
                    row["event_type"] = str(event_value)

        elif isinstance(parsed, list):
            row["_json_array_length"] = len(parsed)
            for item_index, item in enumerate(parsed[:MAX_ARRAY_LENGTH_STORED]):
                row[f"_array_item_{item_index}"] = json.dumps(item) if isinstance(item, (dict, list)) else item
            row["message"] = stripped[:500]
        else:
            row["_json_value"] = str(parsed)
            row["message"] = stripped[:500]

        row["parse_confidence"] = 0.95
        records.append(row)

    return records


def _extract_csv_records(content: str, filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        reader = csv.DictReader(StringIO(content))
        headers = reader.fieldnames or []
        type_hints: dict[str, str] = {}

        sample_rows = []
        for index, raw_row in enumerate(reader):
            if index >= 100:
                break
            sample_rows.append(raw_row)

        for column in headers:
            if not column:
                continue
            examples = [str(row.get(column, "")) for row in sample_rows if row.get(column)]
            if examples:
                type_result = infer_type(column, examples)
                type_hints[column] = type_result.sql_type.value

        reader = csv.DictReader(StringIO(content))
        for index, raw_row in enumerate(reader):
            line_number = index + 2
            row_values: dict[str, Any] = {}
            for key, value in raw_row.items():
                if not key:
                    continue
                safe_key = _sanitize(key)
                if value is None or value == "":
                    row_values[safe_key] = None
                    continue

                sql_type = type_hints.get(key, "TEXT")
                if sql_type == "INTEGER":
                    try:
                        row_values[safe_key] = int(value)
                        continue
                    except ValueError:
                        pass
                elif sql_type == "REAL":
                    try:
                        row_values[safe_key] = float(value)
                        continue
                    except ValueError:
                        pass
                row_values[safe_key] = value

            raw_text = ",".join(str(value) for value in raw_row.values())
            row = _base_row(filename, line_number, line_number, raw_text)
            row.update(row_values)

            timestamp_match = ISO_TIMESTAMP_PATTERN.search(raw_text)
            if timestamp_match:
                row["timestamp_raw"] = timestamp_match.group(0)
                row["timestamp"] = timestamp_match.group(0)

            if "message" not in row:
                row["message"] = raw_text[:500]

            level_match = LOG_LEVEL_PATTERN.search(raw_text)
            if level_match:
                row["log_level"] = level_match.group(1).upper()

            row["parse_confidence"] = 0.9
            records.append(row)
    except Exception as error:  # noqa: BLE001
        logger.warning("CSV extraction error for '%s': %s", filename, error)
    return records


def _extract_syslog_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        match = SYSLOG_RE.match(stripped)
        if not match:
            row = _base_row(filename, index + 1, index + 1, stripped)
            level_match = LOG_LEVEL_PATTERN.search(stripped)
            row["log_level"] = level_match.group(1).upper() if level_match else "INFO"
            row["message"] = stripped[:500]
            row["parse_confidence"] = 0.4
            records.append(row)
            continue

        priority = match.group("priority")
        facility = None
        severity = None
        if priority:
            priority_value = int(priority)
            facility_index = priority_value >> 3
            severity_index = priority_value & 0x07
            facility = (
                SYSLOG_FACILITIES[facility_index] if facility_index < len(SYSLOG_FACILITIES) else str(facility_index)
            )
            severity = (
                SYSLOG_SEVERITIES[severity_index] if severity_index < len(SYSLOG_SEVERITIES) else str(severity_index)
            )

        timestamp_raw = f"{match.group('month')} {match.group('day')} {match.group('time')}"
        row = _base_row(filename, index + 1, index + 1, stripped)
        row.update(
            {
                "timestamp_raw": timestamp_raw,
                "timestamp": timestamp_raw,
                "source": match.group("hostname"),
                "log_level": (severity or "INFO").upper(),
                "message": match.group("message"),
                "parse_confidence": 0.9,
                "priority": int(priority) if priority else None,
                "facility": facility,
                "severity": severity,
                "hostname": match.group("hostname"),
                "process_name": match.group("process"),
                "pid": int(match.group("pid")) if match.group("pid") else None,
            }
        )
        records.append(row)

    return records


def _extract_clf_records(lines: list[str], filename: str, format_hint: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    event_type = "nginx_request" if format_hint == DetectedFormat.NGINX_ACCESS.value else "http_request"

    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        match = CLF_RE.match(stripped)
        if not match:
            row = _base_row(filename, index + 1, index + 1, stripped)
            row["message"] = stripped[:500]
            row["parse_confidence"] = 0.3
            records.append(row)
            continue

        row = _base_row(filename, index + 1, index + 1, stripped)
        row.update(
            {
                "timestamp_raw": match.group("time_local"),
                "timestamp": match.group("time_local"),
                "source": match.group("remote_host"),
                "event_type": event_type,
                "message": f"{match.group('method')} {match.group('path')} -> {match.group('status_code')}",
                "parse_confidence": 0.9,
                "remote_host": match.group("remote_host"),
                "ident": match.group("ident"),
                "auth_user": match.group("auth_user"),
                "request_method": match.group("method"),
                "request_path": match.group("path"),
                "request_protocol": match.group("protocol"),
                "status_code": int(match.group("status_code")),
                "response_size": match.group("response_size"),
                "referer": match.group("referer"),
                "user_agent": match.group("user_agent"),
            }
        )
        records.append(row)

    return records


def _extract_logfmt_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    kv_regex = re.compile(r'(\w[\w.\-]*)=(?:"([^"]*)"|(\S+))')
    records: list[dict[str, Any]] = []

    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        row = _base_row(filename, index + 1, index + 1, stripped)
        key_examples: dict[str, list[str]] = {}

        for match in kv_regex.finditer(stripped):
            key = _sanitize(match.group(1))
            value = match.group(2) if match.group(2) is not None else match.group(3)
            if key not in BASELINE_COLUMN_NAMES:
                row[key] = value
            elif key in {"timestamp", "timestamp_raw", "log_level", "message"}:
                row[key] = value

            examples = key_examples.setdefault(key, [])
            if len(examples) < 3 and value is not None:
                examples.append(value)

        for key, examples in key_examples.items():
            if key in BASELINE_COLUMN_NAMES:
                continue
            if key not in row:
                continue
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
            timestamp_match = ISO_TIMESTAMP_PATTERN.search(stripped)
            if timestamp_match:
                row["timestamp_raw"] = timestamp_match.group(0)
                row["timestamp"] = timestamp_match.group(0)

        level_match = LOG_LEVEL_PATTERN.search(stripped)
        if level_match and "log_level" not in row:
            row["log_level"] = level_match.group(1).upper()

        row["message"] = row.get("message") or stripped[:500]
        row["parse_confidence"] = 0.85
        records.append(row)

    return records


def _extract_xml_records(content: str, filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return records

    def _line_number(_element: ET.Element) -> int:
        return 1

    def _element_to_row(element: ET.Element) -> dict[str, Any]:
        tag_name = element.tag
        if "}" in tag_name:
            _, local_name = tag_name.split("}", 1)
            tag_name = local_name

        row = _base_row(
            filename,
            _line_number(element),
            _line_number(element),
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
            value = (child.text or "").strip()
            if not value:
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
                row[safe_key] = value

        row["parse_confidence"] = 0.9
        row["message"] = f"XML record: {tag_name}"
        return row

    children = list(root)
    if not children:
        records.append(_element_to_row(root))
        return records

    records.extend(_element_to_row(child) for child in children if isinstance(child.tag, str))
    return records


class StructuredPipeline(ParserPipeline):
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

        if not reasons:
            reasons.append("No strong structured signals detected.")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=score >= 0.45,
            score=round(min(score, 1.0), 2),
            reasons=reasons,
            detected_format=detected_format,
            structural_class=StructuralClass.STRUCTURED,
        )

    def parse(self, file_inputs: list[FileInput], classification: "ClassificationResult") -> ParserPipelineResult:
        table_definitions: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []
        total_confidence = 0.0
        processed = 0

        classification_by_id = {fc.file_id: fc for fc in classification.file_classifications if fc.file_id}
        classification_by_name = {fc.filename: fc for fc in classification.file_classifications}

        for file_input in file_inputs:
            file_classification = (
                classification_by_id.get(file_input.file_id) if file_input.file_id else None
            ) or classification_by_name.get(file_input.filename)
            fmt = file_classification.detected_format if file_classification else classification.dominant_format
            lines = file_input.content.splitlines()

            try:
                file_records = self._extract(lines, file_input.content, file_input.filename, fmt)
            except Exception as error:  # noqa: BLE001
                logger.warning("Structured extraction failed for '%s': %s", file_input.filename, error)
                warnings.append(f"Extraction failed for '{file_input.filename}': {error}")
                continue

            if not file_records:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            table_definition, enrich_warnings = _build_table_definition(
                self.parser_key,
                file_input.file_id,
                file_input.filename,
                file_records,
                fmt,
                lines[:100],
            )

            table_definitions.append(table_definition)
            records[table_definition.table_name] = file_records
            total_confidence += file_classification.format_confidence if file_classification else 0.8
            processed += 1
            warnings.extend(enrich_warnings)

        overall_confidence = (total_confidence / processed) if processed > 0 else 0.0
        if not table_definitions:
            warnings.append("No tables produced; all files failed extraction.")

        return ParserPipelineResult(
            table_definitions=table_definitions,
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
        if detected_format in {DetectedFormat.APACHE_ACCESS.value, DetectedFormat.NGINX_ACCESS.value}:
            return _extract_clf_records(lines, filename, detected_format)
        if detected_format in {DetectedFormat.LOGFMT.value, DetectedFormat.KEY_VALUE.value}:
            return _extract_logfmt_records(lines, filename)
        if filename.lower().endswith(".xml"):
            return _extract_xml_records(content, filename)

        logger.warning("Unexpected format '%s' in structured pipeline, falling back to logfmt", detected_format)
        return _extract_logfmt_records(lines, filename)
