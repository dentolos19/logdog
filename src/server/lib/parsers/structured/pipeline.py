"""Structured log parser pipeline.

Handles files classified as ``StructuralClass.STRUCTURED``:
  JSON Lines, CSV, Syslog (RFC 3164), Apache/Nginx CLF, Logfmt, Key-Value.

For each file the pipeline:
  1. Extracts records using format-specific logic.
  2. Infers extra (non-baseline) columns from the records.
  3. Generates a CREATE TABLE DDL statement.
  4. Returns all table definitions and row data via ``ParserPipelineResult``.
"""

from __future__ import annotations

import csv
import json
import logging
import re
from io import StringIO
from typing import TYPE_CHECKING, Any

from lib.parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserPipelineResult,
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

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult

if False:  # TYPE_CHECKING
    from lib.parsers.contracts import ClassificationResult  # noqa: F401

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"

# Syslog facility-name lookup (value 0-23 maps to name).
_SYSLOG_FACILITIES = [
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
_SYSLOG_SEVERITIES = ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"]

# Regex for full syslog line: optional priority + month day time host proc[pid]: msg
_SYSLOG_RE = re.compile(
    r"^(?:<(?P<priority>\d{1,3})>)?"
    r"(?P<month>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(?P<day>\d{1,2})\s+(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<hostname>\S+)\s+(?P<process>\S+?)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<message>.*)",
    re.IGNORECASE,
)

# Apache CLF: host ident authuser [date] "request" status bytes [referer UA]
_CLF_RE = re.compile(
    r"^(?P<remote_host>\S+)\s+(?P<ident>\S+)\s+(?P<auth_user>\S+)\s+"
    r"\[(?P<time_local>[^\]]+)\]\s+"
    r'"(?P<method>\S+)\s+(?P<path>\S+)\s+(?P<protocol>[^"]+)"\s+'
    r"(?P<status_code>\d{3})\s+(?P<response_size>\S+)"
    r'(?:\s+"(?P<referer>[^"]*)"\s+"(?P<user_agent>[^"]*)")?',
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    s = re.sub(r"_+", "_", s).strip("_").lower()
    if not s or s[0].isdigit():
        s = "col_" + s
    return s


def _infer_columns_from_records(
    records: list[dict[str, Any]],
    max_sample: int = 50,
) -> list[ColumnDefinition]:
    """Build ColumnDefinitions for keys that appear in records but are not baseline columns."""
    key_counts: dict[str, int] = {}
    key_examples: dict[str, list[str]] = {}

    for row in records[:max_sample]:
        for k, v in row.items():
            if k in BASELINE_COLUMN_NAMES:
                continue
            key_counts[k] = key_counts.get(k, 0) + 1
            exs = key_examples.setdefault(k, [])
            if len(exs) < 3 and v is not None:
                exs.append(str(v)[:100])

    threshold = max(1, len(records[:max_sample]) // 10)
    extra: list[ColumnDefinition] = []
    for k, count in key_counts.items():
        if count >= threshold:
            examples = key_examples.get(k, [])
            sql_type = "TEXT"
            if all(re.match(r"^-?\d+$", str(e)) for e in examples if e):
                sql_type = "INTEGER"
            elif all(re.match(r"^-?[\d.eE+\-]+$", str(e)) for e in examples if e):
                sql_type = "REAL"
            extra.append(ColumnDefinition(name=k, sql_type=sql_type))

    return extra


def _build_table_definition(
    parser_key: str,
    file_id: str | None,
    filename: str,
    records: list[dict[str, Any]],
) -> TableDefinition:
    table_name = make_table_name(parser_key, file_id, filename)
    extra_cols = _infer_columns_from_records(records)
    all_cols = list(BASELINE_COLUMNS) + extra_cols
    ddl = build_ddl(table_name, all_cols)
    return TableDefinition(table_name=table_name, columns=all_cols, sqlite_ddl=ddl)


def _base_row(filename: str, line_start: int, line_end: int, raw_text: str) -> dict[str, Any]:
    return {
        "source": filename,
        "source_type": "file",
        "schema_version": SCHEMA_VERSION,
        "line_start": line_start,
        "line_end": line_end,
        "raw_text": raw_text[:4000],
    }


# ---------------------------------------------------------------------------
# Format-specific record extractors
# ---------------------------------------------------------------------------


def _extract_json_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        row = _base_row(filename, i + 1, i + 1, stripped)
        # Merge JSON fields; stash anything that clobbers baseline into additional_data.
        extra: dict[str, Any] = {}
        for k, v in obj.items():
            safe_k = _sanitize(k)
            if safe_k not in BASELINE_COLUMN_NAMES:
                extra[safe_k] = v
            elif safe_k in ("timestamp", "timestamp_raw", "log_level", "message", "event_type", "source"):
                row[safe_k] = str(v) if v is not None else None
        row.update(extra)
        if "timestamp_raw" not in row:
            ts_m = ISO_TIMESTAMP_PATTERN.search(stripped)
            if ts_m:
                row["timestamp_raw"] = ts_m.group(0)
                row["timestamp"] = ts_m.group(0)
        level_m = LOG_LEVEL_PATTERN.search(stripped)
        if level_m and "log_level" not in row:
            row["log_level"] = level_m.group(1).upper()
        row["message"] = row.get("message") or stripped[:500]
        row["parse_confidence"] = 0.95
        records.append(row)
    return records


def _extract_csv_records(content: str, filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        reader = csv.DictReader(StringIO(content))
        for i, raw_row in enumerate(reader):
            line_num = i + 2  # account for header row
            row_values = {_sanitize(k): v for k, v in raw_row.items() if k}
            row = _base_row(filename, line_num, line_num, ",".join(str(v) for v in raw_row.values()))
            row.update(row_values)
            ts_m = ISO_TIMESTAMP_PATTERN.search(row["raw_text"])
            if ts_m:
                row["timestamp_raw"] = ts_m.group(0)
                row["timestamp"] = ts_m.group(0)
            row["message"] = row.get("message") or row["raw_text"][:500]
            row["parse_confidence"] = 0.9
            records.append(row)
    except Exception as exc:
        logger.warning("CSV extraction error for '%s': %s", filename, exc)
    return records


def _extract_syslog_records(lines: list[str], filename: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = _SYSLOG_RE.match(stripped)
        if not m:
            # Best-effort: emit raw line with whatever we can extract.
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
            facility = _SYSLOG_FACILITIES[facility_idx] if facility_idx < len(_SYSLOG_FACILITIES) else str(facility_idx)
            severity = _SYSLOG_SEVERITIES[severity_idx] if severity_idx < len(_SYSLOG_SEVERITIES) else str(severity_idx)

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
    records: list[dict[str, Any]] = []
    event_type = "nginx_request" if format_hint == DetectedFormat.NGINX_ACCESS.value else "http_request"

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        m = _CLF_RE.match(stripped)
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
    _kv_re = re.compile(r'(\w[\w.\-]*)=(?:"([^"]*)"|(\S+))')
    records: list[dict[str, Any]] = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        row = _base_row(filename, i + 1, i + 1, stripped)
        for m in _kv_re.finditer(stripped):
            key = _sanitize(m.group(1))
            val = m.group(2) if m.group(2) is not None else m.group(3)
            if key not in BASELINE_COLUMN_NAMES:
                row[key] = val
            elif key in ("timestamp", "timestamp_raw", "log_level", "message"):
                row[key] = val
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


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class StructuredPipeline(ParserPipeline):
    """Parser pipeline for structured log formats.

    Routes each file to the appropriate format-specific extractor based on
    the per-file ``detected_format`` stored in the classification result.
    """

    parser_key = "structured"

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

        # Build a quick lookup: filename → file classification.
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
                logger.warning("Structured extraction failed for '%s': %s", file_input.filename, exc)
                warnings.append(f"Extraction failed for '{file_input.filename}': {exc}")
                continue

            if not file_records:
                warnings.append(f"No records extracted from '{file_input.filename}'.")
                continue

            table_def = _build_table_definition(self.parser_key, file_input.file_id, file_input.filename, file_records)
            table_defs.append(table_def)
            records[table_def.table_name] = file_records
            total_confidence += fc.format_confidence if fc else 0.8
            processed += 1

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
        if detected_format == DetectedFormat.JSON_LINES.value:
            return _extract_json_records(lines, filename)
        if detected_format == DetectedFormat.CSV.value:
            return _extract_csv_records(content, filename)
        if detected_format == DetectedFormat.SYSLOG.value:
            return _extract_syslog_records(lines, filename)
        if detected_format in (DetectedFormat.APACHE_ACCESS.value, DetectedFormat.NGINX_ACCESS.value):
            return _extract_clf_records(lines, filename, detected_format)
        if detected_format in (DetectedFormat.LOGFMT.value, DetectedFormat.KEY_VALUE.value):
            return _extract_logfmt_records(lines, filename)
        # Fallback: treat as logfmt/kv
        logger.warning("Unexpected format '%s' in structured pipeline, falling back to logfmt", detected_format)
        return _extract_logfmt_records(lines, filename)
