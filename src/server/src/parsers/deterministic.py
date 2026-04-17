from __future__ import annotations

import csv
import json
import re
from typing import Any

from parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserPipelineResult,
    ParserSupportRequest,
    ParserSupportResult,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from parsers.registry import ParserPipeline

APACHE_ACCESS_RE = re.compile(
    r"^(?P<ip>\S+)\s+(?P<ident>\S+)\s+(?P<authuser>\S+)\s+\[(?P<timestamp>[^\]]+)\]\s+\"(?P<request>[^\"]*)\"\s+(?P<status>\d{3})\s+(?P<body_bytes>\S+)(?:\s+\"(?P<referer>[^\"]*)\"\s+\"(?P<user_agent>[^\"]*)\")?"
)
SYSLOG_RE = re.compile(
    r"^(?:<(?P<pri>\d{1,3})>)?"
    r"(?P<timestamp>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+"
    r"(?P<process>[\w.\-/]+)(?:\[(?P<pid>\d+)\])?:\s*(?P<message>.*)$",
    re.IGNORECASE,
)
LOGFMT_RE = re.compile(r"(\w[\w.\-]*)=(\"[^\"]*\"|\S+)")
KEY_VALUE_RE = re.compile(r"(\w[\w.\-]*)\s*[:=]\s*(\"[^\"]*\"|\S+)")


class DeterministicParserPipeline(ParserPipeline):
    parser_key: str = ""
    supported_extensions: tuple[str, ...] = tuple()

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        lower_name = request.filename.lower()
        if self.supported_extensions and any(lower_name.endswith(ext) for ext in self.supported_extensions):
            return ParserSupportResult(
                parser_key=self.parser_key,
                supported=True,
                score=0.95,
                reasons=["File extension strongly matches parser."],
            )

        score = self._score_content(request.content)
        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=score >= 0.6,
            score=round(score, 2),
            reasons=["Deterministic parser content scoring."],
        )

    def parse(self, file_inputs: list[Any], classification: Any) -> ParserPipelineResult:
        table_definitions: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []

        for file_input in file_inputs:
            rows, parse_warnings = self._parse_rows(file_input.content, file_input.filename)
            warnings.extend(f"{file_input.filename}: {warning}" for warning in parse_warnings)
            if not rows:
                warnings.append(f"{file_input.filename}: no rows were extracted by parser '{self.parser_key}'.")
                continue

            table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
            columns = self._infer_columns(rows)
            ddl = build_ddl(table_name, columns)
            table_definitions.append(TableDefinition(table_name=table_name, columns=columns, ddl=ddl))
            records[table_name] = rows

        confidence = 0.95 if table_definitions else 0.0
        return ParserPipelineResult(
            table_definitions=table_definitions,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=confidence,
            diagnostics={
                "mode": "deterministic",
                "parser_key": self.parser_key,
                "input_files": len(file_inputs),
                "classified_format": getattr(classification, "dominant_format", "unknown"),
            },
        )

    def _score_content(self, content: str) -> float:
        return 0.6

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        raise NotImplementedError

    def _infer_columns(self, rows: list[dict[str, Any]]) -> list[ColumnDefinition]:
        merged: list[ColumnDefinition] = list(BASELINE_COLUMNS)
        merged.extend(
            [
                ColumnDefinition(name="source", sql_type="TEXT", description="Source filename.", nullable=True),
                ColumnDefinition(name="message", sql_type="TEXT", description="Primary message text.", nullable=True),
                ColumnDefinition(name="log_level", sql_type="TEXT", description="Detected log level.", nullable=True),
            ]
        )
        existing = {column.name for column in merged}

        key_types: dict[str, str] = {}
        for row in rows:
            for key, value in row.items():
                if key in BASELINE_COLUMN_NAMES or key in {"source", "message", "log_level"}:
                    continue
                if key not in key_types:
                    key_types[key] = _infer_sql_type(value)

        for key, sql_type in sorted(key_types.items()):
            if key in existing:
                continue
            merged.append(
                ColumnDefinition(
                    name=key,
                    sql_type=sql_type,
                    description=f"Extracted field '{key}'.",
                    nullable=True,
                )
            )

        return merged


class JsonLinesPipeline(DeterministicParserPipeline):
    parser_key = "json_lines"
    supported_extensions = (".json", ".jsonl", ".ndjson")

    def _score_content(self, content: str) -> float:
        lines = [line.strip() for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = sum(1 for line in lines if line.startswith("{") and line.endswith("}"))
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        lines = [line for line in content.splitlines() if line.strip()]
        for index, line in enumerate(lines, start=1):
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                warnings.append(f"line {index}: invalid JSON object")
                continue
            if not isinstance(parsed, dict):
                warnings.append(f"line {index}: JSON value is not an object")
                continue
            row = _flatten_json(parsed)
            row["source"] = filename
            row.setdefault("message", line[:500])
            row.setdefault("log_level", _infer_log_level(line))
            row["raw"] = line[:4000]
            rows.append(row)
        return rows, warnings


class CsvPipeline(DeterministicParserPipeline):
    parser_key = "csv"
    supported_extensions = (".csv", ".tsv")

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:10] if line.strip()]
        if len(lines) < 2:
            return 0.0
        delimiter = "\t" if "\t" in lines[0] else ","
        expected = lines[0].count(delimiter) + 1
        if expected < 2:
            return 0.0
        matches = sum(1 for line in lines[1:] if line.count(delimiter) + 1 == expected)
        return matches / max(len(lines) - 1, 1)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        warnings: list[str] = []
        lines = [line for line in content.splitlines() if line.strip()]
        if not lines:
            return [], warnings

        delimiter = "\t" if "\t" in lines[0] else ","
        reader = csv.DictReader(lines, delimiter=delimiter)
        rows: list[dict[str, Any]] = []
        for index, raw_row in enumerate(reader, start=2):
            if raw_row is None:
                continue
            row: dict[str, Any] = {
                "source": filename,
                "raw": lines[index - 1][:4000] if index - 1 < len(lines) else "",
                "message": "",
                "log_level": "INFO",
            }
            for key, value in raw_row.items():
                if key is None:
                    continue
                safe_key = _sanitize(key)
                row[safe_key] = _cast_value(value.strip()) if isinstance(value, str) else value
            row["message"] = row.get("message") or row["raw"][:500]
            rows.append(row)
        return rows, warnings


class SyslogPipeline(DeterministicParserPipeline):
    parser_key = "syslog"
    supported_extensions = (".syslog", ".log")

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = sum(1 for line in lines if SYSLOG_RE.match(line))
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for index, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            match = SYSLOG_RE.match(stripped)
            if match is None:
                warnings.append(f"line {index}: not valid syslog format")
                continue
            row: dict[str, Any] = {
                "timestamp": match.group("timestamp"),
                "host": match.group("host"),
                "process": match.group("process"),
                "pid": _cast_value(match.group("pid") or ""),
                "message": (match.group("message") or "")[:500],
                "log_level": _infer_log_level(match.group("message") or stripped),
                "source": filename,
                "raw": stripped[:4000],
            }
            pri = match.group("pri")
            if pri is not None:
                pri_value = int(pri)
                row["syslog_pri"] = pri_value
                row["syslog_facility"] = pri_value // 8
                row["syslog_severity"] = pri_value % 8
            rows.append(row)
        return rows, warnings


class ApacheAccessPipeline(DeterministicParserPipeline):
    parser_key = "apache_access"

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = sum(1 for line in lines if APACHE_ACCESS_RE.match(line))
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        return _parse_access_rows(content, filename)


class NginxAccessPipeline(DeterministicParserPipeline):
    parser_key = "nginx_access"

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = 0
        for line in lines:
            if APACHE_ACCESS_RE.match(line) and ("nginx" in line.lower() or "upstream" in line.lower()):
                hits += 1
        if hits == 0:
            hits = sum(1 for line in lines if APACHE_ACCESS_RE.match(line))
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        return _parse_access_rows(content, filename)


class LogfmtPipeline(DeterministicParserPipeline):
    parser_key = "logfmt"

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = sum(1 for line in lines if len(LOGFMT_RE.findall(line)) >= 2)
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for index, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            pairs = LOGFMT_RE.findall(stripped)
            if len(pairs) < 1:
                warnings.append(f"line {index}: no logfmt pairs")
                continue

            row: dict[str, Any] = {
                "source": filename,
                "raw": stripped[:4000],
                "message": stripped[:500],
                "log_level": _infer_log_level(stripped),
            }
            for key, value in pairs:
                row[_sanitize(key)] = _cast_value(value.strip().strip('"'))
            rows.append(row)
        return rows, warnings


class KeyValuePipeline(DeterministicParserPipeline):
    parser_key = "key_value"

    def _score_content(self, content: str) -> float:
        lines = [line for line in content.splitlines()[:25] if line.strip()]
        if not lines:
            return 0.0
        hits = sum(1 for line in lines if len(KEY_VALUE_RE.findall(line)) >= 2)
        return hits / len(lines)

    def _parse_rows(self, content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        warnings: list[str] = []
        for index, line in enumerate(content.splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            pairs = KEY_VALUE_RE.findall(stripped)
            if len(pairs) < 1:
                warnings.append(f"line {index}: no key-value pairs")
                continue

            row: dict[str, Any] = {
                "source": filename,
                "raw": stripped[:4000],
                "message": stripped[:500],
                "log_level": _infer_log_level(stripped),
            }
            for key, value in pairs:
                row[_sanitize(key)] = _cast_value(value.strip().strip('"'))
            rows.append(row)
        return rows, warnings


def _parse_access_rows(content: str, filename: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for index, line in enumerate(content.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        match = APACHE_ACCESS_RE.match(stripped)
        if match is None:
            warnings.append(f"line {index}: not valid access log format")
            continue

        row: dict[str, Any] = {
            "ip": match.group("ip"),
            "ident": match.group("ident"),
            "authuser": match.group("authuser"),
            "timestamp": match.group("timestamp"),
            "request": match.group("request"),
            "status": _cast_value(match.group("status")),
            "body_bytes": _cast_value(match.group("body_bytes")),
            "referer": match.group("referer"),
            "user_agent": match.group("user_agent"),
            "source": filename,
            "raw": stripped[:4000],
            "message": stripped[:500],
            "log_level": _status_to_level(_cast_value(match.group("status"))),
        }
        rows.append(row)
    return rows, warnings


def _infer_sql_type(value: Any) -> str:
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


def _flatten_json(value: dict[str, Any], prefix: str = "", depth: int = 0) -> dict[str, Any]:
    if depth > 4:
        return {prefix.rstrip("_") or "value": json.dumps(value, ensure_ascii=True)}

    result: dict[str, Any] = {}
    for key, raw in value.items():
        safe_key = _sanitize(key)
        full_key = f"{prefix}{safe_key}" if prefix else safe_key
        if isinstance(raw, dict):
            result.update(_flatten_json(raw, prefix=f"{full_key}_", depth=depth + 1))
        elif isinstance(raw, list):
            if len(raw) <= 10 and all(not isinstance(item, (dict, list)) for item in raw):
                result[full_key] = ",".join(str(item) for item in raw)
            else:
                result[full_key] = json.dumps(raw, ensure_ascii=True)
        else:
            result[full_key] = raw
    return result


def _sanitize(value: str) -> str:
    sanitized = "".join(character if character.isalnum() or character == "_" else "_" for character in value)
    sanitized = "_".join(part for part in sanitized.split("_") if part).lower()
    if not sanitized:
        return "field"
    if sanitized[0].isdigit():
        return f"field_{sanitized}"
    return sanitized


def _cast_value(value: str | None) -> Any:
    if value is None:
        return None

    lowered = value.lower()
    if lowered in {"", "null", "none", "-"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _status_to_level(status: Any) -> str:
    if isinstance(status, int):
        if status >= 500:
            return "ERROR"
        if status >= 400:
            return "WARNING"
    return "INFO"


def _infer_log_level(raw: str) -> str:
    lowered = raw.lower()
    if "critical" in lowered or "panic" in lowered:
        return "CRITICAL"
    if "fatal" in lowered:
        return "FATAL"
    if "error" in lowered:
        return "ERROR"
    if "warn" in lowered:
        return "WARNING"
    if "debug" in lowered:
        return "DEBUG"
    if "trace" in lowered:
        return "TRACE"
    return "INFO"
