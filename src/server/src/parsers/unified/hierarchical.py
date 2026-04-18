from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any

from parsers.normalization import coerce_scalar, sanitize_identifier, unique_identifier

MULTILINE_CONTINUATION_RE = re.compile(r"^(?:\s+at\s|Caused by:|\.{3}\s*\d+\s*more|\s{4,}\S|\t\S)")
KEY_VALUE_RE = re.compile(r"(\w[\w.\-]*)\s*[:=]\s*(\"[^\"]*\"|\S+)")
LOG_LEVEL_RE = re.compile(
    r"\b(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|NOTICE|ALERT|EMERG(?:ENCY)?)\b", re.IGNORECASE
)
TIMESTAMP_RE = re.compile(
    r"(?:\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)
SYSLOG_RE = re.compile(
    r"^(?:<(?P<pri>\d{1,3})>)?"
    r"(?P<ts>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+"
    r"(?P<proc>[\w.\-/]+)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<msg>.*)$",
    re.IGNORECASE,
)

SEVERITY_BY_CODE = {
    0: "EMERGENCY",
    1: "ALERT",
    2: "CRITICAL",
    3: "ERROR",
    4: "WARNING",
    5: "NOTICE",
    6: "INFO",
    7: "DEBUG",
}


@dataclass
class ParseUnit:
    start_line: int
    end_line: int
    raw: str
    fields: dict[str, Any] = field(default_factory=dict)
    level: str = "line"
    confidence: float = 0.5


@dataclass
class HierarchicalParseResult:
    line_units: list[ParseUnit] = field(default_factory=list)
    block_units: list[ParseUnit] = field(default_factory=list)
    file_units: list[ParseUnit] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def all_units(self) -> list[ParseUnit]:
        return self.block_units or self.line_units or self.file_units


class HierarchicalParser:
    def analyze(self, lines: list[str], format_name: str) -> HierarchicalParseResult:
        if not lines:
            return HierarchicalParseResult()

        line_units = self._parse_line_units(lines, format_name)
        block_units = self._parse_block_units(lines, format_name)
        file_units = self._parse_file_units(lines, format_name)

        warnings: list[str] = []
        if not line_units and not block_units and not file_units:
            warnings.append("No parseable units were detected.")

        return HierarchicalParseResult(
            line_units=line_units,
            block_units=block_units,
            file_units=file_units,
            warnings=warnings,
        )

    def _parse_line_units(self, lines: list[str], format_name: str) -> list[ParseUnit]:
        units: list[ParseUnit] = []

        for index, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped:
                continue

            fields, confidence = self.extract_fields(stripped, format_name)
            units.append(
                ParseUnit(
                    start_line=index,
                    end_line=index,
                    raw=stripped,
                    fields=fields,
                    level="line",
                    confidence=confidence,
                )
            )

        return units

    def _parse_block_units(self, lines: list[str], format_name: str) -> list[ParseUnit]:
        if format_name in {"syslog", "json_lines", "json_document", "csv", "apache_clf", "logfmt"}:
            return []

        blocks: list[tuple[int, int, str]] = []
        current_start = 1
        current_lines: list[str] = []

        for index, line in enumerate(lines, start=1):
            stripped = line.rstrip("\n")
            if not stripped.strip():
                if current_lines:
                    blocks.append((current_start, index - 1, "\n".join(current_lines)))
                    current_lines = []
                current_start = index + 1
                continue

            if current_lines and MULTILINE_CONTINUATION_RE.match(stripped):
                current_lines.append(stripped)
                continue

            if current_lines and not MULTILINE_CONTINUATION_RE.match(stripped):
                blocks.append((current_start, index - 1, "\n".join(current_lines)))
                current_start = index
                current_lines = [stripped]
            else:
                current_lines.append(stripped)

        if current_lines:
            blocks.append((current_start, len(lines), "\n".join(current_lines)))

        units: list[ParseUnit] = []
        for start, end, raw in blocks:
            fields, confidence = self.extract_fields(raw, format_name)
            units.append(
                ParseUnit(
                    start_line=start,
                    end_line=end,
                    raw=raw,
                    fields=fields,
                    level="block",
                    confidence=confidence,
                )
            )

        return units

    def _parse_file_units(self, lines: list[str], format_name: str) -> list[ParseUnit]:
        if format_name in {"syslog", "json_lines", "csv", "apache_clf", "logfmt"}:
            return []

        content = "\n".join(lines)
        fields, confidence = self.extract_fields(content, format_name)
        if not fields:
            return []

        return [
            ParseUnit(
                start_line=1,
                end_line=len(lines),
                raw=content[:20000],
                fields=fields,
                level="file",
                confidence=confidence,
            )
        ]

    def extract_fields(self, text: str, format_name: str) -> tuple[dict[str, Any], float]:
        stripped = text.strip()
        if not stripped:
            return {}, 0.0

        if format_name in {"json_lines", "json_document"} or stripped.startswith("{"):
            parsed = self._extract_json(stripped)
            if parsed:
                return parsed, 0.95

        if format_name == "syslog":
            parsed = self._extract_syslog(stripped)
            if parsed:
                return parsed, 0.95

        if format_name == "xml" or stripped.startswith("<"):
            parsed = self._extract_xml(stripped)
            if parsed:
                return parsed, 0.9

        key_values = self._extract_key_values(stripped)
        if key_values:
            key_values.setdefault("message", stripped[:500])
            self._extract_common_fields(stripped, key_values)
            return key_values, 0.8

        fields: dict[str, Any] = {"message": stripped[:500]}
        self._extract_common_fields(stripped, fields)
        return fields, 0.6

    def _extract_syslog(self, text: str) -> dict[str, Any] | None:
        match = SYSLOG_RE.match(text)
        if not match:
            return None

        fields: dict[str, Any] = {
            "timestamp": match.group("ts"),
            "hostname": match.group("host"),
            "process": match.group("proc"),
            "pid": self._cast_value(match.group("pid") or "") if match.group("pid") else None,
        }

        pri_raw = match.group("pri")
        if pri_raw:
            pri = int(pri_raw)
            facility = pri // 8
            severity_code = pri % 8
            fields["syslog_pri"] = pri
            fields["syslog_facility"] = facility
            fields["syslog_severity"] = severity_code
            fields["log_level"] = SEVERITY_BY_CODE.get(severity_code, "INFO")

        message = (match.group("msg") or "").strip()
        if message:
            fields["message"] = message[:500]

            event_match = re.match(r"([A-Z][A-Z0-9_]+)\s*:\s*(.*)$", message)
            if event_match:
                fields["event_type"] = event_match.group(1)
                trailing = event_match.group(2)
                kv_fields = self._extract_key_values(trailing)
                fields.update(kv_fields)
            else:
                kv_fields = self._extract_key_values(message)
                fields.update(kv_fields)

        return fields

    def _extract_json(self, text: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

        if isinstance(parsed, dict):
            return self._flatten_dict(parsed)
        return None

    def _extract_xml(self, text: str) -> dict[str, Any] | None:
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return None

        fields: dict[str, Any] = {}
        used_names: set[str] = set()
        for key, value in root.attrib.items():
            field_name = unique_identifier(f"attr_{self._sanitize(key)}", used_names)
            used_names.add(field_name)
            fields[field_name] = value

        for child in root.iter():
            if child is root:
                continue
            tag = self._sanitize(child.tag)
            if child.text and child.text.strip() and len(child) == 0:
                field_name = unique_identifier(tag, used_names)
                used_names.add(field_name)
                fields[field_name] = child.text.strip()
            for key, value in child.attrib.items():
                field_name = unique_identifier(f"{tag}_attr_{self._sanitize(key)}", used_names)
                used_names.add(field_name)
                fields[field_name] = value

        return fields or None

    def _extract_key_values(self, text: str) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        for match in KEY_VALUE_RE.finditer(text):
            key = self._sanitize(match.group(1))
            value = match.group(2).strip().strip('"')
            fields[key] = self._cast_value(value)
        return fields

    def _extract_common_fields(self, text: str, fields: dict[str, Any]) -> None:
        timestamp_match = TIMESTAMP_RE.search(text)
        if timestamp_match:
            fields.setdefault("timestamp", timestamp_match.group(0))

        log_level_match = LOG_LEVEL_RE.search(text)
        if log_level_match:
            fields.setdefault("log_level", log_level_match.group(1).upper())

    def _flatten_dict(self, value: dict[str, Any], prefix: str = "", depth: int = 0) -> dict[str, Any]:
        used_names: set[str] = set()

        def _flatten(current: dict[str, Any], current_prefix: str = "", current_depth: int = 0) -> dict[str, Any]:
            if current_depth > 4:
                key = unique_identifier(current_prefix.rstrip("_") or "value", used_names)
                used_names.add(key)
                return {key: json.dumps(current, ensure_ascii=True)}

            result: dict[str, Any] = {}
            for key, raw in current.items():
                safe_key = self._sanitize(key)
                full_key = f"{current_prefix}{safe_key}" if current_prefix else safe_key
                if isinstance(raw, dict):
                    result.update(_flatten(raw, current_prefix=f"{full_key}_", current_depth=current_depth + 1))
                elif isinstance(raw, list):
                    unique_key = unique_identifier(full_key, used_names)
                    used_names.add(unique_key)
                    if len(raw) <= 10 and all(not isinstance(item, (dict, list)) for item in raw):
                        result[unique_key] = ",".join(str(item) for item in raw)
                    else:
                        result[unique_key] = json.dumps(raw, ensure_ascii=True)
                else:
                    unique_key = unique_identifier(full_key, used_names)
                    used_names.add(unique_key)
                    result[unique_key] = raw
            return result

        return _flatten(value, prefix, depth)

    @staticmethod
    def _sanitize(value: str) -> str:
        return sanitize_identifier(value)

    @staticmethod
    def _cast_value(value: str) -> Any:
        return coerce_scalar(value)
