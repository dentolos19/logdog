import re
from dataclasses import dataclass, field

BASE_PATTERNS: dict[str, str] = {
    "INT": r"(?:[+-]?\d+)",
    "POS_INT": r"(?:\d+)",
    "FLOAT": r"(?:[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)",
    "WORD": r"(?:\w+)",
    "NOTSPACE": r"(?:\S+)",
    "DATA": r"(?:.*?)",
    "GREEDYDATA": r"(?:.*)",
    "IP": r"(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
    "HOSTNAME": r"(?:[\w\.\-]+)",
    "MAC": r"(?:[\da-fA-F]{2}(?::[\da-fA-F]{2}){5})",
    "ISO8601": r"(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)",
    "TIMESTAMP_ISO": r"(?:\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)",
    "SYSLOG_TS": r"(?:\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
    "HTTPDATE": r"(?:\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4})",
    "LOGLEVEL": r"(?:DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|CRIT(?:ICAL)?|FATAL|SEVERE|ALERT|EMERG(?:ENCY)?)",
    "EQUIP_ID": r"(?:EQP_\d{4})",
    "LOT_ID": r"(?:LOT_\d{4})",
    "WAFER_ID": r"(?:WFR_\d{4})",
    "MODULE_ID": r"(?:MOD_\d{4})",
    "RECIPE_ID": r"(?:RCP_\d{4})",
    "JOB_ID": r"(?:(?:CJOB|PRJOB)_\d{4})",
    "SENSOR_ID": r"(?:SENSOR_\d{4})",
    "STEP_ID": r"(?:(?:PRESTEP|POSTSTEP|\d+(?:\.\d+)*))",
}


def _resolve_pattern(pattern_str: str, base: dict[str, str] = BASE_PATTERNS) -> str:
    def _replace(match: re.Match) -> str:
        pattern_name = match.group(1)
        capture_name = match.group(2)
        raw = base.get(pattern_name, pattern_name)
        if capture_name:
            return f"(?P<{capture_name}>{raw})"
        return raw

    return re.sub(r"%\{(\w+)(?::(\w+))?\}", _replace, pattern_str)


COMPOUND_PATTERNS: dict[str, str] = {
    "SYSLOG": r"%{SYSLOG_TS:timestamp}\s+%{HOSTNAME:host}\s+%{WORD:program}(?:\[%{POS_INT:pid}\])?:\s+%{GREEDYDATA:message}",
    "APACHE_ACCESS": r"%{IP:client_ip}\s+-\s+%{NOTSPACE:user}\s+\[%{HTTPDATE:timestamp}\]\s+\"%{WORD:method}\s+%{NOTSPACE:request}\s+HTTP/%{FLOAT:http_version}\"\s+%{INT:status}\s+%{INT:bytes}",
    "NGINX_ACCESS": r"%{IP:client_ip}\s+-\s+%{NOTSPACE:user}\s+\[%{HTTPDATE:timestamp}\]\s+\"%{WORD:method}\s+%{NOTSPACE:request}\s+HTTP/%{FLOAT:http_version}\"\s+%{INT:status}\s+%{INT:bytes}\s+\"%{DATA:referrer}\"\s+\"%{DATA:user_agent}\"",
    "SECTION_HEADER": r"^---\s+(?P<section_name>\w+(?:\s+\w+)*)\s+---\s*$",
    "KV_JSON_LINE": r'^\s*"(?P<key>[^"]+)"\s*:\s*"?(?P<value>[^",}]*)"?\s*,?\s*$',
    "KV_EQUALS": r"^(?P<key>[\w\.]+)\s*=\s*(?P<value>.+?)\s*$",
    "RECIPE_STEP_HEADER": r"^(?:ROW\s+%{POS_INT:row_num}|RecipeStepID)\s*[:\-]?\s*%{STEP_ID:step_id}",
    "PARQUET_HEADER": r"^(?:LAM|AMAT|TEL|ASM)\s+\w+\s+PARQUET\s*-\s*DATA\s+OVERVIEW",
    "LAM_RECIPE_ROW": r"^ROW\s+(?P<row_num>\d+)(?:\s*[:\-]\s*(?P<step_id>[^\(]+?))?(?:\s*\((?P<description>[^\)]+)\))?\s*$",
    "LAM_MODULE_KEYS": r"^\s*ModuleID:\s*%{MODULE_ID:module_id}",
    "LAM_KV_FLAT": r"^(?P<key>[A-Za-z][\w]+)\s*=\s*(?P<value>.+?)(?:\s+(?P<unit>[a-zA-Z/%]+))?\s*$",
    "RECIPE_DETAIL_ROW": r"^\s*(?P<index>\d+)\s+(?P<key>\w+)\s+(?P<value>\S+)\s+(?P<unit>\S*)\s+(?P<type>\w+)\s*$",
}


COMPILED_PATTERNS: dict[str, re.Pattern] = {}
for name, pattern in COMPOUND_PATTERNS.items():
    resolved = _resolve_pattern(pattern)
    COMPILED_PATTERNS[name] = re.compile(resolved, re.IGNORECASE)


@dataclass
class GrokMatch:
    pattern_name: str
    fields: dict[str, str]
    confidence: float
    matched_text: str = ""
    remaining_text: str = ""


@dataclass
class GrokResult:
    matched: bool
    matches: list[GrokMatch] = field(default_factory=list)
    unmatched_lines: list[str] = field(default_factory=list)

    @property
    def overall_confidence(self) -> float:
        if not self.matches:
            return 0.0
        return sum(match.confidence for match in self.matches) / (len(self.matches) + len(self.unmatched_lines))


class GrokEngine:
    def __init__(self, custom_patterns: dict[str, str] | None = None):
        self.patterns = dict(COMPILED_PATTERNS)
        if custom_patterns:
            for name, pattern in custom_patterns.items():
                resolved = _resolve_pattern(pattern)
                self.patterns[name] = re.compile(resolved, re.IGNORECASE)

    def match_line(self, line: str) -> GrokMatch | None:
        best_match: GrokMatch | None = None
        for pattern_name, pattern in self.patterns.items():
            match = pattern.search(line)
            if match and match.groupdict():
                fields = {key: value for key, value in match.groupdict().items() if value is not None}
                confidence = self._score(fields, match, line)
                if best_match is None or confidence > best_match.confidence:
                    best_match = GrokMatch(
                        pattern_name=pattern_name,
                        fields=fields,
                        confidence=confidence,
                        matched_text=match.group(0),
                        remaining_text=line[match.end() :].strip(),
                    )
        return best_match

    def match_block(self, text: str) -> GrokResult:
        lines = text.strip().splitlines()
        result = GrokResult(matched=False)
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            match = self.match_line(stripped)
            if match and match.confidence >= 0.3:
                result.matches.append(match)
            else:
                result.unmatched_lines.append(stripped)
        result.matched = len(result.matches) > 0
        return result

    def detect_format(self, text: str) -> str | None:
        if self._is_lam_parquet(text):
            return "LAM_PARQUET"
        if self._is_section_delimited(text):
            return "SECTION_DELIMITED"

        lines = text.strip().splitlines()[:20]
        pattern_counts: dict[str, int] = {}
        for line in lines:
            matched = self.match_line(line.strip())
            if matched:
                pattern_counts[matched.pattern_name] = pattern_counts.get(matched.pattern_name, 0) + 1

        if not pattern_counts:
            return None

        dominant = max(pattern_counts, key=pattern_counts.get)
        if dominant.startswith("LAM_"):
            return "LAM_PARQUET"
        if dominant in {"SYSLOG", "APACHE_ACCESS", "NGINX_ACCESS"}:
            return dominant
        if dominant in {"KV_EQUALS", "KV_JSON_LINE"}:
            return "KEY_VALUE"
        return dominant

    @staticmethod
    def _score(fields: dict, match: re.Match, line: str) -> float:
        coverage = len(match.group(0)) / max(len(line), 1)
        field_bonus = min(len(fields) / 5.0, 1.0) * 0.3
        return min(coverage * 0.7 + field_bonus, 1.0)

    @staticmethod
    def _is_lam_parquet(text: str) -> bool:
        indicators = [
            "LAM RECIPE DETAIL PARQUET",
            "ControlJobKeys",
            "ModuleProcessReportKeys",
            "RecipeConstants",
            "RecipeDetail",
        ]
        count = sum(1 for indicator in indicators if indicator in text)
        return count >= 3

    @staticmethod
    def _is_section_delimited(text: str) -> bool:
        return text.count("--- ") >= 2 and ("Keys ---" in text or "Attributes ---" in text)
