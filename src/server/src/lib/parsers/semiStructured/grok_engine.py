"""
Step 1: Grok/Regex Engine
=========================
Applies standard regex patterns to semi-structured log lines.
Supports composable named-group patterns (Grok-style) for common
semiconductor manufacturing log formats plus standard syslog/Apache/nginx.

Returns a match result with extracted fields and a confidence score.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Base Grok-style patterns (reusable building blocks)
# ---------------------------------------------------------------------------
BASE_PATTERNS: dict[str, str] = {
    # Primitives
    "INT": r"(?:[+-]?\d+)",
    "POS_INT": r"(?:\d+)",
    "FLOAT": r"(?:[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)",
    "WORD": r"(?:\w+)",
    "NOTSPACE": r"(?:\S+)",
    "DATA": r"(?:.*?)",
    "GREEDYDATA": r"(?:.*)",
    # Networking
    "IP": r"(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
    "HOSTNAME": r"(?:[\w\.\-]+)",
    "MAC": r"(?:[\da-fA-F]{2}(?::[\da-fA-F]{2}){5})",
    # Timestamps
    "ISO8601": r"(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)",
    "TIMESTAMP_ISO": r"(?:\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)",
    "SYSLOG_TS": r"(?:\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
    "HTTPDATE": r"(?:\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4})",
    # Log levels
    "LOGLEVEL": r"(?:DEBUG|INFO|NOTICE|WARN(?:ING)?|ERROR|CRIT(?:ICAL)?|FATAL|SEVERE|ALERT|EMERG(?:ENCY)?)",
    # Identifiers (semiconductor domain)
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
    """Replace %{PATTERN_NAME:field_name} with the underlying regex."""

    def _replace(m: re.Match) -> str:
        name = m.group(1)
        capture = m.group(2)
        raw = base.get(name, name)
        if capture:
            return f"(?P<{capture}>{raw})"
        return raw

    return re.sub(r"%\{(\w+)(?::(\w+))?\}", _replace, pattern_str)


# ---------------------------------------------------------------------------
# Compound patterns for common log formats
# ---------------------------------------------------------------------------
COMPOUND_PATTERNS: dict[str, str] = {
    # Standard formats
    "SYSLOG": r"%{SYSLOG_TS:timestamp}\s+%{HOSTNAME:host}\s+%{WORD:program}(?:\[%{POS_INT:pid}\])?:\s+%{GREEDYDATA:message}",
    "APACHE_ACCESS": r"%{IP:client_ip}\s+-\s+%{NOTSPACE:user}\s+\[%{HTTPDATE:timestamp}\]\s+\"%{WORD:method}\s+%{NOTSPACE:request}\s+HTTP/%{FLOAT:http_version}\"\s+%{INT:status}\s+%{INT:bytes}",
    "NGINX_ACCESS": r"%{IP:client_ip}\s+-\s+%{NOTSPACE:user}\s+\[%{HTTPDATE:timestamp}\]\s+\"%{WORD:method}\s+%{NOTSPACE:request}\s+HTTP/%{FLOAT:http_version}\"\s+%{INT:status}\s+%{INT:bytes}\s+\"%{DATA:referrer}\"\s+\"%{DATA:user_agent}\"",
    # Semiconductor / equipment log formats
    "SECTION_HEADER": r"^---\s+(?P<section_name>\w+(?:\s+\w+)*)\s+---\s*$",
    "KV_JSON_LINE": r'^\s*"(?P<key>[^"]+)"\s*:\s*"?(?P<value>[^",}]*)"?\s*,?\s*$',
    "KV_EQUALS": r"^(?P<key>[\w\.]+)\s*=\s*(?P<value>.+?)\s*$",
    "RECIPE_STEP_HEADER": r"^(?:ROW\s+%{POS_INT:row_num}|RecipeStepID)\s*[:\-]?\s*%{STEP_ID:step_id}",
    "PARQUET_HEADER": r"^(?:LAM|AMAT|TEL|ASM)\s+\w+\s+PARQUET\s*-\s*DATA\s+OVERVIEW",
    # Vendor 3 (LAM) specific
    "LAM_RECIPE_ROW": r"^ROW\s+(?P<row_num>\d+)(?:\s*[:\-]\s*(?P<step_id>[^\(]+?))?(?:\s*\((?P<description>[^\)]+)\))?\s*$",
    "LAM_MODULE_KEYS": r"^\s*ModuleID:\s*%{MODULE_ID:module_id}",
    "LAM_KV_FLAT": r"^(?P<key>[A-Za-z][\w]+)\s*=\s*(?P<value>.+?)(?:\s+(?P<unit>[a-zA-Z/%]+))?\s*$",
    # Tabular recipe detail (Key | Value | Unit | Type)
    "RECIPE_DETAIL_ROW": r"^\s*(?P<index>\d+)\s+(?P<key>\w+)\s+(?P<value>\S+)\s+(?P<unit>\S*)\s+(?P<type>\w+)\s*$",
}

# Pre-compile all patterns
COMPILED_PATTERNS: dict[str, re.Pattern] = {}
for name, pat in COMPOUND_PATTERNS.items():
    resolved = _resolve_pattern(pat)
    try:
        COMPILED_PATTERNS[name] = re.compile(resolved, re.IGNORECASE)
    except re.error as e:
        print(f"Warning: failed to compile pattern '{name}': {e}")


# ---------------------------------------------------------------------------
# Match result
# ---------------------------------------------------------------------------
@dataclass
class GrokMatch:
    pattern_name: str
    fields: dict[str, str]
    confidence: float  # 0.0–1.0
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
        return sum(m.confidence for m in self.matches) / (len(self.matches) + len(self.unmatched_lines))


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class GrokEngine:
    """Apply Grok/Regex patterns to raw log text."""

    def __init__(self, custom_patterns: Optional[dict[str, str]] = None):
        self.patterns = dict(COMPILED_PATTERNS)
        if custom_patterns:
            for name, pat in custom_patterns.items():
                resolved = _resolve_pattern(pat)
                self.patterns[name] = re.compile(resolved, re.IGNORECASE)

    # ---- public API -------------------------------------------------------

    def match_line(self, line: str) -> Optional[GrokMatch]:
        """Try every pattern against a single line. Return best match."""
        best: Optional[GrokMatch] = None
        for name, pat in self.patterns.items():
            m = pat.search(line)
            if m and m.groupdict():
                fields = {k: v for k, v in m.groupdict().items() if v is not None}
                conf = self._score(fields, m, line)
                if best is None or conf > best.confidence:
                    best = GrokMatch(
                        pattern_name=name,
                        fields=fields,
                        confidence=conf,
                        matched_text=m.group(0),
                        remaining_text=line[m.end() :].strip(),
                    )
        return best

    def match_block(self, text: str) -> GrokResult:
        """Match a multi-line block of log text."""
        lines = text.strip().splitlines()
        result = GrokResult(matched=False)
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            m = self.match_line(stripped)
            if m and m.confidence >= 0.3:
                result.matches.append(m)
            else:
                result.unmatched_lines.append(stripped)
        result.matched = len(result.matches) > 0
        return result

    def detect_format(self, text: str) -> Optional[str]:
        """Detect the overall log format from a text sample."""
        # Quick structural checks
        if self._is_lam_parquet(text):
            return "LAM_PARQUET"
        if self._is_section_delimited(text):
            return "SECTION_DELIMITED"

        # Try line-by-line pattern matching
        lines = text.strip().splitlines()[:20]  # sample first 20 lines
        pattern_counts: dict[str, int] = {}
        for line in lines:
            m = self.match_line(line.strip())
            if m:
                pattern_counts[m.pattern_name] = pattern_counts.get(m.pattern_name, 0) + 1

        if pattern_counts:
            dominant = max(pattern_counts, key=pattern_counts.get)
            if dominant.startswith("LAM_"):
                return "LAM_PARQUET"
            if dominant in ("SYSLOG", "APACHE_ACCESS", "NGINX_ACCESS"):
                return dominant
            if dominant in ("KV_EQUALS", "KV_JSON_LINE"):
                return "KEY_VALUE"
            return dominant
        return None

    # ---- private helpers --------------------------------------------------

    @staticmethod
    def _score(fields: dict, match: re.Match, line: str) -> float:
        """Heuristic confidence score based on coverage and field count."""
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
        count = sum(1 for ind in indicators if ind in text)
        return count >= 3

    @staticmethod
    def _is_section_delimited(text: str) -> bool:
        return text.count("--- ") >= 2 and ("Keys ---" in text or "Attributes ---" in text)
