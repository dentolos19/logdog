"""
Step 3: Delimiter Splitting
===========================
Handles key-value pair splitting for various delimiter styles found
in semiconductor manufacturing logs:
  - key=value
  - key: value
  - key\tvalue (tab-separated)
  - key|value  (pipe-separated)
  - "key": "value" (JSON-style without full JSON structure)

Also handles multi-value lines and nested delimiters.
"""

import re
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class KVPair:
    key: str
    value: Any
    delimiter: str  # '=', ':', '\t', '|', 'json'
    raw_line: str = ""


class DelimiterSplitter:
    """Split raw text into key-value pairs using detected delimiters."""

    # Priority order: more specific → less specific
    _STRATEGIES = [
        ("json", re.compile(r'^\s*"([^"]+)"\s*:\s*"?([^",}\]]*)"?\s*,?\s*$')),
        ("=", re.compile(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$")),
        (":", re.compile(r"^([A-Za-z_][\w\s]{0,30}?)\s*:\s+(.+)$")),
        ("|", re.compile(r"^([^|]+)\|(.+)$")),
        ("\t", re.compile(r"^([^\t]+)\t+(.+)$")),
    ]

    def split_line(self, line: str) -> Optional[KVPair]:
        """Try to split a single line into a key-value pair."""
        stripped = line.strip()
        if not stripped:
            return None

        for delim, pattern in self._STRATEGIES:
            m = pattern.match(stripped)
            if m:
                key = m.group(1).strip().strip('"')
                value = m.group(2).strip().strip('"')
                return KVPair(
                    key=key,
                    value=self._cast(value),
                    delimiter=delim,
                    raw_line=line,
                )
        return None

    def split_block(self, text: str) -> list[KVPair]:
        """Split a multi-line block into key-value pairs."""
        pairs: list[KVPair] = []
        for line in text.splitlines():
            pair = self.split_line(line)
            if pair:
                pairs.append(pair)
        return pairs

    def detect_delimiter(self, text: str) -> Optional[str]:
        """Detect the dominant delimiter in a block of text."""
        counts: dict[str, int] = {}
        for line in text.splitlines()[:30]:
            pair = self.split_line(line)
            if pair:
                counts[pair.delimiter] = counts.get(pair.delimiter, 0) + 1
        if counts:
            return max(counts, key=counts.get)
        return None

    def split_inline_kv(self, text: str, delimiter: str = ",") -> list[KVPair]:
        """Split inline key=value pairs (e.g., 'key1=val1, key2=val2')."""
        pairs: list[KVPair] = []
        segments = text.split(delimiter)
        for seg in segments:
            seg = seg.strip()
            if "=" in seg:
                k, _, v = seg.partition("=")
                pairs.append(
                    KVPair(
                        key=k.strip(),
                        value=self._cast(v.strip()),
                        delimiter="=",
                        raw_line=seg,
                    )
                )
        return pairs

    @staticmethod
    def _cast(value: str) -> Any:
        """Cast string values to appropriate types."""
        if value.lower() in ("null", "none", ""):
            return None
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
