import re
from dataclasses import dataclass
from typing import Any


@dataclass
class KVPair:
    key: str
    value: Any
    delimiter: str
    raw_line: str = ""


class DelimiterSplitter:
    _STRATEGIES = [
        ("json", re.compile(r'^\s*"([^"]+)"\s*:\s*"?([^",}\]]*)"?\s*,?\s*$')),
        ("=", re.compile(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$")),
        (":", re.compile(r"^([A-Za-z_][\w\s]{0,30}?)\s*:\s+(.+)$")),
        ("|", re.compile(r"^([^|]+)\|(.+)$")),
        ("\t", re.compile(r"^([^\t]+)\t+(.+)$")),
    ]

    def split_line(self, line: str) -> KVPair | None:
        stripped = line.strip()
        if not stripped:
            return None

        for delimiter, pattern in self._STRATEGIES:
            match = pattern.match(stripped)
            if match:
                key = match.group(1).strip().strip('"')
                value = match.group(2).strip().strip('"')
                return KVPair(key=key, value=self._cast(value), delimiter=delimiter, raw_line=line)
        return None

    def split_block(self, text: str) -> list[KVPair]:
        pairs: list[KVPair] = []
        for line in text.splitlines():
            pair = self.split_line(line)
            if pair is not None:
                pairs.append(pair)
        return pairs

    def detect_delimiter(self, text: str) -> str | None:
        counts: dict[str, int] = {}
        for line in text.splitlines()[:30]:
            pair = self.split_line(line)
            if pair is not None:
                counts[pair.delimiter] = counts.get(pair.delimiter, 0) + 1
        if not counts:
            return None
        return max(counts, key=counts.get)

    def split_inline_kv(self, text: str, delimiter: str = ",") -> list[KVPair]:
        pairs: list[KVPair] = []
        for segment in text.split(delimiter):
            stripped = segment.strip()
            if "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            pairs.append(KVPair(key=key.strip(), value=self._cast(value.strip()), delimiter="=", raw_line=stripped))
        return pairs

    @staticmethod
    def _cast(value: str) -> Any:
        lowered = value.lower()
        if lowered in {"null", "none", ""}:
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
