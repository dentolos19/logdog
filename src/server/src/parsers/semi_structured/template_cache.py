import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class CachedTemplate:
    template_id: str
    fingerprint: str
    field_mapping: dict[str, Any]
    format_type: str
    created_at: float = field(default_factory=time.time)
    hit_count: int = 0
    last_hit: float = 0.0
    source: str = "ai_fallback"

    def touch(self):
        self.hit_count += 1
        self.last_hit = time.time()


class TemplateCache:
    def __init__(self, max_entries: int = 10_000):
        self._cache: dict[str, CachedTemplate] = {}
        self._max = max_entries

    def get(self, text: str) -> CachedTemplate | None:
        fingerprint = self.fingerprint(text)
        template = self._cache.get(fingerprint)
        if template is not None:
            template.touch()
        return template

    def put(
        self,
        text: str,
        field_mapping: dict[str, Any],
        format_type: str = "unknown",
        source: str = "ai_fallback",
    ) -> CachedTemplate:
        fingerprint = self.fingerprint(text)
        template = CachedTemplate(
            template_id=fingerprint[:12],
            fingerprint=fingerprint,
            field_mapping=field_mapping,
            format_type=format_type,
            source=source,
        )
        self._cache[fingerprint] = template
        self._evict_if_needed()
        return template

    def has(self, text: str) -> bool:
        return self.fingerprint(text) in self._cache

    @property
    def size(self) -> int:
        return len(self._cache)

    def stats(self) -> dict[str, Any]:
        total_hits = sum(template.hit_count for template in self._cache.values())
        return {
            "entries": len(self._cache),
            "total_hits": total_hits,
            "sources": {
                "ai_fallback": sum(1 for template in self._cache.values() if template.source == "ai_fallback"),
                "manual": sum(1 for template in self._cache.values() if template.source == "manual"),
            },
        }

    @staticmethod
    def fingerprint(text: str) -> str:
        lines = text.strip().splitlines()
        skeleton_parts: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            skeleton_parts.append(_skeletonize(stripped))
        skeleton = "\n".join(skeleton_parts)
        return hashlib.sha256(skeleton.encode()).hexdigest()

    def _evict_if_needed(self):
        if len(self._cache) <= self._max:
            return
        entries = sorted(self._cache.items(), key=lambda item: item[1].last_hit)
        to_remove = max(1, len(entries) // 10)
        for fingerprint, _ in entries[:to_remove]:
            del self._cache[fingerprint]


_NUM_RE = re.compile(r"\d+(?:\.\d+)?")
_QUOTED_RE = re.compile(r'"[^"]*"')
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


def _skeletonize(line: str) -> str:
    if line.startswith("---") and line.endswith("---"):
        return "--- SECTION ---"
    if re.match(r"^ROW\s+\d+", line):
        return "ROW N"

    result = _ISO_DATE_RE.sub("DATE", line)
    result = _QUOTED_RE.sub('"V"', result)
    result = _NUM_RE.sub("N", result)
    return result
