"""
Template Cache
==============
Caches field mappings discovered by the AI fallback so that
subsequent logs of the same structural shape bypass the LLM entirely.

Uses a structural fingerprint (hash of key patterns, delimiters,
and line structure) to identify matching templates.
"""

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CachedTemplate:
    template_id: str
    fingerprint: str
    field_mapping: dict[str, Any]  # raw_key → {canonical_key, type, unit}
    format_type: str  # e.g. "LAM_PARQUET", "KEY_VALUE"
    created_at: float = field(default_factory=time.time)
    hit_count: int = 0
    last_hit: float = 0.0
    source: str = "ai_fallback"  # 'ai_fallback' or 'manual'

    def touch(self):
        self.hit_count += 1
        self.last_hit = time.time()


class TemplateCache:
    """In-memory cache for log template field mappings."""

    def __init__(self, max_entries: int = 10_000):
        self._cache: dict[str, CachedTemplate] = {}
        self._max = max_entries

    # ---- Public API -------------------------------------------------------

    def get(self, text: str) -> Optional[CachedTemplate]:
        """Look up a cached template by structural fingerprint."""
        fp = self.fingerprint(text)
        tmpl = self._cache.get(fp)
        if tmpl:
            tmpl.touch()
        return tmpl

    def put(
        self,
        text: str,
        field_mapping: dict[str, Any],
        format_type: str = "unknown",
        source: str = "ai_fallback",
    ) -> CachedTemplate:
        """Store a template mapping for a log's structural shape."""
        fp = self.fingerprint(text)
        tmpl = CachedTemplate(
            template_id=fp[:12],
            fingerprint=fp,
            field_mapping=field_mapping,
            format_type=format_type,
            source=source,
        )
        self._cache[fp] = tmpl
        self._evict_if_needed()
        return tmpl

    def has(self, text: str) -> bool:
        return self.fingerprint(text) in self._cache

    @property
    def size(self) -> int:
        return len(self._cache)

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        total_hits = sum(t.hit_count for t in self._cache.values())
        return {
            "entries": len(self._cache),
            "total_hits": total_hits,
            "sources": {
                "ai_fallback": sum(1 for t in self._cache.values() if t.source == "ai_fallback"),
                "manual": sum(1 for t in self._cache.values() if t.source == "manual"),
            },
        }

    # ---- Fingerprinting ---------------------------------------------------

    @staticmethod
    def fingerprint(text: str) -> str:
        """
        Generate a structural fingerprint that captures the log's shape
        without being sensitive to specific values.

        Strategy:
        - Normalize each line to its structural skeleton
        - Hash the skeleton list
        """
        lines = text.strip().splitlines()
        skeleton_parts: list[str] = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            skeleton_parts.append(_skeletonize(stripped))

        skeleton = "\n".join(skeleton_parts)
        return hashlib.sha256(skeleton.encode()).hexdigest()

    # ---- Private ----------------------------------------------------------

    def _evict_if_needed(self):
        """Evict least-recently-used entries if cache is full."""
        if len(self._cache) <= self._max:
            return
        # Sort by last_hit ascending, remove bottom 10%
        entries = sorted(self._cache.items(), key=lambda x: x[1].last_hit)
        to_remove = max(1, len(entries) // 10)
        for fp, _ in entries[:to_remove]:
            del self._cache[fp]


# ---------------------------------------------------------------------------
# Skeleton helpers
# ---------------------------------------------------------------------------
import re

_NUM_RE = re.compile(r"\d+(?:\.\d+)?")
_QUOTED_RE = re.compile(r'"[^"]*"')
_ISO_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


def _skeletonize(line: str) -> str:
    """
    Reduce a line to its structural pattern.

    Examples:
        '"CtrlJobID": "CJOB_0001"'  →  '"K": "V"'
        'N2_A = 1000.0  sccm'       →  'K = N  U'
        '--- ControlJobKeys ---'     →  '--- S ---'
    """
    # Preserve section headers
    if line.startswith("---") and line.endswith("---"):
        return "--- SECTION ---"

    # Preserve ROW markers
    if re.match(r"^ROW\s+\d+", line):
        return "ROW N"

    # Replace ISO dates
    result = _ISO_DATE_RE.sub("DATE", line)
    # Replace quoted strings
    result = _QUOTED_RE.sub('"V"', result)
    # Replace numbers
    result = _NUM_RE.sub("N", result)

    return result
