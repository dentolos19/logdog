from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 30 * 24 * 3600
MAX_CACHE_SIZE = 1000


@dataclass
class CachedSchema:
    schema_key: str
    format_name: str
    domain: str
    columns: list[dict[str, Any]]
    extraction_strategy: str
    sample_hash: str
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    null_rates: dict[str, float] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.created_at) > DEFAULT_TTL_SECONDS

    @property
    def health_score(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.5
        return self.success_count / total


class SchemaCache:
    def __init__(self, max_size: int = MAX_CACHE_SIZE):
        self._cache: dict[str, CachedSchema] = {}
        self._max_size = max_size

    def get(self, sample_lines: list[str], format_name: str, domain: str) -> CachedSchema | None:
        sample_hash = self._compute_sample_hash(sample_lines)
        schema_key = self._schema_key(format_name, domain, sample_hash)

        cached = self._cache.get(schema_key)
        if cached is None:
            return None

        if cached.is_expired:
            self._cache.pop(schema_key, None)
            return None

        cached.last_accessed = time.time()
        cached.access_count += 1
        return cached

    def put(
        self,
        sample_lines: list[str],
        format_name: str,
        domain: str,
        columns: list[dict[str, Any]],
        extraction_strategy: str,
    ) -> CachedSchema:
        sample_hash = self._compute_sample_hash(sample_lines)
        schema_key = self._schema_key(format_name, domain, sample_hash)

        if len(self._cache) >= self._max_size:
            self._evict()

        cached = CachedSchema(
            schema_key=schema_key,
            format_name=format_name,
            domain=domain,
            columns=columns,
            extraction_strategy=extraction_strategy,
            sample_hash=sample_hash,
        )
        self._cache[schema_key] = cached
        return cached

    def record_success(self, schema_key: str) -> None:
        cached = self._cache.get(schema_key)
        if cached:
            cached.success_count += 1

    def record_failure(self, schema_key: str, null_rates: dict[str, float]) -> None:
        cached = self._cache.get(schema_key)
        if cached:
            cached.failure_count += 1
            cached.null_rates.update(null_rates)

    def get_by_format(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 5,
    ) -> list[CachedSchema]:
        candidates: list[CachedSchema] = []
        for cached in self._cache.values():
            if cached.format_name != format_name:
                continue
            if domain and cached.domain != domain:
                continue
            if not cached.is_expired:
                candidates.append(cached)

        candidates.sort(key=lambda c: (-c.health_score, -c.access_count))
        return candidates[:max_count]

    def stats(self) -> dict[str, Any]:
        total = len(self._cache)
        expired = sum(1 for c in self._cache.values() if c.is_expired)
        healthy = sum(1 for c in self._cache.values() if c.health_score > 0.7)
        return {
            "total_schemas": total,
            "expired": expired,
            "healthy": healthy,
            "max_size": self._max_size,
        }

    def clear(self) -> None:
        self._cache.clear()

    def _evict(self) -> None:
        expired_keys = [key for key, cached in self._cache.items() if cached.is_expired]
        for key in expired_keys:
            self._cache.pop(key, None)

        if len(self._cache) >= self._max_size:
            sorted_keys = sorted(
                self._cache.keys(),
                key=lambda k: (
                    self._cache[k].health_score,
                    self._cache[k].access_count,
                    self._cache[k].last_accessed,
                ),
            )
            for key in sorted_keys[: len(self._cache) - self._max_size + 10]:
                self._cache.pop(key, None)

    @staticmethod
    def _compute_sample_hash(sample_lines: list[str]) -> str:
        content = "\n".join(line[:200] for line in sample_lines[:20])
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:16]

    @staticmethod
    def _schema_key(format_name: str, domain: str, sample_hash: str) -> str:
        return f"{format_name}:{domain}:{sample_hash}"
