from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from lib.database import SessionLocal
from lib.models import FewShotEntry

logger = logging.getLogger(__name__)


@dataclass
class FewShotExample:
    format_name: str
    domain: str
    profile_name: str | None
    fingerprint: str | None
    sample_lines: list[str]
    schema: dict[str, Any]
    confidence: float = 1.0
    usage_count: int = 0
    last_used: float = 0.0
    created_at: float = field(default_factory=time.time)

    @property
    def signature(self) -> str:
        profile_name = self.profile_name or "default"
        fingerprint = self.fingerprint or "no_fp"
        content = f"{self.format_name}:{self.domain}:{profile_name}:{fingerprint}:" + json.dumps(
            sorted(self.schema.keys()), ensure_ascii=True
        )
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:12]


class FewShotStore:
    def __init__(self, max_examples_per_format: int = 10, use_persistence: bool = True):
        self._examples: dict[str, list[FewShotExample]] = {}
        self._max_examples_per_format = max_examples_per_format
        self._use_persistence = use_persistence

    def add_example(
        self,
        format_name: str,
        domain: str,
        sample_lines: list[str],
        schema: dict[str, Any],
        confidence: float = 1.0,
        profile_name: str | None = None,
        fingerprint: str | None = None,
    ) -> FewShotExample:
        example = FewShotExample(
            format_name=format_name,
            domain=domain,
            profile_name=profile_name,
            fingerprint=fingerprint,
            sample_lines=sample_lines[:10],
            schema=schema,
            confidence=confidence,
        )

        format_key = self._format_key(format_name, domain, profile_name)
        examples = self._examples.setdefault(format_key, [])

        existing = next((e for e in examples if e.signature == example.signature), None)
        if existing:
            existing.usage_count += 1
            existing.last_used = time.time()
            if self._use_persistence:
                self._upsert_db(existing)
            return existing

        if len(examples) >= self._max_examples_per_format:
            examples.sort(key=lambda e: (e.usage_count, e.last_used))
            examples.pop(0)

        examples.append(example)
        if self._use_persistence:
            self._upsert_db(example)
        logger.debug("Added few-shot example for format '%s' (domain: '%s')", format_name, domain)
        return example

    def get_examples(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
        profile_name: str | None = None,
        fingerprint: str | None = None,
    ) -> list[FewShotExample]:
        examples: list[FewShotExample] = []

        format_key = ""
        if domain:
            format_key = self._format_key(format_name, domain, profile_name)
            examples.extend(self._examples.get(format_key, []))

        general_key = self._format_key(format_name, "unknown", profile_name)
        should_include_general = True
        if domain:
            should_include_general = general_key != format_key
        if should_include_general:
            examples.extend(self._examples.get(general_key, []))

        if not examples:
            for key, stored_examples in self._examples.items():
                if key.startswith(f"{format_name}:") and (profile_name is None or key.endswith(f":{profile_name}")):
                    examples.extend(stored_examples)

        if self._use_persistence:
            examples.extend(
                self._load_examples_from_db(
                    format_name=format_name,
                    domain=domain,
                    profile_name=profile_name,
                    fingerprint=fingerprint,
                    max_count=max_count,
                )
            )

        unique: dict[str, FewShotExample] = {}
        for example in examples:
            unique[example.signature] = example

        deduped = list(unique.values())
        deduped.sort(key=lambda e: (-e.usage_count, -e.confidence))
        selected = deduped[:max_count]

        for example in selected:
            example.usage_count += 1
            example.last_used = time.time()
            if self._use_persistence:
                self._upsert_db(example)

        return selected

    def get_example_texts(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
        profile_name: str | None = None,
        fingerprint: str | None = None,
    ) -> list[str]:
        examples = self.get_examples(
            format_name,
            domain,
            max_count,
            profile_name=profile_name,
            fingerprint=fingerprint,
        )
        return ["\n".join(line[:500] for line in example.sample_lines[:5]) for example in examples]

    def get_example_schemas(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
        profile_name: str | None = None,
        fingerprint: str | None = None,
    ) -> list[dict[str, Any]]:
        examples = self.get_examples(
            format_name,
            domain,
            max_count,
            profile_name=profile_name,
            fingerprint=fingerprint,
        )
        return [example.schema for example in examples]

    def record_successful_parse(
        self,
        format_name: str,
        domain: str,
        sample_lines: list[str],
        schema: dict[str, Any],
        confidence: float = 1.0,
        profile_name: str | None = None,
        fingerprint: str | None = None,
    ) -> None:
        self.add_example(
            format_name=format_name,
            domain=domain,
            sample_lines=sample_lines,
            schema=schema,
            confidence=confidence,
            profile_name=profile_name,
            fingerprint=fingerprint,
        )

    def stats(self) -> dict[str, Any]:
        total_examples = sum(len(examples) for examples in self._examples.values())
        format_counts = {key: len(examples) for key, examples in self._examples.items()}
        persisted_total = 0
        if self._use_persistence:
            db = SessionLocal()
            try:
                persisted_total = db.query(FewShotEntry).count()
            finally:
                db.close()
        return {
            "total_examples": total_examples,
            "persisted_total_examples": persisted_total,
            "formats": format_counts,
            "max_per_format": self._max_examples_per_format,
        }

    def clear(self) -> None:
        self._examples.clear()
        if self._use_persistence:
            db = SessionLocal()
            try:
                db.query(FewShotEntry).delete()
                db.commit()
            finally:
                db.close()

    @staticmethod
    def _format_key(format_name: str, domain: str, profile_name: str | None = None) -> str:
        profile_part = profile_name or "default"
        return f"{format_name}:{domain}:{profile_part}"

    def _load_examples_from_db(
        self,
        format_name: str,
        domain: str | None,
        profile_name: str | None,
        fingerprint: str | None,
        max_count: int,
    ) -> list[FewShotExample]:
        db = SessionLocal()
        try:
            query = db.query(FewShotEntry).filter(FewShotEntry.format_name == format_name)
            if domain:
                query = query.filter(FewShotEntry.domain == domain)
            if profile_name is not None:
                query = query.filter(FewShotEntry.profile_name == profile_name)

            if fingerprint is not None:
                exact = query.filter(FewShotEntry.fingerprint == fingerprint)
                rows = exact.order_by(FewShotEntry.usage_count.desc(), FewShotEntry.confidence.desc()).all()
                if not rows:
                    rows = query.order_by(FewShotEntry.usage_count.desc(), FewShotEntry.confidence.desc()).all()
            else:
                rows = query.order_by(FewShotEntry.usage_count.desc(), FewShotEntry.confidence.desc()).all()

            return [self._to_example(row) for row in rows[: max_count * 2]]
        finally:
            db.close()

    def _upsert_db(self, example: FewShotExample) -> None:
        db = SessionLocal()
        try:
            existing = db.query(FewShotEntry).filter(FewShotEntry.signature == example.signature).first()
            if existing is None:
                existing = FewShotEntry(signature=example.signature)
                db.add(existing)

            existing.format_name = example.format_name
            existing.domain = example.domain
            existing.profile_name = example.profile_name
            existing.fingerprint = example.fingerprint
            existing.confidence = max(0.0, min(example.confidence, 1.0))
            existing.usage_count = max(0, int(example.usage_count))
            existing.sample_lines = json.dumps(example.sample_lines, ensure_ascii=True)
            existing.schema = json.dumps(example.schema, ensure_ascii=True)
            if example.last_used > 0:
                existing.last_used = datetime.fromtimestamp(example.last_used, tz=timezone.utc)
            db.commit()
        finally:
            db.close()

    @staticmethod
    def _to_example(row: FewShotEntry) -> FewShotExample:
        try:
            sample_lines = json.loads(row.sample_lines)
        except json.JSONDecodeError:
            sample_lines = []

        try:
            schema = json.loads(row.schema)
        except json.JSONDecodeError:
            schema = {}

        last_used = row.last_used.timestamp() if row.last_used else 0.0
        created_at = row.created_at.timestamp() if row.created_at else time.time()

        return FewShotExample(
            format_name=row.format_name,
            domain=row.domain,
            profile_name=row.profile_name,
            fingerprint=row.fingerprint,
            sample_lines=sample_lines if isinstance(sample_lines, list) else [],
            schema=schema if isinstance(schema, dict) else {},
            confidence=float(row.confidence or 0.0),
            usage_count=int(row.usage_count or 0),
            last_used=last_used,
            created_at=created_at,
        )
