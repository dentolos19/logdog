from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FewShotExample:
    format_name: str
    domain: str
    sample_lines: list[str]
    schema: dict[str, Any]
    confidence: float = 1.0
    usage_count: int = 0
    last_used: float = 0.0
    created_at: float = field(default_factory=time.time)

    @property
    def signature(self) -> str:
        content = self.format_name + json.dumps(sorted(self.schema.keys()), ensure_ascii=True)
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()[:12]


class FewShotStore:
    def __init__(self, max_examples_per_format: int = 10):
        self._examples: dict[str, list[FewShotExample]] = {}
        self._max_examples_per_format = max_examples_per_format

    def add_example(
        self,
        format_name: str,
        domain: str,
        sample_lines: list[str],
        schema: dict[str, Any],
        confidence: float = 1.0,
    ) -> FewShotExample:
        example = FewShotExample(
            format_name=format_name,
            domain=domain,
            sample_lines=sample_lines[:10],
            schema=schema,
            confidence=confidence,
        )

        format_key = self._format_key(format_name, domain)
        examples = self._examples.setdefault(format_key, [])

        existing = next((e for e in examples if e.signature == example.signature), None)
        if existing:
            existing.usage_count += 1
            existing.last_used = time.time()
            return existing

        if len(examples) >= self._max_examples_per_format:
            examples.sort(key=lambda e: (e.usage_count, e.last_used))
            examples.pop(0)

        examples.append(example)
        logger.debug("Added few-shot example for format '%s' (domain: '%s')", format_name, domain)
        return example

    def get_examples(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
    ) -> list[FewShotExample]:
        examples: list[FewShotExample] = []

        if domain:
            format_key = self._format_key(format_name, domain)
            examples.extend(self._examples.get(format_key, []))

        general_key = self._format_key(format_name, "unknown")
        if general_key != format_key if domain else "":
            examples.extend(self._examples.get(general_key, []))

        if not examples:
            for key, stored_examples in self._examples.items():
                if key.startswith(f"{format_name}:"):
                    examples.extend(stored_examples)

        examples.sort(key=lambda e: (-e.usage_count, -e.confidence))
        selected = examples[:max_count]

        for example in selected:
            example.usage_count += 1
            example.last_used = time.time()

        return selected

    def get_example_texts(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
    ) -> list[str]:
        examples = self.get_examples(format_name, domain, max_count)
        return ["\n".join(line[:500] for line in example.sample_lines[:5]) for example in examples]

    def get_example_schemas(
        self,
        format_name: str,
        domain: str | None = None,
        max_count: int = 3,
    ) -> list[dict[str, Any]]:
        examples = self.get_examples(format_name, domain, max_count)
        return [example.schema for example in examples]

    def record_successful_parse(
        self,
        format_name: str,
        domain: str,
        sample_lines: list[str],
        schema: dict[str, Any],
        confidence: float = 1.0,
    ) -> None:
        self.add_example(
            format_name=format_name,
            domain=domain,
            sample_lines=sample_lines,
            schema=schema,
            confidence=confidence,
        )

    def stats(self) -> dict[str, Any]:
        total_examples = sum(len(examples) for examples in self._examples.values())
        format_counts = {key: len(examples) for key, examples in self._examples.items()}
        return {
            "total_examples": total_examples,
            "formats": format_counts,
            "max_per_format": self._max_examples_per_format,
        }

    def clear(self) -> None:
        self._examples.clear()

    @staticmethod
    def _format_key(format_name: str, domain: str) -> str:
        return f"{format_name}:{domain}"
