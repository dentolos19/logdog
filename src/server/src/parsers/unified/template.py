from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field

NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")
HEX_RE = re.compile(r"\b[0-9A-Fa-f]{8,}\b")
UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.IGNORECASE)
IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
QUOTED_RE = re.compile(r'"[^\"]*"|\'[^\']*\'')


def _skeletonize(text: str) -> str:
    normalized = text.strip()
    normalized = UUID_RE.sub("<UUID>", normalized)
    normalized = IP_RE.sub("<IP>", normalized)
    normalized = HEX_RE.sub("<HEX>", normalized)
    normalized = QUOTED_RE.sub('"<STR>"', normalized)
    normalized = NUMBER_RE.sub("<NUM>", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


@dataclass
class Template:
    template_id: str
    skeleton: str
    examples: list[str] = field(default_factory=list)
    count: int = 0
    created_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)


@dataclass
class TemplateEvolutionResult:
    template_id: str
    skeleton: str
    merged_into: str | None = None
    split_from: str | None = None
    changed: bool = False


class TemplateEvolutionEngine:
    def __init__(self, max_templates: int = 5000):
        self._templates: dict[str, Template] = {}
        self._max_templates = max_templates

    def register(self, text: str) -> TemplateEvolutionResult:
        skeleton = _skeletonize(text)
        template_id = self._make_template_id(skeleton)

        existing = self._templates.get(template_id)
        if existing:
            existing.count += 1
            existing.last_seen = time.time()
            if len(existing.examples) < 5:
                existing.examples.append(text[:1000])
            return TemplateEvolutionResult(template_id=template_id, skeleton=skeleton)

        if len(self._templates) >= self._max_templates:
            self._evict()

        self._templates[template_id] = Template(
            template_id=template_id,
            skeleton=skeleton,
            examples=[text[:1000]],
            count=1,
        )

        similar_id = self._find_similar_template(template_id, skeleton)
        if similar_id:
            return TemplateEvolutionResult(
                template_id=template_id,
                skeleton=skeleton,
                merged_into=similar_id,
                changed=True,
            )

        return TemplateEvolutionResult(template_id=template_id, skeleton=skeleton)

    def evolve(self) -> list[TemplateEvolutionResult]:
        results: list[TemplateEvolutionResult] = []
        template_items = list(self._templates.items())

        for i, (template_id, template) in enumerate(template_items):
            for other_id, other in template_items[i + 1 :]:
                similarity = self._jaccard_similarity(template.skeleton, other.skeleton)
                if similarity >= 0.92:
                    winner, loser = self._select_merge_target(template_id, other_id)
                    self._merge_templates(winner, loser)
                    results.append(
                        TemplateEvolutionResult(
                            template_id=loser,
                            skeleton=self._templates[winner].skeleton,
                            merged_into=winner,
                            changed=True,
                        )
                    )

        return results

    def detect_drift(self, recent_texts: list[str], baseline_template_id: str) -> float:
        baseline = self._templates.get(baseline_template_id)
        if not baseline or not recent_texts:
            return 0.0

        recent_skeletons = [_skeletonize(text) for text in recent_texts[:100]]
        avg_similarity = sum(self._jaccard_similarity(baseline.skeleton, sk) for sk in recent_skeletons) / len(
            recent_skeletons
        )
        return round(max(0.0, 1.0 - avg_similarity), 3)

    def get(self, template_id: str) -> Template | None:
        return self._templates.get(template_id)

    def top_templates(self, limit: int = 20) -> list[Template]:
        return sorted(self._templates.values(), key=lambda template: (-template.count, -template.last_seen))[:limit]

    def stats(self) -> dict[str, int]:
        return {
            "templates": len(self._templates),
            "max_templates": self._max_templates,
            "high_volume_templates": sum(1 for template in self._templates.values() if template.count >= 100),
        }

    def _find_similar_template(self, template_id: str, skeleton: str) -> str | None:
        for existing_id, existing in self._templates.items():
            if existing_id == template_id:
                continue
            if self._jaccard_similarity(existing.skeleton, skeleton) >= 0.95:
                return existing_id
        return None

    def _merge_templates(self, winner_id: str, loser_id: str) -> None:
        if winner_id == loser_id:
            return

        winner = self._templates.get(winner_id)
        loser = self._templates.get(loser_id)
        if not winner or not loser:
            return

        winner.count += loser.count
        winner.last_seen = max(winner.last_seen, loser.last_seen)
        winner.examples = (winner.examples + loser.examples)[:10]
        self._templates.pop(loser_id, None)

    def _select_merge_target(self, first_id: str, second_id: str) -> tuple[str, str]:
        first = self._templates[first_id]
        second = self._templates[second_id]
        if first.count >= second.count:
            return first_id, second_id
        return second_id, first_id

    def _evict(self) -> None:
        candidates = sorted(self._templates.values(), key=lambda template: (template.count, template.last_seen))
        for template in candidates[: max(1, len(candidates) // 10)]:
            self._templates.pop(template.template_id, None)

    @staticmethod
    def _make_template_id(skeleton: str) -> str:
        return hashlib.sha256(skeleton.encode()).hexdigest()[:12]

    @staticmethod
    def _jaccard_similarity(first: str, second: str) -> float:
        first_tokens = set(first.split())
        second_tokens = set(second.split())
        if not first_tokens and not second_tokens:
            return 1.0
        if not first_tokens or not second_tokens:
            return 0.0
        intersection = len(first_tokens & second_tokens)
        union = len(first_tokens | second_tokens)
        return intersection / union if union else 0.0
