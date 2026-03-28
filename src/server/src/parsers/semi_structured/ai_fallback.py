import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import src.parsers.ai_wrappers as ai
from src.parsers.semi_structured.template_cache import TemplateCache

logger = logging.getLogger(__name__)


@dataclass
class AIFallbackConfig:
    api_key: str | None = None
    model: str | None = None
    endpoint: str = "https://openrouter.ai/api/v1"
    default_thinking_level: str = "low"
    max_input_tokens: int = 4000
    timeout_seconds: float = 10.0
    max_retries: int = 2
    cache_enabled: bool = True
    input_price_per_m: float = 0.25
    output_price_per_m: float = 1.50


@dataclass
class AIFallbackResult:
    success: bool
    fields: dict[str, Any] = field(default_factory=dict)
    format_type: str = "unknown"
    section_map: dict[str, int] = field(default_factory=dict)
    thinking_level: str = "low"
    cached: bool = False
    template_id: str | None = None
    latency_ms: float = 0.0
    estimated_cost_usd: float = 0.0
    error: str | None = None


@dataclass
class CostTracker:
    total_calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total else 0.0

    def record_call(self, input_tokens: int, output_tokens: int, config: AIFallbackConfig):
        self.total_calls += 1
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.total_cost_usd += (
            input_tokens / 1_000_000 * config.input_price_per_m + output_tokens / 1_000_000 * config.output_price_per_m
        )
        self.cache_misses += 1

    def record_cache_hit(self):
        self.cache_hits += 1

    def summary(self) -> dict[str, Any]:
        return {
            "total_calls": self.total_calls,
            "total_cost_usd": round(self.total_cost_usd, 6),
            "cache_hit_rate": f"{self.cache_hit_rate:.1%}",
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "avg_cost_per_call": round(self.total_cost_usd / max(self.total_calls, 1), 6),
        }


class AIFallback:
    def __init__(self, config: AIFallbackConfig | None = None, template_cache: TemplateCache | None = None):
        self.config = config or AIFallbackConfig()
        self.cache = template_cache or TemplateCache()
        self.cost_tracker = CostTracker()

    def extract(self, raw_text: str, context: dict | None = None) -> AIFallbackResult:
        start = time.time()

        if self.config.cache_enabled:
            cached = self.cache.get(raw_text)
            if cached is not None:
                self.cost_tracker.record_cache_hit()
                return AIFallbackResult(
                    success=True,
                    fields=cached.field_mapping,
                    format_type=cached.format_type,
                    cached=True,
                    template_id=cached.template_id,
                    latency_ms=(time.time() - start) * 1000,
                )

        thinking_level = self._assess_complexity(raw_text)
        result = self._call_openrouter(raw_text, thinking_level, context)
        result.latency_ms = (time.time() - start) * 1000

        if result.success and self.config.cache_enabled:
            template = self.cache.put(
                text=raw_text,
                field_mapping=result.fields,
                format_type=result.format_type,
                source="ai_fallback",
            )
            result.template_id = template.template_id

        return result

    def _assess_complexity(self, text: str) -> str:
        lines = text.strip().splitlines()
        line_count = len(lines)
        delimiters = set()

        for line in lines[:50]:
            if "=" in line:
                delimiters.add("=")
            if '": ' in line:
                delimiters.add("json")
            if "\t" in line:
                delimiters.add("tab")
            if "|" in line:
                delimiters.add("pipe")
            if "---" in line:
                delimiters.add("section")

        if len(delimiters) >= 3 or line_count > 100:
            return "high"
        if len(delimiters) >= 2 or line_count > 30:
            return "medium"
        return "low"

    def _call_openrouter(self, raw_text: str, thinking_level: str, context: dict | None = None) -> AIFallbackResult:
        resolved_api_key = ai.resolve_openrouter_api_key(self.config.api_key)
        if not ai.has_openrouter_api_key(resolved_api_key):
            logger.info("No API key configured. Using local fallback.")
            return self._local_fallback(raw_text, thinking_level)

        truncated = raw_text[: self.config.max_input_tokens * 4]
        context_json = json.dumps(context or {}, ensure_ascii=True)

        invocation = ai.extract_semi_structured_fields(
            raw_text=truncated,
            thinking_level=thinking_level,
            context_json=context_json,
            model=self.config.model,
            api_key=resolved_api_key,
        )

        if invocation.response is None:
            if invocation.warning:
                logger.warning("Semi-structured AI fallback failed: %s", invocation.warning)
            return self._local_fallback(raw_text, thinking_level)

        response = invocation.response
        fields = dict(response.fields)
        section_map = dict(response.section_map)
        format_type = response.format_type or "unknown"
        fields["_format_type"] = format_type
        fields["_section_map"] = section_map

        estimated_input_tokens = len(truncated) // 4
        estimated_output_tokens = len(json.dumps(fields, ensure_ascii=True)) // 4
        self.cost_tracker.record_call(estimated_input_tokens, estimated_output_tokens, self.config)

        return AIFallbackResult(
            success=len(fields) > 2,
            fields=fields,
            format_type=format_type,
            section_map=section_map,
            thinking_level=thinking_level,
            estimated_cost_usd=(
                estimated_input_tokens / 1_000_000 * self.config.input_price_per_m
                + estimated_output_tokens / 1_000_000 * self.config.output_price_per_m
            ),
        )

    def _local_fallback(self, text: str, thinking_level: str) -> AIFallbackResult:
        fields: dict[str, Any] = {}
        section_map: dict[str, int] = {}
        format_type = "unknown"

        lines = text.strip().splitlines()
        current_section = "_root"

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped in {"{", "}", "[", "]"}:
                continue

            if stripped.startswith("---") and stripped.endswith("---"):
                current_section = stripped.strip("- ").strip()
                section_map[current_section] = 0
                continue

            row_match = __import__("re").match(r"^ROW\s+(\d+)", stripped)
            if row_match:
                current_section = f"row_{row_match.group(1)}"
                section_map[current_section] = 0
                continue

            json_kv_match = __import__("re").match(r'^\s*"([^"]+)"\s*:\s*(.+?)\s*,?\s*$', stripped)
            if json_kv_match:
                key = json_kv_match.group(1)
                value = json_kv_match.group(2).strip().strip('"')
                prefix = current_section.lower().replace(" ", "_")
                full_key = f"{prefix}.{key}" if current_section != "_root" else key
                fields[full_key] = self._smart_cast(value)
                section_map[current_section] = section_map.get(current_section, 0) + 1
                format_type = "section_delimited_json"
                continue

            equals_match = __import__("re").match(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$", stripped)
            if equals_match:
                fields[equals_match.group(1)] = self._smart_cast(equals_match.group(2).strip())
                section_map[current_section] = section_map.get(current_section, 0) + 1
                if format_type == "unknown":
                    format_type = "key_value"
                continue

            colon_match = __import__("re").match(r"^([A-Za-z_][\w\s]{0,25}?)\s*:\s+(.+)$", stripped)
            if colon_match:
                key = colon_match.group(1).strip().replace(" ", "_")
                fields[key] = self._smart_cast(colon_match.group(2).strip())
                section_map[current_section] = section_map.get(current_section, 0) + 1

        fields["_format_type"] = format_type
        fields["_section_map"] = section_map

        estimated_input_tokens = len(text) // 4
        estimated_output_tokens = len(json.dumps(fields)) // 4
        self.cost_tracker.record_call(estimated_input_tokens, estimated_output_tokens, self.config)

        return AIFallbackResult(
            success=len(fields) > 2,
            fields=fields,
            format_type=format_type,
            section_map=section_map,
            thinking_level=thinking_level,
            estimated_cost_usd=(
                estimated_input_tokens / 1_000_000 * self.config.input_price_per_m
                + estimated_output_tokens / 1_000_000 * self.config.output_price_per_m
            ),
        )

    @staticmethod
    def _smart_cast(value: str) -> Any:
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
