"""
AI Fallback — Gemini 3.1 Flash-Lite
====================================
Called when the regex/grok engine + field extractor + delimiter splitter
+ fuzzy matcher cannot parse a log with sufficient confidence.

Sends the unknown log format to Gemini 3.1 Flash-Lite to extract
structured fields as JSON.

Key design decisions (from decision notes):
  - Model: gemini-3.1-flash-lite-preview
  - response_mime_type: "application/json" for structured output
  - thinking_level: "low" for simple formats, "medium"/"high" for ambiguous
  - Template caching: once the LLM identifies a pattern, cache it
"""

import json
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from lib import ai
from pydantic import BaseModel, Field

from .template_cache import TemplateCache, CachedTemplate

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# System prompt for field extraction
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
You are a log parsing assistant for semiconductor manufacturing equipment.
Your job is to extract structured fields from unknown or semi-structured log formats.

Given a raw log text snippet, extract ALL identifiable fields as a flat JSON object.

Rules:
1. Return ONLY valid JSON — no markdown, no explanations.
2. Use snake_case keys (e.g., "ctrl_job_id", "wafer_start_time").
3. Preserve original values — do not transform or convert them.
4. For nested structures, flatten with dot notation (e.g., "events.start_name").
5. If a field has a unit, include it as a separate key with suffix "_unit"
   (e.g., "pressure": 500.0, "pressure_unit": "mtorr").
6. Include a "_format_type" field indicating the detected format
   (e.g., "lam_parquet", "key_value", "tabular", "custom").
7. Include a "_section_map" field listing detected sections and their field counts
   (e.g., {"ControlJobKeys": 2, "ProcessJobAttributes": 15}).

Common semiconductor log fields to look for:
- Equipment/tool identifiers (EquipmentID, ModuleID, etc.)
- Job identifiers (CtrlJobID, PRJobID, LotID, WaferID)
- Timestamps (start/end times in ISO 8601)
- Recipe parameters (gas flows, pressures, RF power, temperatures)
- Recipe step metadata (step names, step IDs, durations)
- Sensor readings (SensorID, values, units)
- Events and state transitions
"""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class AIFallbackConfig:
    """Configuration for the AI fallback module."""

    # None = use OPENROUTER_API_KEY from environment; empty string disables AI calls.
    api_key: Optional[str] = None
    # None = use OPENROUTER_MODEL from environment.
    model: Optional[str] = None
    endpoint: str = "https://generativelanguage.googleapis.com/v1beta/models"
    default_thinking_level: str = "low"  # low, medium, high
    max_input_tokens: int = 4000  # truncate input if too long
    timeout_seconds: float = 10.0
    max_retries: int = 2
    cache_enabled: bool = True

    # Cost tracking
    input_price_per_m: float = 0.25  # $0.25 / 1M input tokens
    output_price_per_m: float = 1.50  # $1.50 / 1M output tokens


# ---------------------------------------------------------------------------
# AI Fallback Result
# ---------------------------------------------------------------------------
@dataclass
class AIFallbackResult:
    success: bool
    fields: dict[str, Any] = field(default_factory=dict)
    format_type: str = "unknown"
    section_map: dict[str, int] = field(default_factory=dict)
    thinking_level: str = "low"
    cached: bool = False
    template_id: Optional[str] = None
    latency_ms: float = 0.0
    estimated_cost_usd: float = 0.0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Cost Tracker
# ---------------------------------------------------------------------------
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
        return self.cache_hits / total if total > 0 else 0.0

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


class LlmSemiStructuredResponse(BaseModel):
    """Structured response for semi-structured fallback extraction."""

    fields: dict[str, Any] = Field(default_factory=dict)
    format_type: str = "unknown"
    section_map: dict[str, int] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# AI Fallback Engine
# ---------------------------------------------------------------------------
class AIFallback:
    """
    AI fallback for unrecognized log formats.

    Workflow:
    1. Check template cache — if this log shape was seen before, reuse mapping
    2. If cache miss, determine thinking level based on complexity
    3. Call Gemini 3.1 Flash-Lite API
    4. Parse response, cache the template, return fields
    """

    def __init__(
        self,
        config: Optional[AIFallbackConfig] = None,
        template_cache: Optional[TemplateCache] = None,
    ):
        self.config = config or AIFallbackConfig()
        self.cache = template_cache or TemplateCache()
        self.cost_tracker = CostTracker()

    def extract(self, raw_text: str, context: Optional[dict] = None) -> AIFallbackResult:
        """
        Main entry point — extract fields from unknown log text.

        Args:
            raw_text: The raw log text that regex couldn't parse
            context: Optional context (e.g., vendor hint, equipment type)

        Returns:
            AIFallbackResult with extracted fields
        """
        start = time.time()

        # Step 1: Check cache
        if self.config.cache_enabled:
            cached = self.cache.get(raw_text)
            if cached:
                self.cost_tracker.record_cache_hit()
                return AIFallbackResult(
                    success=True,
                    fields=cached.field_mapping,
                    format_type=cached.format_type,
                    cached=True,
                    template_id=cached.template_id,
                    latency_ms=(time.time() - start) * 1000,
                )

        # Step 2: Determine thinking level
        thinking_level = self._assess_complexity(raw_text)

        # Step 3: Call LLM
        result = self._call_gemini(raw_text, thinking_level, context)
        result.latency_ms = (time.time() - start) * 1000

        # Step 4: Cache on success
        if result.success and self.config.cache_enabled:
            tmpl = self.cache.put(
                text=raw_text,
                field_mapping=result.fields,
                format_type=result.format_type,
                source="ai_fallback",
            )
            result.template_id = tmpl.template_id

        return result

    # ---- Complexity assessment --------------------------------------------

    def _assess_complexity(self, text: str) -> str:
        """Determine thinking level based on log complexity."""
        lines = text.strip().splitlines()
        line_count = len(lines)
        unique_delimiters = set()

        for line in lines[:50]:
            if "=" in line:
                unique_delimiters.add("=")
            if '": ' in line:
                unique_delimiters.add("json")
            if "\t" in line:
                unique_delimiters.add("tab")
            if "|" in line:
                unique_delimiters.add("pipe")
            if "---" in line:
                unique_delimiters.add("section")

        # Multiple delimiter types or very long → higher thinking
        if len(unique_delimiters) >= 3 or line_count > 100:
            return "high"
        if len(unique_delimiters) >= 2 or line_count > 30:
            return "medium"
        return "low"

    # ---- AI API call --------------------------------------------------

    def _call_gemini(
        self,
        raw_text: str,
        thinking_level: str,
        context: Optional[dict] = None,
    ) -> AIFallbackResult:
        """
        Call a centralized LLM backend for field extraction.

        Uses lib.ai to keep AI client setup and model selection centralized.
        Falls back to local heuristics if no key is configured or invocation fails.
        """
        resolved_api_key = ai.resolve_openrouter_api_key(self.config.api_key)
        if not ai.has_openrouter_api_key(resolved_api_key):
            logger.info("No API key configured — using local heuristic fallback")
            return self._local_fallback(raw_text, thinking_level)

        # Build the prompt payload.
        truncated = raw_text[: self.config.max_input_tokens * 4]  # rough char limit
        context_json = json.dumps(context or {}, ensure_ascii=True)
        budget = self._thinking_budget(thinking_level)

        system_prompt = (
            f"{SYSTEM_PROMPT}\n\n"
            "Return JSON matching this schema exactly:\n"
            "- fields: object containing extracted flat key/value pairs\n"
            "- format_type: short format identifier\n"
            "- section_map: object of section_name -> field_count\n"
            "Do not include prose or markdown."
        )
        user_prompt = (
            f"Thinking level: {thinking_level} (budget={budget}).\n"
            f"Context JSON: {context_json}\n\n"
            "Extract structured fields from this log:\n\n"
            f"{truncated}"
        )
        messages = [
            ("system", system_prompt),
            ("human", user_prompt),
        ]

        invocation = ai.invoke_structured_openrouter(
            messages,
            LlmSemiStructuredResponse,
            context="Semi-structured AI fallback",
            model=self.config.model,
            api_key=resolved_api_key,
            temperature=0.1,
            max_tokens=4096,
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

        est_input_tokens = len(truncated) // 4
        est_output_tokens = len(json.dumps(fields, ensure_ascii=True)) // 4
        self.cost_tracker.record_call(
            est_input_tokens,
            est_output_tokens,
            self.config,
        )

        return AIFallbackResult(
            success=len(fields) > 2,
            fields=fields,
            format_type=format_type,
            section_map=section_map,
            thinking_level=thinking_level,
            estimated_cost_usd=(
                est_input_tokens / 1_000_000 * self.config.input_price_per_m
                + est_output_tokens / 1_000_000 * self.config.output_price_per_m
            ),
        )

    def _thinking_budget(self, level: str) -> int:
        """Map thinking level to token budget."""
        return {"low": 128, "medium": 512, "high": 2048}.get(level, 256)

    # ---- Local fallback (for when API is unavailable) ---------------------

    def _local_fallback(self, text: str, thinking_level: str) -> AIFallbackResult:
        """
        Heuristic-based fallback that mimics what the LLM would return.
        Used when API key is not configured or API is unreachable.
        """
        fields: dict[str, Any] = {}
        section_map: dict[str, int] = {}
        format_type = "unknown"

        lines = text.strip().splitlines()
        current_section = "_root"

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped in ("{", "}", "[", "]"):
                continue

            # Section headers
            if stripped.startswith("---") and stripped.endswith("---"):
                current_section = stripped.strip("- ").strip()
                section_map[current_section] = 0
                continue

            # ROW markers
            import re

            row_m = re.match(r"^ROW\s+(\d+)", stripped)
            if row_m:
                current_section = f"row_{row_m.group(1)}"
                section_map[current_section] = 0
                continue

            # JSON-style KV
            jkv = re.match(r'^\s*"([^"]+)"\s*:\s*(.+?)\s*,?\s*$', stripped)
            if jkv:
                key = jkv.group(1)
                val = jkv.group(2).strip().strip('"')
                prefix = current_section.lower().replace(" ", "_")
                full_key = f"{prefix}.{key}" if current_section != "_root" else key
                fields[full_key] = self._smart_cast(val)
                section_map[current_section] = section_map.get(current_section, 0) + 1
                format_type = "section_delimited_json"
                continue

            # Equals KV
            ekv = re.match(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$", stripped)
            if ekv:
                fields[ekv.group(1)] = self._smart_cast(ekv.group(2).strip())
                section_map[current_section] = section_map.get(current_section, 0) + 1
                format_type = format_type if format_type != "unknown" else "key_value"
                continue

            # Colon KV
            ckv = re.match(r"^([A-Za-z_][\w\s]{0,25}?)\s*:\s+(.+)$", stripped)
            if ckv:
                key = ckv.group(1).strip().replace(" ", "_")
                fields[key] = self._smart_cast(ckv.group(2).strip())
                section_map[current_section] = section_map.get(current_section, 0) + 1
                continue

        fields["_format_type"] = format_type
        fields["_section_map"] = section_map

        est_input_tokens = len(text) // 4
        est_output_tokens = len(json.dumps(fields)) // 4
        self.cost_tracker.record_call(est_input_tokens, est_output_tokens, self.config)

        return AIFallbackResult(
            success=len(fields) > 2,  # at least some fields extracted
            fields=fields,
            format_type=format_type,
            section_map=section_map,
            thinking_level=thinking_level,
            estimated_cost_usd=(
                est_input_tokens / 1_000_000 * self.config.input_price_per_m
                + est_output_tokens / 1_000_000 * self.config.output_price_per_m
            ),
        )

    @staticmethod
    def _smart_cast(val: str) -> Any:
        if val.lower() in ("null", "none", ""):
            return None
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        try:
            if "." in val:
                return float(val)
            return int(val)
        except ValueError:
            return val


# ---------------------------------------------------------------------------
# Gemini API request builder (for reference / production use)
# ---------------------------------------------------------------------------
def build_gemini_request(
    raw_text: str,
    thinking_level: str = "low",
    model: str = "gemini-3.1-flash-lite-preview",
) -> dict:
    """
    Build the complete Gemini API request payload.
    Can be used directly with the google-genai SDK or REST API.

    Usage with REST:
        POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={API_KEY}

    Usage with google-genai SDK:
        from google import genai
        client = genai.Client(api_key=API_KEY)
        response = client.models.generate_content(
            model=model,
            contents=raw_text,
            config=genai.types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                response_mime_type="application/json",
                thinking_config=genai.types.ThinkingConfig(
                    thinking_budget=budget
                ),
            )
        )
    """
    budget = {"low": 128, "medium": 512, "high": 2048}.get(thinking_level, 256)

    return {
        "model": model,
        "contents": [{"role": "user", "parts": [{"text": f"Extract structured fields from this log:\n\n{raw_text}"}]}],
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.1,
            "thinkingConfig": {"thinkingBudget": budget},
        },
    }
