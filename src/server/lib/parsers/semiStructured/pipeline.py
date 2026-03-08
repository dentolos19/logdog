"""
Semi-Structured Log Parsing Pipeline
=====================================
Orchestrates the 4-step parsing pipeline with AI fallback:

    ┌─────────────────────────────┐
    │  1. Grok/Regex Engine       │  ← pattern matching
    └──────────┬──────────────────┘
               ▼
    ┌─────────────────────────────┐
    │  2. Field Extraction        │  ← structure from raw text
    └──────────┬──────────────────┘
               ▼
    ┌─────────────────────────────┐
    │  3. Delimiter Splitting     │  ← key-value pairs
    └──────────┬──────────────────┘
               ▼
    ┌─────────────────────────────┐
    │  4. Fuzzy Header Matching   │  ← canonical field names
    └──────────┬──────────────────┘
               ▼
       confidence >= threshold?
         ├── YES → Normalizer → LogRow
         └── NO  → AI Fallback (Gemini 3.1 Flash-Lite)
                      ↓
                   Normalizer → LogRow

Template caching ensures the LLM is only called for truly novel formats.
"""

import json
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from .grok_engine import GrokEngine, GrokResult
from .field_extractor import FieldExtractor, ExtractionResult, ExtractedField
from .delimiter_splitter import DelimiterSplitter
from .fuzzy_matcher import FuzzyMatcher
from .ai_fallback import AIFallback, AIFallbackConfig, AIFallbackResult
from .template_cache import TemplateCache
from .normalizer import Normalizer, LogRow

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------
@dataclass
class PipelineConfig:
    """Configuration for the semi-structured parsing pipeline."""
    confidence_threshold: float = 0.5     # min confidence to skip AI fallback
    ai_fallback_enabled: bool = True
    ai_config: Optional[AIFallbackConfig] = None
    log_group_id: str = "default"
    fuzzy_threshold: float = 0.75         # fuzzy matcher similarity threshold


# ---------------------------------------------------------------------------
# Pipeline result
# ---------------------------------------------------------------------------
@dataclass
class PipelineResult:
    """Result of processing a log through the pipeline."""
    log_row: LogRow
    grok_result: Optional[GrokResult] = None
    extraction_result: Optional[ExtractionResult] = None
    ai_result: Optional[AIFallbackResult] = None
    stages_executed: list[str] = field(default_factory=list)
    total_latency_ms: float = 0.0
    ai_fallback_used: bool = False
    confidence: float = 0.0
    format_detected: Optional[str] = None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
class SemiStructuredPipeline:
    """
    Main orchestrator for the semi-structured log parsing pipeline.

    Usage:
        pipeline = SemiStructuredPipeline()
        result = pipeline.process(raw_log_text)
        log_row = result.log_row
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()

        # Initialize components
        self.grok = GrokEngine()
        self.extractor = FieldExtractor()
        self.splitter = DelimiterSplitter()
        self.matcher = FuzzyMatcher(similarity_threshold=self.config.fuzzy_threshold)
        self.cache = TemplateCache()
        self.normalizer = Normalizer(fuzzy_matcher=self.matcher)
        self.ai_fallback = AIFallback(
            config=self.config.ai_config or AIFallbackConfig(),
            template_cache=self.cache,
        )

    def process(self, raw_text: str) -> PipelineResult:
        """
        Process a raw log text through the full pipeline.

        Returns a PipelineResult containing the normalized LogRow
        and diagnostic information about which stages ran.
        """
        start = time.time()
        result = PipelineResult(log_row=LogRow())

        # ─── Stage 1: Grok/Regex Pattern Matching ─────────────────────
        grok_result = self.grok.match_block(raw_text)
        result.grok_result = grok_result
        result.stages_executed.append("grok_engine")

        format_hint = self.grok.detect_format(raw_text)
        result.format_detected = format_hint

        logger.debug(
            f"Grok: {len(grok_result.matches)} matches, "
            f"{len(grok_result.unmatched_lines)} unmatched, "
            f"format={format_hint}"
        )

        # ─── Stage 2: Field Extraction ────────────────────────────────
        extraction = self.extractor.extract(raw_text, format_hint=format_hint)
        result.extraction_result = extraction
        result.stages_executed.append("field_extraction")

        logger.debug(
            f"Extractor: {len(extraction.fields)} fields, "
            f"{len(extraction.sections)} sections, "
            f"confidence={extraction.confidence:.2f}"
        )

        # ─── Stage 3: Delimiter Splitting (for unmatched lines) ───────
        if grok_result.unmatched_lines:
            unmatched_text = "\n".join(grok_result.unmatched_lines)
            kv_pairs = self.splitter.split_block(unmatched_text)
            # Merge into extraction result
            for kv in kv_pairs:
                from .field_extractor import ExtractedField
                extraction.fields.append(ExtractedField(
                    key=kv.key,
                    value=kv.value,
                    source_section="delimiter_split",
                ))
            result.stages_executed.append("delimiter_splitting")

        # ─── Stage 4: Fuzzy Header Matching ───────────────────────────
        raw_keys = [f.key for f in extraction.fields]
        fuzzy_matches = self.matcher.match_keys(raw_keys)
        result.stages_executed.append("fuzzy_matching")

        # Calculate overall confidence
        if extraction.fields:
            matched_ratio = len(fuzzy_matches) / len(extraction.fields)
            result.confidence = (extraction.confidence * 0.6 + matched_ratio * 0.4)
        else:
            result.confidence = 0.0

        logger.debug(
            f"Fuzzy: {len(fuzzy_matches)}/{len(raw_keys)} keys matched, "
            f"overall_confidence={result.confidence:.2f}"
        )

        # ─── Decision: AI Fallback or Normalize? ──────────────────────
        if result.confidence >= self.config.confidence_threshold:
            # Sufficient confidence — normalize directly
            result.log_row = self.normalizer.normalize(
                extraction=extraction,
                raw_text=raw_text,
                log_group_id=self.config.log_group_id,
                parse_confidence=result.confidence,
            )
        elif self.config.ai_fallback_enabled:
            # Low confidence — invoke AI fallback
            result.stages_executed.append("ai_fallback")
            result.ai_fallback_used = True

            ai_result = self.ai_fallback.extract(raw_text)
            result.ai_result = ai_result

            if ai_result.success:
                result.log_row = self.normalizer.normalize_from_dict(
                    fields=ai_result.fields,
                    raw_text=raw_text,
                    template_id=ai_result.template_id,
                    log_group_id=self.config.log_group_id,
                    parse_confidence=result.confidence,
                )
                logger.info(
                    f"AI fallback: success, cached={ai_result.cached}, "
                    f"template_id={ai_result.template_id}"
                )
            else:
                # Even AI fallback failed — use whatever we have
                result.log_row = self.normalizer.normalize(
                    extraction=extraction,
                    raw_text=raw_text,
                    log_group_id=self.config.log_group_id,
                    parse_confidence=result.confidence,
                )
                logger.warning(f"AI fallback failed: {ai_result.error}")
        else:
            # AI fallback disabled — normalize best effort
            result.log_row = self.normalizer.normalize(
                extraction=extraction,
                raw_text=raw_text,
                log_group_id=self.config.log_group_id,
                parse_confidence=result.confidence,
            )

        result.total_latency_ms = (time.time() - start) * 1000

        # ─── Normalize stage always runs ──────────────────────────────
        result.stages_executed.append("normalizer")

        return result

    # ---- Batch processing -------------------------------------------------

    def process_batch(self, texts: list[str]) -> list[PipelineResult]:
        """Process multiple log entries."""
        return [self.process(text) for text in texts]

    # ---- Diagnostics ------------------------------------------------------

    def diagnostics(self) -> dict[str, Any]:
        """Return pipeline diagnostics and cost tracking."""
        return {
            "template_cache": self.cache.stats(),
            "ai_cost_tracker": self.ai_fallback.cost_tracker.summary(),
            "config": {
                "confidence_threshold": self.config.confidence_threshold,
                "ai_fallback_enabled": self.config.ai_fallback_enabled,
                "fuzzy_threshold": self.config.fuzzy_threshold,
            },
        }
