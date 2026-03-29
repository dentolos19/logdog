import logging
import time
from dataclasses import dataclass, field
from typing import Any

from parsers.semi_structured.ai_fallback import AIFallback, AIFallbackConfig, AIFallbackResult
from parsers.semi_structured.delimiter_splitter import DelimiterSplitter
from parsers.semi_structured.field_extractor import ExtractedField, ExtractionResult, FieldExtractor
from parsers.semi_structured.fuzzy_matcher import FuzzyMatcher
from parsers.semi_structured.grok_engine import GrokEngine, GrokResult
from parsers.semi_structured.normalizer import LogRow, Normalizer
from parsers.semi_structured.template_cache import TemplateCache

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    confidence_threshold: float = 0.5
    ai_fallback_enabled: bool = True
    ai_config: AIFallbackConfig | None = None
    log_group_id: str = "default"
    fuzzy_threshold: float = 0.75


@dataclass
class PipelineResult:
    log_row: LogRow
    grok_result: GrokResult | None = None
    extraction_result: ExtractionResult | None = None
    ai_result: AIFallbackResult | None = None
    stages_executed: list[str] = field(default_factory=list)
    total_latency_ms: float = 0.0
    ai_fallback_used: bool = False
    confidence: float = 0.0
    format_detected: str | None = None


class SemiStructuredPipeline:
    def __init__(self, config: PipelineConfig | None = None):
        self.config = config or PipelineConfig()
        self.grok = GrokEngine()
        self.extractor = FieldExtractor()
        self.splitter = DelimiterSplitter()
        self.matcher = FuzzyMatcher(similarity_threshold=self.config.fuzzy_threshold)
        self.cache = TemplateCache()
        self.normalizer = Normalizer(fuzzy_matcher=self.matcher)
        self.ai_fallback = AIFallback(config=self.config.ai_config or AIFallbackConfig(), template_cache=self.cache)

    def process(self, raw_text: str) -> PipelineResult:
        start = time.time()
        result = PipelineResult(log_row=LogRow())

        grok_result = self.grok.match_block(raw_text)
        result.grok_result = grok_result
        result.stages_executed.append("grok_engine")

        format_hint = self.grok.detect_format(raw_text)
        result.format_detected = format_hint

        extraction = self.extractor.extract(raw_text, format_hint=format_hint)
        result.extraction_result = extraction
        result.stages_executed.append("field_extraction")

        if grok_result.unmatched_lines:
            unmatched_text = "\n".join(grok_result.unmatched_lines)
            kv_pairs = self.splitter.split_block(unmatched_text)
            for kv_pair in kv_pairs:
                extraction.fields.append(
                    ExtractedField(
                        key=kv_pair.key,
                        value=kv_pair.value,
                        source_section="delimiter_split",
                    )
                )
            result.stages_executed.append("delimiter_splitting")

        raw_keys = [field_item.key for field_item in extraction.fields]
        fuzzy_matches = self.matcher.match_keys(raw_keys)
        result.stages_executed.append("fuzzy_matching")

        if extraction.fields:
            matched_ratio = len(fuzzy_matches) / len(extraction.fields)
            result.confidence = extraction.confidence * 0.6 + matched_ratio * 0.4
        else:
            result.confidence = 0.0

        if result.confidence >= self.config.confidence_threshold:
            result.log_row = self.normalizer.normalize(
                extraction=extraction,
                raw_text=raw_text,
                log_group_id=self.config.log_group_id,
                parse_confidence=result.confidence,
            )
        elif self.config.ai_fallback_enabled:
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
            else:
                result.log_row = self.normalizer.normalize(
                    extraction=extraction,
                    raw_text=raw_text,
                    log_group_id=self.config.log_group_id,
                    parse_confidence=result.confidence,
                )
                if ai_result.error:
                    logger.warning("AI fallback failed: %s", ai_result.error)
        else:
            result.log_row = self.normalizer.normalize(
                extraction=extraction,
                raw_text=raw_text,
                log_group_id=self.config.log_group_id,
                parse_confidence=result.confidence,
            )

        result.total_latency_ms = (time.time() - start) * 1000
        result.stages_executed.append("normalizer")
        return result

    def process_batch(self, texts: list[str]) -> list[PipelineResult]:
        return [self.process(text) for text in texts]

    def diagnostics(self) -> dict[str, Any]:
        return {
            "template_cache": self.cache.stats(),
            "ai_cost_tracker": self.ai_fallback.cost_tracker.summary(),
            "config": {
                "confidence_threshold": self.config.confidence_threshold,
                "ai_fallback_enabled": self.config.ai_fallback_enabled,
                "fuzzy_threshold": self.config.fuzzy_threshold,
            },
        }
