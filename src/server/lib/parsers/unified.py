"""Unified Parser — single entry point for all log formats.

Performs deep format discovery using MIME sniffing + semiconductor signal
detection, then routes to the appropriate sub-pipeline via the
``ParserRegistry``.

The ``UnifiedParser`` replaces the per-file routing in ``orchestrator.py``
with a single class that performs:
  1. **Deep Format Discovery**: ``python-magic`` MIME sniffing combined with
     a 'Semiconductor Signal' regex pass detecting ``ERRCODE=``, ``0x[A-F0-9]``,
     ``RF_``, ``CHAMBER_``, etc.
  2. **Protocol Routing**: Dispatches to ``StructuredPipeline`` (JSON/XML/CSV),
     ``SemiStructuredPipeline`` (Syslog/KV), or ``UnstructuredPipeline``
     (Plain Text/Binary).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lib.parsers.contracts import (
    ParserPipelineResult,
    StructuralClass,
)
from lib.parsers.registry import ParserRegistry

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult
    from lib.parsers.preprocessor import FileInput

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Semiconductor signal patterns for routing decisions
# ---------------------------------------------------------------------------

SEMICONDUCTOR_SIGNALS: list[tuple[str, re.Pattern[str]]] = [
    ("errcode",   re.compile(r"ERRCODE\s*=", re.IGNORECASE)),
    ("hex_value", re.compile(r"0x[A-Fa-f0-9]{2,}")),
    ("rf_code",   re.compile(r"\bRF_\d+\b")),
    ("chamber",   re.compile(r"\bCHAMBER[_-]?\w+\b", re.IGNORECASE)),
    ("wafer_id",  re.compile(r"\bW\d{2,4}\b")),
    ("lot_id",    re.compile(r"\b(?:LOT|FOUP)[_-]?\w+\b", re.IGNORECASE)),
    ("tool_id",   re.compile(r"\b(?:TOOL|EQP)[_-]?\w+\b", re.IGNORECASE)),
    ("recipe",    re.compile(r"\bRECIPE[_-]?\w+\b", re.IGNORECASE)),
]


# ---------------------------------------------------------------------------
# Discovery result
# ---------------------------------------------------------------------------


@dataclass
class FormatDiscoveryResult:
    """Result of deep format discovery for a single file."""

    mime_type: str = "text/plain"
    structural_class: StructuralClass = StructuralClass.UNSTRUCTURED
    semiconductor_signals: dict[str, int] = field(default_factory=dict)
    signal_density: float = 0.0  # signals per line
    recommended_parser: str = "unstructured"
    reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Unified Parser
# ---------------------------------------------------------------------------


class UnifiedParser:
    """Single entry point for all log parsing.

    Performs:
      1. MIME sniffing via ``python-magic``
      2. Semiconductor signal regex pass
      3. Protocol routing to the correct sub-pipeline
    """

    def discover_format(self, file_input: "FileInput") -> FormatDiscoveryResult:
        """Deep format discovery combining MIME + signal detection."""
        result = FormatDiscoveryResult()

        # --- MIME sniffing ---
        mime = self._sniff_mime(file_input.content)
        result.mime_type = mime

        # --- Semiconductor signal pass ---
        lines = file_input.content.splitlines()[:200]
        signal_counts: dict[str, int] = {}
        for line in lines:
            for signal_name, pattern in SEMICONDUCTOR_SIGNALS:
                if pattern.search(line):
                    signal_counts[signal_name] = signal_counts.get(signal_name, 0) + 1

        result.semiconductor_signals = signal_counts
        result.signal_density = sum(signal_counts.values()) / max(len(lines), 1)

        # --- Route decision via MIME type ---
        match mime:
            case m if "json" in m:
                result.structural_class = StructuralClass.STRUCTURED
                result.recommended_parser = "structured"
                result.reasons.append(f"MIME type {m} indicates JSON")
            case m if "xml" in m:
                result.structural_class = StructuralClass.STRUCTURED
                result.recommended_parser = "structured"
                result.reasons.append(f"MIME type {m} indicates XML")
            case m if "csv" in m or "comma-separated" in m:
                result.structural_class = StructuralClass.STRUCTURED
                result.recommended_parser = "structured"
                result.reasons.append(f"MIME type {m} indicates CSV")
            case m if "octet-stream" in m or "application/x-" in m:
                result.structural_class = StructuralClass.UNSTRUCTURED
                result.recommended_parser = "unstructured"
                result.reasons.append("Binary content detected via MIME")
            case _:
                # Fall through to content-based detection via registry scoring
                pass

        # Content-based override for text types that may be semi-structured
        if result.recommended_parser == "unstructured" and mime.startswith("text/"):
            ranked = ParserRegistry.support_for_file(file_input, mime_type=mime)
            if ranked and ranked[0].supported and ranked[0].score > 0.7:
                result.recommended_parser = ranked[0].parser_key
                result.structural_class = (
                    ranked[0].structural_class or StructuralClass.UNSTRUCTURED
                )
                result.reasons.append(
                    f"Score-based routing: {ranked[0].parser_key} ({ranked[0].score:.2f})"
                )

        # Signal density annotation
        if result.signal_density > 0.3:
            result.reasons.append(
                f"High semiconductor signal density ({result.signal_density:.2f}/line)"
            )

        return result

    def ingest(
        self,
        file_inputs: list["FileInput"],
        classification: "ClassificationResult",
    ) -> ParserPipelineResult:
        """Route files through discovery and dispatch to sub-pipelines."""
        grouped: dict[str, list["FileInput"]] = {}

        for fi in file_inputs:
            discovery = self.discover_format(fi)
            key = discovery.recommended_parser
            grouped.setdefault(key, []).append(fi)
            logger.info(
                "UnifiedParser: %s -> %s (mime=%s, signals=%d, density=%.2f)",
                fi.filename,
                key,
                discovery.mime_type,
                sum(discovery.semiconductor_signals.values()),
                discovery.signal_density,
            )

        # Dispatch to sub-pipelines and merge results
        all_table_defs = []
        all_records: dict[str, list[dict[str, Any]]] = {}
        all_warnings: list[str] = []
        confidence_sum = 0.0
        confidence_count = 0

        for parser_key, files in grouped.items():
            try:
                pipeline = ParserRegistry.route(parser_key)
            except KeyError as exc:
                all_warnings.append(str(exc))
                continue

            try:
                result = pipeline.ingest(files, classification)
            except Exception as exc:  # noqa: BLE001
                logger.exception("UnifiedParser: pipeline '%s' failed", parser_key)
                all_warnings.append(f"Pipeline '{parser_key}' failed: {exc}")
                continue

            all_table_defs.extend(result.table_definitions)
            all_records.update(result.records)
            all_warnings.extend(result.warnings)
            confidence_sum += result.confidence
            confidence_count += 1

        return ParserPipelineResult(
            table_definitions=all_table_defs,
            records=all_records,
            parser_key="unified",
            warnings=all_warnings,
            confidence=round(confidence_sum / max(confidence_count, 1), 2),
        )

    @staticmethod
    def _sniff_mime(content: str) -> str:
        """MIME-sniff using python-magic, falling back to text/plain."""
        try:
            import magic

            mime = magic.from_buffer(
                content[:8192].encode("utf-8", errors="replace"), mime=True
            )
            return mime or "text/plain"
        except Exception:  # noqa: BLE001
            return "text/plain"
