"""Parser pipeline registry.

New parser pipelines register themselves here. The orchestrator uses the
registry to dispatch classified file inputs to the appropriate pipeline.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.parsers.contracts import (
        ClassificationResult,
        FileParserSelection,
        ParserPipelineResult,
        ParserSupportRequest,
        ParserSupportResult,
    )
    from lib.parsers.preprocessor import FileInput

logger = logging.getLogger(__name__)


class ParserPipeline(ABC):
    """Abstract base for all parser pipeline implementations.

    Subclasses must set ``parser_key`` at the class level and implement
    ``parse()``.  Register an instance via ``ParserRegistry.register()``.
    """

    #: Unique routing key, e.g. "structured", "semi_structured", "unstructured".
    parser_key: str = ""

    @abstractmethod
    def parse(
        self,
        file_inputs: list["FileInput"],
        classification: "ClassificationResult",
    ) -> "ParserPipelineResult":
        """Parse the given files and return table definitions plus row data."""
        ...

    @abstractmethod
    def supports(self, request: "ParserSupportRequest") -> "ParserSupportResult":
        """Check if this parser can handle a file and return a support score."""
        ...

    def ingest(
        self,
        file_inputs: list["FileInput"],
        classification: "ClassificationResult",
    ) -> "ParserPipelineResult":
        """Common ingestion entrypoint that defaults to parse()."""
        return self.parse(file_inputs, classification)


class _ParserRegistry:
    """Singleton registry mapping parser keys to pipeline instances."""

    def __init__(self) -> None:
        self._pipelines: dict[str, ParserPipeline] = {}
        self._fallback_order = {
            "structured": 0,
            "semi_structured": 1,
            "unstructured": 2,
        }

    def register(self, pipeline: ParserPipeline) -> None:
        if pipeline.parser_key in self._pipelines:
            logger.warning(
                "Overwriting registered parser pipeline for key '%s'",
                pipeline.parser_key,
            )
        self._pipelines[pipeline.parser_key] = pipeline
        logger.debug("Registered parser pipeline: %s", pipeline.parser_key)

    def route(self, parser_key: str) -> ParserPipeline:
        if parser_key not in self._pipelines:
            raise KeyError(
                f"No parser pipeline registered for key '{parser_key}'. Registered: {sorted(self._pipelines)}"
            )
        return self._pipelines[parser_key]

    def registered_keys(self) -> list[str]:
        return list(self._pipelines.keys())

    def support_for_file(
        self,
        file_input: "FileInput",
        mime_type: str | None = None,
    ) -> list["ParserSupportResult"]:
        from lib.parsers.contracts import ParserSupportRequest, ParserSupportResult

        request = ParserSupportRequest(
            file_id=file_input.file_id,
            filename=file_input.filename,
            content=file_input.content,
            mime_type=mime_type,
        )

        results: list[ParserSupportResult] = []
        for parser_key, pipeline in self._pipelines.items():
            try:
                result = pipeline.supports(request)
            except Exception as exc:  # noqa: BLE001
                logger.warning("supports() failed for parser '%s': %s", parser_key, exc)
                result = ParserSupportResult(
                    parser_key=parser_key,
                    supported=False,
                    score=0.0,
                    reasons=[f"Support check failed: {exc}"],
                )
            results.append(result)

        return sorted(
            results,
            key=lambda item: (
                1 if item.supported else 0,
                item.score,
                -self._fallback_order.get(item.parser_key, 99),
            ),
            reverse=True,
        )

    def resolve_for_files(
        self,
        file_inputs: list["FileInput"],
        mime_types_by_file_id: dict[str, str] | None = None,
        preferred_keys: list[str] | None = None,
    ) -> tuple[dict[str, list["FileInput"]], list["FileParserSelection"], list[str]]:
        from lib.parsers.contracts import FileParserSelection

        grouped: dict[str, list[FileInput]] = {}
        selections: list[FileParserSelection] = []
        warnings: list[str] = []

        preferred_rank = {key: idx for idx, key in enumerate(preferred_keys or [])}

        for file_input in file_inputs:
            mime_type: str | None = None
            if mime_types_by_file_id and file_input.file_id:
                mime_type = mime_types_by_file_id.get(file_input.file_id)

            ranked = self.support_for_file(file_input=file_input, mime_type=mime_type)
            if not ranked:
                warnings.append(f"No parsers registered for '{file_input.filename}'.")
                continue

            def _sort_key(item: "ParserSupportResult") -> tuple[int, float, int, int]:
                preferred_pos = preferred_rank.get(item.parser_key, 999)
                fallback_pos = self._fallback_order.get(item.parser_key, 99)
                return (
                    1 if item.supported else 0,
                    item.score,
                    -preferred_pos,
                    -fallback_pos,
                )

            best = sorted(ranked, key=_sort_key, reverse=True)[0]

            if not best.supported:
                warnings.append(
                    f"No parser strongly supports '{file_input.filename}', using '{best.parser_key}' fallback "
                    f"(score={best.score:.2f})."
                )

            grouped.setdefault(best.parser_key, []).append(file_input)
            selections.append(
                FileParserSelection(
                    file_id=file_input.file_id,
                    filename=file_input.filename,
                    parser_key=best.parser_key,
                    score=best.score,
                    reasons=best.reasons,
                )
            )

        return grouped, selections, warnings


#: Global parser registry — import this singleton everywhere.
ParserRegistry = _ParserRegistry()
