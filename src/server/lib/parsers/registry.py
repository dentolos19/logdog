"""Parser pipeline registry.

New parser pipelines register themselves here. The orchestrator uses the
registry to dispatch classified file inputs to the appropriate pipeline.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.parsers.contracts import ClassificationResult, ParserPipelineResult
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


class _ParserRegistry:
    """Singleton registry mapping parser keys to pipeline instances."""

    def __init__(self) -> None:
        self._pipelines: dict[str, ParserPipeline] = {}

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


#: Global parser registry — import this singleton everywhere.
ParserRegistry = _ParserRegistry()
