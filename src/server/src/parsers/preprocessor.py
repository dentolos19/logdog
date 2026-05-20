from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel

from parsers.contracts import (
    INGESTION_SCHEMA_VERSION,
    ClassificationResult,
    FileClassification,
    StructuralClass,
)

logger = logging.getLogger(__name__)


class FileInput(BaseModel):
    file_id: str | None = None
    filename: str
    content: str


class LogPreprocessorService:
    """Generic file profiler — no format-specific detection.

    Every file is classified as ``ai_universal`` format and routed to
    the AI-driven parser.  Heuristic format detection and LLM-based
    classification have been removed; the AI parser handles all formats.
    """

    def __init__(
        self,
        table_name: str = "logs",
        use_llm: bool = True,
        profile_name: str | None = None,
        schema_cache: Any = None,
        few_shot_store: Any = None,
    ) -> None:
        self.table_name = table_name
        self.use_llm = use_llm
        self.profile_name = (profile_name or "default").strip() or "default"

    def classify(self, files: list[FileInput]) -> ClassificationResult:
        """Classify all files as universal AI-parsable format."""
        file_classifications: list[FileClassification] = []
        diagnostics: dict[str, Any] = {
            "mode": "generic",
            "parser": "universal_ai",
            "files": [],
        }

        for file_input in files:
            lines = file_input.content.splitlines()
            if not lines:
                file_classifications.append(
                    FileClassification(
                        file_id=file_input.file_id,
                        filename=file_input.filename,
                        detected_format="ai_universal",
                        structural_class=StructuralClass.UNSTRUCTURED,
                        format_confidence=0.0,
                        line_count=0,
                        warnings=["File is empty."],
                    )
                )
                diagnostics["files"].append({
                    "filename": file_input.filename,
                    "detected_format": "ai_universal",
                    "format_confidence": 0.0,
                    "line_count": 0,
                    "empty": True,
                })
                continue

            # Profile the file (basic info, no format detection)
            non_empty = [line for line in lines if line.strip()]
            line_count = len(lines)
            non_empty_count = len(non_empty)

            file_classifications.append(
                FileClassification(
                    file_id=file_input.file_id,
                    filename=file_input.filename,
                    detected_format="ai_universal",
                    structural_class=StructuralClass.UNSTRUCTURED,
                    format_confidence=0.5,
                    line_count=line_count,
                )
            )
            diagnostics["files"].append({
                "filename": file_input.filename,
                "detected_format": "ai_universal",
                "format_confidence": 0.5,
                "line_count": line_count,
                "non_empty_lines": non_empty_count,
            })

        dominant_format = "ai_universal"
        structural_class_overall = StructuralClass.UNSTRUCTURED
        selected_parser_key = "universal_ai"
        confidence = self._compute_confidence(file_classifications)

        return ClassificationResult(
            schema_version=INGESTION_SCHEMA_VERSION,
            dominant_format=dominant_format,
            structural_class=structural_class_overall,
            selected_parser_key=selected_parser_key,
            file_classifications=file_classifications,
            warnings=[],
            confidence=confidence,
            diagnostics=diagnostics,
        )

    def classify_with_llm(self, files: list[FileInput]) -> ClassificationResult:
        """Compatibility alias — delegates to *classify()*.

        LLM-assisted format classification has been removed in favor of
        the universal AI parser which handles format detection internally.
        """
        return self.classify(files)

    @staticmethod
    def _compute_confidence(classifications: list[FileClassification]) -> float:
        if not classifications:
            return 0.0
        return sum(fc.format_confidence for fc in classifications) / len(classifications)
