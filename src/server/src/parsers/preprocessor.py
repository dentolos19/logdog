from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from src.parsers.contracts import INGESTION_SCHEMA_VERSION, ClassificationResult, FileClassification, StructuralClass

MAX_SAMPLE_LINES = 30
MAX_SAMPLE_RECORDS = 5
MAX_LINE_LENGTH = 2000

SCHEMA_VERSION = "1.0.0"


class ColumnKind(str, Enum):
    BASELINE = "baseline"
    DETECTED = "detected"
    LLM_INFERRED = "llm_inferred"


class SqlType(str, Enum):
    TEXT = "TEXT"
    INTEGER = "INTEGER"
    REAL = "REAL"


class DetectedFormat(str, Enum):
    JSON_LINES = "json_lines"
    XML = "xml"
    CSV = "csv"
    SYSLOG = "syslog"
    APACHE_ACCESS = "apache_access"
    NGINX_ACCESS = "nginx_access"
    LOGFMT = "logfmt"
    KEY_VALUE = "key_value"
    PLAIN_TEXT = "plain_text"
    UNKNOWN = "unknown"


class SegmentationStrategy(str, Enum):
    PER_LINE = "per_line"
    PER_MULTILINE_CLUSTER = "per_multiline_cluster"
    PER_FILE = "per_file"
    MIXED = "mixed"


class InferredColumn(BaseModel):
    name: str
    sql_type: SqlType = SqlType.TEXT
    description: str = ""
    nullable: bool = True
    kind: ColumnKind = ColumnKind.DETECTED
    example_values: list[str] = Field(default_factory=list)


class SegmentationResult(BaseModel):
    strategy: SegmentationStrategy
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = ""


class FileObservation(BaseModel):
    filename: str
    line_count: int
    detected_format: DetectedFormat
    format_confidence: float = Field(ge=0.0, le=1.0)
    segmentation_hint: SegmentationStrategy
    sample_size: int
    warnings: list[str] = Field(default_factory=list)


class SampleRecord(BaseModel):
    source_file: str
    line_start: int
    line_end: int
    fields: dict[str, Any]


class FileInput(BaseModel):
    file_id: str | None = None
    filename: str
    content: str


SYSLOG_PATTERN = re.compile(
    r"^(<\d{1,3}>)?"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+"
    r"\S+\s+\S+",
    re.IGNORECASE,
)

APACHE_CLF_PATTERN = re.compile(
    r"^\S+\s+\S+\s+\S+\s+\[.+?\]\s+\".+?\"\s+\d{3}\s+\d+",
)

LOGFMT_PATTERN = re.compile(
    r"^(?:\w[\w.\-]*=(?:\"[^\"]*\"|\S+)\s*){2,}",
)

KEY_VALUE_PATTERN = re.compile(
    r"(?:\w[\w.\-]*\s*[:=]\s*(?:\"[^\"]*\"|\S+)\s*){2,}",
)

ISO_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
)

MULTILINE_CONTINUATION_PATTERN = re.compile(
    r"^(?:\s+at\s|Caused by:|\.{3}\s*\d+\s*more|\s{4,}\S|\t\S)",
)

LOG_LEVEL_PATTERN = re.compile(
    r"\b(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|NOTICE|ALERT|EMERG(?:ENCY)?)\b",
    re.IGNORECASE,
)


class LogPreprocessorService:
    def __init__(self, table_name: str = "logs") -> None:
        self.table_name = table_name

    def classify(self, files: list[FileInput]) -> ClassificationResult:
        warnings: list[str] = []
        file_classifications: list[FileClassification] = []
        observations: list[FileObservation] = []

        for file_input in files:
            lines = file_input.content.splitlines()
            if not lines:
                warnings.append(f"{file_input.filename}: file is empty, skipped.")
                file_classifications.append(
                    FileClassification(
                        file_id=file_input.file_id,
                        filename=file_input.filename,
                        detected_format=DetectedFormat.UNKNOWN.value,
                        structural_class=StructuralClass.UNSTRUCTURED,
                        format_confidence=0.0,
                        line_count=0,
                        warnings=["File is empty."],
                    )
                )
                continue

            detected_format, format_confidence = self._detect_format(lines)
            segmentation = self._detect_segmentation(lines, detected_format)
            structural_class = self._format_to_structural_class(detected_format, format_confidence)

            file_warnings: list[str] = []
            if format_confidence < 0.5:
                file_warnings.append(f"Low format confidence ({format_confidence:.2f}) results may be approximate.")

            observations.append(
                FileObservation(
                    filename=file_input.filename,
                    line_count=len(lines),
                    detected_format=detected_format,
                    format_confidence=format_confidence,
                    segmentation_hint=segmentation.strategy,
                    sample_size=min(len(lines), MAX_SAMPLE_LINES),
                    warnings=file_warnings,
                )
            )
            file_classifications.append(
                FileClassification(
                    file_id=file_input.file_id,
                    filename=file_input.filename,
                    detected_format=detected_format.value,
                    structural_class=structural_class,
                    format_confidence=format_confidence,
                    line_count=len(lines),
                    warnings=file_warnings,
                )
            )

        dominant_format = self._dominant_format(observations)
        structural_class_overall = self._dominant_structural_class(file_classifications)
        selected_parser_key = self._select_parser_key(structural_class_overall)
        confidence = self._compute_confidence(observations)

        return ClassificationResult(
            schema_version=INGESTION_SCHEMA_VERSION,
            dominant_format=dominant_format.value,
            structural_class=structural_class_overall,
            selected_parser_key=selected_parser_key,
            file_classifications=file_classifications,
            warnings=warnings,
            confidence=confidence,
        )

    @staticmethod
    def _format_to_structural_class(fmt: DetectedFormat, confidence: float) -> StructuralClass:
        if fmt in {
            DetectedFormat.JSON_LINES,
            DetectedFormat.XML,
            DetectedFormat.CSV,
            DetectedFormat.SYSLOG,
            DetectedFormat.APACHE_ACCESS,
            DetectedFormat.NGINX_ACCESS,
            DetectedFormat.LOGFMT,
        }:
            return StructuralClass.STRUCTURED
        if fmt == DetectedFormat.KEY_VALUE:
            return StructuralClass.STRUCTURED if confidence >= 0.6 else StructuralClass.SEMI_STRUCTURED
        return StructuralClass.UNSTRUCTURED

    @staticmethod
    def _dominant_structural_class(file_classifications: list[FileClassification]) -> StructuralClass:
        if not file_classifications:
            return StructuralClass.UNSTRUCTURED

        counts: dict[StructuralClass, int] = {}
        for file_classification in file_classifications:
            counts[file_classification.structural_class] = counts.get(file_classification.structural_class, 0) + 1

        return max(counts, key=lambda item: counts[item])

    @staticmethod
    def _select_parser_key(structural_class: StructuralClass) -> str:
        if structural_class == StructuralClass.STRUCTURED:
            return "structured"
        if structural_class == StructuralClass.SEMI_STRUCTURED:
            return "semi_structured"
        return "unstructured"

    def _detect_format(self, lines: list[str]) -> tuple[DetectedFormat, float]:
        sample = [line for line in lines[:50] if line.strip()]
        if not sample:
            return DetectedFormat.UNKNOWN, 0.0

        scores: dict[DetectedFormat, float] = {}

        json_hits = sum(1 for line in sample if self._is_json_object(line))
        if json_hits > 0:
            scores[DetectedFormat.JSON_LINES] = json_hits / len(sample)

        xml_score = self._score_xml(sample)
        if xml_score > 0:
            scores[DetectedFormat.XML] = xml_score

        csv_score = self._score_csv(sample)
        if csv_score > 0:
            scores[DetectedFormat.CSV] = csv_score

        syslog_hits = sum(1 for line in sample if SYSLOG_PATTERN.match(line))
        if syslog_hits > 0:
            scores[DetectedFormat.SYSLOG] = syslog_hits / len(sample)

        clf_hits = sum(1 for line in sample if APACHE_CLF_PATTERN.match(line))
        if clf_hits > 0:
            has_nginx_markers = any("upstream" in line.lower() or "nginx" in line.lower() for line in sample)
            if has_nginx_markers:
                scores[DetectedFormat.NGINX_ACCESS] = clf_hits / len(sample)
            else:
                scores[DetectedFormat.APACHE_ACCESS] = clf_hits / len(sample)

        logfmt_hits = sum(1 for line in sample if LOGFMT_PATTERN.match(line))
        if logfmt_hits > 0:
            scores[DetectedFormat.LOGFMT] = logfmt_hits / len(sample)

        kv_hits = sum(1 for line in sample if KEY_VALUE_PATTERN.search(line))
        if kv_hits > 0 and DetectedFormat.LOGFMT not in scores:
            scores[DetectedFormat.KEY_VALUE] = kv_hits / len(sample) * 0.8

        if not scores:
            return DetectedFormat.PLAIN_TEXT, 0.3

        best_format = max(scores, key=lambda key: scores[key])
        return best_format, round(min(scores[best_format], 1.0), 2)

    def _detect_segmentation(self, lines: list[str], detected_format: DetectedFormat) -> SegmentationResult:
        if detected_format == DetectedFormat.XML:
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_FILE,
                confidence=0.9,
                rationale="XML payload is parsed as a whole document before row extraction.",
            )

        if len(lines) <= 3 and detected_format in (DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN):
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_FILE,
                confidence=0.7,
                rationale="Very short unstructured file treated as one record.",
            )

        continuation_count = sum(1 for line in lines if MULTILINE_CONTINUATION_PATTERN.match(line))
        continuation_ratio = continuation_count / len(lines) if lines else 0.0
        if continuation_ratio > 0.15:
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_MULTILINE_CLUSTER,
                confidence=round(min(0.6 + continuation_ratio, 0.95), 2),
                rationale=f"{continuation_count}/{len(lines)} lines match multiline continuation patterns.",
            )

        return SegmentationResult(
            strategy=SegmentationStrategy.PER_LINE,
            confidence=0.9,
            rationale=f"Defaulted to line segmentation for format '{detected_format.value}'.",
        )

    @staticmethod
    def _compute_confidence(observations: list[FileObservation]) -> float:
        if not observations:
            return 0.0

        average_format_confidence = sum(observation.format_confidence for observation in observations) / len(
            observations
        )
        base = average_format_confidence * 0.7

        total_warnings = sum(len(observation.warnings) for observation in observations)
        if total_warnings > 0:
            base -= min(total_warnings * 0.05, 0.2)

        return round(max(0.0, min(base, 1.0)), 2)

    @staticmethod
    def _dominant_format(observations: list[FileObservation]) -> DetectedFormat:
        if not observations:
            return DetectedFormat.UNKNOWN

        format_counts: dict[DetectedFormat, int] = {}
        for observation in observations:
            format_counts[observation.detected_format] = format_counts.get(observation.detected_format, 0) + 1
        return max(format_counts, key=lambda detected_format: format_counts[detected_format])

    @staticmethod
    def _is_json_object(line: str) -> bool:
        stripped = line.strip()
        if not stripped.startswith("{"):
            return False
        try:
            parsed = json.loads(stripped)
            return isinstance(parsed, dict)
        except (json.JSONDecodeError, ValueError):
            return False

    @staticmethod
    def _score_xml(lines: list[str]) -> float:
        document = "\n".join(lines).strip()
        if not document.startswith("<"):
            return 0.0
        try:
            ET.fromstring(document)
            return 0.95
        except ET.ParseError:
            return 0.0

    @staticmethod
    def _score_csv(sample: list[str]) -> float:
        if len(sample) < 2:
            return 0.0

        header = sample[0]
        if "," not in header:
            return 0.0

        expected_columns = header.count(",") + 1
        if expected_columns < 2:
            return 0.0

        matching = 0
        data_sample = sample[1:10]
        for line in data_sample:
            actual_columns = line.count(",") + 1
            if actual_columns == expected_columns:
                matching += 1

        if not data_sample:
            return 0.0

        return matching / len(data_sample)
