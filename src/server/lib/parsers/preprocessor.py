import json
import logging
import os
import re
from enum import Enum
from typing import Any

from langchain_openrouter import ChatOpenRouter
from lib.parsers.contracts import (
    INGESTION_SCHEMA_VERSION,
    ClassificationResult,
    FileClassification,
    StructuralClass,
)
from pydantic import BaseModel, Field, SecretStr

# Lazy import to avoid circular dependency at module level.
_unstructured_parser = None


def _get_unstructured_parser():
    global _unstructured_parser
    if _unstructured_parser is None:
        from lib.parsers import unstructured_parser as _up

        _unstructured_parser = _up
    return _unstructured_parser


logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "inception/mercury-2")

# Limits for the sample lines sent to the LLM.
MAX_SAMPLE_LINES = 30
MAX_SAMPLE_RECORDS = 5
MAX_LINE_LENGTH = 2000

SCHEMA_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Pydantic Result Models
# ---------------------------------------------------------------------------


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


class GeneratedTable(BaseModel):
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumn]
    is_normalized: bool = False
    file_id: str | None = None
    file_name: str | None = None


class PreprocessorResult(BaseModel):
    schema_summary: str
    schema_version: str = SCHEMA_VERSION
    table_name: str
    sqlite_ddl: str
    columns: list[InferredColumn]
    generated_tables: list[GeneratedTable] = Field(default_factory=list)
    segmentation: SegmentationResult
    sample_records: list[SampleRecord]
    file_observations: list[FileObservation]
    warnings: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# LLM Response Schemas (for structured output)
# ---------------------------------------------------------------------------


class LlmColumnSuggestion(BaseModel):
    """A single column suggested or enriched by the LLM."""

    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True


class LlmSchemaResponse(BaseModel):
    """Structured response from the LLM for schema inference."""

    columns: list[LlmColumnSuggestion] = Field(default_factory=list)
    summary: str = ""
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex patterns for format detection
# ---------------------------------------------------------------------------

# Syslog RFC 3164: "<priority>Mon DD HH:MM:SS hostname process[pid]: message"
# or without priority: "Mon DD HH:MM:SS hostname process[pid]: message"
SYSLOG_PATTERN = re.compile(
    r"^(<\d{1,3}>)?"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+"
    r"\S+\s+\S+",
    re.IGNORECASE,
)

# Apache/Nginx Combined Log Format:
# 127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
APACHE_CLF_PATTERN = re.compile(
    r"^\S+\s+\S+\s+\S+\s+\[.+?\]\s+\".+?\"\s+\d{3}\s+\d+",
)

# Logfmt: key=value key2=value2 key3="quoted value"
LOGFMT_PATTERN = re.compile(
    r"^(?:\w[\w.\-]*=(?:\"[^\"]*\"|\S+)\s*){2,}",
)

# Generic key=value pairs (less strict than logfmt).
KEY_VALUE_PATTERN = re.compile(
    r"(?:\w[\w.\-]*\s*[:=]\s*(?:\"[^\"]*\"|\S+)\s*){2,}",
)

# ISO-8601 timestamp at start of line (common in structured logs).
ISO_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
)

# Multiline continuation indicators.
MULTILINE_CONTINUATION_PATTERN = re.compile(
    r"^(?:\s+at\s|Caused by:|\.{3}\s*\d+\s*more|\s{4,}\S|\t\S)",
)

# Common log level tokens.
LOG_LEVEL_PATTERN = re.compile(
    r"\b(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|NOTICE|ALERT|EMERG(?:ENCY)?)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class LogPreprocessorService:
    """Inspects raw log files and infers the target tabular schema.

    Uses deterministic heuristics first, then optionally calls an LLM via
    OpenRouter to enrich column descriptions and discover semantic fields.
    """

    def __init__(self, table_name: str = "logs") -> None:
        self.table_name = table_name
        self._llm_available = bool(OPENROUTER_API_KEY)

    # ------------------------------------------------------------------
    # Public API — new classification-only path
    # ------------------------------------------------------------------

    def classify(self, files: list[FileInput]) -> ClassificationResult:
        """Classify files and return routing metadata without schema generation.

        This is the entry point for the new async ingestion pipeline.  It runs
        quickly (no LLM calls) and returns enough information for the background
        worker to route files to the appropriate parser pipeline.
        """
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
                file_warnings.append(f"Low format confidence ({format_confidence:.2f}) — results may be approximate.")

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
        confidence = self._compute_confidence(observations, False)

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
        """Map a detected format (and its confidence) to a structural class."""
        if fmt in (
            DetectedFormat.JSON_LINES,
            DetectedFormat.CSV,
            DetectedFormat.SYSLOG,
            DetectedFormat.APACHE_ACCESS,
            DetectedFormat.NGINX_ACCESS,
            DetectedFormat.LOGFMT,
        ):
            return StructuralClass.STRUCTURED
        if fmt == DetectedFormat.KEY_VALUE:
            return StructuralClass.STRUCTURED if confidence >= 0.6 else StructuralClass.SEMI_STRUCTURED
        # PLAIN_TEXT, UNKNOWN
        return StructuralClass.UNSTRUCTURED

    @staticmethod
    def _dominant_structural_class(file_classifications: list[FileClassification]) -> StructuralClass:
        """Return the most common structural class across files."""
        if not file_classifications:
            return StructuralClass.UNSTRUCTURED
        from collections import Counter

        counts: Counter[StructuralClass] = Counter(fc.structural_class for fc in file_classifications)
        return counts.most_common(1)[0][0]

    @staticmethod
    def _select_parser_key(structural_class: StructuralClass) -> str:
        """Map structural class to a registered parser pipeline key."""
        if structural_class == StructuralClass.STRUCTURED:
            return "structured"
        if structural_class == StructuralClass.SEMI_STRUCTURED:
            return "semi_structured"
        return "unstructured"

    # ------------------------------------------------------------------
    # Public API — legacy full-preprocess path (kept for backward compat)
    # ------------------------------------------------------------------

    def preprocess(self, files: list[FileInput]) -> PreprocessorResult:
        """Orchestrate the full preprocessing pipeline."""

        warnings: list[str] = []
        assumptions: list[str] = []
        all_observations: list[FileObservation] = []
        per_file_columns: list[tuple[FileInput, list[InferredColumn]]] = []
        all_sample_lines: list[str] = []

        # Phase 1: Per-file heuristic analysis.
        for file_input in files:
            lines = file_input.content.splitlines()
            if not lines:
                warnings.append(f"{file_input.filename}: file is empty, skipped.")
                all_observations.append(
                    FileObservation(
                        filename=file_input.filename,
                        line_count=0,
                        detected_format=DetectedFormat.UNKNOWN,
                        format_confidence=0.0,
                        segmentation_hint=SegmentationStrategy.PER_FILE,
                        sample_size=0,
                        warnings=["File is empty."],
                    )
                )
                per_file_columns.append((file_input, []))
                continue

            detected_format, format_confidence = self._detect_format(lines)
            segmentation = self._detect_segmentation(lines, detected_format)
            heuristic_columns = self._extract_heuristic_columns(lines, detected_format, format_confidence)

            # When the unstructured parser was used for a KV-classified file,
            # the domain-aware extraction is more reliable than the raw KV
            # score suggests.  Boost format_confidence based on how many
            # domain columns were discovered.
            if detected_format == DetectedFormat.KEY_VALUE and format_confidence < 0.6:
                domain_col_count = sum(
                    1
                    for col in heuristic_columns
                    if col.name
                    in (
                        "wafer_id",
                        "tool_id",
                        "recipe_id",
                        "process_step",
                        "template",
                        "template_cluster_id",
                    )
                )
                if domain_col_count >= 2:
                    format_confidence = max(format_confidence, 0.65)

            file_warnings: list[str] = []
            if format_confidence < 0.5:
                file_warnings.append(f"Low format confidence ({format_confidence:.2f}) — results may be approximate.")

            all_observations.append(
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
            per_file_columns.append((file_input, heuristic_columns))

            # Collect sample lines for LLM (first N non-empty lines, truncated).
            for line in lines[:MAX_SAMPLE_LINES]:
                trimmed = line[:MAX_LINE_LENGTH]
                if trimmed.strip():
                    all_sample_lines.append(trimmed)

        if not per_file_columns:
            # All files were empty.
            assumptions.append("All input files were empty; returning baseline schema only.")
            per_file_columns.append((FileInput(filename="unknown", content=""), []))

        # Phase 2: Merge columns across files + add baseline.
        baseline_columns = self._build_baseline_columns()
        merged_heuristic = self._merge_columns([columns for _, columns in per_file_columns])

        # Phase 3: LLM enrichment (optional).
        dominant_format = self._dominant_format(all_observations)
        llm_columns: list[InferredColumn] = []
        schema_summary = ""

        if self._llm_available and all_sample_lines:
            try:
                llm_result = self._call_llm_for_schema(
                    detected_format=dominant_format,
                    sample_lines=all_sample_lines[:MAX_SAMPLE_LINES],
                    heuristic_columns=merged_heuristic,
                )
                llm_columns = llm_result["columns"]
                schema_summary = llm_result["summary"]
                warnings.extend(llm_result.get("warnings", []))
            except Exception as error:
                logger.warning("LLM schema inference failed, continuing with heuristics only: %s", error)
                warnings.append(f"LLM enrichment failed ({type(error).__name__}); using heuristic-only schema.")
        else:
            if not self._llm_available:
                warnings.append("OPENROUTER_API_KEY not set; LLM enrichment skipped.")

        # Phase 4: Final column list = baseline + heuristic + LLM-inferred.
        final_columns = self._reconcile_columns(baseline_columns, merged_heuristic, llm_columns)

        # Phase 5: Generate DDL, samples, and summary.
        sqlite_ddl = self._generate_ddl(self.table_name, final_columns)

        generated_tables = [
            GeneratedTable(
                table_name=self.table_name,
                sqlite_ddl=sqlite_ddl,
                columns=final_columns,
                is_normalized=True,
            )
        ]

        for file_input, heuristic_columns in per_file_columns:
            file_llm_columns = [
                column for column in llm_columns if column.name in {item.name for item in heuristic_columns}
            ]
            file_columns = self._reconcile_columns(baseline_columns, heuristic_columns, file_llm_columns)
            file_table_name = self._build_file_table_name(file_input)
            generated_tables.append(
                GeneratedTable(
                    table_name=file_table_name,
                    sqlite_ddl=self._generate_ddl(file_table_name, file_columns),
                    columns=file_columns,
                    file_id=file_input.file_id,
                    file_name=file_input.filename,
                )
            )

        overall_segmentation = self._overall_segmentation(all_observations)

        sample_records = self._extract_samples(
            files=files,
            segmentation=overall_segmentation,
            columns=final_columns,
        )

        if not schema_summary:
            schema_summary = self._generate_heuristic_summary(final_columns, all_observations)

        overall_confidence = self._compute_confidence(all_observations, bool(llm_columns))

        return PreprocessorResult(
            schema_summary=schema_summary,
            schema_version=SCHEMA_VERSION,
            table_name=self.table_name,
            sqlite_ddl=sqlite_ddl,
            columns=final_columns,
            generated_tables=generated_tables,
            segmentation=overall_segmentation,
            sample_records=sample_records,
            file_observations=all_observations,
            warnings=warnings,
            assumptions=assumptions,
            confidence=overall_confidence,
        )

    # ------------------------------------------------------------------
    # Format Detection
    # ------------------------------------------------------------------

    def _detect_format(self, lines: list[str]) -> tuple[DetectedFormat, float]:
        """Detect the log format using regex heuristics on the first N non-empty lines."""

        sample = [line for line in lines[:50] if line.strip()]
        if not sample:
            return DetectedFormat.UNKNOWN, 0.0

        scores: dict[DetectedFormat, float] = {}

        # JSON lines: each line is a valid JSON object.
        json_hits = sum(1 for line in sample if self._is_json_object(line))
        if json_hits > 0:
            scores[DetectedFormat.JSON_LINES] = json_hits / len(sample)

        # CSV: first line looks like a header with commas, subsequent lines match column count.
        csv_score = self._score_csv(sample)
        if csv_score > 0:
            scores[DetectedFormat.CSV] = csv_score

        # Syslog.
        syslog_hits = sum(1 for line in sample if SYSLOG_PATTERN.match(line))
        if syslog_hits > 0:
            scores[DetectedFormat.SYSLOG] = syslog_hits / len(sample)

        # Apache / Nginx CLF.
        clf_hits = sum(1 for line in sample if APACHE_CLF_PATTERN.match(line))
        if clf_hits > 0:
            # Distinguish Apache vs Nginx by looking for upstream indicators.
            has_nginx_markers = any("upstream" in line.lower() or "nginx" in line.lower() for line in sample)
            if has_nginx_markers:
                scores[DetectedFormat.NGINX_ACCESS] = clf_hits / len(sample)
            else:
                scores[DetectedFormat.APACHE_ACCESS] = clf_hits / len(sample)

        # Logfmt.
        logfmt_hits = sum(1 for line in sample if LOGFMT_PATTERN.match(line))
        if logfmt_hits > 0:
            scores[DetectedFormat.LOGFMT] = logfmt_hits / len(sample)

        # Generic key=value.
        kv_hits = sum(1 for line in sample if KEY_VALUE_PATTERN.search(line))
        if kv_hits > 0 and DetectedFormat.LOGFMT not in scores:
            scores[DetectedFormat.KEY_VALUE] = kv_hits / len(sample) * 0.8

        if not scores:
            return DetectedFormat.PLAIN_TEXT, 0.3

        best_format = max(scores, key=lambda key: scores[key])
        return best_format, round(min(scores[best_format], 1.0), 2)

    # ------------------------------------------------------------------
    # Segmentation Detection
    # ------------------------------------------------------------------

    def _detect_segmentation(self, lines: list[str], detected_format: DetectedFormat) -> SegmentationResult:
        """Decide how records should be grouped from lines."""

        # Whole-file as a single record when file is very short or unstructured.
        if len(lines) <= 3 and detected_format in (DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN):
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_FILE,
                confidence=0.7,
                rationale="Very short file with unstructured content treated as a single record.",
            )

        # Check for multiline continuation patterns (stack traces, indented continuations).
        continuation_count = sum(1 for line in lines if MULTILINE_CONTINUATION_PATTERN.match(line))
        continuation_ratio = continuation_count / len(lines) if lines else 0.0

        if continuation_ratio > 0.15:
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_MULTILINE_CLUSTER,
                confidence=round(min(0.6 + continuation_ratio, 0.95), 2),
                rationale=f"{continuation_count}/{len(lines)} lines match multiline continuation patterns.",
            )

        # Per-line for most structured formats.
        if detected_format in (
            DetectedFormat.JSON_LINES,
            DetectedFormat.CSV,
            DetectedFormat.SYSLOG,
            DetectedFormat.APACHE_ACCESS,
            DetectedFormat.NGINX_ACCESS,
            DetectedFormat.LOGFMT,
            DetectedFormat.KEY_VALUE,
        ):
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_LINE,
                confidence=0.9,
                rationale=f"Structured format ({detected_format.value}) with one record per line.",
            )

        return SegmentationResult(
            strategy=SegmentationStrategy.PER_LINE,
            confidence=0.5,
            rationale="Defaulting to per-line segmentation for unstructured or unknown format.",
        )

    # ------------------------------------------------------------------
    # Heuristic Column Extraction
    # ------------------------------------------------------------------

    def _extract_heuristic_columns(
        self,
        lines: list[str],
        detected_format: DetectedFormat,
        format_confidence: float = 1.0,
    ) -> list[InferredColumn]:
        """Derive columns from the detected format."""

        if detected_format == DetectedFormat.JSON_LINES:
            return self._columns_from_json(lines)
        if detected_format == DetectedFormat.CSV:
            return self._columns_from_csv(lines)
        if detected_format in (DetectedFormat.SYSLOG,):
            return self._columns_from_syslog()
        if detected_format in (DetectedFormat.APACHE_ACCESS, DetectedFormat.NGINX_ACCESS):
            return self._columns_from_clf()
        if detected_format in (DetectedFormat.LOGFMT, DetectedFormat.KEY_VALUE):
            # Low-confidence KV often means mixed prose with embedded key=value
            # pairs (e.g., semiconductor fab logs). Run the unstructured parser
            # which has domain-aware extraction and compare results.
            if format_confidence < 0.6:
                kv_columns = self._columns_from_logfmt(lines)
                unstructured_columns = _get_unstructured_parser().extract_unstructured_columns(lines)
                if len(unstructured_columns) >= len(kv_columns):
                    return unstructured_columns
                return kv_columns
            return self._columns_from_logfmt(lines)
        if detected_format in (DetectedFormat.PLAIN_TEXT, DetectedFormat.UNKNOWN):
            return _get_unstructured_parser().extract_unstructured_columns(lines)
        return []

    def _columns_from_json(self, lines: list[str]) -> list[InferredColumn]:
        """Extract column names from up to 20 JSON lines."""

        all_keys: dict[str, set[str]] = {}  # key -> set of example values
        for line in lines[:20]:
            try:
                parsed = json.loads(line.strip())
            except (json.JSONDecodeError, ValueError):
                continue

            if not isinstance(parsed, dict):
                continue

            for key, value in parsed.items():
                safe_key = self._sanitize_column_name(key)
                if safe_key not in all_keys:
                    all_keys[safe_key] = set()
                if value is not None:
                    example_str = str(value)[:100]
                    if len(all_keys[safe_key]) < 3:
                        all_keys[safe_key].add(example_str)

        return [
            InferredColumn(
                name=key,
                sql_type=SqlType.TEXT,
                description=f"Extracted from JSON field '{key}'.",
                nullable=True,
                kind=ColumnKind.DETECTED,
                example_values=list(examples),
            )
            for key, examples in all_keys.items()
        ]

    def _columns_from_csv(self, lines: list[str]) -> list[InferredColumn]:
        """Extract column names from a CSV header row."""

        if not lines:
            return []

        header = lines[0].strip()
        column_names = [
            self._sanitize_column_name(header_column.strip().strip('"')) for header_column in header.split(",")
        ]

        columns: list[InferredColumn] = []
        # Collect example values from the first few data rows.
        data_rows = lines[1:6]
        for index, column_name in enumerate(column_names):
            examples: list[str] = []
            for row in data_rows:
                parts = row.split(",")
                if index < len(parts):
                    value = parts[index].strip().strip('"')
                    if value and len(examples) < 3:
                        examples.append(value[:100])

            columns.append(
                InferredColumn(
                    name=column_name,
                    sql_type=SqlType.TEXT,
                    description=f"CSV column '{column_name}'.",
                    nullable=True,
                    kind=ColumnKind.DETECTED,
                    example_values=examples,
                )
            )

        return columns

    def _columns_from_syslog(self) -> list[InferredColumn]:
        """Standard syslog RFC 3164 columns."""

        return [
            InferredColumn(
                name="priority",
                sql_type=SqlType.INTEGER,
                description="Syslog priority value.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="facility", sql_type=SqlType.TEXT, description="Syslog facility name.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="severity", sql_type=SqlType.TEXT, description="Syslog severity level.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="hostname", sql_type=SqlType.TEXT, description="Originating hostname.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="process_name",
                sql_type=SqlType.TEXT,
                description="Name of the process that generated the log.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(name="pid", sql_type=SqlType.INTEGER, description="Process ID.", kind=ColumnKind.DETECTED),
        ]

    def _columns_from_clf(self) -> list[InferredColumn]:
        """Apache/Nginx Combined Log Format columns."""

        return [
            InferredColumn(
                name="remote_host", sql_type=SqlType.TEXT, description="Client IP address.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="ident",
                sql_type=SqlType.TEXT,
                description="RFC 1413 identity (usually '-').",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="auth_user",
                sql_type=SqlType.TEXT,
                description="Authenticated user name.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="request_method",
                sql_type=SqlType.TEXT,
                description="HTTP method (GET, POST, etc.).",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="request_path", sql_type=SqlType.TEXT, description="Requested URI path.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="request_protocol",
                sql_type=SqlType.TEXT,
                description="HTTP protocol version.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="status_code",
                sql_type=SqlType.INTEGER,
                description="HTTP response status code.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="response_size",
                sql_type=SqlType.INTEGER,
                description="Response body size in bytes.",
                kind=ColumnKind.DETECTED,
            ),
            InferredColumn(
                name="referer", sql_type=SqlType.TEXT, description="HTTP Referer header.", kind=ColumnKind.DETECTED
            ),
            InferredColumn(
                name="user_agent",
                sql_type=SqlType.TEXT,
                description="Client User-Agent string.",
                kind=ColumnKind.DETECTED,
            ),
        ]

    def _columns_from_logfmt(self, lines: list[str]) -> list[InferredColumn]:
        """Extract key names from logfmt / key=value lines."""

        all_keys: dict[str, set[str]] = {}
        for line in lines[:20]:
            for match in re.finditer(r"(\w[\w.\-]*)=(?:\"([^\"]*)\"|(\S+))", line):
                key = self._sanitize_column_name(match.group(1))
                value = match.group(2) if match.group(2) is not None else match.group(3)
                if key not in all_keys:
                    all_keys[key] = set()
                if value and len(all_keys[key]) < 3:
                    all_keys[key].add(value[:100])

        return [
            InferredColumn(
                name=key,
                sql_type=SqlType.TEXT,
                description=f"Logfmt field '{key}'.",
                nullable=True,
                kind=ColumnKind.DETECTED,
                example_values=list(examples),
            )
            for key, examples in all_keys.items()
        ]

    # ------------------------------------------------------------------
    # Baseline Columns
    # ------------------------------------------------------------------

    def _build_baseline_columns(self) -> list[InferredColumn]:
        """The 15 required columns that are always present in every schema."""

        return [
            InferredColumn(
                name="id",
                sql_type=SqlType.INTEGER,
                description="Auto-incrementing primary key for each parsed record.",
                nullable=False,
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="timestamp",
                sql_type=SqlType.TEXT,
                description="Normalized ISO-8601 timestamp of the log event, if detectable.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="timestamp_raw",
                sql_type=SqlType.TEXT,
                description="Original timestamp string exactly as it appeared in the log.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="source",
                sql_type=SqlType.TEXT,
                description="Identifier for the source of the log (e.g., filename, hostname, service name).",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="source_type",
                sql_type=SqlType.TEXT,
                description="Category of the source (e.g., 'file', 'stream', 'api').",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="log_level",
                sql_type=SqlType.TEXT,
                description="Severity level of the log entry (e.g., INFO, WARN, ERROR).",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="event_type",
                sql_type=SqlType.TEXT,
                description="Classified type of the event (e.g., 'request', 'error', 'metric').",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="message",
                sql_type=SqlType.TEXT,
                description="Primary human-readable message content of the log entry.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="raw_text",
                sql_type=SqlType.TEXT,
                description="Complete original text of the log record, preserved for traceability.",
                nullable=False,
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="record_group_id",
                sql_type=SqlType.TEXT,
                description="Identifier linking related records from the same multiline cluster or transaction.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="line_start",
                sql_type=SqlType.INTEGER,
                description="1-based line number where this record starts in the source file.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="line_end",
                sql_type=SqlType.INTEGER,
                description="1-based line number where this record ends in the source file.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="parse_confidence",
                sql_type=SqlType.REAL,
                description="Confidence score (0.0-1.0) of how accurately this record was parsed.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="schema_version",
                sql_type=SqlType.TEXT,
                description="Version of the schema used to parse this record.",
                kind=ColumnKind.BASELINE,
            ),
            InferredColumn(
                name="additional_data",
                sql_type=SqlType.TEXT,
                description="JSON object containing any extra fields that did not map to named columns.",
                kind=ColumnKind.BASELINE,
            ),
        ]

    # ------------------------------------------------------------------
    # Column Merging & Reconciliation
    # ------------------------------------------------------------------

    def _merge_columns(self, per_file_columns: list[list[InferredColumn]]) -> list[InferredColumn]:
        """Union columns from multiple files into a single list, preserving descriptions and examples."""

        merged: dict[str, InferredColumn] = {}

        for column_list in per_file_columns:
            for column in column_list:
                if column.name in merged:
                    existing = merged[column.name]
                    # Merge example values (cap at 5).
                    combined_examples = list(set(existing.example_values + column.example_values))[:5]
                    merged[column.name] = existing.model_copy(update={"example_values": combined_examples})
                else:
                    merged[column.name] = column

        return list(merged.values())

    def _reconcile_columns(
        self,
        baseline: list[InferredColumn],
        heuristic: list[InferredColumn],
        llm_inferred: list[InferredColumn],
    ) -> list[InferredColumn]:
        """Combine baseline, heuristic, and LLM-inferred columns into a final ordered list.

        Baseline columns come first. Heuristic columns that overlap with baseline
        names are skipped (their descriptions may enrich the baseline). LLM columns
        add any genuinely new fields.
        """

        baseline_names = {column.name for column in baseline}
        final: dict[str, InferredColumn] = {column.name: column for column in baseline}

        # Enrich baseline descriptions from heuristic / LLM if they are more specific.
        for column in heuristic:
            if column.name in baseline_names:
                existing = final[column.name]
                if column.example_values and not existing.example_values:
                    final[column.name] = existing.model_copy(update={"example_values": column.example_values})
            else:
                final[column.name] = column

        for column in llm_inferred:
            if column.name not in final:
                final[column.name] = column.model_copy(update={"kind": ColumnKind.LLM_INFERRED})
            else:
                # LLM may provide better descriptions.
                existing = final[column.name]
                if column.description and len(column.description) > len(existing.description):
                    final[column.name] = existing.model_copy(update={"description": column.description})

        return list(final.values())

    # ------------------------------------------------------------------
    # DDL Generation
    # ------------------------------------------------------------------

    def _generate_ddl(self, table_name: str, columns: list[InferredColumn]) -> str:
        """Generate a valid SQLite CREATE TABLE IF NOT EXISTS statement."""

        safe_table = self._quote_identifier(table_name)
        column_defs: list[str] = []

        for column in columns:
            safe_name = self._quote_identifier(column.name)
            parts = [safe_name, column.sql_type.value]

            if column.name == "id":
                parts.append("PRIMARY KEY AUTOINCREMENT")
            if not column.nullable:
                parts.append("NOT NULL")

            column_defs.append("    " + " ".join(parts))

        columns_sql = ",\n".join(column_defs)
        return f"CREATE TABLE IF NOT EXISTS {safe_table} (\n{columns_sql}\n);"

    def _build_file_table_name(self, file_input: FileInput) -> str:
        if file_input.file_id is not None and file_input.file_id != "":
            return f"{self.table_name}_{file_input.file_id}"

        fallback_suffix = self._sanitize_column_name(file_input.filename) or "file"
        return f"{self.table_name}_{fallback_suffix}"

    # ------------------------------------------------------------------
    # Sample Record Extraction
    # ------------------------------------------------------------------

    def _extract_samples(
        self,
        files: list[FileInput],
        segmentation: SegmentationResult,
        columns: list[InferredColumn],
        max_samples: int = MAX_SAMPLE_RECORDS,
    ) -> list[SampleRecord]:
        """Extract a few flattened sample records from the input files."""

        column_names = {column.name for column in columns}
        samples: list[SampleRecord] = []

        for file_input in files:
            lines = file_input.content.splitlines()
            if not lines:
                continue

            if segmentation.strategy == SegmentationStrategy.PER_FILE:
                record_fields = self._flatten_record("\n".join(lines), file_input.filename, column_names)
                samples.append(
                    SampleRecord(
                        source_file=file_input.filename,
                        line_start=1,
                        line_end=len(lines),
                        fields=record_fields,
                    )
                )
            elif segmentation.strategy == SegmentationStrategy.PER_MULTILINE_CLUSTER:
                clusters = self._segment_multiline(lines)
                for start_line, end_line, cluster_text in clusters[: max(1, max_samples // max(len(files), 1))]:
                    record_fields = self._flatten_record(cluster_text, file_input.filename, column_names)
                    samples.append(
                        SampleRecord(
                            source_file=file_input.filename,
                            line_start=start_line,
                            line_end=end_line,
                            fields=record_fields,
                        )
                    )
            else:
                # Per-line.
                for index, line in enumerate(lines[: max(1, max_samples // max(len(files), 1))]):
                    if not line.strip():
                        continue
                    record_fields = self._flatten_record(line, file_input.filename, column_names)
                    samples.append(
                        SampleRecord(
                            source_file=file_input.filename,
                            line_start=index + 1,
                            line_end=index + 1,
                            fields=record_fields,
                        )
                    )

            if len(samples) >= max_samples:
                break

        return samples[:max_samples]

    def _flatten_record(self, raw_text: str, filename: str, column_names: set[str]) -> dict[str, Any]:
        """Build a flat key-value dict for one sample record."""

        fields: dict[str, Any] = {
            "raw_text": raw_text,
            "source": filename,
            "source_type": "file",
        }

        # Try to extract timestamp.
        iso_match = ISO_TIMESTAMP_PATTERN.search(raw_text)
        if iso_match:
            fields["timestamp_raw"] = iso_match.group(0)
            fields["timestamp"] = iso_match.group(0)

        # Try to extract log level.
        level_match = LOG_LEVEL_PATTERN.search(raw_text)
        if level_match:
            fields["log_level"] = level_match.group(1).upper()

        # Try JSON parse for structured content.
        stripped = raw_text.strip()
        if stripped.startswith("{"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, dict):
                    for key, value in parsed.items():
                        safe_key = self._sanitize_column_name(key)
                        if safe_key in column_names:
                            fields[safe_key] = value
                        else:
                            # Store in additional_data.
                            additional = json.loads(fields.get("additional_data", "{}"))
                            additional[key] = value
                            fields["additional_data"] = json.dumps(additional)
            except (json.JSONDecodeError, ValueError):
                pass
        else:
            # For non-JSON text, use unstructured field extraction.
            up = _get_unstructured_parser()
            extra = up.extract_fields_heuristic(raw_text)
            for key, value in extra.items():
                if key in column_names and key not in fields:
                    fields[key] = value

        # If we could not extract a message, use the full text (trimmed).
        if "message" not in fields:
            fields["message"] = raw_text.strip()[:500]

        return fields

    def _segment_multiline(self, lines: list[str]) -> list[tuple[int, int, str]]:
        """Group lines into multiline clusters. Returns (start_line_1based, end_line_1based, text)."""

        clusters: list[tuple[int, int, str]] = []
        current_start = 0
        current_lines: list[str] = []

        for index, line in enumerate(lines):
            if MULTILINE_CONTINUATION_PATTERN.match(line) and current_lines:
                # Continuation of the current cluster.
                current_lines.append(line)
            else:
                # Flush previous cluster.
                if current_lines:
                    clusters.append(
                        (
                            current_start + 1,
                            current_start + len(current_lines),
                            "\n".join(current_lines),
                        )
                    )
                current_start = index
                current_lines = [line]

        # Flush final cluster.
        if current_lines:
            clusters.append(
                (
                    current_start + 1,
                    current_start + len(current_lines),
                    "\n".join(current_lines),
                )
            )

        return clusters

    # ------------------------------------------------------------------
    # LLM Integration
    # ------------------------------------------------------------------

    def _call_llm_for_schema(
        self,
        detected_format: DetectedFormat,
        sample_lines: list[str],
        heuristic_columns: list[InferredColumn],
    ) -> dict[str, Any]:
        """Call the LLM to enrich the schema with semantic understanding."""

        model = ChatOpenRouter(
            model=OPENROUTER_MODEL,
            api_key=SecretStr(OPENROUTER_API_KEY),
            temperature=0.0,
            max_tokens=4096,
            app_title="NAISC",
            app_url="https://naisc.dennise.me",
        )

        structured_model = model.with_structured_output(LlmSchemaResponse, method="json_schema", strict=True)

        heuristic_summary = (
            "\n".join(
                f"  - {column.name} ({column.sql_type.value}): {column.description}" for column in heuristic_columns
            )
            or "  (none detected)"
        )

        sample_text = "\n".join(sample_lines[:MAX_SAMPLE_LINES])

        system_prompt = (
            "You are an expert log analyst and database schema designer. "
            "Your task is to analyze raw log samples and propose a flat tabular schema "
            "suitable for a SQLite table. Focus on:\n"
            "1. Confirming or correcting the heuristic-detected columns.\n"
            "2. Adding any new columns justified by patterns in the log data.\n"
            "3. Writing a clear, LLM-friendly description for each column so that "
            "a downstream LLM parser can correctly extract values.\n"
            "4. Providing a brief summary of the schema.\n\n"
            "Rules:\n"
            "- Column names must be lowercase snake_case, valid SQLite identifiers.\n"
            "- Only add columns that are well-justified by the sample data.\n"
            "- sql_type must be one of: TEXT, INTEGER, REAL.\n"
            "- Do NOT include baseline columns (id, timestamp, timestamp_raw, source, "
            "source_type, log_level, event_type, message, raw_text, record_group_id, "
            "line_start, line_end, parse_confidence, schema_version, additional_data) — "
            "those are always present and managed separately.\n"
        )

        user_prompt = (
            f"Detected format: {detected_format.value}\n\n"
            f"Heuristic-detected columns:\n{heuristic_summary}\n\n"
            f"Sample log lines:\n```\n{sample_text}\n```\n\n"
            "Please analyze these samples and return your schema suggestion."
        )

        messages = [
            ("system", system_prompt),
            ("human", user_prompt),
        ]

        response: LlmSchemaResponse = structured_model.invoke(messages)

        inferred_columns: list[InferredColumn] = []
        for suggestion in response.columns:
            sql_type = SqlType.TEXT
            if suggestion.sql_type.upper() in ("INTEGER", "INT"):
                sql_type = SqlType.INTEGER
            elif suggestion.sql_type.upper() in ("REAL", "FLOAT", "DOUBLE"):
                sql_type = SqlType.REAL

            inferred_columns.append(
                InferredColumn(
                    name=self._sanitize_column_name(suggestion.name),
                    sql_type=sql_type,
                    description=suggestion.description,
                    nullable=suggestion.nullable,
                    kind=ColumnKind.LLM_INFERRED,
                )
            )

        return {
            "columns": inferred_columns,
            "summary": response.summary,
            "warnings": response.warnings,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dominant_format(self, observations: list[FileObservation]) -> DetectedFormat:
        """Return the most common detected format across file observations."""

        if not observations:
            return DetectedFormat.UNKNOWN

        format_counts: dict[DetectedFormat, int] = {}
        for observation in observations:
            format_counts[observation.detected_format] = format_counts.get(observation.detected_format, 0) + 1

        return max(format_counts, key=lambda key: format_counts[key])

    def _overall_segmentation(self, observations: list[FileObservation]) -> SegmentationResult:
        """Compute a single segmentation strategy from per-file hints."""

        if not observations:
            return SegmentationResult(
                strategy=SegmentationStrategy.PER_LINE, confidence=0.5, rationale="No files analyzed."
            )

        strategies = [observation.segmentation_hint for observation in observations]
        unique = set(strategies)

        if len(unique) == 1:
            return SegmentationResult(
                strategy=strategies[0],
                confidence=0.85,
                rationale=f"All {len(observations)} file(s) agree on {strategies[0].value}.",
            )

        return SegmentationResult(
            strategy=SegmentationStrategy.MIXED,
            confidence=0.5,
            rationale=f"Files disagree on segmentation: {', '.join(strategy.value for strategy in unique)}.",
        )

    def _compute_confidence(self, observations: list[FileObservation], llm_enriched: bool) -> float:
        """Compute an overall confidence score."""

        if not observations:
            return 0.0

        avg_format_confidence = sum(observation.format_confidence for observation in observations) / len(observations)
        base = avg_format_confidence * 0.7

        if llm_enriched:
            base += 0.2

        # Penalize for many format warnings.
        total_warnings = sum(len(observation.warnings) for observation in observations)
        if total_warnings > 0:
            base -= min(total_warnings * 0.05, 0.2)

        return round(max(0.0, min(base, 1.0)), 2)

    def _generate_heuristic_summary(self, columns: list[InferredColumn], observations: list[FileObservation]) -> str:
        """Generate a plain summary when the LLM is unavailable."""

        format_strs = list({observation.detected_format.value for observation in observations})
        column_count = len(columns)
        baseline_count = sum(1 for column in columns if column.kind == ColumnKind.BASELINE)
        detected_count = sum(1 for column in columns if column.kind == ColumnKind.DETECTED)

        return (
            f"Inferred schema with {column_count} columns "
            f"({baseline_count} baseline, {detected_count} detected) "
            f"from {len(observations)} file(s). "
            f"Detected format(s): {', '.join(format_strs)}."
        )

    @staticmethod
    def _is_json_object(line: str) -> bool:
        """Check if a line is a valid JSON object (without full parsing on failure)."""

        stripped = line.strip()
        if not stripped.startswith("{"):
            return False
        try:
            parsed = json.loads(stripped)
            return isinstance(parsed, dict)
        except (json.JSONDecodeError, ValueError):
            return False

    @staticmethod
    def _score_csv(sample: list[str]) -> float:
        """Heuristic score for CSV format."""

        if len(sample) < 2:
            return 0.0

        header = sample[0]
        if "," not in header:
            return 0.0

        expected_columns = header.count(",") + 1
        if expected_columns < 2:
            return 0.0

        matching = 0
        for line in sample[1:10]:
            actual_columns = line.count(",") + 1
            if actual_columns == expected_columns:
                matching += 1

        if not sample[1:10]:
            return 0.0

        return matching / len(sample[1:10])

    @staticmethod
    def _sanitize_column_name(name: str) -> str:
        """Normalize a string into a valid lowercase snake_case column name."""

        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
        sanitized = re.sub(r"_+", "_", sanitized).strip("_").lower()
        if not sanitized or sanitized[0].isdigit():
            sanitized = "col_" + sanitized
        return sanitized

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a SQLite identifier to prevent injection."""

        return '"' + identifier.replace('"', '""') + '"'
