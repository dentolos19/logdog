from __future__ import annotations

import json
import logging
import re
from typing import Any

from pydantic import BaseModel

from parsers.binary import extract_printable_strings, is_probably_binary
from parsers.contracts import (
    BINARY_PARSER_KEY,
    ClassificationResult,
    FileClassification,
    INGESTION_SCHEMA_VERSION,
    StructuralClass,
)

logger = logging.getLogger(__name__)

# Regex patterns for structural analysis
_TIMESTAMP_CANDIDATE_RE = re.compile(
    r"\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}"
)
_KEY_VALUE_RE = re.compile(
    r"\b[a-zA-Z_][a-zA-Z0-9_.]*=(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
)
_LOG_LEVEL_RE = re.compile(
    r"\b(?:TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|NOTICE)\b",
    re.IGNORECASE,
)

CONFIDENCE_FORMULA_VERSION = "classification-v1"
"""Version identifier for the classification confidence formula."""


# ── Helper: clamp confidence to [0, 1] ──────────────────────────────────────


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


# ── Per-line analysis helpers ──────────────────────────────────────────────


def _is_json_line(line: str) -> bool:
    """Check if a line is parseable as a JSON object or array."""
    stripped = line.strip()
    if not stripped:
        return False
    if not (stripped.startswith("{") or stripped.startswith("[")):
        return False
    try:
        parsed = json.loads(stripped)
        return isinstance(parsed, (dict, list))
    except (json.JSONDecodeError, ValueError):
        pass
    return False


def _has_delimiter_consistency(lines: list[str]) -> float:
    """Score how consistently delimiter-separated the lines are.

    Checks for CSV-like delimiters (comma, tab, pipe, semicolon)
    and returns the fraction of non-empty lines that have the
    same detected delimiter.
    """
    non_empty = [ln for ln in lines if ln.strip()]
    if len(non_empty) < 2:
        return 0.0

    delimiters = ",|\t;"
    delimiter_counts: dict[str, int] = {}
    for line in non_empty[:200]:
        stripped = line.strip()
        # Skip lines that are clearly JSON or key=value
        if stripped.startswith("{") or _KEY_VALUE_RE.match(stripped):
            continue
        best_delim = ""
        best_count = 0
        for d in delimiters:
            count = stripped.count(d)
            if count > best_count:
                best_count = count
                best_delim = d
        if best_count >= 1:
            delimiter_counts[best_delim] = delimiter_counts.get(best_delim, 0) + 1

    if not delimiter_counts:
        return 0.0

    total_matched = sum(delimiter_counts.values())
    most_common = max(delimiter_counts.values())
    return most_common / total_matched if total_matched > 0 else 0.0


def _timestamp_hit_rate(lines: list[str]) -> float:
    """Fraction of non-empty lines containing a timestamp candidate."""
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    hits = sum(1 for ln in non_empty if _TIMESTAMP_CANDIDATE_RE.search(ln))
    return hits / len(non_empty)


def _key_value_detectability(lines: list[str]) -> float:
    """Fraction of non-empty lines containing key=value patterns."""
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    hits = sum(1 for ln in non_empty if _KEY_VALUE_RE.search(ln))
    return hits / len(non_empty)


def _json_parseability(lines: list[str]) -> float:
    """Fraction of non-empty lines that are valid JSON."""
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    hits = sum(1 for ln in non_empty if _is_json_line(ln))
    return hits / len(non_empty)


def _log_level_hit_rate(lines: list[str]) -> float:
    """Fraction of non-empty lines containing a log level keyword."""
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    hits = sum(1 for ln in non_empty if _LOG_LEVEL_RE.search(ln))
    return hits / len(non_empty)


def _structured_line_ratio(lines: list[str]) -> float:
    """Fraction of non-empty lines that look structurally parseable.

    A line is 'structured' if it contains at least one of:
      - JSON parseability
      - key=value pattern
      - timestamp candidate
      - delimiter consistency signal
      - log level keyword
    """
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    structured = 0
    for ln in non_empty:
        if (
            _is_json_line(ln)
            or _KEY_VALUE_RE.search(ln)
            or _TIMESTAMP_CANDIDATE_RE.search(ln)
            or _LOG_LEVEL_RE.search(ln)
        ):
            structured += 1
    return structured / len(non_empty)


def _malformed_line_ratio(lines: list[str]) -> float:
    """Rough malformed line fraction: lines with very high
    non-alphanumeric density or extremely long unbroken tokens.

    Structural characters common in well-formed data (JSON brackets,
    quotes, colons, commas, etc.) are NOT counted as malformed signals.
    """
    non_empty = [ln for ln in lines if ln.strip()]
    if not non_empty:
        return 0.0
    # Structural characters that are common in well-formed data
    allowed_special = set(" _-:/.{}\"[],=;+'\\()")
    malformed = 0
    for ln in non_empty:
        stripped = ln.strip()
        # Line with >50% non-alphanumeric characters (excluding allowed structural chars)
        if len(stripped) > 0:
            special_count = sum(1 for c in stripped if not c.isalnum() and c not in allowed_special)
            if special_count / len(stripped) > 0.5:
                malformed += 1
                continue
        # Very long unbroken token
        tokens = stripped.split()
        if tokens and max(len(t) for t in tokens) > 500:
            malformed += 1
            continue
    return malformed / len(non_empty)


def _non_empty_ratio(lines: list[str]) -> float:
    """Fraction of total lines that are non-empty."""
    if not lines:
        return 0.0
    non_empty = sum(1 for ln in lines if ln.strip())
    return non_empty / len(lines)


def _compute_file_format_confidence(lines: list[str]) -> tuple[float, dict[str, float]]:
    """Compute a deterministic format confidence score for a file.

    Returns (confidence, components).
    """
    if not lines:
        return 0.0, {}

    ner = _non_empty_ratio(lines)
    tshr = _timestamp_hit_rate(lines)
    kvr = _key_value_detectability(lines)
    jpr = _json_parseability(lines)
    dcr = _has_delimiter_consistency(lines)
    slr = _structured_line_ratio(lines)
    mlr = _malformed_line_ratio(lines)
    llr = _log_level_hit_rate(lines)

    components = {
        "non_empty_ratio": round(ner, 4),
        "structured_line_ratio": round(slr, 4),
        "timestamp_hit_rate": round(tshr, 4),
        "json_parseability": round(jpr, 4),
        "key_value_detectability": round(kvr, 4),
        "delimiter_consistency": round(dcr, 4),
        "malformed_line_ratio": round(mlr, 4),
        "log_level_hit_rate": round(llr, 4),
    }

    # Weighted composite formula.
    # Structuredness signals get the most weight.
    # Malformed lines subtract but are bounded.
    confidence = (
        0.05 * ner
        + 0.30 * slr
        + 0.25 * max(jpr, dcr, kvr)
        + 0.20 * tshr
        + 0.10 * llr
        + 0.10 * (1.0 - mlr)
    )

    return _clamp(confidence), components


class FileInput(BaseModel):
    file_id: str | None = None
    filename: str
    content: str
    raw_bytes: bytes | None = None
    is_binary: bool = False
    encoding: str | None = None
    mime_type: str | None = None
    byte_length: int | None = None
    sha256: str | None = None


class LogPreprocessorService:
    """Generic file profiler — no format-specific detection.

    Every file is classified as ``ai_universal`` format and routed to
    the AI-driven parser.  Heuristic format detection and LLM-based
    classification have been removed; the AI parser handles all formats.

    Classification confidence is computed deterministically from observable
    file signals (timestamp presence, JSON parseability, structure ratio, etc.)
    rather than using placeholders.
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
        """Classify all files as universal AI-parsable or binary format.

        Binary files (detected by extension, MIME type, or byte heuristics)
        are classified as ``binary`` format and routed to the binary parser.
        All other files are classified as ``ai_universal``.

        Confidence is computed from observable file quality signals.
        """
        file_classifications: list[FileClassification] = []
        has_binary = False
        has_text = False
        diagnostics: dict[str, Any] = {
            "mode": "generic",
            "parser": "universal_ai",
            "files": [],
        }

        for file_input in files:
            # Binary detection
            raw_bytes = file_input.raw_bytes
            if raw_bytes is not None:
                is_binary = file_input.is_binary or is_probably_binary(
                    raw_bytes, file_input.filename, file_input.mime_type
                )
            else:
                is_binary = file_input.is_binary

            if is_binary:
                has_binary = True
                if raw_bytes:
                    strings = extract_printable_strings(raw_bytes)
                    line_count = len(strings)
                else:
                    line_count = 0

                file_classifications.append(
                    FileClassification(
                        file_id=file_input.file_id,
                        filename=file_input.filename,
                        detected_format="binary",
                        structural_class=StructuralClass.BINARY,
                        format_confidence=1.0,
                        line_count=line_count,
                    )
                )
                diagnostics["files"].append({
                    "filename": file_input.filename,
                    "detected_format": "binary",
                    "format_confidence": 1.0,
                    "line_count": line_count,
                    "binary": True,
                })
                continue

            has_text = True
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

            # Compute deterministic confidence from file signals
            confidence, components = _compute_file_format_confidence(lines)

            non_empty = [line for line in lines if line.strip()]
            line_count = len(lines)
            non_empty_count = len(non_empty)

            file_classifications.append(
                FileClassification(
                    file_id=file_input.file_id,
                    filename=file_input.filename,
                    detected_format="ai_universal",
                    structural_class=StructuralClass.UNSTRUCTURED,
                    format_confidence=confidence,
                    line_count=line_count,
                )
            )
            diagnostics["files"].append({
                "filename": file_input.filename,
                "detected_format": "ai_universal",
                "format_confidence": confidence,
                "line_count": line_count,
                "non_empty_lines": non_empty_count,
                "confidence_components": components,
                "confidence_formula_version": CONFIDENCE_FORMULA_VERSION,
            })

        if has_binary and not has_text:
            dominant_format = "binary"
            structural_class_overall = StructuralClass.BINARY
            selected_parser_key = BINARY_PARSER_KEY
        else:
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
        """Compute overall classification confidence as the mean of per-file scores."""
        if not classifications:
            return 0.0
        return sum(fc.format_confidence for fc in classifications) / len(classifications)
