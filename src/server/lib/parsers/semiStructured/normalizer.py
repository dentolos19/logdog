"""
Normalizer
==========
Converts extracted fields from any parsing path into the unified
LogRow schema that aligns with the preprocessor baseline columns.

Baseline columns (always present — mirrors preprocessor._build_baseline_columns):
  - id:               Unique row identifier (MD5 hash-based)
  - timestamp:        Normalized ISO-8601 timestamp
  - timestamp_raw:    Original timestamp string as found in the log
  - source:           Source identifier (equipment ID, hostname, etc.)
  - source_type:      Category of source ('file', 'stream', 'api')
  - log_level:        Severity level (INFO, WARN, ERROR, etc.)
  - event_type:       Classified event type ('equipment_event', 'syslog', etc.)
  - message:          Human-readable summary
  - raw_text:         Complete original log text, preserved for traceability
  - record_group_id:  Links related records from the same multiline cluster
  - line_start:       1-based line number where this record starts
  - line_end:         1-based line number where this record ends
  - parse_confidence: Confidence score (0.0-1.0) for this record
  - schema_version:   Version of the schema used to parse this record
  - additional_data:  JSON blob of all extra extracted fields

Pipeline-only fields (routing / caching, not in baseline table):
  - raw_hash:      SHA-256 of raw_text (deduplication)
  - template_id:   Template cache ID (if AI fallback was used)
  - log_group_id:  Routing key for the Log Mapper

Semiconductor-extended fields:
  - equipment_id, lot_id, wafer_id, recipe_id, step_id, module_id
"""

import hashlib
import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from .field_extractor import ExtractionResult
from .fuzzy_matcher import FuzzyMatcher

SCHEMA_VERSION = "1.0.0"


@dataclass
class LogRow:
    """
    Unified log row schema.
    Baseline columns mirror preprocessor._build_baseline_columns() so that
    output from the SemiStructured pipeline is directly compatible with the
    schema produced by the preprocessor.
    """

    # ── Baseline columns ─────────────────────────────────────────────────────
    id: str = ""
    timestamp: Optional[str] = None
    timestamp_raw: Optional[str] = None
    source: str = ""
    source_type: str = "file"
    log_level: str = "INFO"
    event_type: str = "log"
    message: str = ""
    raw_text: str = ""
    record_group_id: Optional[str] = None
    line_start: Optional[int] = None
    line_end: Optional[int] = None
    parse_confidence: float = 0.0
    schema_version: str = SCHEMA_VERSION
    additional_data: dict[str, Any] = field(default_factory=dict)

    # ── Pipeline-only fields (not stored in baseline table) ──────────────────
    raw_hash: str = ""
    template_id: Optional[str] = None
    log_group_id: str = "default"

    # ── Semiconductor-extended fields ─────────────────────────────────────────
    equipment_id: Optional[str] = None
    lot_id: Optional[str] = None
    wafer_id: Optional[str] = None
    recipe_id: Optional[str] = None
    step_id: Optional[str] = None
    module_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["additional_data"] = json.dumps(d["additional_data"])
        return d

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


class Normalizer:
    """Normalize extracted fields into the unified LogRow baseline schema."""

    def __init__(self, fuzzy_matcher: Optional[FuzzyMatcher] = None):
        self.matcher = fuzzy_matcher or FuzzyMatcher()

        self._timestamp_keys = [
            "timestamp",
            "start_time",
            "end_time",
            "CtrlJobStartTime",
            "WaferStartTime",
            "RecipeStartTime",
            "CtrlJobEndTime",
            "WaferEndTime",
            "RecipeEndTime",
            "DateTime",
            "datetime",
            "time",
            "ts",
        ]
        self._source_keys = [
            "equipment_id",
            "EquipmentID",
            "MachineID",
            "source",
            "host",
            "hostname",
            "program",
        ]
        self._log_level_keys = [
            "level",
            "log_level",
            "severity",
            "loglevel",
        ]
        self._message_keys = [
            "message",
            "msg",
            "text",
            "description",
            "RecipeStepName",
            "step_description",
        ]

    def normalize(
        self,
        extraction: ExtractionResult,
        raw_text: str = "",
        template_id: Optional[str] = None,
        log_group_id: str = "default",
        parse_confidence: float = 0.0,
        source_type: str = "file",
        line_start: Optional[int] = None,
        line_end: Optional[int] = None,
        record_group_id: Optional[str] = None,
    ) -> LogRow:
        """Convert an ExtractionResult into a LogRow aligned with baseline columns."""

        flat = extraction.to_flat_dict()
        remapped = self.matcher.remap_dict(flat)

        ts = self._find_field(remapped, self._timestamp_keys)

        return LogRow(
            # Baseline
            id=self._generate_id(raw_text),
            timestamp=ts,
            timestamp_raw=ts,
            source=self._find_field(remapped, self._source_keys) or "",
            source_type=source_type,
            log_level=self._find_field(remapped, self._log_level_keys) or "INFO",
            event_type=self._infer_event_type(extraction.format_detected, remapped),
            message=self._build_message(remapped, extraction),
            raw_text=raw_text,
            record_group_id=record_group_id,
            line_start=line_start,
            line_end=line_end,
            parse_confidence=round(parse_confidence, 4),
            schema_version=SCHEMA_VERSION,
            additional_data=remapped,
            # Pipeline-only
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
            # Semiconductor-extended
            equipment_id=self._find_field(remapped, ["equipment_id", "EquipmentID"]),
            lot_id=self._find_field(remapped, ["lot_id", "LotID"]),
            wafer_id=self._find_field(remapped, ["wafer_id", "WaferID"]),
            recipe_id=self._find_field(remapped, ["recipe_id", "RecipeID", "ModuleRecipeID"]),
            step_id=self._find_field(remapped, ["recipe_step_id", "RecipeStepID", "step_id"]),
            module_id=self._find_field(remapped, ["module_id", "ModuleID"]),
        )

    def normalize_from_dict(
        self,
        fields: dict[str, Any],
        raw_text: str = "",
        template_id: Optional[str] = None,
        log_group_id: str = "default",
        parse_confidence: float = 0.0,
        source_type: str = "file",
        line_start: Optional[int] = None,
        line_end: Optional[int] = None,
        record_group_id: Optional[str] = None,
    ) -> LogRow:
        """Normalize from a plain dict (e.g., AI fallback output)."""
        remapped = self.matcher.remap_dict(fields)

        format_type = remapped.pop("_format_type", "unknown")
        remapped.pop("_section_map", {})

        ts = self._find_field(remapped, self._timestamp_keys)

        return LogRow(
            # Baseline
            id=self._generate_id(raw_text),
            timestamp=ts,
            timestamp_raw=ts,
            source=self._find_field(remapped, self._source_keys) or "",
            source_type=source_type,
            log_level=self._find_field(remapped, self._log_level_keys) or "INFO",
            event_type=format_type if format_type != "unknown" else "log",
            message=f"[{format_type}] Parsed via AI fallback",
            raw_text=raw_text,
            record_group_id=record_group_id,
            line_start=line_start,
            line_end=line_end,
            parse_confidence=round(parse_confidence, 4),
            schema_version=SCHEMA_VERSION,
            additional_data=remapped,
            # Pipeline-only
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
            # Semiconductor-extended
            equipment_id=self._find_field(remapped, ["equipment_id", "EquipmentID"]),
            lot_id=self._find_field(remapped, ["lot_id", "LotID"]),
            wafer_id=self._find_field(remapped, ["wafer_id", "WaferID"]),
            recipe_id=self._find_field(remapped, ["recipe_id", "RecipeID"]),
            step_id=self._find_field(remapped, ["recipe_step_id", "RecipeStepID"]),
            module_id=self._find_field(remapped, ["module_id", "ModuleID"]),
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _generate_id(raw_text: str) -> str:
        if raw_text:
            return hashlib.md5(raw_text.encode()).hexdigest()[:16]
        return uuid.uuid4().hex[:16]

    @staticmethod
    def _find_field(data: dict, candidates: list[str]) -> Optional[str]:
        """Find the first matching field from a priority list.
        Searches by exact key, case-insensitive key, then by leaf key."""
        for key in candidates:
            if key in data and data[key] is not None:
                return str(data[key])

        lower_data = {k.lower(): v for k, v in data.items()}
        for key in candidates:
            v = lower_data.get(key.lower())
            if v is not None:
                return str(v)

        leaf_map: dict[str, Any] = {}
        for k, v in data.items():
            leaf = k.rsplit(".", 1)[-1] if "." in k else k
            if leaf.lower() not in leaf_map:
                leaf_map[leaf.lower()] = v

        for key in candidates:
            v = leaf_map.get(key.lower())
            if v is not None:
                return str(v)

        return None

    @staticmethod
    def _infer_event_type(format_detected: Optional[str], data: dict) -> str:
        """Derive event_type from the detected format or field presence."""
        if format_detected:
            fmt = format_detected.upper()
            if fmt == "SYSLOG":
                return "syslog"
            if fmt in ("JSON", "JSON_LINES"):
                return "json_log"
            if fmt in ("KEY_VALUE", "LOGFMT"):
                return "key_value_log"
            if fmt in ("SECTION_DELIMITED", "LAM_PARQUET"):
                return "equipment_event"
            if fmt == "CSV":
                return "csv_record"

        sem_keys = {"equipment_id", "EquipmentID", "lot_id", "LotID", "wafer_id", "WaferID"}
        if any(k in data for k in sem_keys):
            return "equipment_event"

        return "log"

    @staticmethod
    def _build_message(data: dict, extraction: ExtractionResult) -> str:
        """Build a human-readable message summary."""
        parts: list[str] = []
        parts.append(f"[{extraction.format_detected or 'unknown'}]")

        for key in ["equipment_id", "lot_id", "recipe_step_id", "recipe_name"]:
            if key in data and data[key]:
                parts.append(f"{key}={data[key]}")

        if not parts[1:]:
            parts.append(f"{len(extraction.fields)} fields extracted from {len(extraction.sections)} sections")

        return " | ".join(parts)
