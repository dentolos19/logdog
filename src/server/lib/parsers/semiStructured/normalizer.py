"""
Normalizer
==========
Converts extracted fields from any parsing path into the unified
LogRow schema that feeds into the downstream Log Mapper / storage layer.

LogRow schema:
  - id:            Unique row identifier (hash-based)
  - timestamp:     ISO 8601 datetime
  - level:         Log level (INFO, WARN, ERROR, etc.)
  - source:        Source identifier (equipment ID, module, etc.)
  - message:       Human-readable summary
  - metadata:      JSON blob of all extracted fields
  - raw_hash:      SHA-256 of the original raw text
  - template_id:   Template cache ID (if AI fallback was used)
  - log_group_id:  Routing key for Log Mapper
"""

import hashlib
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Optional

from .field_extractor import ExtractionResult, ExtractedField
from .fuzzy_matcher import FuzzyMatcher


@dataclass
class LogRow:
    """Unified log row schema — the output of all three pipelines."""
    id: str = ""
    timestamp: Optional[str] = None
    level: str = "INFO"
    source: str = ""
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    raw_hash: str = ""
    template_id: Optional[str] = None
    log_group_id: str = "default"

    # Extended fields for semiconductor logs
    equipment_id: Optional[str] = None
    lot_id: Optional[str] = None
    wafer_id: Optional[str] = None
    recipe_id: Optional[str] = None
    step_id: Optional[str] = None
    module_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["metadata"] = json.dumps(d["metadata"])
        return d

    def to_json(self) -> str:
        d = asdict(self)
        return json.dumps(d, indent=2, default=str)


class Normalizer:
    """Normalize extracted fields into unified LogRow schema."""

    def __init__(self, fuzzy_matcher: Optional[FuzzyMatcher] = None):
        self.matcher = fuzzy_matcher or FuzzyMatcher()

        # Priority lists for finding key fields
        self._timestamp_keys = [
            "timestamp", "start_time", "end_time",
            "CtrlJobStartTime", "WaferStartTime", "RecipeStartTime",
            "CtrlJobEndTime", "WaferEndTime", "RecipeEndTime",
            "DateTime", "datetime", "time", "ts",
        ]
        self._source_keys = [
            "equipment_id", "EquipmentID", "MachineID",
            "source", "host", "hostname", "program",
        ]
        self._level_keys = [
            "level", "log_level", "severity", "loglevel",
        ]
        self._message_keys = [
            "message", "msg", "text", "description",
            "RecipeStepName", "step_description",
        ]

    def normalize(
        self,
        extraction: ExtractionResult,
        raw_text: str = "",
        template_id: Optional[str] = None,
        log_group_id: str = "default",
    ) -> LogRow:
        """Convert an ExtractionResult into a LogRow."""

        # Build flat dict of all fields
        flat = extraction.to_flat_dict()

        # Remap keys through fuzzy matcher
        remapped = self.matcher.remap_dict(flat)

        # Extract canonical fields
        row = LogRow(
            id=self._generate_id(raw_text),
            timestamp=self._find_field(remapped, self._timestamp_keys),
            level=self._find_field(remapped, self._level_keys) or "INFO",
            source=self._find_field(remapped, self._source_keys) or "",
            message=self._build_message(remapped, extraction),
            metadata=remapped,
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,

            # Semiconductor-specific fields
            equipment_id=self._find_field(remapped, ["equipment_id", "EquipmentID"]),
            lot_id=self._find_field(remapped, ["lot_id", "LotID"]),
            wafer_id=self._find_field(remapped, ["wafer_id", "WaferID"]),
            recipe_id=self._find_field(remapped, ["recipe_id", "RecipeID", "ModuleRecipeID"]),
            step_id=self._find_field(remapped, ["recipe_step_id", "RecipeStepID", "step_id"]),
            module_id=self._find_field(remapped, ["module_id", "ModuleID"]),
        )

        return row

    def normalize_from_dict(
        self,
        fields: dict[str, Any],
        raw_text: str = "",
        template_id: Optional[str] = None,
        log_group_id: str = "default",
    ) -> LogRow:
        """Normalize from a plain dict (e.g., AI fallback output)."""
        remapped = self.matcher.remap_dict(fields)

        # Remove internal fields
        format_type = remapped.pop("_format_type", "unknown")
        section_map = remapped.pop("_section_map", {})

        row = LogRow(
            id=self._generate_id(raw_text),
            timestamp=self._find_field(remapped, self._timestamp_keys),
            level=self._find_field(remapped, self._level_keys) or "INFO",
            source=self._find_field(remapped, self._source_keys) or "",
            message=f"[{format_type}] Parsed via AI fallback",
            metadata=remapped,
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
            equipment_id=self._find_field(remapped, ["equipment_id", "EquipmentID"]),
            lot_id=self._find_field(remapped, ["lot_id", "LotID"]),
            wafer_id=self._find_field(remapped, ["wafer_id", "WaferID"]),
            recipe_id=self._find_field(remapped, ["recipe_id", "RecipeID"]),
            step_id=self._find_field(remapped, ["recipe_step_id", "RecipeStepID"]),
            module_id=self._find_field(remapped, ["module_id", "ModuleID"]),
        )
        return row

    # ---- Helpers ----------------------------------------------------------

    @staticmethod
    def _generate_id(raw_text: str) -> str:
        if raw_text:
            return hashlib.md5(raw_text.encode()).hexdigest()[:16]
        return uuid.uuid4().hex[:16]

    @staticmethod
    def _find_field(data: dict, candidates: list[str]) -> Optional[str]:
        """Find the first matching field from a priority list.
        Searches by exact key, then by leaf key (after last dot)."""
        # 1. Exact key match
        for key in candidates:
            if key in data and data[key] is not None:
                return str(data[key])

        # 2. Case-insensitive exact match
        lower_data = {k.lower(): (k, v) for k, v in data.items()}
        for key in candidates:
            if key.lower() in lower_data:
                _, v = lower_data[key.lower()]
                if v is not None:
                    return str(v)

        # 3. Match by leaf key (e.g., "Keys.CtrlJobID" matches "CtrlJobID")
        leaf_map: dict[str, Any] = {}
        for k, v in data.items():
            leaf = k.rsplit(".", 1)[-1] if "." in k else k
            # Don't overwrite — first occurrence wins (usually most specific)
            if leaf.lower() not in leaf_map:
                leaf_map[leaf.lower()] = v

        for key in candidates:
            if key.lower() in leaf_map and leaf_map[key.lower()] is not None:
                return str(leaf_map[key.lower()])

        return None

    @staticmethod
    def _build_message(data: dict, extraction: ExtractionResult) -> str:
        """Build a human-readable message summary."""
        parts: list[str] = []

        fmt = extraction.format_detected or "unknown"
        parts.append(f"[{fmt}]")

        # Add key identifiers
        for key in ["equipment_id", "lot_id", "recipe_step_id", "recipe_name"]:
            if key in data and data[key]:
                parts.append(f"{key}={data[key]}")

        if not parts[1:]:
            field_count = len(extraction.fields)
            section_count = len(extraction.sections)
            parts.append(f"{field_count} fields extracted from {section_count} sections")

        return " | ".join(parts)
