import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from parsers.semi_structured.field_extractor import ExtractionResult
from parsers.semi_structured.fuzzy_matcher import FuzzyMatcher


@dataclass
class LogRow:
    id: str = ""
    timestamp: str | None = None
    raw: str = ""
    extra: dict[str, Any] = field(default_factory=dict)
    raw_hash: str = ""
    template_id: str | None = None
    log_group_id: str = "default"

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["extra"] = json.dumps(result["extra"])
        return result

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


class Normalizer:
    def __init__(self, fuzzy_matcher: FuzzyMatcher | None = None):
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
        self._log_level_keys = ["level", "log_level", "severity", "loglevel"]

    def normalize(
        self,
        extraction: ExtractionResult,
        raw_text: str = "",
        template_id: str | None = None,
        log_group_id: str = "default",
        parse_confidence: float = 0.0,
    ) -> LogRow:
        flat = extraction.to_flat_dict()
        remapped = self.matcher.remap_dict(flat)
        timestamp = self._find_field(remapped, self._timestamp_keys)

        extra: dict[str, Any] = dict(remapped)
        source = self._find_field(remapped, self._source_keys)
        if source:
            extra["source"] = source
        extra["log_level"] = self._find_field(remapped, self._log_level_keys) or "INFO"
        extra["message"] = self._build_message(remapped, extraction)
        extra["parse_confidence"] = round(parse_confidence, 4)
        extra["equipment_id"] = self._find_field(remapped, ["equipment_id", "EquipmentID"])
        extra["lot_id"] = self._find_field(remapped, ["lot_id", "LotID"])
        extra["wafer_id"] = self._find_field(remapped, ["wafer_id", "WaferID"])
        extra["recipe_id"] = self._find_field(remapped, ["recipe_id", "RecipeID", "ModuleRecipeID"])
        extra["step_id"] = self._find_field(remapped, ["recipe_step_id", "RecipeStepID", "step_id"])
        extra["module_id"] = self._find_field(remapped, ["module_id", "ModuleID"])

        return LogRow(
            id=self._generate_id(raw_text),
            timestamp=timestamp,
            raw=raw_text,
            extra=extra,
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
        )

    def normalize_from_dict(
        self,
        fields: dict[str, Any],
        raw_text: str = "",
        template_id: str | None = None,
        log_group_id: str = "default",
        parse_confidence: float = 0.0,
    ) -> LogRow:
        remapped = self.matcher.remap_dict(fields)

        format_type = remapped.pop("_format_type", "unknown")
        remapped.pop("_section_map", {})
        timestamp = self._find_field(remapped, self._timestamp_keys)

        extra: dict[str, Any] = dict(remapped)
        source = self._find_field(remapped, self._source_keys)
        if source:
            extra["source"] = source
        extra["log_level"] = self._find_field(remapped, self._log_level_keys) or "INFO"
        extra["message"] = f"[{format_type}] Parsed via AI fallback"
        extra["parse_confidence"] = round(parse_confidence, 4)
        extra["equipment_id"] = self._find_field(remapped, ["equipment_id", "EquipmentID"])
        extra["lot_id"] = self._find_field(remapped, ["lot_id", "LotID"])
        extra["wafer_id"] = self._find_field(remapped, ["wafer_id", "WaferID"])
        extra["recipe_id"] = self._find_field(remapped, ["recipe_id", "RecipeID"])
        extra["step_id"] = self._find_field(remapped, ["recipe_step_id", "RecipeStepID"])
        extra["module_id"] = self._find_field(remapped, ["module_id", "ModuleID"])

        return LogRow(
            id=self._generate_id(raw_text),
            timestamp=timestamp,
            raw=raw_text,
            extra=extra,
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
        )

    @staticmethod
    def _generate_id(raw_text: str) -> str:
        if raw_text:
            return hashlib.md5(raw_text.encode(), usedforsecurity=False).hexdigest()[:16]
        return uuid.uuid4().hex[:16]

    @staticmethod
    def _find_field(data: dict, candidates: list[str]) -> str | None:
        for key in candidates:
            if key in data and data[key] is not None:
                return str(data[key])

        lower_data = {key.lower(): value for key, value in data.items()}
        for key in candidates:
            value = lower_data.get(key.lower())
            if value is not None:
                return str(value)

        leaf_map: dict[str, Any] = {}
        for key, value in data.items():
            leaf = key.rsplit(".", 1)[-1] if "." in key else key
            if leaf.lower() not in leaf_map:
                leaf_map[leaf.lower()] = value

        for key in candidates:
            value = leaf_map.get(key.lower())
            if value is not None:
                return str(value)

        return None

    @staticmethod
    def _build_message(data: dict, extraction: ExtractionResult) -> str:
        parts = [f"[{extraction.format_detected or 'unknown'}]"]
        for key in ["equipment_id", "lot_id", "recipe_step_id", "recipe_name"]:
            if key in data and data[key]:
                parts.append(f"{key}={data[key]}")
        if len(parts) == 1:
            parts.append(f"{len(extraction.fields)} fields extracted from {len(extraction.sections)} sections")
        return " | ".join(parts)
