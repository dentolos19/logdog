import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from src.parsers.semi_structured.field_extractor import ExtractionResult
from src.parsers.semi_structured.fuzzy_matcher import FuzzyMatcher

SCHEMA_VERSION = "1.0.0"


@dataclass
class LogRow:
    id: str = ""
    timestamp: str | None = None
    timestamp_raw: str | None = None
    source: str = ""
    source_type: str = "file"
    log_level: str = "INFO"
    event_type: str = "log"
    message: str = ""
    raw_text: str = ""
    record_group_id: str | None = None
    line_start: int | None = None
    line_end: int | None = None
    parse_confidence: float = 0.0
    schema_version: str = SCHEMA_VERSION
    additional_data: dict[str, Any] = field(default_factory=dict)
    raw_hash: str = ""
    template_id: str | None = None
    log_group_id: str = "default"
    equipment_id: str | None = None
    lot_id: str | None = None
    wafer_id: str | None = None
    recipe_id: str | None = None
    step_id: str | None = None
    module_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["additional_data"] = json.dumps(result["additional_data"])
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
        source_type: str = "file",
        line_start: int | None = None,
        line_end: int | None = None,
        record_group_id: str | None = None,
    ) -> LogRow:
        flat = extraction.to_flat_dict()
        remapped = self.matcher.remap_dict(flat)
        timestamp = self._find_field(remapped, self._timestamp_keys)

        return LogRow(
            id=self._generate_id(raw_text),
            timestamp=timestamp,
            timestamp_raw=timestamp,
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
            raw_hash=hashlib.sha256(raw_text.encode()).hexdigest() if raw_text else "",
            template_id=template_id,
            log_group_id=log_group_id,
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
        template_id: str | None = None,
        log_group_id: str = "default",
        parse_confidence: float = 0.0,
        source_type: str = "file",
        line_start: int | None = None,
        line_end: int | None = None,
        record_group_id: str | None = None,
    ) -> LogRow:
        remapped = self.matcher.remap_dict(fields)

        format_type = remapped.pop("_format_type", "unknown")
        remapped.pop("_section_map", {})
        timestamp = self._find_field(remapped, self._timestamp_keys)

        return LogRow(
            id=self._generate_id(raw_text),
            timestamp=timestamp,
            timestamp_raw=timestamp,
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
    def _infer_event_type(format_detected: str | None, data: dict) -> str:
        if format_detected:
            fmt = format_detected.upper()
            if fmt == "SYSLOG":
                return "syslog"
            if fmt in {"JSON", "JSON_LINES"}:
                return "json_log"
            if fmt in {"KEY_VALUE", "LOGFMT"}:
                return "key_value_log"
            if fmt in {"SECTION_DELIMITED", "LAM_PARQUET"}:
                return "equipment_event"
            if fmt == "CSV":
                return "csv_record"

        semiconductor_keys = {"equipment_id", "EquipmentID", "lot_id", "LotID", "wafer_id", "WaferID"}
        if any(key in data for key in semiconductor_keys):
            return "equipment_event"
        return "log"

    @staticmethod
    def _build_message(data: dict, extraction: ExtractionResult) -> str:
        parts = [f"[{extraction.format_detected or 'unknown'}]"]
        for key in ["equipment_id", "lot_id", "recipe_step_id", "recipe_name"]:
            if key in data and data[key]:
                parts.append(f"{key}={data[key]}")
        if len(parts) == 1:
            parts.append(f"{len(extraction.fields)} fields extracted from {len(extraction.sections)} sections")
        return " | ".join(parts)
