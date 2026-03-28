import json
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExtractedField:
    key: str
    value: Any
    unit: str | None = None
    data_type: str | None = None
    source_section: str | None = None


@dataclass
class ExtractionResult:
    fields: list[ExtractedField] = field(default_factory=list)
    sections: dict[str, list[ExtractedField]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    format_detected: str | None = None

    def to_flat_dict(self) -> dict[str, Any]:
        output: dict[str, Any] = {}
        output.update(self.metadata)
        for field_item in self.fields:
            prefix = f"{field_item.source_section}." if field_item.source_section else ""
            output[f"{prefix}{field_item.key}"] = field_item.value
            if field_item.unit:
                output[f"{prefix}{field_item.key}__unit"] = field_item.unit
        return output


_SECTION_RE = re.compile(r"^---\s+(.+?)\s+---\s*$")
_JSON_KV_RE = re.compile(r'^\s*"([^"]+)"\s*:\s*(.+?)\s*,?\s*$')
_KV_EQUALS_RE = re.compile(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$")
_TABULAR_ROW_RE = re.compile(r"^\s*(\d+)\s+([\w\.]+)\s+([\S]+)\s+([\w%/]*)\s+([\w]*)\s*$")
_FLAT_KV_WITH_UNIT_RE = re.compile(
    r"^([\w\.]+)\s*=\s*(.+?)(?:\s+(sccm|mtorr|watts|volts|count|s|%|AngstromPerMin|kHz|W|mTorr))\s*$",
    re.IGNORECASE,
)
_RECIPE_CONSTANTS_RE = re.compile(r"RecipeConstants\s*\((\d+)\s+items?\)", re.IGNORECASE)


class FieldExtractor:
    def extract(self, text: str, format_hint: str | None = None) -> ExtractionResult:
        result = ExtractionResult()

        if self._try_json(text, result):
            result.format_detected = "JSON"
            result.confidence = 0.95
            return result

        if format_hint == "LAM_PARQUET" or format_hint == "SECTION_DELIMITED" or _SECTION_RE.search(text):
            self._extract_section_delimited(text, result)
            result.format_detected = "SECTION_DELIMITED"
            return result

        if "=" in text and text.count("=") >= 3:
            self._extract_kv_pairs(text, result)
            result.format_detected = "KEY_VALUE"
            return result

        self._extract_heuristic(text, result)
        result.format_detected = "HEURISTIC"
        return result

    def _try_json(self, text: str, result: ExtractionResult) -> bool:
        stripped = text.strip()
        if not (stripped.startswith("{") or stripped.startswith("[")):
            return False

        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            return False

        self._flatten_json(data, result, prefix="")
        return True

    def _flatten_json(
        self,
        obj: Any,
        result: ExtractionResult,
        prefix: str = "",
        section: str | None = None,
    ):
        if isinstance(obj, dict):
            for key, value in obj.items():
                full_key = key if not prefix else f"{prefix}.{key}"
                if isinstance(value, (dict, list)):
                    self._flatten_json(value, result, full_key, section or key)
                else:
                    result.fields.append(
                        ExtractedField(
                            key=full_key,
                            value=value,
                            data_type=self._infer_type(value),
                            source_section=section,
                        )
                    )
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                self._flatten_json(item, result, f"{prefix}[{index}]", section)

    def _extract_section_delimited(self, text: str, result: ExtractionResult):
        lines = text.splitlines()
        current_section: str | None = None
        section_lines: dict[str, list[str]] = {}
        metadata_lines: list[str] = []

        for line in lines:
            section_match = _SECTION_RE.match(line.strip())
            if section_match:
                current_section = section_match.group(1).strip()
                section_lines.setdefault(current_section, [])
                continue

            row_match = re.match(
                r"^ROW\s+(\d+)\s*(?:[:\-]\s*(.+?))?(?:\s*\((.+)\))?\s*$",
                line.strip(),
                re.IGNORECASE,
            )
            if row_match:
                result.metadata["current_row"] = int(row_match.group(1))
                if row_match.group(2):
                    result.metadata["step_id"] = row_match.group(2).strip()
                if row_match.group(3):
                    result.metadata["step_description"] = row_match.group(3).strip()
                current_section = f"ROW_{row_match.group(1)}"
                section_lines.setdefault(current_section, [])
                continue

            recipe_constants_match = _RECIPE_CONSTANTS_RE.search(line)
            if recipe_constants_match:
                result.metadata["recipe_constants_count"] = int(recipe_constants_match.group(1))
                current_section = "RecipeConstants"
                section_lines.setdefault(current_section, [])
                continue

            if current_section:
                section_lines.setdefault(current_section, []).append(line)
            else:
                metadata_lines.append(line)

        for line in metadata_lines:
            kv_match = _KV_EQUALS_RE.match(line.strip())
            if kv_match:
                result.metadata[kv_match.group(1)] = self._cast_value(kv_match.group(2).strip())

        for section_name, section_content in section_lines.items():
            parsed_fields = self._parse_section_block(section_name, section_content)
            if parsed_fields:
                result.sections[section_name] = parsed_fields
                result.fields.extend(parsed_fields)

        total_fields = len(result.fields)
        result.confidence = min(total_fields / 20.0, 1.0) if total_fields > 0 else 0.0

    def _parse_section_block(self, section: str, lines: list[str]) -> list[ExtractedField]:
        fields: list[ExtractedField] = []
        joined = "\n".join(lines).strip()

        if joined.startswith("{") or joined.startswith("["):
            try:
                fixed = self._fix_json_block(joined)
                data = json.loads(fixed)
                temp_result = ExtractionResult()
                self._flatten_json(data, temp_result, section=section)
                for field_item in temp_result.fields:
                    field_item.source_section = section
                return temp_result.fields
            except (json.JSONDecodeError, ValueError):
                pass

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped in {"{", "}", "[", "]", "(empty)"}:
                continue

            json_kv_match = _JSON_KV_RE.match(stripped)
            if json_kv_match:
                key = json_kv_match.group(1)
                raw_value = json_kv_match.group(2).strip().strip('"')
                casted = self._cast_value(raw_value)
                fields.append(
                    ExtractedField(
                        key=key,
                        value=casted,
                        data_type=self._infer_type(casted),
                        source_section=section,
                    )
                )
                continue

            flat_kv_match = _FLAT_KV_WITH_UNIT_RE.match(stripped)
            if flat_kv_match:
                fields.append(
                    ExtractedField(
                        key=flat_kv_match.group(1),
                        value=self._cast_value(flat_kv_match.group(2).strip()),
                        unit=flat_kv_match.group(3),
                        source_section=section,
                    )
                )
                continue

            kv_match = _KV_EQUALS_RE.match(stripped)
            if kv_match:
                fields.append(
                    ExtractedField(
                        key=kv_match.group(1),
                        value=self._cast_value(kv_match.group(2).strip()),
                        source_section=section,
                    )
                )
                continue

            tabular_match = _TABULAR_ROW_RE.match(stripped)
            if tabular_match:
                fields.append(
                    ExtractedField(
                        key=tabular_match.group(2),
                        value=self._cast_value(tabular_match.group(3)),
                        unit=tabular_match.group(4) if tabular_match.group(4) else None,
                        data_type=tabular_match.group(5) if tabular_match.group(5) else None,
                        source_section=section,
                    )
                )
                continue

            flat_match = re.match(r"^([\w\.]+)\s+=\s+(.+?)(?:\s{2,}(\S+))?\s*$", stripped)
            if flat_match:
                fields.append(
                    ExtractedField(
                        key=flat_match.group(1),
                        value=self._cast_value(flat_match.group(2).strip()),
                        unit=flat_match.group(3),
                        source_section=section,
                    )
                )

        return fields

    def _extract_kv_pairs(self, text: str, result: ExtractionResult):
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            kv_match = _KV_EQUALS_RE.match(stripped)
            if kv_match:
                result.fields.append(
                    ExtractedField(
                        key=kv_match.group(1),
                        value=self._cast_value(kv_match.group(2).strip()),
                    )
                )
        result.confidence = min(len(result.fields) / 10.0, 1.0)

    def _extract_heuristic(self, text: str, result: ExtractionResult):
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if ":" in stripped and not stripped.startswith("http"):
                parts = stripped.split(":", 1)
                if len(parts) == 2 and len(parts[0].split()) <= 3:
                    result.fields.append(ExtractedField(key=parts[0].strip(), value=self._cast_value(parts[1].strip())))
        result.confidence = min(len(result.fields) / 10.0, 0.5)

    @staticmethod
    def _cast_value(raw: str) -> Any:
        lowered = raw.lower()
        if lowered in {"null", "none", ""}:
            return None
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        try:
            if "." in raw or "e" in lowered:
                return float(raw)
            return int(raw)
        except ValueError:
            return raw

    @staticmethod
    def _infer_type(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        return "string"

    @staticmethod
    def _fix_json_block(text: str) -> str:
        fixed = re.sub(r",\s*([}\]])", r"\1", text)
        if not fixed.startswith("{") and not fixed.startswith("["):
            return "{" + fixed + "}"
        return fixed
