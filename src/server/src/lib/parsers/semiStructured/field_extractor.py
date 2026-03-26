"""
Step 2: Field Extraction
========================
Extracts structured fields from semi-structured log text using
heuristics tuned for semiconductor manufacturing log formats.

Handles:
- JSON-like key-value blocks (Vendor 1 / Vendor 2 style)
- Section-delimited blocks (--- SectionName ---)
- Tabular data (RecipeDetail / RecipeConstants tables)
- Flat text with embedded key=value pairs
"""

import re
import json
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ExtractedField:
    key: str
    value: Any
    unit: Optional[str] = None
    data_type: Optional[str] = None  # string, float, int, bool, null
    source_section: Optional[str] = None


@dataclass
class ExtractionResult:
    fields: list[ExtractedField] = field(default_factory=list)
    sections: dict[str, list[ExtractedField]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    format_detected: Optional[str] = None

    def to_flat_dict(self) -> dict[str, Any]:
        """Flatten all fields into a single dict for normalization."""
        out: dict[str, Any] = {}
        out.update(self.metadata)
        for f in self.fields:
            prefix = f"{f.source_section}." if f.source_section else ""
            out[f"{prefix}{f.key}"] = f.value
            if f.unit:
                out[f"{prefix}{f.key}__unit"] = f.unit
        return out


# ---------------------------------------------------------------------------
# Patterns used in extraction
# ---------------------------------------------------------------------------
_SECTION_RE = re.compile(r"^---\s+(.+?)\s+---\s*$")
_JSON_KV_RE = re.compile(r'^\s*"([^"]+)"\s*:\s*(.+?)\s*,?\s*$')
_KV_EQUALS_RE = re.compile(r"^([A-Za-z_][\w\.]*)\s*=\s*(.+)$")
_TABULAR_HEADER_RE = re.compile(r"^\s*#\s+Key\s+Value\s+Unit\s+Type", re.IGNORECASE)
_TABULAR_ROW_RE = re.compile(r"^\s*(\d+)\s+([\w\.]+)\s+([\S]+)\s+([\w%/]*)\s+([\w]*)\s*$")
_FLAT_KV_WITH_UNIT_RE = re.compile(
    r"^([\w\.]+)\s*=\s*(.+?)(?:\s+(sccm|mtorr|watts|volts|count|s|%|AngstromPerMin|kHz|W|mTorr))\s*$",
    re.IGNORECASE,
)
_RECIPE_CONSTANTS_RE = re.compile(r"RecipeConstants\s*\((\d+)\s+items?\)", re.IGNORECASE)


class FieldExtractor:
    """Extract structured fields from semi-structured log text."""

    def extract(self, text: str, format_hint: Optional[str] = None) -> ExtractionResult:
        """Main entry point — detect format and extract fields."""
        result = ExtractionResult()

        # Try JSON parse first
        if self._try_json(text, result):
            result.format_detected = "JSON"
            result.confidence = 0.95
            return result

        # Section-delimited format (Vendor 3 / LAM style)
        if format_hint == "LAM_PARQUET" or format_hint == "SECTION_DELIMITED" or _SECTION_RE.search(text):
            self._extract_section_delimited(text, result)
            result.format_detected = "SECTION_DELIMITED"
            return result

        # Key=Value format
        if "=" in text and text.count("=") >= 3:
            self._extract_kv_pairs(text, result)
            result.format_detected = "KEY_VALUE"
            return result

        # Fallback: line-by-line heuristic
        self._extract_heuristic(text, result)
        result.format_detected = "HEURISTIC"
        return result

    # ---- JSON extraction --------------------------------------------------

    def _try_json(self, text: str, result: ExtractionResult) -> bool:
        """Attempt to parse the entire text as JSON."""
        stripped = text.strip()
        if not (stripped.startswith("{") or stripped.startswith("[")):
            return False
        try:
            data = json.loads(stripped)
            self._flatten_json(data, result, prefix="")
            return True
        except json.JSONDecodeError:
            return False

    def _flatten_json(
        self,
        obj: Any,
        result: ExtractionResult,
        prefix: str = "",
        section: Optional[str] = None,
    ):
        """Recursively flatten JSON into ExtractedFields."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = k if not prefix else f"{prefix}.{k}"
                if isinstance(v, (dict, list)):
                    self._flatten_json(v, result, full_key, section or k)
                else:
                    ef = ExtractedField(
                        key=full_key,
                        value=v,
                        data_type=self._infer_type(v),
                        source_section=section,
                    )
                    result.fields.append(ef)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self._flatten_json(item, result, f"{prefix}[{i}]", section)

    # ---- Section-delimited extraction (Vendor 3) --------------------------

    def _extract_section_delimited(self, text: str, result: ExtractionResult):
        """Parse --- Section --- delimited blocks."""
        lines = text.splitlines()
        current_section: Optional[str] = None
        section_lines: dict[str, list[str]] = {}
        meta_lines: list[str] = []

        for line in lines:
            sec_match = _SECTION_RE.match(line.strip())
            if sec_match:
                current_section = sec_match.group(1).strip()
                section_lines.setdefault(current_section, [])
                continue

            # ROW headers
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

            # RecipeConstants header
            rc_match = _RECIPE_CONSTANTS_RE.search(line)
            if rc_match:
                result.metadata["recipe_constants_count"] = int(rc_match.group(1))
                current_section = "RecipeConstants"
                section_lines.setdefault(current_section, [])
                continue

            if current_section:
                section_lines.setdefault(current_section, []).append(line)
            else:
                meta_lines.append(line)

        # Parse metadata lines (file info at top)
        for line in meta_lines:
            kv = _KV_EQUALS_RE.match(line.strip())
            if kv:
                result.metadata[kv.group(1)] = self._cast_value(kv.group(2).strip())

        # Parse each section
        for sec_name, sec_lines in section_lines.items():
            parsed = self._parse_section_block(sec_name, sec_lines)
            if parsed:
                result.sections[sec_name] = parsed
                result.fields.extend(parsed)

        total = len(result.fields)
        result.confidence = min(total / 20.0, 1.0) if total > 0 else 0.0

    def _parse_section_block(self, section: str, lines: list[str]) -> list[ExtractedField]:
        """Parse lines within a section into fields."""
        fields: list[ExtractedField] = []

        # Detect if block is JSON-like (curly braces)
        joined = "\n".join(lines).strip()
        if joined.startswith("{") or joined.startswith("["):
            try:
                fixed = self._fix_json_block(joined)
                data = json.loads(fixed)
                tmp = ExtractionResult()
                self._flatten_json(data, tmp, section=section)
                for f in tmp.fields:
                    f.source_section = section
                return tmp.fields
            except (json.JSONDecodeError, ValueError):
                pass

        # Try JSON-style key-value lines
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped in ("{", "}", "[", "]", "(empty)"):
                continue

            # "Key": "Value" pattern
            jkv = _JSON_KV_RE.match(stripped)
            if jkv:
                key, raw_val = jkv.group(1), jkv.group(2).strip().strip('"')
                fields.append(
                    ExtractedField(
                        key=key,
                        value=self._cast_value(raw_val),
                        data_type=self._infer_type(self._cast_value(raw_val)),
                        source_section=section,
                    )
                )
                continue

            # Key = Value (with optional unit)
            fkv = _FLAT_KV_WITH_UNIT_RE.match(stripped)
            if fkv:
                fields.append(
                    ExtractedField(
                        key=fkv.group(1),
                        value=self._cast_value(fkv.group(2).strip()),
                        unit=fkv.group(3),
                        source_section=section,
                    )
                )
                continue

            kv = _KV_EQUALS_RE.match(stripped)
            if kv:
                fields.append(
                    ExtractedField(
                        key=kv.group(1),
                        value=self._cast_value(kv.group(2).strip()),
                        source_section=section,
                    )
                )
                continue

            # Tabular row: index Key Value Unit Type
            tr = _TABULAR_ROW_RE.match(stripped)
            if tr:
                fields.append(
                    ExtractedField(
                        key=tr.group(2),
                        value=self._cast_value(tr.group(3)),
                        unit=tr.group(4) if tr.group(4) else None,
                        data_type=tr.group(5) if tr.group(5) else None,
                        source_section=section,
                    )
                )
                continue

            # Flat "Key   = Value   unit" lines (Vendor 3 recipe detail)
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

    # ---- Key=Value extraction ---------------------------------------------

    def _extract_kv_pairs(self, text: str, result: ExtractionResult):
        """Extract from pure key=value text."""
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            kv = _KV_EQUALS_RE.match(stripped)
            if kv:
                result.fields.append(
                    ExtractedField(
                        key=kv.group(1),
                        value=self._cast_value(kv.group(2).strip()),
                    )
                )
        result.confidence = min(len(result.fields) / 10.0, 1.0)

    # ---- Heuristic extraction (fallback) ----------------------------------

    def _extract_heuristic(self, text: str, result: ExtractionResult):
        """Last-resort line-by-line extraction."""
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            # Try colon-separated
            if ":" in stripped and not stripped.startswith("http"):
                parts = stripped.split(":", 1)
                if len(parts) == 2 and len(parts[0].split()) <= 3:
                    result.fields.append(
                        ExtractedField(
                            key=parts[0].strip(),
                            value=self._cast_value(parts[1].strip()),
                        )
                    )
        result.confidence = min(len(result.fields) / 10.0, 0.5)

    # ---- Helpers ----------------------------------------------------------

    @staticmethod
    def _cast_value(raw: str) -> Any:
        """Cast string to appropriate Python type."""
        if raw.lower() == "null" or raw.lower() == "none" or raw == "":
            return None
        if raw.lower() == "true":
            return True
        if raw.lower() == "false":
            return False
        try:
            if "." in raw or "e" in raw.lower():
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
        """Attempt to fix common JSON issues in log blocks."""
        # Remove trailing commas before } or ]
        text = re.sub(r",\s*([}\]])", r"\1", text)
        # Ensure it's wrapped properly
        if not text.startswith("{") and not text.startswith("["):
            text = "{" + text + "}"
        return text
