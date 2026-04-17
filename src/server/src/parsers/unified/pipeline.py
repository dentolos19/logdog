from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from parsers.contracts import (
    BASELINE_COLUMN_NAMES,
    BASELINE_COLUMNS,
    ColumnDefinition,
    ParserPipelineResult,
    ParserSupportRequest,
    ParserSupportResult,
    TableDefinition,
    build_ddl,
    make_table_name,
)
from parsers.registry import ParserPipeline
from parsers.unified.anomaly import AnomalyDetector
from parsers.unified.binary import BinaryHandler
from parsers.unified.chunker import AdaptiveChunker
from parsers.unified.fingerprint import FingerprintEngine
from parsers.unified.hierarchical import HierarchicalParser, ParseUnit
from parsers.unified.schema import SelfCorrectingSchemaInferer
from parsers.unified.template import TemplateEvolutionEngine

if TYPE_CHECKING:
    from parsers.contracts import ClassificationResult
    from parsers.preprocessor import FileInput

logger = logging.getLogger(__name__)


@dataclass
class _ParsedFileResult:
    table_definition: TableDefinition
    rows: list[dict[str, Any]]
    warnings: list[str]
    confidence: float


class UnifiedPipeline(ParserPipeline):
    parser_key = "unified"

    def __init__(self) -> None:
        self.binary_handler = BinaryHandler()
        self.fingerprint_engine = FingerprintEngine()
        self.chunker = AdaptiveChunker()
        self.hierarchical_parser = HierarchicalParser()
        self.schema_inferer = SelfCorrectingSchemaInferer()
        self.template_engine = TemplateEvolutionEngine()
        self.anomaly_detector = AnomalyDetector()

    def supports(self, request: ParserSupportRequest) -> ParserSupportResult:
        reasons = ["Unified pipeline handles all log formats."]
        score = 0.9

        lines = request.content.splitlines()
        if not lines:
            score = 0.4
            reasons.append("Input is empty.")
        elif self.binary_handler.is_binary_extension(request.filename):
            score = 0.95
            reasons.append("Binary extension detected; unified decoder available.")
        else:
            fingerprint = self.fingerprint_engine.fingerprint(lines)
            score = max(0.75, min(0.99, 0.6 + fingerprint.confidence * 0.4))
            reasons.append(f"Detected format '{fingerprint.format_name}' with confidence {fingerprint.confidence:.2f}.")

        return ParserSupportResult(
            parser_key=self.parser_key,
            supported=True,
            score=round(score, 2),
            reasons=reasons,
        )

    def parse(self, file_inputs: list["FileInput"], classification: "ClassificationResult") -> ParserPipelineResult:
        table_definitions: list[TableDefinition] = []
        records: dict[str, list[dict[str, Any]]] = {}
        warnings: list[str] = []
        confidence_total = 0.0
        confidence_count = 0

        for file_input in file_inputs:
            try:
                parsed = self._parse_single_file(file_input, classification)
            except Exception as error:
                logger.exception("Unified pipeline failed for '%s'", file_input.filename)
                warnings.append(f"{file_input.filename}: unified parse failed: {error}")
                continue

            if parsed is None:
                warnings.append(f"{file_input.filename}: no parseable records detected.")
                continue

            table_definitions.append(parsed.table_definition)
            records[parsed.table_definition.table_name] = parsed.rows
            warnings.extend(parsed.warnings)
            confidence_total += parsed.confidence
            confidence_count += 1

        return ParserPipelineResult(
            table_definitions=table_definitions,
            records=records,
            parser_key=self.parser_key,
            warnings=warnings,
            confidence=round(confidence_total / max(confidence_count, 1), 2),
        )

    def _parse_single_file(
        self,
        file_input: "FileInput",
        classification: "ClassificationResult",
    ) -> _ParsedFileResult | None:
        file_warnings: list[str] = []
        raw_lines = file_input.content.splitlines()

        if not raw_lines:
            return None

        lines = raw_lines
        if self.binary_handler.is_binary_extension(file_input.filename):
            decoded = self.binary_handler.analyze_and_decode(file_input.content)
            if decoded.decoded_lines:
                lines = decoded.decoded_lines
            file_warnings.extend(decoded.warnings)

        lines = [line for line in lines if line.strip()]
        if not lines:
            return None

        fingerprint = self.fingerprint_engine.fingerprint(lines)
        normalized_format = self._normalize_format(file_input.filename, lines, fingerprint.format_name)
        similar = self.fingerprint_engine.find_similar(lines, threshold=0.85)
        if similar:
            file_warnings.append(
                f"Found similar known format '{similar.format_name}' (fingerprint={similar.fingerprint})."
            )

        chunking = self.chunker.chunk_lines(lines)
        if chunking.strategy != "single":
            file_warnings.append(
                f"Adaptive chunking used strategy '{chunking.strategy}' with {len(chunking.chunks)} chunk(s)."
            )

        all_units: list[ParseUnit] = []
        if normalized_format == "csv":
            all_units = self._parse_csv_units(lines)
        elif normalized_format in {"json_lines", "json_document"}:
            all_units = self._parse_json_units(lines)

        if not all_units:
            for chunk in chunking.chunks:
                parsed = self.hierarchical_parser.analyze(chunk.lines, normalized_format)
                file_warnings.extend(parsed.warnings)
                all_units.extend(parsed.all_units)

        all_units = self._prune_noise_units(all_units, normalized_format)
        all_units = self._ensure_minimal_field_coverage(all_units)

        if not all_units:
            return None

        template_changes = 0
        for unit in all_units:
            evolution = self.template_engine.register(unit.raw)
            unit.fields.setdefault("template_id", evolution.template_id)
            unit.fields.setdefault("template_skeleton", evolution.skeleton)
            if evolution.changed:
                template_changes += 1

        if template_changes > 0:
            file_warnings.append(f"Template evolution detected {template_changes} merge/split change(s).")

        profile_context = {}
        profile_name: str | None = None
        if isinstance(classification.diagnostics, dict):
            maybe_profile = classification.diagnostics.get("profile")
            if isinstance(maybe_profile, dict):
                profile_context = maybe_profile
                name_value = maybe_profile.get("name")
                if isinstance(name_value, str):
                    profile_name = name_value

        schema_result = self.schema_inferer.infer(
            sample_lines=[unit.raw for unit in all_units[:100]],
            format_name=normalized_format,
            domain="unified",
            profile_name=profile_name,
            profile_context=profile_context,
        )
        file_warnings.extend(schema_result.warnings)

        rows = self._build_rows(file_input.filename, all_units, schema_result.columns)
        if not rows:
            return None

        anomaly_report = self.anomaly_detector.detect(all_units, schema_result.null_rates)
        if anomaly_report.anomalies:
            critical_count = anomaly_report.summary.get("critical", 0)
            high_count = anomaly_report.summary.get("high", 0)
            file_warnings.append(
                f"Anomalies detected: critical={critical_count}, high={high_count}, total={len(anomaly_report.anomalies)}."
            )

        columns = self._merge_columns(schema_result.columns)
        table_name = make_table_name(self.parser_key, file_input.file_id, file_input.filename)
        ddl = build_ddl(table_name, columns)
        table_definition = TableDefinition(table_name=table_name, columns=columns, ddl=ddl)

        row_confidence = sum(unit.confidence for unit in all_units) / len(all_units)
        confidence = min(1.0, (row_confidence + fingerprint.confidence + schema_result.confidence) / 3)

        return _ParsedFileResult(
            table_definition=table_definition,
            rows=rows,
            warnings=file_warnings,
            confidence=round(confidence, 2),
        )

    def _normalize_format(self, filename: str, lines: list[str], detected_format: str) -> str:
        lower_name = filename.lower()
        if lower_name.endswith(".json"):
            return "json_lines"
        if lower_name.endswith(".csv"):
            return "csv"
        if lower_name.endswith(".xml"):
            return "xml"

        sample = [line.strip() for line in lines[:30] if line.strip()]
        if not sample:
            return detected_format

        csv_like_count = sum(1 for line in sample[:10] if "," in line)
        if lower_name.endswith(".csv") or (csv_like_count >= 3 and all("{" not in line for line in sample[:10])):
            return "csv"

        if all((line.startswith("{") and line.endswith("}")) for line in sample[:10]):
            return "json_lines"

        if any(line.startswith("<") and line.endswith(">") for line in sample[:10]):
            return "xml"

        if "syslog" in detected_format:
            return "syslog"

        if detected_format == "plain_text":
            syslog_like = 0
            for line in sample[:20]:
                if line.startswith("<") and ("Mar " in line or "Jan " in line):
                    syslog_like += 1
            if syslog_like >= 3:
                return "syslog"

        return detected_format

    def _parse_json_units(self, lines: list[str]) -> list[ParseUnit]:
        units: list[ParseUnit] = []
        content = "\n".join(lines).strip()

        if content:
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict):
                    units.append(
                        ParseUnit(
                            start_line=1,
                            end_line=len(lines),
                            raw=content[:4000],
                            fields=self._flatten_json(parsed),
                            level="file",
                            confidence=0.98,
                        )
                    )
                    return units
                if isinstance(parsed, list):
                    for index, item in enumerate(parsed, start=1):
                        if not isinstance(item, dict):
                            continue
                        units.append(
                            ParseUnit(
                                start_line=index,
                                end_line=index,
                                raw=json.dumps(item, ensure_ascii=True),
                                fields=self._flatten_json(item),
                                level="line",
                                confidence=0.97,
                            )
                        )
                    if units:
                        return units
            except (json.JSONDecodeError, ValueError):
                pass

        for index, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped or stripped in {"{", "}", "[", "]", ","}:
                continue
            try:
                parsed = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(parsed, dict):
                continue
            units.append(
                ParseUnit(
                    start_line=index,
                    end_line=index,
                    raw=stripped,
                    fields=self._flatten_json(parsed),
                    level="line",
                    confidence=0.95,
                )
            )

        return units

    def _parse_csv_units(self, lines: list[str]) -> list[ParseUnit]:
        if not lines:
            return []

        content = "\n".join(lines)
        reader = csv.DictReader(content.splitlines())
        if not reader.fieldnames:
            return []

        units: list[ParseUnit] = []
        for row_index, row in enumerate(reader, start=2):
            fields: dict[str, Any] = {}
            for key, value in row.items():
                if key is None:
                    continue
                safe_key = self._sanitize(key)
                if value is None:
                    continue
                casted = self._cast_value(value.strip())
                fields[safe_key] = casted

            if not fields:
                continue

            raw_line = lines[row_index - 1] if row_index - 1 < len(lines) else json.dumps(fields, ensure_ascii=True)
            units.append(
                ParseUnit(
                    start_line=row_index,
                    end_line=row_index,
                    raw=raw_line,
                    fields=fields,
                    level="line",
                    confidence=0.95,
                )
            )

        return units

    def _prune_noise_units(self, units: list[ParseUnit], normalized_format: str) -> list[ParseUnit]:
        pruned: list[ParseUnit] = []
        brace_noise = {"{", "}", "[", "]", ","}

        for unit in units:
            raw = unit.raw.strip()
            if raw in brace_noise:
                continue

            field_keys = set(unit.fields.keys())
            meaningful_keys = field_keys - {"message", "template_id", "template_skeleton"}

            if normalized_format in {"json_lines", "csv", "syslog"} and not meaningful_keys and unit.confidence < 0.8:
                continue

            pruned.append(unit)

        return pruned

    def _flatten_json(self, value: dict[str, Any], prefix: str = "", depth: int = 0) -> dict[str, Any]:
        if depth > 4:
            return {prefix.rstrip("_") or "value": json.dumps(value, ensure_ascii=True)}

        result: dict[str, Any] = {}
        for key, raw in value.items():
            safe_key = self._sanitize(key)
            full_key = f"{prefix}{safe_key}" if prefix else safe_key
            if isinstance(raw, dict):
                result.update(self._flatten_json(raw, prefix=f"{full_key}_", depth=depth + 1))
            elif isinstance(raw, list):
                if len(raw) <= 10 and all(not isinstance(item, (dict, list)) for item in raw):
                    result[full_key] = ",".join(str(item) for item in raw)
                else:
                    result[full_key] = json.dumps(raw, ensure_ascii=True)
            else:
                result[full_key] = raw
        return result

    def _ensure_minimal_field_coverage(self, units: list[ParseUnit]) -> list[ParseUnit]:
        normalized: list[ParseUnit] = []
        for unit in units:
            unit.fields.setdefault("message", unit.raw[:500])
            if "log_level" not in unit.fields:
                unit.fields["log_level"] = self._infer_log_level(unit.raw)
            normalized.append(unit)
        return normalized

    @staticmethod
    def _infer_log_level(raw: str) -> str:
        lowered = raw.lower()
        if "critical" in lowered or "panic" in lowered:
            return "CRITICAL"
        if "fatal" in lowered:
            return "FATAL"
        if "error" in lowered:
            return "ERROR"
        if "warn" in lowered:
            return "WARNING"
        if "debug" in lowered:
            return "DEBUG"
        if "trace" in lowered:
            return "TRACE"
        return "INFO"

    def _build_rows(
        self,
        filename: str,
        units: list[ParseUnit],
        columns: list[ColumnDefinition],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        allowed = (
            BASELINE_COLUMN_NAMES
            | {column.name for column in columns}
            | {
                "source",
                "message",
                "log_level",
                "parse_confidence",
            }
        )

        for unit in units:
            row: dict[str, Any] = {
                "source": filename,
                "raw": unit.raw[:4000],
                "parse_confidence": round(unit.confidence, 3),
            }
            overflow: dict[str, Any] = {}

            for key, value in unit.fields.items():
                safe_key = self._sanitize(key)
                if safe_key in allowed:
                    row[safe_key] = value
                else:
                    overflow[safe_key] = value

            if "message" not in row:
                row["message"] = unit.raw[:500]

            if overflow:
                row["extra"] = json.dumps(overflow, default=str, ensure_ascii=True)

            rows.append(row)

        return rows

    @staticmethod
    def _merge_columns(inferred_columns: list[ColumnDefinition]) -> list[ColumnDefinition]:
        merged: list[ColumnDefinition] = list(BASELINE_COLUMNS)
        merged.extend(
            [
                ColumnDefinition(name="source", sql_type="TEXT", description="Source filename.", nullable=True),
                ColumnDefinition(name="message", sql_type="TEXT", description="Primary message text.", nullable=True),
                ColumnDefinition(name="log_level", sql_type="TEXT", description="Detected log level.", nullable=True),
                ColumnDefinition(
                    name="parse_confidence",
                    sql_type="REAL",
                    description="Per-row parse confidence.",
                    nullable=True,
                ),
            ]
        )
        existing = {column.name for column in merged}

        for column in inferred_columns:
            if column.name in existing:
                continue
            merged.append(column)
            existing.add(column.name)

        return merged

    @staticmethod
    def _sanitize(value: str) -> str:
        sanitized = "".join(character if character.isalnum() or character == "_" else "_" for character in value)
        sanitized = "_".join(part for part in sanitized.split("_") if part).lower()
        if not sanitized:
            return "field"
        if sanitized[0].isdigit():
            return f"field_{sanitized}"
        return sanitized

    @staticmethod
    def _cast_value(value: str) -> Any:
        lowered = value.lower()
        if lowered in {"", "null", "none"}:
            return None
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
