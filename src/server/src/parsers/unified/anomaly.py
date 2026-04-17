from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from parsers.unified.hierarchical import ParseUnit

ERROR_LEVELS = {"ERROR", "FATAL", "CRITICAL", "ALERT", "EMERGENCY"}
INJECTION_PATTERNS = [
    re.compile(r"(?:'\s+or\s+1=1|union\s+select|drop\s+table)", re.IGNORECASE),
    re.compile(r"(?:<script|javascript:|onerror=)", re.IGNORECASE),
    re.compile(r"(?:\$\(|`.+`|\|\s*sh\b)", re.IGNORECASE),
]


@dataclass
class Anomaly:
    type: str
    severity: str
    message: str
    line_start: int
    line_end: int
    confidence: float
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyReport:
    anomalies: list[Anomaly] = field(default_factory=list)

    @property
    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for anomaly in self.anomalies:
            counts[anomaly.severity] = counts.get(anomaly.severity, 0) + 1
        return counts


class AnomalyDetector:
    def detect(self, units: list[ParseUnit], null_rates: dict[str, float] | None = None) -> AnomalyReport:
        anomalies: list[Anomaly] = []
        if not units:
            return AnomalyReport(anomalies=[])

        anomalies.extend(self._detect_format_drift(units))
        anomalies.extend(self._detect_security_patterns(units))
        anomalies.extend(self._detect_schema_breakage(units, null_rates or {}))
        anomalies.extend(self._detect_high_error_density(units))

        return AnomalyReport(anomalies=anomalies)

    def _detect_format_drift(self, units: list[ParseUnit]) -> list[Anomaly]:
        anomalies: list[Anomaly] = []
        confidences = [unit.confidence for unit in units]
        if not confidences:
            return anomalies

        low_conf_units = [unit for unit in units if unit.confidence < 0.4]
        ratio = len(low_conf_units) / len(units)
        if ratio > 0.25:
            first = low_conf_units[0]
            last = low_conf_units[-1]
            anomalies.append(
                Anomaly(
                    type="format_drift",
                    severity="high",
                    message=f"Potential format drift detected ({ratio:.0%} low-confidence units).",
                    line_start=first.start_line,
                    line_end=last.end_line,
                    confidence=min(0.95, 0.5 + ratio),
                )
            )

        return anomalies

    def _detect_security_patterns(self, units: list[ParseUnit]) -> list[Anomaly]:
        anomalies: list[Anomaly] = []

        for unit in units:
            raw_lower = unit.raw.lower()
            for pattern in INJECTION_PATTERNS:
                if pattern.search(raw_lower):
                    anomalies.append(
                        Anomaly(
                            type="security_pattern",
                            severity="critical",
                            message="Potential injection or script payload detected in log data.",
                            line_start=unit.start_line,
                            line_end=unit.end_line,
                            confidence=0.9,
                            details={"pattern": pattern.pattern},
                        )
                    )
                    break

        return anomalies

    def _detect_schema_breakage(self, units: list[ParseUnit], null_rates: dict[str, float]) -> list[Anomaly]:
        anomalies: list[Anomaly] = []
        for field_name, rate in null_rates.items():
            if rate >= 0.9:
                anomalies.append(
                    Anomaly(
                        type="schema_breakage",
                        severity="medium",
                        message=f"Field '{field_name}' has {rate:.0%} null rate, suggesting schema mismatch.",
                        line_start=units[0].start_line,
                        line_end=units[-1].end_line,
                        confidence=min(0.95, rate),
                        details={"field": field_name, "null_rate": rate},
                    )
                )
        return anomalies

    def _detect_high_error_density(self, units: list[ParseUnit]) -> list[Anomaly]:
        anomalies: list[Anomaly] = []
        if not units:
            return anomalies

        error_units = [
            unit
            for unit in units
            if str(unit.fields.get("log_level", "")).upper() in ERROR_LEVELS
            or any(level.lower() in unit.raw.lower() for level in ["error", "fatal", "critical", "panic"])
        ]
        ratio = len(error_units) / len(units)
        if ratio >= 0.3:
            first = error_units[0]
            last = error_units[-1]
            anomalies.append(
                Anomaly(
                    type="high_error_density",
                    severity="high",
                    message=f"High error density detected ({ratio:.0%} of parsed units indicate errors).",
                    line_start=first.start_line,
                    line_end=last.end_line,
                    confidence=min(0.95, 0.4 + ratio),
                    details={"error_ratio": ratio},
                )
            )

        return anomalies
