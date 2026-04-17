from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Any

STRUCTURE_PATTERNS: dict[str, re.Pattern] = {
    "json_line": re.compile(r"^\s*\{.*\}\s*$"),
    "xml_tag": re.compile(r"^\s*<[\w!?][^>]*>\s*$"),
    "csv_row": re.compile(r"^(?:[^,\n]+,){1,}[^,\n]*$"),
    "syslog": re.compile(
        r"^(?:<\d{1,3}>)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\S+\s+\S+",
        re.IGNORECASE,
    ),
    "key_value": re.compile(r"(?:\w[\w.\-]*\s*[:=]\s*(?:\"[^\"]*\"|\S+)\s*){2,}"),
    "logfmt": re.compile(r"^(?:\w[\w.\-]*=(?:\"[^\"]*\"|\S+)\s*){2,}"),
    "apache_clf": re.compile(r"^\S+\s+\S+\s+\S+\s+\[.+?\]\s+\".+?\"\s+\d{3}\s+\d+"),
    "hex_dump": re.compile(r"^[0-9A-Fa-f]{4,8}\s+(?:[0-9A-Fa-f]{2}\s){4,}"),
    "section_header": re.compile(r"^---\s+.+\s+---\s*$"),
    "timestamp_iso": re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),
    "log_level": re.compile(r"\b(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\b", re.IGNORECASE),
}

JSON_FILE_PATTERN = re.compile(r"^\s*[\[{]")


@dataclass
class FormatFingerprint:
    fingerprint: str
    format_name: str
    confidence: float
    line_count: int
    sample_lines: int
    delimiter_counts: dict[str, int] = field(default_factory=dict)
    avg_line_length: float = 0.0
    line_length_std: float = 0.0
    has_timestamps: bool = False
    has_log_levels: bool = False
    structural_class: str = "unknown"
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "format_name": self.format_name,
            "confidence": self.confidence,
            "line_count": self.line_count,
            "sample_lines": self.sample_lines,
            "delimiter_counts": self.delimiter_counts,
            "avg_line_length": self.avg_line_length,
            "line_length_std": self.line_length_std,
            "has_timestamps": self.has_timestamps,
            "has_log_levels": self.has_log_levels,
            "structural_class": self.structural_class,
        }


class FingerprintEngine:
    def __init__(self, max_cache_size: int = 5000) -> None:
        self._cache: dict[str, FormatFingerprint] = {}
        self._max_cache_size = max_cache_size

    def fingerprint(self, lines: list[str]) -> FormatFingerprint:
        sample = [line for line in lines[:100] if line.strip()]
        if not sample:
            return FormatFingerprint(
                fingerprint=self._compute_fingerprint([]),
                format_name="empty",
                confidence=1.0,
                line_count=0,
                sample_lines=0,
            )

        content_hash = self._compute_fingerprint(sample)
        cached = self._cache.get(content_hash)
        if cached:
            return cached

        scores = self._score_formats(sample)
        structural_class = self._classify_structure(scores)
        line_lengths = [len(line) for line in sample]
        avg_length = sum(line_lengths) / len(line_lengths)
        length_std = (sum((line_length - avg_length) ** 2 for line_length in line_lengths) / len(line_lengths)) ** 0.5

        has_timestamps = (
            sum(1 for line in sample if STRUCTURE_PATTERNS["timestamp_iso"].search(line)) / len(sample) > 0.3
        )
        has_log_levels = sum(1 for line in sample if STRUCTURE_PATTERNS["log_level"].search(line)) / len(sample) > 0.3

        best_format = max(scores, key=lambda k: scores[k])
        confidence = scores[best_format]

        delimiter_counts = self._count_delimiters(sample)

        result = FormatFingerprint(
            fingerprint=content_hash,
            format_name=best_format,
            confidence=round(confidence, 3),
            line_count=len(lines),
            sample_lines=len(sample),
            delimiter_counts=delimiter_counts,
            avg_line_length=round(avg_length, 1),
            line_length_std=round(length_std, 1),
            has_timestamps=has_timestamps,
            has_log_levels=has_log_levels,
            structural_class=structural_class,
        )

        if len(self._cache) >= self._max_cache_size:
            oldest_key = min(self._cache, key=lambda k: self._cache[k].created_at)
            self._cache.pop(oldest_key)

        self._cache[content_hash] = result
        return result

    def find_similar(self, lines: list[str], threshold: float = 0.8) -> FormatFingerprint | None:
        sample = [line for line in lines[:50] if line.strip()]
        if not sample:
            return None

        current_fp = self.fingerprint(sample)
        best_match: FormatFingerprint | None = None
        best_score = 0.0

        for cached_fp in self._cache.values():
            if cached_fp.fingerprint == current_fp.fingerprint:
                continue

            score = self._similarity_score(current_fp, cached_fp)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = cached_fp

        return best_match

    def stats(self) -> dict[str, Any]:
        format_counts: dict[str, int] = {}
        for fp in self._cache.values():
            format_counts[fp.format_name] = format_counts.get(fp.format_name, 0) + 1

        return {
            "total_fingerprints": len(self._cache),
            "max_cache_size": self._max_cache_size,
            "format_distribution": format_counts,
        }

    def clear(self) -> None:
        self._cache.clear()

    @staticmethod
    def _compute_fingerprint(lines: list[str]) -> str:
        content = "\n".join(line[:200] for line in lines[:20])
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    @staticmethod
    def _score_formats(lines: list[str]) -> dict[str, float]:
        scores: dict[str, float] = {}
        total = len(lines)

        json_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["json_line"].match(line))
        if json_hits > 0:
            scores["json_lines"] = json_hits / total
        else:
            json_file_hits = sum(1 for line in lines[:10] if JSON_FILE_PATTERN.match(line))
            if json_file_hits >= 1:
                scores["json_document"] = min(0.85, json_file_hits / max(min(total, 10), 1))

        xml_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["xml_tag"].match(line))
        if xml_hits > 0:
            scores["xml"] = xml_hits / total

        csv_score = FingerprintEngine._score_csv(lines)
        if csv_score > 0:
            scores["csv"] = csv_score

        syslog_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["syslog"].match(line))
        if syslog_hits > 0:
            scores["syslog"] = syslog_hits / total

        clf_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["apache_clf"].match(line))
        if clf_hits > 0:
            scores["apache_clf"] = clf_hits / total

        logfmt_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["logfmt"].match(line))
        if logfmt_hits > 0:
            scores["logfmt"] = logfmt_hits / total

        kv_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["key_value"].search(line))
        if kv_hits > 0:
            scores["key_value"] = kv_hits / total * 0.8

        hex_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["hex_dump"].match(line))
        if hex_hits > 0:
            scores["hex_dump"] = hex_hits / total

        section_hits = sum(1 for line in lines if STRUCTURE_PATTERNS["section_header"].match(line))
        if section_hits > 0:
            scores["section_delimited"] = section_hits / total

        if not scores:
            scores["plain_text"] = 0.5

        return scores

    @staticmethod
    def _score_csv(lines: list[str]) -> float:
        if len(lines) < 2:
            return 0.0

        header = lines[0]
        if "," not in header:
            return 0.0

        expected = header.count(",") + 1
        if expected < 2:
            return 0.0

        matching = sum(1 for line in lines[1:10] if line.count(",") + 1 == expected)
        data_count = min(len(lines) - 1, 9)
        return matching / data_count if data_count > 0 else 0.0

    @staticmethod
    def _classify_structure(scores: dict[str, float]) -> str:
        if not scores:
            return "unknown"

        best = max(scores, key=lambda k: scores[k])
        if best in {"json_lines", "xml", "csv", "syslog", "apache_clf", "logfmt"}:
            return "structured"
        if best == "json_document":
            return "structured"
        if best in {"key_value", "section_delimited"}:
            return "semi_structured"
        return "unstructured"

    @staticmethod
    def _count_delimiters(lines: list[str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for line in lines[:50]:
            counts["comma"] = counts.get("comma", 0) + line.count(",")
            counts["tab"] = counts.get("tab", 0) + line.count("\t")
            counts["pipe"] = counts.get("pipe", 0) + line.count("|")
            counts["equals"] = counts.get("equals", 0) + line.count("=")
            counts["colon"] = counts.get("colon", 0) + line.count(":")
        return counts

    @staticmethod
    def _similarity_score(fp1: FormatFingerprint, fp2: FormatFingerprint) -> float:
        score = 0.0

        if fp1.format_name == fp2.format_name:
            score += 0.4

        if fp1.structural_class == fp2.structural_class:
            score += 0.2

        if fp1.has_timestamps == fp2.has_timestamps:
            score += 0.1
        if fp1.has_log_levels == fp2.has_log_levels:
            score += 0.1

        length_ratio = min(fp1.avg_line_length, fp2.avg_line_length) / max(fp1.avg_line_length, fp2.avg_line_length, 1)
        score += length_ratio * 0.2

        return score
