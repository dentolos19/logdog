from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any

MAX_IDENTIFIER_LENGTH = 63
NUMBER_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
ISO_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")


def sanitize_identifier(value: str, max_length: int = MAX_IDENTIFIER_LENGTH) -> str:
    sanitized = "".join(character if character.isalnum() or character == "_" else "_" for character in value.strip())
    sanitized = "_".join(part for part in sanitized.split("_") if part).lower()
    if not sanitized:
        sanitized = "field"
    if sanitized[0].isdigit():
        sanitized = f"field_{sanitized}"

    if len(sanitized) <= max_length:
        return sanitized

    digest = hashlib.md5(sanitized.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    prefix_length = max(1, max_length - 9)
    truncated = sanitized[:prefix_length].rstrip("_") or "field"
    return f"{truncated}_{digest}"


def unique_identifier(base: str, existing: set[str], max_length: int = MAX_IDENTIFIER_LENGTH) -> str:
    candidate = sanitize_identifier(base, max_length=max_length)
    if candidate not in existing:
        return candidate

    index = 2
    while True:
        suffix = f"_{index}"
        prefix_length = max(1, max_length - len(suffix))
        truncated = candidate[:prefix_length].rstrip("_") or "field"
        unique_name = f"{truncated}{suffix}"
        if unique_name not in existing:
            return unique_name
        index += 1


def normalize_iso_timestamp(value: str) -> str | None:
    candidate = value.strip().replace(" ", "T")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.isoformat()


def coerce_scalar(value: Any, preserve_empty: bool = True) -> Any:
    if value is None:
        return None

    if isinstance(value, (bool, int, float)):
        return value

    text = str(value).strip()
    if text == "":
        return "" if preserve_empty else None

    lowered = text.lower()
    if lowered in {"null", "none", "nan", "n/a"}:
        return None
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"pass", "warn", "warning", "fail", "error", "ok"}:
        return lowered.upper().replace("WARNING", "WARN")

    cleaned = text.rstrip("|,;:])}")
    if NUMBER_RE.match(cleaned):
        try:
            if "." in cleaned:
                return float(cleaned)
            return int(cleaned)
        except ValueError:
            pass

    if ISO_TIMESTAMP_RE.match(text):
        normalized = normalize_iso_timestamp(text)
        if normalized is not None:
            return normalized

    return text


def sanitize_db_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {key: sanitize_db_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_db_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_db_value(item) for item in value]
    return value
