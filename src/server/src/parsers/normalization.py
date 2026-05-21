from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

_TSTYPE = datetime | None

# Common timestamp formats
_TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%d/%b/%Y:%H:%M:%S %z",
    "%b %d %H:%M:%S",
]

# Log level keywords
_LOG_LEVEL_KEYWORDS = {
    "trace": "TRACE",
    "debug": "DEBUG",
    "info": "INFO",
    "information": "INFO",
    "notice": "NOTICE",
    "warn": "WARNING",
    "warning": "WARNING",
    "error": "ERROR",
    "err": "ERROR",
    "fatal": "FATAL",
    "critical": "CRITICAL",
    "crit": "CRITICAL",
    "alert": "ALERT",
    "emerg": "EMERGENCY",
    "emergency": "EMERGENCY",
}

# Regex for sanitizing identifiers
_IDENTIFIER_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")
_IDENTIFIER_START_RE = re.compile(r"^[^a-zA-Z_]")


def sanitize_identifier(name: str) -> str:
    """Convert a string into a valid SQL identifier."""
    if not name:
        return "_unnamed"
    # Replace non-alphanumeric/underscore with underscore
    sanitized = _IDENTIFIER_SANITIZE_RE.sub("_", name)
    # Ensure starts with letter or underscore
    sanitized = _IDENTIFIER_START_RE.sub("_", sanitized)
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Strip leading/trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized or "_unnamed"


def unique_identifier(base: str, seen: set[str]) -> str:
    """Ensure an identifier is unique within a set."""
    candidate = base
    counter = 1
    while candidate in seen:
        candidate = f"{base}_{counter}"
        counter += 1
    seen.add(candidate)
    return candidate


def coerce_scalar(value: str) -> Any:
    """Try to coerce a string value into int, float, bool, or datetime."""
    if not value or not value.strip():
        return None

    stripped = value.strip()

    # Boolean
    if stripped.lower() in ("true", "yes", "on"):
        return True
    if stripped.lower() in ("false", "no", "off"):
        return False

    # Timestamp (try before integer—epoch ms looks like a number)
    ts = normalize_iso_timestamp(stripped)
    if ts:
        return ts

    # Integer — guard against PostgreSQL INTEGER overflow (> 2^31-1)
    try:
        ival = int(stripped)
        max_pg_int = 2_147_483_647
        if abs(ival) > max_pg_int:
            # Large integer → keep as string so it maps to TEXT / BIGINT
            return stripped
        return ival
    except ValueError:
        pass

    # Float
    try:
        return float(stripped)
    except ValueError:
        pass

    return stripped


def normalize_iso_timestamp(value: str) -> str | None:
    """Try to parse a timestamp string and return ISO-8601 format.

    Handles epoch milliseconds (13-digit), epoch seconds (10-digit),
    ISO-8601, RFC-2822, and common log formats.
    """
    if not value or not value.strip():
        return None

    stripped = value.strip()

    # Epoch milliseconds — 13 digits, starts around 2001
    if stripped.isdigit() and len(stripped) == 13:
        try:
            ms_val = int(stripped) / 1000.0
            dt = datetime.fromtimestamp(ms_val, tz=timezone.utc)
            return dt.isoformat()
        except (ValueError, OSError):
            pass

    # Epoch seconds — 10 digits, in a reasonable range
    if stripped.isdigit() and len(stripped) == 10:
        try:
            sec_val = int(stripped)
            if 946_684_800 <= sec_val <= 4_102_444_800:  # 2000-2100
                dt = datetime.fromtimestamp(sec_val, tz=timezone.utc)
                return dt.isoformat()
        except (ValueError, OSError):
            pass

    for fmt in _TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(stripped, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    return None


def parse_timestamp(value: Any) -> datetime | None:
    """Parse a timestamp value into a timezone-aware UTC datetime.

    Handles ISO-8601 strings, Unix seconds (int/float 10-digit),
    Unix milliseconds (int/float 13-digit), and ``datetime`` objects.
    Returns ``None`` if the value cannot be parsed.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        # Unix milliseconds (13 digits, roughly post-2001)
        if value > 10_000_000_000:  # > 1000 seconds from epoch
            # Treat as milliseconds
            dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            return dt
        # Unix seconds
        if 946_684_800 <= value <= 4_102_444_800:  # 2000-2100
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            return dt
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        # Epoch milliseconds (13-digit string)
        if stripped.isdigit() and len(stripped) == 13:
            try:
                ms_val = int(stripped) / 1000.0
                dt = datetime.fromtimestamp(ms_val, tz=timezone.utc)
                return dt
            except (ValueError, OSError):
                pass
        # Epoch seconds (10-digit string)
        if stripped.isdigit() and len(stripped) == 10:
            try:
                sec_val = int(stripped)
                if 946_684_800 <= sec_val <= 4_102_444_800:
                    dt = datetime.fromtimestamp(sec_val, tz=timezone.utc)
                    return dt
            except (ValueError, OSError):
                pass
        # Common timestamp formats
        for fmt in _TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(stripped, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return None
    return None


def sanitize_db_value(value: Any) -> Any:
    """Sanitize a value for database storage."""
    if value is None:
        return None
    if isinstance(value, str):
        # Remove null bytes
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {k: sanitize_db_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_db_value(item) for item in value]
    return value


def infer_log_level(text: str) -> str | None:
    """Infer log level from text content.

    Returns the level string (e.g. ``"ERROR"``) or ``None``
    if no level keyword is detected.
    """
    if not text:
        return None

    upper = text.upper()
    for keyword, level in _LOG_LEVEL_KEYWORDS.items():
        if keyword.upper() in upper:
            return level

    return None


def infer_sql_type(values: list[Any]) -> str:
    """Infer SQL type from a list of sample values."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "TEXT"

    # Check if all are booleans
    if all(isinstance(v, bool) for v in non_null):
        return "BOOLEAN"

    # Check if all are integers
    if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
        max_pg_int = 2_147_483_647  # PostgreSQL INTEGER max
        for v in non_null:
            if isinstance(v, int) and abs(v) > max_pg_int:
                return "BIGINT"
        return "INTEGER"

    # Check if all are floats or ints
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
        return "FLOAT"

    # Check if all look like timestamps
    if all(isinstance(v, str) and parse_timestamp(v) for v in non_null):
        return "TIMESTAMP"
    if all(parse_timestamp(v) for v in non_null):
        return "TIMESTAMP"

    return "TEXT"
