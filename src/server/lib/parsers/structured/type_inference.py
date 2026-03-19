"""Semantic type inference for structured data columns.

Provides intelligent type detection beyond simple regex patterns,
including IP addresses, URLs, emails, UUIDs, timestamps, and domain-specific types.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

BASELINE_COLUMN_NAMES: frozenset[str] = frozenset(
    {
        "id",
        "timestamp",
        "timestamp_raw",
        "source",
        "source_type",
        "log_level",
        "event_type",
        "message",
        "raw_text",
        "record_group_id",
        "line_start",
        "line_end",
        "parse_confidence",
        "schema_version",
        "additional_data",
    }
)


class SqlType(str, Enum):
    TEXT = "TEXT"
    INTEGER = "INTEGER"
    REAL = "REAL"


class SemanticType(str, Enum):
    UNKNOWN = "unknown"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    REAL = "real"
    TEXT = "text"
    TIMESTAMP = "timestamp"
    ISO_TIMESTAMP = "iso_timestamp"
    UNIX_TIMESTAMP = "unix_timestamp"
    BOOLEAN_STRING = "boolean_string"
    NULL = "null"
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    EMAIL = "email"
    URL = "url"
    UUID = "uuid"
    SEMVER = "semver"
    HEX = "hex"
    DURATION = "duration"
    FILESIZE = "filesize"
    JSON = "json"
    XML = "xml"
    ARRAY = "array"
    OBJECT = "object"
    ENUM = "enum"
    COUNTRY_CODE = "country_code"
    CURRENCY_CODE = "currency_code"
    LANGUAGE_CODE = "language_code"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    MAC_ADDRESS = "mac_address"
    MD5 = "md5"
    SHA256 = "sha256"
    BASE64 = "base64"
    HTML = "html"
    PATH = "path"
    HOSTNAME = "hostname"
    PORT = "port"
    USER_AGENT = "user_agent"
    HTTP_METHOD = "http_method"
    HTTP_STATUS = "http_status"


@dataclass
class TypeInferenceResult:
    sql_type: SqlType
    semantic_type: SemanticType
    confidence: float
    examples: list[str]
    description: str


PATTERNS: dict[SemanticType, re.Pattern[str]] = {
    SemanticType.BOOLEAN_STRING: re.compile(
        r"^(true|false|yes|no|on|off|1|0|y|n)$", re.IGNORECASE
    ),
    SemanticType.ISO_TIMESTAMP: re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
        r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
    ),
    SemanticType.UNIX_TIMESTAMP: re.compile(r"^1[0-9]{9}$|^1[0-9]{10}$"),
    SemanticType.IPV4: re.compile(
        r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    ),
    SemanticType.IPV6: re.compile(
        r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,7}:$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}$"
        r"|[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}$"
        r"|:(?::[0-9a-fA-F]{1,4}){1,7}$"
        r"|::(?:[fF]{4})?:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    ),
    SemanticType.EMAIL: re.compile(r"^[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}$"),
    SemanticType.URL: re.compile(r"^https?://[^\s]+$"),
    SemanticType.UUID: re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    ),
    SemanticType.SEMVER: re.compile(r"^\d+\.\d+\.\d+(?:-[\w.]+)?(?:\+[\w.]+)?$"),
    SemanticType.HEX: re.compile(r"^0x[0-9a-fA-F]+$|^[0-9a-fA-F]+$"),
    SemanticType.DURATION: re.compile(r"^\d+(?:\.\d+)?(?:ms|s|m|h|d)?$"),
    SemanticType.FILESIZE: re.compile(
        r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB|PB|K|M|G|T|P)?$", re.IGNORECASE
    ),
    SemanticType.MD5: re.compile(r"^[0-9a-fA-F]{32}$"),
    SemanticType.SHA256: re.compile(r"^[0-9a-fA-F]{64}$"),
    SemanticType.BASE64: re.compile(r"^[A-Za-z0-9+/]+=*$"),
    SemanticType.HTML: re.compile(
        r"<[a-zA-Z][^>]*>.*?</[a-zA-Z][^>]*>|<[a-zA-Z][^>]*/?>"
    ),
    SemanticType.PATH: re.compile(r"^(?:/[^\s/]+)+/?$|^[a-zA-Z]:\\[^\s]+$"),
    SemanticType.HOSTNAME: re.compile(
        r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)"
        r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
    ),
    SemanticType.PORT: re.compile(
        r"^(?:\d{1,3}:)?\d{1,5}$|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}$"
    ),
    SemanticType.USER_AGENT: re.compile(
        r"Mozilla/|Opera/|Chrome/|Safari/|Firefox/|Edge/|MSIE "
    ),
    SemanticType.HTTP_METHOD: re.compile(
        r"^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)$"
    ),
    SemanticType.HTTP_STATUS: re.compile(r"^[1-5]\d{2}$"),
    SemanticType.COUNTRY_CODE: re.compile(r"^[A-Z]{2}$|^[A-Z]{3}$"),
    SemanticType.CURRENCY_CODE: re.compile(r"^[A-Z]{3}$"),
    SemanticType.LANGUAGE_CODE: re.compile(r"^[a-z]{2}$|^[a-z]{2}-[A-Z]{2}$"),
    SemanticType.LATITUDE: re.compile(r"^-?\d{1,2}\.\d+$"),
    SemanticType.LONGITUDE: re.compile(r"^-?\d{1,3}\.\d+$"),
    SemanticType.MAC_ADDRESS: re.compile(r"^(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$"),
}

NAME_BASED_TYPES: dict[str, SemanticType] = {
    "id": SemanticType.TEXT,
    "uuid": SemanticType.UUID,
    "guid": SemanticType.UUID,
    "email": SemanticType.EMAIL,
    "email_address": SemanticType.EMAIL,
    "url": SemanticType.URL,
    "uri": SemanticType.URL,
    "link": SemanticType.URL,
    "ip": SemanticType.IPV4,
    "ip_address": SemanticType.IPV4,
    "ipv4": SemanticType.IPV4,
    "ipv6": SemanticType.IPV6,
    "mac": SemanticType.MAC_ADDRESS,
    "mac_address": SemanticType.MAC_ADDRESS,
    "port": SemanticType.PORT,
    "host": SemanticType.HOSTNAME,
    "hostname": SemanticType.HOSTNAME,
    "server": SemanticType.HOSTNAME,
    "client": SemanticType.HOSTNAME,
    "user_agent": SemanticType.USER_AGENT,
    "timestamp": SemanticType.ISO_TIMESTAMP,
    "time": SemanticType.ISO_TIMESTAMP,
    "datetime": SemanticType.ISO_TIMESTAMP,
    "date": SemanticType.ISO_TIMESTAMP,
    "created_at": SemanticType.ISO_TIMESTAMP,
    "updated_at": SemanticType.ISO_TIMESTAMP,
    "deleted_at": SemanticType.ISO_TIMESTAMP,
    "expires_at": SemanticType.ISO_TIMESTAMP,
    "start_time": SemanticType.ISO_TIMESTAMP,
    "end_time": SemanticType.ISO_TIMESTAMP,
    "duration": SemanticType.DURATION,
    "latency": SemanticType.DURATION,
    "response_time": SemanticType.DURATION,
    "ttl": SemanticType.DURATION,
    "age": SemanticType.DURATION,
    "size": SemanticType.FILESIZE,
    "length": SemanticType.INTEGER,
    "count": SemanticType.INTEGER,
    "total": SemanticType.INTEGER,
    "max": SemanticType.INTEGER,
    "min": SemanticType.INTEGER,
    "sum": SemanticType.INTEGER,
    "avg": SemanticType.REAL,
    "price": SemanticType.REAL,
    "rate": SemanticType.REAL,
    "ratio": SemanticType.REAL,
    "percent": SemanticType.REAL,
    "percentage": SemanticType.REAL,
    "version": SemanticType.SEMVER,
    "semver": SemanticType.SEMVER,
    "md5": SemanticType.MD5,
    "sha256": SemanticType.SHA256,
    "hash": SemanticType.HEX,
    "country": SemanticType.COUNTRY_CODE,
    "country_code": SemanticType.COUNTRY_CODE,
    "currency": SemanticType.CURRENCY_CODE,
    "currency_code": SemanticType.CURRENCY_CODE,
    "language": SemanticType.LANGUAGE_CODE,
    "lang": SemanticType.LANGUAGE_CODE,
    "status": SemanticType.TEXT,
    "state": SemanticType.TEXT,
    "type": SemanticType.TEXT,
    "kind": SemanticType.TEXT,
    "category": SemanticType.TEXT,
    "name": SemanticType.TEXT,
    "title": SemanticType.TEXT,
    "description": SemanticType.TEXT,
    "message": SemanticType.TEXT,
    "error": SemanticType.TEXT,
    "error_message": SemanticType.TEXT,
    "method": SemanticType.HTTP_METHOD,
    "path": SemanticType.PATH,
    "file": SemanticType.PATH,
    "file_path": SemanticType.PATH,
    "latitude": SemanticType.LATITUDE,
    "lat": SemanticType.LATITUDE,
    "longitude": SemanticType.LONGITUDE,
    "lng": SemanticType.LONGITUDE,
    "lon": SemanticType.LONGITUDE,
    "coordinate": SemanticType.TEXT,
    "location": SemanticType.TEXT,
    "address": SemanticType.TEXT,
    "phone": SemanticType.TEXT,
    "phone_number": SemanticType.TEXT,
    "password": SemanticType.TEXT,
    "token": SemanticType.TEXT,
    "api_key": SemanticType.TEXT,
    "secret": SemanticType.TEXT,
    "key": SemanticType.TEXT,
    "value": SemanticType.TEXT,
    "data": SemanticType.TEXT,
    "payload": SemanticType.TEXT,
    "body": SemanticType.TEXT,
    "content": SemanticType.TEXT,
    "result": SemanticType.TEXT,
    "response": SemanticType.TEXT,
    "request": SemanticType.TEXT,
    "user_id": SemanticType.TEXT,
    "user": SemanticType.TEXT,
    "username": SemanticType.TEXT,
    "user_name": SemanticType.TEXT,
    "first_name": SemanticType.TEXT,
    "last_name": SemanticType.TEXT,
    "full_name": SemanticType.TEXT,
    "role": SemanticType.TEXT,
    "permission": SemanticType.TEXT,
    "group": SemanticType.TEXT,
    "team": SemanticType.TEXT,
    "organization": SemanticType.TEXT,
    "company": SemanticType.TEXT,
    "project": SemanticType.TEXT,
    "app": SemanticType.TEXT,
    "application": SemanticType.TEXT,
    "service": SemanticType.TEXT,
    "environment": SemanticType.TEXT,
    "region": SemanticType.TEXT,
    "zone": SemanticType.TEXT,
    "cluster": SemanticType.TEXT,
    "namespace": SemanticType.TEXT,
    "pod": SemanticType.TEXT,
    "container": SemanticType.TEXT,
    "image": SemanticType.TEXT,
    "tag": SemanticType.TEXT,
    "label": SemanticType.TEXT,
    "annotation": SemanticType.TEXT,
    "event": SemanticType.TEXT,
    "event_type": SemanticType.TEXT,
    "action": SemanticType.TEXT,
    "operation": SemanticType.TEXT,
    "resource": SemanticType.TEXT,
    "config": SemanticType.TEXT,
    "settings": SemanticType.TEXT,
    "metadata": SemanticType.TEXT,
    "attributes": SemanticType.TEXT,
    "extra": SemanticType.TEXT,
    "options": SemanticType.TEXT,
    "params": SemanticType.TEXT,
    "query": SemanticType.TEXT,
    "header": SemanticType.TEXT,
    "headers": SemanticType.TEXT,
    "cookie": SemanticType.TEXT,
    "session": SemanticType.TEXT,
    "transaction": SemanticType.TEXT,
    "order": SemanticType.INTEGER,
    "priority": SemanticType.INTEGER,
    "score": SemanticType.REAL,
    "rating": SemanticType.REAL,
    "temperature": SemanticType.REAL,
    "pressure": SemanticType.REAL,
    "humidity": SemanticType.REAL,
    "voltage": SemanticType.REAL,
    "current": SemanticType.REAL,
    "power": SemanticType.REAL,
    "energy": SemanticType.REAL,
    "frequency": SemanticType.REAL,
}


def infer_type(column_name: str, examples: list[str]) -> TypeInferenceResult:
    """Infer the semantic type of a column based on its name and example values.

    Args:
        column_name: The name of the column (used for name-based inference)
        examples: List of example values (non-empty strings, max 100 chars each)

    Returns:
        TypeInferenceResult with SQL type, semantic type, confidence, and description
    """
    if not examples:
        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.UNKNOWN,
            confidence=0.0,
            examples=[],
            description="No example values to infer type from.",
        )

    filtered_examples = [
        ex for ex in examples if ex is not None and str(ex).strip() != ""
    ]
    if not filtered_examples:
        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.UNKNOWN,
            confidence=0.0,
            examples=[],
            description="All example values are null or empty.",
        )

    name_lower = column_name.lower().strip()

    if name_lower in NAME_BASED_TYPES:
        semantic_type = NAME_BASED_TYPES[name_lower]
        sql_type = _semantic_to_sql_type(semantic_type)
        confidence = 0.7 if semantic_type in PATTERNS else 0.6
        matches = _count_matches(filtered_examples, semantic_type)
        if matches == len(filtered_examples):
            confidence = min(confidence + 0.2, 0.95)
        return TypeInferenceResult(
            sql_type=sql_type,
            semantic_type=semantic_type,
            confidence=confidence,
            examples=filtered_examples[:5],
            description=_get_description(semantic_type, column_name),
        )

    semantic_types_score: dict[SemanticType, tuple[int, float]] = {}
    for semantic_type, pattern in PATTERNS.items():
        matches = _count_matches(filtered_examples, semantic_type)
        if matches > 0:
            match_ratio = matches / len(filtered_examples)
            confidence_boost = match_ratio * 0.5
            semantic_types_score[semantic_type] = (
                matches,
                min(0.5 + confidence_boost, 0.95),
            )

    if not semantic_types_score:
        numeric_result = _try_numeric_type(filtered_examples)
        if numeric_result:
            return numeric_result

        bool_result = _try_boolean_type(filtered_examples)
        if bool_result:
            return bool_result

        json_result = _try_json_type(filtered_examples)
        if json_result:
            return json_result

        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.TEXT,
            confidence=0.5,
            examples=filtered_examples[:5],
            description=f"Generic text field; no specific type pattern detected.",
        )

    best_type = max(semantic_types_score.items(), key=lambda x: (x[1][0], x[1][1]))
    semantic_type = best_type[0]
    _, confidence = best_type[1]

    return TypeInferenceResult(
        sql_type=_semantic_to_sql_type(semantic_type),
        semantic_type=semantic_type,
        confidence=confidence,
        examples=filtered_examples[:5],
        description=_get_description(semantic_type, column_name),
    )


def _count_matches(examples: list[str], semantic_type: SemanticType) -> int:
    """Count how many examples match the given semantic type pattern."""
    if semantic_type not in PATTERNS:
        return 0
    pattern = PATTERNS[semantic_type]
    count = 0
    for ex in examples:
        if pattern.match(str(ex).strip()):
            count += 1
    return count


def _semantic_to_sql_type(semantic_type: SemanticType) -> SqlType:
    """Map semantic type to SQL type."""
    if semantic_type in (
        SemanticType.INTEGER,
        SemanticType.UNIX_TIMESTAMP,
        SemanticType.PORT,
        SemanticType.HTTP_STATUS,
    ):
        return SqlType.INTEGER
    if semantic_type in (
        SemanticType.REAL,
        SemanticType.LATITUDE,
        SemanticType.LONGITUDE,
        SemanticType.DURATION,
    ):
        return SqlType.REAL
    if semantic_type in (
        SemanticType.BOOLEAN,
        SemanticType.BOOLEAN_STRING,
    ):
        return SqlType.TEXT
    return SqlType.TEXT


def _get_description(semantic_type: SemanticType, column_name: str) -> str:
    """Generate a human-readable description for the inferred type."""
    descriptions = {
        SemanticType.BOOLEAN_STRING: f"Boolean-like string values (true/false, yes/no, on/off, 1/0)",
        SemanticType.ISO_TIMESTAMP: f"ISO 8601 timestamp values",
        SemanticType.UNIX_TIMESTAMP: f"Unix timestamp (seconds since epoch)",
        SemanticType.IPV4: f"IPv4 address",
        SemanticType.IPV6: f"IPv6 address",
        SemanticType.EMAIL: f"Email address",
        SemanticType.URL: f"URL or URI",
        SemanticType.UUID: f"Universally unique identifier",
        SemanticType.SEMVER: f"Semantic version string",
        SemanticType.HEX: f"Hexadecimal value",
        SemanticType.DURATION: f"Duration (time interval)",
        SemanticType.FILESIZE: f"File size value",
        SemanticType.JSON: f"JSON-encoded string",
        SemanticType.XML: f"XML-encoded string",
        SemanticType.ARRAY: f"Array or list (JSON array)",
        SemanticType.OBJECT: f"Object or dictionary (JSON object)",
        SemanticType.ENUM: f"Enumeration value from a fixed set",
        SemanticType.COUNTRY_CODE: f"ISO country code",
        SemanticType.CURRENCY_CODE: f"ISO currency code",
        SemanticType.LANGUAGE_CODE: f"ISO language code",
        SemanticType.LATITUDE: f"Geographic latitude coordinate",
        SemanticType.LONGITUDE: f"Geographic longitude coordinate",
        SemanticType.MAC_ADDRESS: f"MAC hardware address",
        SemanticType.MD5: f"MD5 hash digest",
        SemanticType.SHA256: f"SHA-256 hash digest",
        SemanticType.BASE64: f"Base64-encoded string",
        SemanticType.HTML: f"HTML markup",
        SemanticType.PATH: f"File or directory path",
        SemanticType.HOSTNAME: f"Network hostname",
        SemanticType.PORT: f"Network port number",
        SemanticType.USER_AGENT: f"HTTP User-Agent string",
        SemanticType.HTTP_METHOD: f"HTTP request method",
        SemanticType.HTTP_STATUS: f"HTTP response status code",
    }
    base = descriptions.get(semantic_type, f"Field with {semantic_type.value} type")
    return f"{base} inferred from column '{column_name}'"


def _try_numeric_type(examples: list[str]) -> TypeInferenceResult | None:
    """Try to infer numeric types (INTEGER or REAL)."""
    int_count = 0
    float_count = 0

    for ex in examples:
        s = str(ex).strip()
        if re.match(r"^-?\d+$", s):
            int_count += 1
        elif re.match(r"^-?[\d.eE+-]+$", s):
            try:
                float(s)
                float_count += 1
            except ValueError:
                continue

    total = len(examples)
    if int_count == total:
        return TypeInferenceResult(
            sql_type=SqlType.INTEGER,
            semantic_type=SemanticType.INTEGER,
            confidence=0.85,
            examples=examples[:5],
            description="Integer values",
        )
    if float_count == total:
        return TypeInferenceResult(
            sql_type=SqlType.REAL,
            semantic_type=SemanticType.REAL,
            confidence=0.85,
            examples=examples[:5],
            description="Floating-point numeric values",
        )
    if int_count + float_count >= total * 0.8:
        return TypeInferenceResult(
            sql_type=SqlType.REAL,
            semantic_type=SemanticType.REAL,
            confidence=0.6,
            examples=examples[:5],
            description="Numeric values (mixed integer and float)",
        )
    return None


def _try_boolean_type(examples: list[str]) -> TypeInferenceResult | None:
    """Try to infer boolean types."""
    bool_pattern = PATTERNS[SemanticType.BOOLEAN_STRING]
    bool_count = sum(1 for ex in examples if bool_pattern.match(str(ex).strip()))
    if bool_count >= len(examples) * 0.8:
        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.BOOLEAN_STRING,
            confidence=0.75,
            examples=examples[:5],
            description="Boolean-like string values (true/false, yes/no, on/off, 1/0)",
        )
    return None


def _try_json_type(examples: list[str]) -> TypeInferenceResult | None:
    """Try to infer JSON-encoded types."""
    import json

    json_count = 0
    for ex in examples:
        s = str(ex).strip()
        try:
            parsed = json.loads(s)
            if isinstance(parsed, (dict, list)):
                json_count += 1
        except (json.JSONDecodeError, ValueError):
            continue

    if json_count >= len(examples) * 0.8:
        first_parsed = None
        for ex in examples:
            try:
                first_parsed = json.loads(str(ex).strip())
                break
            except (json.JSONDecodeError, ValueError):
                continue

        if isinstance(first_parsed, list):
            return TypeInferenceResult(
                sql_type=SqlType.TEXT,
                semantic_type=SemanticType.ARRAY,
                confidence=0.8,
                examples=examples[:5],
                description="JSON array (stored as serialized TEXT)",
            )
        if isinstance(first_parsed, dict):
            return TypeInferenceResult(
                sql_type=SqlType.TEXT,
                semantic_type=SemanticType.OBJECT,
                confidence=0.8,
                examples=examples[:5],
                description="JSON object (stored as serialized TEXT)",
            )

    return None


def infer_columns_from_records(
    records: list[dict[str, Any]],
    max_sample: int = 100,
) -> list[tuple[str, SqlType, SemanticType, float, list[str]]]:
    """Infer column definitions from a list of record dictionaries.

    Args:
        records: List of dictionaries representing records
        max_sample: Maximum number of records to sample for type inference

    Returns:
        List of tuples: (column_name, sql_type, semantic_type, confidence, examples)
    """
    if not records:
        return []

    key_examples: dict[str, list[str]] = {}
    key_counts: dict[str, int] = {}

    for row in records[:max_sample]:
        for k, v in row.items():
            if k in BASELINE_COLUMN_NAMES:
                continue
            key_counts[k] = key_counts.get(k, 0) + 1
            examples = key_examples.setdefault(k, [])
            if len(examples) < 5 and v is not None:
                examples.append(str(v)[:100])

    threshold = max(1, len(records[:max_sample]) // 10)

    results: list[tuple[str, SqlType, SemanticType, float, list[str]]] = []
    for k, count in key_counts.items():
        if count >= threshold:
            examples = key_examples.get(k, [])
            inference = infer_type(k, examples)
            results.append(
                (
                    k,
                    inference.sql_type,
                    inference.semantic_type,
                    inference.confidence,
                    inference.examples,
                )
            )

    return results
