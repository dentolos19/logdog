from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from parsers.contracts import BASELINE_COLUMN_NAMES


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
    SemanticType.BOOLEAN_STRING: re.compile(r"^(true|false|yes|no|on|off|1|0|y|n)$", re.IGNORECASE),
    SemanticType.ISO_TIMESTAMP: re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
    ),
    SemanticType.UNIX_TIMESTAMP: re.compile(r"^1[0-9]{9}$|^1[0-9]{10}$"),
    SemanticType.IPV4: re.compile(
        r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    ),
    SemanticType.IPV6: re.compile(
        r"^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,7}:$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}$"
        r"|^(?:[0-9a-fA-F]{1,3}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}$"
        r"|^(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}$"
        r"|[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}$"
        r"|:(?::[0-9a-fA-F]{1,4}){1,7}$"
        r"|::(?:[fF]{4})?:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    ),
    SemanticType.EMAIL: re.compile(r"^[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}$"),
    SemanticType.URL: re.compile(r"^https?://[^\s]+$"),
    SemanticType.UUID: re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
    SemanticType.SEMVER: re.compile(r"^\d+\.\d+\.\d+(?:-[\w.]+)?(?:\+[\w.]+)?$"),
    SemanticType.HEX: re.compile(r"^0x[0-9a-fA-F]+$|^[0-9a-fA-F]+$"),
    SemanticType.DURATION: re.compile(r"^\d+(?:\.\d+)?(?:ms|s|m|h|d)?$"),
    SemanticType.FILESIZE: re.compile(r"^\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB|PB|K|M|G|T|P)?$", re.IGNORECASE),
    SemanticType.MD5: re.compile(r"^[0-9a-fA-F]{32}$"),
    SemanticType.SHA256: re.compile(r"^[0-9a-fA-F]{64}$"),
    SemanticType.BASE64: re.compile(r"^[A-Za-z0-9+/]+=*$"),
    SemanticType.HTML: re.compile(r"<[a-zA-Z][^>]*>.*?</[a-zA-Z][^>]*>|<[a-zA-Z][^>]*/?>"),
    SemanticType.PATH: re.compile(r"^(?:/[^\s/]+)+/?$|^[a-zA-Z]:\\[^\s]+$"),
    SemanticType.HOSTNAME: re.compile(
        r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
    ),
    SemanticType.PORT: re.compile(r"^(?:\d{1,3}:)?\d{1,5}$|^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}$"),
    SemanticType.USER_AGENT: re.compile(r"Mozilla/|Opera/|Chrome/|Safari/|Firefox/|Edge/|MSIE "),
    SemanticType.HTTP_METHOD: re.compile(r"^(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)$"),
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
    if not examples:
        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.UNKNOWN,
            confidence=0.0,
            examples=[],
            description="No example values to infer type from.",
        )

    filtered_examples = [example for example in examples if example is not None and str(example).strip() != ""]
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

    scored_types: dict[SemanticType, tuple[int, float]] = {}
    for semantic_type in PATTERNS:
        matches = _count_matches(filtered_examples, semantic_type)
        if matches > 0:
            match_ratio = matches / len(filtered_examples)
            confidence_boost = match_ratio * 0.5
            scored_types[semantic_type] = (matches, min(0.5 + confidence_boost, 0.95))

    if not scored_types:
        numeric_result = _try_numeric_type(filtered_examples)
        if numeric_result is not None:
            return numeric_result

        boolean_result = _try_boolean_type(filtered_examples)
        if boolean_result is not None:
            return boolean_result

        json_result = _try_json_type(filtered_examples)
        if json_result is not None:
            return json_result

        return TypeInferenceResult(
            sql_type=SqlType.TEXT,
            semantic_type=SemanticType.TEXT,
            confidence=0.5,
            examples=filtered_examples[:5],
            description="Generic text field; no specific type pattern detected.",
        )

    semantic_type = max(scored_types, key=lambda item: (scored_types[item][0], scored_types[item][1]))
    confidence = scored_types[semantic_type][1]
    return TypeInferenceResult(
        sql_type=_semantic_to_sql_type(semantic_type),
        semantic_type=semantic_type,
        confidence=confidence,
        examples=filtered_examples[:5],
        description=_get_description(semantic_type, column_name),
    )


def _count_matches(examples: list[str], semantic_type: SemanticType) -> int:
    if semantic_type not in PATTERNS:
        return 0

    pattern = PATTERNS[semantic_type]
    count = 0
    for example in examples:
        if pattern.match(str(example).strip()):
            count += 1
    return count


def _semantic_to_sql_type(semantic_type: SemanticType) -> SqlType:
    if semantic_type in {
        SemanticType.INTEGER,
        SemanticType.UNIX_TIMESTAMP,
        SemanticType.PORT,
        SemanticType.HTTP_STATUS,
    }:
        return SqlType.INTEGER
    if semantic_type in {SemanticType.REAL, SemanticType.LATITUDE, SemanticType.LONGITUDE, SemanticType.DURATION}:
        return SqlType.REAL
    return SqlType.TEXT


def _get_description(semantic_type: SemanticType, column_name: str) -> str:
    descriptions = {
        SemanticType.BOOLEAN_STRING: "Boolean-like string values (true/false, yes/no, on/off, 1/0)",
        SemanticType.ISO_TIMESTAMP: "ISO 8601 timestamp values",
        SemanticType.UNIX_TIMESTAMP: "Unix timestamp (seconds since epoch)",
        SemanticType.IPV4: "IPv4 address",
        SemanticType.IPV6: "IPv6 address",
        SemanticType.EMAIL: "Email address",
        SemanticType.URL: "URL or URI",
        SemanticType.UUID: "Universally unique identifier",
        SemanticType.SEMVER: "Semantic version string",
        SemanticType.HEX: "Hexadecimal value",
        SemanticType.DURATION: "Duration (time interval)",
        SemanticType.FILESIZE: "File size value",
        SemanticType.JSON: "JSON-encoded string",
        SemanticType.XML: "XML-encoded string",
        SemanticType.ARRAY: "Array or list (JSON array)",
        SemanticType.OBJECT: "Object or dictionary (JSON object)",
        SemanticType.ENUM: "Enumeration value from a fixed set",
        SemanticType.COUNTRY_CODE: "ISO country code",
        SemanticType.CURRENCY_CODE: "ISO currency code",
        SemanticType.LANGUAGE_CODE: "ISO language code",
        SemanticType.LATITUDE: "Geographic latitude coordinate",
        SemanticType.LONGITUDE: "Geographic longitude coordinate",
        SemanticType.MAC_ADDRESS: "MAC hardware address",
        SemanticType.MD5: "MD5 hash digest",
        SemanticType.SHA256: "SHA-256 hash digest",
        SemanticType.BASE64: "Base64-encoded string",
        SemanticType.HTML: "HTML markup",
        SemanticType.PATH: "File or directory path",
        SemanticType.HOSTNAME: "Network hostname",
        SemanticType.PORT: "Network port number",
        SemanticType.USER_AGENT: "HTTP User-Agent string",
        SemanticType.HTTP_METHOD: "HTTP request method",
        SemanticType.HTTP_STATUS: "HTTP response status code",
    }

    base = descriptions.get(semantic_type, f"Field with {semantic_type.value} type")
    return f"{base} inferred from column '{column_name}'"


def _try_numeric_type(examples: list[str]) -> TypeInferenceResult | None:
    integer_count = 0
    float_count = 0

    for example in examples:
        value = str(example).strip()
        if re.match(r"^-?\d+$", value):
            integer_count += 1
        elif re.match(r"^-?[\d.eE+-]+$", value):
            try:
                float(value)
                float_count += 1
            except ValueError:
                continue

    total = len(examples)
    if integer_count == total:
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
    if integer_count + float_count >= total * 0.8:
        return TypeInferenceResult(
            sql_type=SqlType.REAL,
            semantic_type=SemanticType.REAL,
            confidence=0.6,
            examples=examples[:5],
            description="Numeric values (mixed integer and float)",
        )
    return None


def _try_boolean_type(examples: list[str]) -> TypeInferenceResult | None:
    bool_pattern = PATTERNS[SemanticType.BOOLEAN_STRING]
    bool_count = sum(1 for example in examples if bool_pattern.match(str(example).strip()))
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
    json_count = 0
    for example in examples:
        value = str(example).strip()
        try:
            parsed = json.loads(value)
            if isinstance(parsed, (dict, list)):
                json_count += 1
        except (json.JSONDecodeError, ValueError):
            continue

    if json_count >= len(examples) * 0.8:
        first_parsed = None
        for example in examples:
            try:
                first_parsed = json.loads(str(example).strip())
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
    if not records:
        return []

    key_examples: dict[str, list[str]] = {}
    key_counts: dict[str, int] = {}

    for row in records[:max_sample]:
        for key, value in row.items():
            if key in BASELINE_COLUMN_NAMES:
                continue
            key_counts[key] = key_counts.get(key, 0) + 1
            examples = key_examples.setdefault(key, [])
            if len(examples) < 5 and value is not None:
                examples.append(str(value)[:100])

    threshold = max(1, len(records[:max_sample]) // 10)
    results: list[tuple[str, SqlType, SemanticType, float, list[str]]] = []

    for key, count in key_counts.items():
        if count < threshold:
            continue
        examples = key_examples.get(key, [])
        inference = infer_type(key, examples)
        results.append((key, inference.sql_type, inference.semantic_type, inference.confidence, inference.examples))

    return results
