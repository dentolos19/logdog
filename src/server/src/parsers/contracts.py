from __future__ import annotations

import re
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

INGESTION_SCHEMA_VERSION = "2.0.0"


class StructuralClass(str, Enum):
    STRUCTURED = "structured"
    SEMI_STRUCTURED = "semi_structured"
    UNSTRUCTURED = "unstructured"


class ColumnDefinition(BaseModel):
    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True
    primary_key: bool = False


class TableDefinition(BaseModel):
    table_name: str
    columns: list[ColumnDefinition]
    ddl: str


class ParserPipelineResult(BaseModel):
    table_definitions: list[TableDefinition]
    records: dict[str, list[dict[str, Any]]]
    parser_key: str
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)

    @property
    def row_counts(self) -> dict[str, int]:
        return {table_name: len(rows) for table_name, rows in self.records.items()}


class FileClassification(BaseModel):
    file_id: str | None = None
    filename: str
    detected_format: str
    structural_class: StructuralClass
    format_confidence: float = Field(ge=0.0, le=1.0)
    line_count: int
    warnings: list[str] = Field(default_factory=list)


class ClassificationResult(BaseModel):
    schema_version: str = INGESTION_SCHEMA_VERSION
    dominant_format: str
    structural_class: StructuralClass
    selected_parser_key: str
    file_classifications: list[FileClassification]
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)


class ParserSupportRequest(BaseModel):
    file_id: str | None = None
    filename: str
    content: str
    mime_type: str | None = None


class ParserSupportResult(BaseModel):
    parser_key: str
    supported: bool
    score: float = Field(ge=0.0, le=1.0, default=0.0)
    reasons: list[str] = Field(default_factory=list)
    detected_format: str | None = None
    structural_class: StructuralClass | None = None


class FileParserSelection(BaseModel):
    file_id: str | None = None
    filename: str
    parser_key: str
    score: float = Field(ge=0.0, le=1.0)
    reasons: list[str] = Field(default_factory=list)


BASELINE_COLUMNS: list[ColumnDefinition] = [
    ColumnDefinition(
        name="id",
        sql_type="INTEGER",
        nullable=False,
        primary_key=True,
        description="Auto-incrementing primary key.",
    ),
    ColumnDefinition(
        name="timestamp",
        sql_type="TEXT",
        description="Normalized ISO-8601 timestamp, if detectable.",
    ),
    ColumnDefinition(
        name="timestamp_raw",
        sql_type="TEXT",
        description="Original timestamp string as found in the log.",
    ),
    ColumnDefinition(
        name="source",
        sql_type="TEXT",
        description="Source identifier (filename, hostname, service).",
    ),
    ColumnDefinition(
        name="source_type",
        sql_type="TEXT",
        description="Category of source ('file', 'stream', 'api').",
    ),
    ColumnDefinition(
        name="log_level",
        sql_type="TEXT",
        description="Severity level (INFO, WARN, ERROR, etc.).",
    ),
    ColumnDefinition(name="event_type", sql_type="TEXT", description="Classified event type."),
    ColumnDefinition(name="message", sql_type="TEXT", description="Human-readable message content."),
    ColumnDefinition(
        name="raw_text",
        sql_type="TEXT",
        nullable=False,
        description="Complete original log text.",
    ),
    ColumnDefinition(
        name="record_group_id",
        sql_type="TEXT",
        description="Links related multiline records.",
    ),
    ColumnDefinition(
        name="line_start",
        sql_type="INTEGER",
        description="1-based source line where this record starts.",
    ),
    ColumnDefinition(
        name="line_end",
        sql_type="INTEGER",
        description="1-based source line where this record ends.",
    ),
    ColumnDefinition(
        name="parse_confidence",
        sql_type="REAL",
        description="Confidence score (0.0-1.0).",
    ),
    ColumnDefinition(
        name="schema_version",
        sql_type="TEXT",
        description="Schema version used to parse this record.",
    ),
    ColumnDefinition(
        name="additional_data",
        sql_type="TEXT",
        description="JSON blob for extra fields not mapped to columns.",
    ),
]

BASELINE_COLUMN_NAMES: frozenset[str] = frozenset(column.name for column in BASELINE_COLUMNS)


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def build_ddl(table_name: str, columns: list[ColumnDefinition]) -> str:
    safe_table = _quote_identifier(table_name)
    column_definitions: list[str] = []

    for column in columns:
        safe_name = _quote_identifier(column.name)
        parts = [safe_name, column.sql_type]
        if column.primary_key:
            if column.sql_type.upper() in {"INTEGER", "INT", "BIGINT"}:
                parts = [safe_name, "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"]
            else:
                parts.append("PRIMARY KEY")
        if not column.nullable and not column.primary_key:
            parts.append("NOT NULL")
        column_definitions.append("    " + " ".join(parts))

    columns_sql = ",\n".join(column_definitions)
    return f"CREATE TABLE IF NOT EXISTS {safe_table} (\n{columns_sql}\n);"


def make_table_name(parser_key: str, file_id: str | None, filename: str) -> str:
    if file_id:
        safe_file_id = re.sub(r"[^a-z0-9]", "", file_id.lower())[:12]
    else:
        safe_filename = re.sub(r"[^a-z0-9]", "_", filename.lower()).strip("_")[:20]
        safe_file_id = safe_filename or "file"

    safe_parser_key = re.sub(r"[^a-z0-9]", "_", parser_key.lower()).strip("_")
    return f"{safe_parser_key}_{safe_file_id}"
