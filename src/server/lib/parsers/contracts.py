"""Parser pipeline domain contracts.

These types form the boundary between the classification stage (preprocessor)
and the full parsing stage (parser pipelines). They are independent of the
legacy ``PreprocessorResult`` shape so both can coexist while old records
transition to the new format.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

INGESTION_SCHEMA_VERSION = "2.0.0"


class StructuralClass(str, Enum):
    STRUCTURED = "structured"
    SEMI_STRUCTURED = "semi_structured"
    UNSTRUCTURED = "unstructured"


# ---------------------------------------------------------------------------
# Parser output types
# ---------------------------------------------------------------------------


class ColumnDefinition(BaseModel):
    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True
    primary_key: bool = False


class TableDefinition(BaseModel):
    table_name: str
    columns: list[ColumnDefinition]
    sqlite_ddl: str


class ParserPipelineResult(BaseModel):
    table_definitions: list[TableDefinition]
    # Maps table_name -> list of row dicts. Rows should not include "id"
    # (autoincrement primary key) so SQLite assigns it automatically.
    records: dict[str, list[dict[str, Any]]]
    parser_key: str
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)

    @property
    def row_counts(self) -> dict[str, int]:
        return {name: len(rows) for name, rows in self.records.items()}


# ---------------------------------------------------------------------------
# Classification types
# ---------------------------------------------------------------------------


class FileClassification(BaseModel):
    file_id: str | None = None
    filename: str
    detected_format: str  # DetectedFormat enum value
    structural_class: StructuralClass
    format_confidence: float = Field(ge=0.0, le=1.0)
    line_count: int
    warnings: list[str] = Field(default_factory=list)


class ClassificationResult(BaseModel):
    schema_version: str = INGESTION_SCHEMA_VERSION
    dominant_format: str  # DetectedFormat enum value
    structural_class: StructuralClass
    selected_parser_key: str
    file_classifications: list[FileClassification]
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)


# ---------------------------------------------------------------------------
# Common parser entrypoint contracts
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Shared DDL helpers
# ---------------------------------------------------------------------------

#: Baseline columns present in every parser-generated table.
BASELINE_COLUMNS: list[ColumnDefinition] = [
    ColumnDefinition(
        name="id", sql_type="INTEGER", nullable=False, primary_key=True, description="Auto-incrementing primary key."
    ),
    ColumnDefinition(name="timestamp", sql_type="TEXT", description="Normalized ISO-8601 timestamp, if detectable."),
    ColumnDefinition(
        name="timestamp_raw", sql_type="TEXT", description="Original timestamp string as found in the log."
    ),
    ColumnDefinition(name="source", sql_type="TEXT", description="Source identifier (filename, hostname, service)."),
    ColumnDefinition(name="source_type", sql_type="TEXT", description="Category of source ('file', 'stream', 'api')."),
    ColumnDefinition(name="log_level", sql_type="TEXT", description="Severity level (INFO, WARN, ERROR, etc.)."),
    ColumnDefinition(name="event_type", sql_type="TEXT", description="Classified event type."),
    ColumnDefinition(name="message", sql_type="TEXT", description="Human-readable message content."),
    ColumnDefinition(name="raw_text", sql_type="TEXT", nullable=False, description="Complete original log text."),
    ColumnDefinition(name="record_group_id", sql_type="TEXT", description="Links related multiline records."),
    ColumnDefinition(
        name="line_start", sql_type="INTEGER", description="1-based source line where this record starts."
    ),
    ColumnDefinition(name="line_end", sql_type="INTEGER", description="1-based source line where this record ends."),
    ColumnDefinition(name="parse_confidence", sql_type="REAL", description="Confidence score (0.0–1.0)."),
    ColumnDefinition(name="schema_version", sql_type="TEXT", description="Schema version used to parse this record."),
    ColumnDefinition(
        name="additional_data", sql_type="TEXT", description="JSON blob for extra fields not mapped to columns."
    ),
]

BASELINE_COLUMN_NAMES: frozenset[str] = frozenset(col.name for col in BASELINE_COLUMNS)


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def build_ddl(table_name: str, columns: list[ColumnDefinition]) -> str:
    """Generate a CREATE TABLE IF NOT EXISTS DDL statement."""
    safe_table = _quote_identifier(table_name)
    column_defs: list[str] = []

    for col in columns:
        safe_name = _quote_identifier(col.name)
        parts = [safe_name, col.sql_type]
        if col.primary_key:
            parts.append("PRIMARY KEY AUTOINCREMENT")
        if not col.nullable and not col.primary_key:
            parts.append("NOT NULL")
        column_defs.append("    " + " ".join(parts))

    columns_sql = ",\n".join(column_defs)
    return f"CREATE TABLE IF NOT EXISTS {safe_table} (\n{columns_sql}\n);"


def make_table_name(parser_key: str, file_id: str | None, filename: str) -> str:
    """Build a deterministic, collision-free table name from parser identity + file identity."""
    import re

    if file_id:
        safe_id = re.sub(r"[^a-z0-9]", "", file_id.lower())[:12]
    else:
        safe_name = re.sub(r"[^a-z0-9]", "_", filename.lower()).strip("_")[:20]
        safe_id = safe_name or "file"

    safe_parser = re.sub(r"[^a-z0-9]", "_", parser_key.lower()).strip("_")
    return f"{safe_parser}_{safe_id}"
