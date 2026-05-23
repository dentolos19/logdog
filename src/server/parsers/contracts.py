from __future__ import annotations

from enum import Enum
from pathlib import Path
from uuid import uuid4
from typing import Any

from pydantic import BaseModel, Field

INGESTION_SCHEMA_VERSION = "2.0.0"


class StructuralClass(str, Enum):
    STRUCTURED = "structured"
    SEMI_STRUCTURED = "semi_structured"
    UNSTRUCTURED = "unstructured"
    BINARY = "binary"


class ColumnDefinition(BaseModel):
    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True
    primary_key: bool = False


class TableDefinition(BaseModel):
    table_name: str
    display_name: str = ""
    columns: list[ColumnDefinition]
    ddl: str


class ParserPipelineResult(BaseModel):
    table_definitions: list[TableDefinition]
    records: dict[str, list[dict[str, Any]]]
    parser_key: str
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)
    diagnostics: dict[str, Any] = Field(default_factory=dict)

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
    diagnostics: dict[str, Any] = Field(default_factory=dict)


BINARY_PARSER_KEY = "binary_file"


class ParserSupportRequest(BaseModel):
    file_id: str | None = None
    filename: str
    content: str = ""
    mime_type: str | None = None
    is_binary: bool = False


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
        sql_type="TEXT",
        nullable=False,
        primary_key=True,
        description="Log-provided ID or generated UUIDv7 string.",
    ),
    ColumnDefinition(
        name="timestamp",
        sql_type="TIMESTAMP",
        description="Normalized timezone-aware timestamp.",
    ),
    ColumnDefinition(
        name="raw",
        sql_type="TEXT",
        nullable=False,
        description="Complete original log text.",
    ),
    ColumnDefinition(
        name="extra",
        sql_type="TEXT",
        description="JSON blob for sparse or event-specific fields.",
    ),
]

BASELINE_COLUMN_NAMES: frozenset[str] = frozenset(column.name for column in BASELINE_COLUMNS)

# Binary overflow column — used by BinaryFileParser to store raw unparsed bytes
BINARY_OVERFLOW_COLUMN: ColumnDefinition = ColumnDefinition(
    name="raw_binary_overflow",
    sql_type="BYTEA",
    nullable=True,
    description="Raw unparsed binary bytes that could not be decoded as text.",
)

BINARY_OVERFLOW_COLUMN_NAME: str = BINARY_OVERFLOW_COLUMN.name

# ── AI Parser Contracts ────────────────────────────────────────────────────


class AiColumnPlan(BaseModel):
    """AI-suggested column for a parsed log table."""

    name: str = Field(description="Canonical column name")
    type: str = Field(default="TEXT", description="SQL type: TEXT, INTEGER, BIGINT, FLOAT, BOOLEAN, DATETIME")
    description: str = Field(default="", description="Semantic description of the field")
    nullable: bool = True
    source_pattern: str = Field(default="", description="What the AI looked for (e.g. 'machine ID', 'event code')")


class AiSchemaPlan(BaseModel):
    """Full AI-suggested schema plan for a parsed log file."""

    table_name: str = Field(default="", description="Suggested table name (short, kebab-case)")
    display_name: str = Field(default="", description="Human-friendly table name")
    columns: list[AiColumnPlan] = Field(description="Suggested columns")
    confidence: float = Field(ge=0.0, le=1.0, description="Overall confidence in this schema")
    notes: str = Field(default="", description="Any caveats or notes about the extraction")


class AiExtractionBatch(BaseModel):
    """Structured result from AI extraction of one chunk."""

    rows: list[dict[str, Any]] = Field(description="Extracted row objects")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Per-batch confidence")
    warnings: list[str] = Field(default_factory=list, description="Warnings for this batch")


class AiExtractionDiagnostics(BaseModel):
    """Diagnostics for AI-powered parser runs."""

    model: str = ""
    prompt_version: str = ""
    schema_cache_hit: bool = False
    schema_confidence: float = 0.0
    batch_count: int = 0
    total_rows: int = 0
    failed_batch_count: int = 0
    repair_batch_count: int = 0
    average_confidence: float = 0.0
    fallback_reason: str = ""
    json_enriched_count: int = 0
    confidence_components: dict[str, float] = Field(default_factory=dict)
    confidence_formula_version: str = ""


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


def make_display_name(parser_key: str, file_id: str | None, filename: str) -> str:
    base_name = Path(filename).stem
    if ":" in base_name:
        base_name = base_name.split(":")[-1]

    # When only one parser exists (AI Universal), don't label the display name
    if parser_key == "ai_universal":
        return base_name.replace("_", " ").title()

    name_parts = [part for part in [base_name, parser_key] if part]
    combined = " ".join(name_parts)
    return combined.replace("_", " ").title()


def make_megabase_table_name() -> str:
    return str(uuid4())
