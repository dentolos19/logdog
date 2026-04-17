from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ExtractionStrategy(str, Enum):
    PER_LINE = "per_line"
    PER_MULTILINE_BLOCK = "per_multiline_block"
    PER_JSON_OBJECT = "per_json_object"
    PER_XML_ELEMENT = "per_xml_element"
    PER_FILE = "per_file"


class LogFormatCategory(str, Enum):
    STRUCTURED = "structured"
    SEMI_STRUCTURED = "semi_structured"
    UNSTRUCTURED = "unstructured"
    BINARY = "binary"
    UNKNOWN = "unknown"


class LogFormatDomain(str, Enum):
    WEB_SERVER = "web_server"
    SYSTEM = "system"
    APPLICATION = "application"
    SEMICONDUCTOR = "semiconductor"
    NETWORK = "network"
    DATABASE = "database"
    CONTAINER = "container"
    IOT = "iot"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class LlmColumnDefinition(BaseModel):
    name: str
    sql_type: str = Field(default="TEXT", description="SQL column type")
    description: str = Field(default="", description="Human-readable description")
    nullable: bool = Field(default=True)
    is_timestamp: bool = Field(default=False, description="Whether this column contains timestamps")
    is_identifier: bool = Field(default=False, description="Whether this column contains unique identifiers")
    example_values: list[str] = Field(default_factory=list)


class LlmSchemaResponse(BaseModel):
    table_name: str = Field(description="Suggested table name for this log format")
    columns: list[LlmColumnDefinition] = Field(description="Extracted column definitions")
    extraction_strategy: ExtractionStrategy = Field(description="Recommended extraction strategy")
    format_category: LogFormatCategory = Field(description="Structural classification")
    format_domain: LogFormatDomain = Field(default=LogFormatDomain.UNKNOWN, description="Domain hint")
    summary: str = Field(default="", description="Brief summary of the log format")
    warnings: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)


class LlmRecordResponse(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict, description="Extracted key-value fields")
    is_noise: bool = Field(default=False, description="Whether this record is heartbeat/noise")
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)
    warnings: list[str] = Field(default_factory=list)


class LlmFormatDetectionResponse(BaseModel):
    format_name: str = Field(description="Detected format name (e.g., 'json_lines', 'syslog', 'apache_access')")
    format_category: LogFormatCategory = Field(description="Structural classification")
    format_domain: LogFormatDomain = Field(default=LogFormatDomain.UNKNOWN, description="Domain hint")
    extraction_strategy: ExtractionStrategy = Field(description="Recommended extraction strategy")
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)
    reasoning: str = Field(default="", description="Why this format was detected")
    warnings: list[str] = Field(default_factory=list)


class LlmBatchExtractionResponse(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list, description="Extracted records from the batch")
    null_rates: dict[str, float] = Field(default_factory=dict, description="Null rate per field")
    warnings: list[str] = Field(default_factory=list)


class TokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0


class LlmInvocationResult(BaseModel):
    success: bool = True
    response: Any | None = None
    token_usage: TokenUsage = Field(default_factory=TokenUsage)
    warning: str | None = None
    retry_count: int = 0
