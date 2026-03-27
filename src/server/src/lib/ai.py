import logging
import os
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from langchain_openrouter import ChatOpenRouter
from pydantic import BaseModel, Field, SecretStr

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_TITLE = os.getenv("OPENROUTER_TITLE", "Logdog")
OPENROUTER_REFERER = os.getenv("OPENROUTER_REFERER", "https://dennise.me")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "inception/mercury-2")
OPENROUTER_KEONI_MODEL = os.getenv("OPENROUTER_KEONI_MODEL", OPENROUTER_MODEL)
OPENROUTER_VISION_MODEL = os.getenv("OPENROUTER_VISION_MODEL", "anthropic/claude-sonnet-4")

StructuredResponseT = TypeVar("StructuredResponseT", bound=BaseModel)


class LlmColumnSuggestion(BaseModel):
    """A single column suggested or enriched by the LLM."""

    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True


class LlmSchemaResponse(BaseModel):
    """Structured response from the LLM for schema inference."""

    columns: list[LlmColumnSuggestion] = Field(default_factory=list)
    summary: str = ""
    event_type_hint: str = ""
    warnings: list[str] = Field(default_factory=list)


class LlmFieldExtraction(BaseModel):
    """One field extracted by the LLM from unstructured text."""

    name: str
    sql_type: str = "TEXT"
    description: str = ""
    example_values: list[str] = Field(default_factory=list)


class LlmUnstructuredResponse(BaseModel):
    """Structured LLM response for unstructured log analysis."""

    fields: list[LlmFieldExtraction] = Field(default_factory=list)
    summary: str = ""
    event_type_hint: str = ""
    heartbeat_templates: list[str] = Field(
        default_factory=list,
        description="Template strings identified as periodic heartbeats or status pings with no actionable content.",
    )
    warnings: list[str] = Field(default_factory=list)


class LlmStructuredColumn(BaseModel):
    """A column suggested by the LLM for structured parser enrichment."""

    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True
    examples: list[str] = Field(default_factory=list)


class LlmStructuredSchemaResponse(BaseModel):
    """Structured response from the LLM for structured parser enrichment."""

    columns: list[LlmStructuredColumn] = Field(default_factory=list)
    summary: str = ""
    event_type_hint: str = ""
    warnings: list[str] = Field(default_factory=list)


class LlmSemiStructuredResponse(BaseModel):
    """Structured response for semi-structured fallback extraction."""

    fields: dict[str, Any] = Field(default_factory=dict)
    format_type: str = "unknown"
    section_map: dict[str, int] = Field(default_factory=dict)


class LlmKeoniRecordResponse(BaseModel):
    """Structured response for single-record log parsing in keoni pipeline."""

    timestamp: str | None = None
    log_level: str = "INFO"
    event_type: str = "log"
    message: str = ""
    parse_confidence: float = 0.5


class LlmKeoniCommentResponse(BaseModel):
    """Structured response for one-line log commentary."""

    comment: str = ""


@dataclass(frozen=True)
class StructuredInvocationResult(Generic[StructuredResponseT]):
    response: StructuredResponseT | None
    warning: str | None = None


def resolve_openrouter_model(model: str | None = None) -> str:
    return model or OPENROUTER_MODEL


def resolve_keoni_model(model: str | None = None) -> str:
    return model or OPENROUTER_KEONI_MODEL


def resolve_openrouter_vision_model(model: str | None = None) -> str:
    return model or OPENROUTER_VISION_MODEL


def resolve_openrouter_api_key(api_key: str | None = None) -> str:
    return OPENROUTER_API_KEY if api_key is None else api_key


def has_openrouter_api_key(api_key: str | None = None) -> bool:
    return bool(resolve_openrouter_api_key(api_key))


def get_openrouter_client(
    *,
    model: str | None = None,
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> ChatOpenRouter:
    resolved_model = resolve_openrouter_model(model)
    resolved_api_key = resolve_openrouter_api_key(api_key)
    return ChatOpenRouter(
        model=resolved_model,
        api_key=SecretStr(resolved_api_key),
        temperature=temperature,
        max_tokens=max_tokens,
        app_title=OPENROUTER_TITLE,
        app_url=OPENROUTER_REFERER,
    )


def invoke_structured_openrouter(
    messages: list[tuple[str, str]],
    response_model: type[StructuredResponseT],
    *,
    context: str,
    model: str | None = None,
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> StructuredInvocationResult:
    if not has_openrouter_api_key(api_key):
        return StructuredInvocationResult(
            response=None,
            warning="OPENROUTER_API_KEY not set; LLM enrichment skipped.",
        )

    structured_model = get_openrouter_client(
        model=model,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
    ).with_structured_output(
        response_model,
        method="json_schema",
        strict=True,
    )

    try:
        raw_response = structured_model.invoke(messages)
        if isinstance(raw_response, response_model):
            response = raw_response
        else:
            response = response_model.model_validate(raw_response)
        return StructuredInvocationResult(response=response)
    except Exception as exc:
        logger.warning("%s failed, continuing without LLM output: %s", context, exc)
        return StructuredInvocationResult(
            response=None,
            warning=f"LLM enrichment failed ({type(exc).__name__}): {exc}",
        )


def invoke_openrouter_text(
    messages: list[tuple[str, Any]],
    *,
    context: str,
    model: str | None = None,
    api_key: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> StructuredInvocationResult[str]:
    """Invoke OpenRouter and return plain text content."""
    if not has_openrouter_api_key(api_key):
        return StructuredInvocationResult(
            response=None,
            warning="OPENROUTER_API_KEY not set; LLM enrichment skipped.",
        )

    model_client = get_openrouter_client(
        model=model,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    try:
        raw_response = model_client.invoke(messages)
        content = getattr(raw_response, "content", raw_response)
        if isinstance(content, list):
            text_chunks: list[str] = []
            for item in content:
                if isinstance(item, str):
                    text_chunks.append(item)
                elif isinstance(item, dict) and item.get("type") == "text":
                    text_chunks.append(str(item.get("text", "")))
                else:
                    text_chunks.append(str(item))
            response_text = "\n".join(chunk for chunk in text_chunks if chunk).strip()
        else:
            response_text = str(content).strip()

        return StructuredInvocationResult(response=response_text)
    except Exception as exc:
        logger.warning("%s failed, continuing without LLM output: %s", context, exc)
        return StructuredInvocationResult(
            response=None,
            warning=f"LLM enrichment failed ({type(exc).__name__}): {exc}",
        )


def infer_structured_schema(
    *,
    detected_format: str,
    sample_text: str,
    sample_line_count: int,
    heuristic_summary: str,
    api_key: str | None = None,
    model: str | None = None,
) -> StructuredInvocationResult[LlmStructuredSchemaResponse]:
    """Infer structured schema columns from sample text using LLM."""
    format_descriptions = {
        "json": "JSON Lines (newline-delimited JSON objects)",
        "xml": "XML (Extensible Markup Language)",
        "csv": "CSV (Comma-Separated Values)",
        "syslog": "Syslog (RFC 3164)",
        "apache_access": "Apache Combined Log Format",
        "nginx_access": "Nginx Access Log",
        "logfmt": "Logfmt (key=value pairs)",
        "key_value": "Key-Value pairs",
    }

    format_desc = format_descriptions.get(detected_format, detected_format)
    system_prompt = (
        "You are an expert log analyst and database schema designer specializing in structured data.\n"
        "Your task is to analyze raw structured data samples and propose a flat tabular schema\n"
        "suitable for a SQLite table. Focus on:\n"
        "1. Confirming or correcting the heuristic-detected columns.\n"
        "2. Adding any new columns justified by patterns in the data.\n"
        "3. Inferring semantic types (IP addresses, URLs, timestamps, etc.) when evident.\n"
        "4. Writing clear descriptions for each column.\n\n"
        "Rules:\n"
        "- Column names must be lowercase snake_case, valid SQLite identifiers.\n"
        "- Only add columns that are well-justified by the sample data.\n"
        "- sql_type must be one of: TEXT, INTEGER, REAL.\n"
        "- Do NOT include baseline columns (id, timestamp, timestamp_raw, source,\n"
        "  source_type, log_level, event_type, message, raw_text, record_group_id,\n"
        "  line_start, line_end, parse_confidence, schema_version, additional_data).\n"
        "- Provide 2-3 example values for each column when available.\n"
        "- Suggest a brief event_type_hint if the data represents a known event category.\n"
    )

    user_prompt = (
        f"Detected format: {format_desc}\n\n"
        f"Heuristic-detected columns:\n{heuristic_summary or '  (none detected)'}\n\n"
        f"Sample data (first {sample_line_count} lines):\n"
        f"```\n{sample_text}\n```\n\n"
        "Please analyze these samples and return your schema suggestion as JSON."
    )

    return invoke_structured_openrouter(
        [("system", system_prompt), ("human", user_prompt)],
        LlmStructuredSchemaResponse,
        context="LLM structured schema inference",
        model=model,
        api_key=api_key,
        temperature=0.0,
        max_tokens=4096,
    )


def infer_unstructured_fields(
    *,
    sample_text: str,
    heuristic_summary: str,
    api_key: str | None = None,
    model: str | None = None,
) -> StructuredInvocationResult[LlmUnstructuredResponse]:
    """Infer additional unstructured fields using LLM."""
    system_prompt = (
        "You are an expert log analyst specializing in unstructured and semi-structured logs.\n"
        "Analyze the provided raw log samples and identify additional fields that can be\n"
        "extracted into table columns. Focus on:\n"
        "1. Domain-specific identifiers (wafer IDs, tool names, recipe names, etc.)\n"
        "2. Repeated key-value patterns not yet captured.\n"
        "3. Numeric measurements or counters.\n"
        "4. A suggested event_type classification for this log source.\n\n"
        "Rules:\n"
        "- Column names must be lowercase snake_case.\n"
        "- sql_type must be TEXT, INTEGER, or REAL.\n"
        "- Do NOT repeat baseline columns (id, timestamp, timestamp_raw, source,\n"
        "  source_type, log_level, event_type, message, raw_text, record_group_id,\n"
        "  line_start, line_end, parse_confidence, schema_version, additional_data).\n"
        "- Do NOT repeat columns already detected by heuristics.\n"
    )

    user_prompt = (
        f"Already-detected columns:\n{heuristic_summary or '  (none detected)'}\n\n"
        f"Sample unstructured log lines:\n```\n{sample_text}\n```\n\n"
        "Identify any additional extractable fields."
    )

    return invoke_structured_openrouter(
        [("system", system_prompt), ("human", user_prompt)],
        LlmUnstructuredResponse,
        context="LLM unstructured enrichment",
        model=model,
        api_key=api_key,
        temperature=0.0,
        max_tokens=4096,
    )


def extract_semi_structured_fields(
    *,
    raw_text: str,
    thinking_level: str,
    context_json: str,
    api_key: str | None = None,
    model: str | None = None,
) -> StructuredInvocationResult[LlmSemiStructuredResponse]:
    """Extract fields from unknown/semi-structured logs using LLM."""
    budget = {"low": 128, "medium": 512, "high": 2048}.get(thinking_level, 256)
    system_prompt = (
        "You are a log parsing assistant for semiconductor manufacturing equipment.\n"
        "Given a raw log text snippet, extract ALL identifiable fields as a flat JSON object.\n\n"
        "Rules:\n"
        "1. Return ONLY valid JSON, no markdown, no explanations.\n"
        "2. Use snake_case keys.\n"
        "3. Preserve original values.\n"
        "4. For nested structures, flatten with dot notation.\n"
        "5. If a field has a unit, include it as a separate key with suffix _unit.\n"
        "6. Include a format_type field indicating detected format.\n"
        "7. Include a section_map field listing detected sections and their field counts."
    )
    user_prompt = (
        f"Thinking level: {thinking_level} (budget={budget}).\n"
        f"Context JSON: {context_json}\n\n"
        "Extract structured fields from this log:\n\n"
        f"{raw_text}"
    )
    return invoke_structured_openrouter(
        [("system", system_prompt), ("human", user_prompt)],
        LlmSemiStructuredResponse,
        context="Semi-structured AI fallback",
        model=model,
        api_key=api_key,
        temperature=0.1,
        max_tokens=4096,
    )


def analyze_keoni_record(
    *,
    content: str,
    api_key: str | None = None,
    model: str | None = None,
) -> StructuredInvocationResult[LlmKeoniRecordResponse]:
    """Parse a single raw log record into baseline fields for keoni pipeline."""
    prompt = (
        "Analyze log:\n"
        f"{content}\n\n"
        "Return JSON with fields: timestamp, log_level, event_type, message, parse_confidence (0-1)."
    )
    return invoke_structured_openrouter(
        [("human", prompt)],
        LlmKeoniRecordResponse,
        context="Keoni record parsing",
        model=resolve_keoni_model(model),
        api_key=api_key,
        temperature=0.1,
        max_tokens=300,
    )


def comment_keoni_record(
    *,
    message: str,
    log_level: str,
    event_type: str,
    api_key: str | None = None,
    model: str | None = None,
) -> StructuredInvocationResult[LlmKeoniCommentResponse]:
    """Generate a brief one-line commentary for a log record."""
    prompt = (
        f"Log Level: {log_level}\n"
        f"Event Type: {event_type}\n"
        f"Message: {message}\n\n"
        "Briefly comment (1 line) on this log entry."
    )
    return invoke_structured_openrouter(
        [("human", prompt)],
        LlmKeoniCommentResponse,
        context="Keoni log comment generation",
        model=resolve_keoni_model(model),
        api_key=api_key,
        temperature=0.1,
        max_tokens=100,
    )


def extract_text_from_image(
    *,
    page_image_b64: str,
    page_num: int,
    model: str,
    api_key: str | None = None,
) -> StructuredInvocationResult[str]:
    """Extract OCR text from a base64 PNG image using a vision-capable model."""
    messages: list[tuple[str, Any]] = [
        (
            "system",
            "You are an OCR assistant. Extract ALL text from the provided image exactly "
            "as it appears, preserving line breaks, spacing, and formatting. Do not add "
            "commentary; output only the raw extracted text.",
        ),
        (
            "human",
            [
                {"type": "text", "text": f"Extract all text from page {page_num}:"},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{page_image_b64}"},
                },
            ],
        ),
    ]
    return invoke_openrouter_text(
        messages,
        context="PDF OCR page extraction",
        model=resolve_openrouter_vision_model(model),
        api_key=api_key,
        temperature=0.0,
        max_tokens=4096,
    )
