from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from langchain_openrouter import ChatOpenRouter
from pydantic import BaseModel, Field

T = TypeVar("T", bound=BaseModel)

DEFAULT_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4.1")
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_TOKENS = 2048


@dataclass
class LlmInvocation(Generic[T]):
    response: T | None = None
    warning: str | None = None


class LlmStructuredColumn(BaseModel):
    name: str
    sql_type: str = "TEXT"
    description: str = ""
    nullable: bool = True


class LlmStructuredSchemaResponse(BaseModel):
    columns: list[LlmStructuredColumn] = Field(default_factory=list)
    summary: str = ""
    event_type_hint: str | None = None
    warnings: list[str] = Field(default_factory=list)


class LlmUnstructuredField(BaseModel):
    name: str
    sql_type: str = "TEXT"
    description: str = ""
    example_values: list[str] = Field(default_factory=list)


class LlmUnstructuredResponse(BaseModel):
    fields: list[LlmUnstructuredField] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class LlmSemiStructuredResponse(BaseModel):
    fields: dict[str, Any] = Field(default_factory=dict)
    format_type: str = "unknown"
    section_map: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


def resolve_openrouter_api_key(api_key: str | None = None) -> str | None:
    if api_key is not None:
        return api_key.strip() or None
    env_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    return env_key or None


def has_openrouter_api_key(api_key: str | None = None) -> bool:
    return bool(resolve_openrouter_api_key(api_key))


def _build_client(
    model: str | None = None,
    api_key: str | None = None,
    temperature: float = DEFAULT_TEMPERATURE,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> ChatOpenRouter:
    resolved_key = resolve_openrouter_api_key(api_key)
    if not resolved_key:
        raise ValueError("OPENROUTER_API_KEY is not set.")

    return ChatOpenRouter(
        model=model or DEFAULT_MODEL,
        temperature=temperature,
        max_tokens=max_tokens,
        api_key=resolved_key,
    )


def _with_structured_output(client: ChatOpenRouter, schema: type[T]):
    if hasattr(client, "with_structured_output"):
        return client.with_structured_output(schema)
    if hasattr(client, "withStructuredOutput"):
        return client.withStructuredOutput(schema)
    raise RuntimeError("Structured output is not supported by the configured ChatOpenRouter client.")


def _invoke_structured(
    schema: type[T],
    prompt: str,
    system_prompt: str,
    model: str | None = None,
    api_key: str | None = None,
) -> LlmInvocation[T]:
    if not has_openrouter_api_key(api_key):
        return LlmInvocation(warning="OPENROUTER_API_KEY not set; LLM enrichment skipped.")

    try:
        client = _build_client(model=model, api_key=api_key)
        structured_model = _with_structured_output(client, schema)
        response = structured_model.invoke(
            [
                ("system", system_prompt),
                ("human", prompt),
            ]
        )
        if isinstance(response, schema):
            return LlmInvocation(response=response)
        return LlmInvocation(response=schema.model_validate(response))
    except Exception as error:  # noqa: BLE001
        return LlmInvocation(warning=f"LLM invocation failed: {error}")


def infer_structured_schema(
    detected_format: str,
    sample_text: str,
    sample_line_count: int,
    heuristic_summary: str,
    model: str | None = None,
    api_key: str | None = None,
) -> LlmInvocation[LlmStructuredSchemaResponse]:
    system_prompt = (
        "You are an expert log schema inference assistant. "
        "Infer table columns that reduce nulls while preserving semantic meaning."
    )
    prompt = (
        f"Detected format: {detected_format}\n"
        f"Sample line count: {sample_line_count}\n"
        "Existing heuristic columns:\n"
        f"{heuristic_summary}\n\n"
        "Sample logs:\n"
        f"{sample_text}\n\n"
        "Return a concise summary and additional columns only if they are stable across records."
    )
    return _invoke_structured(
        schema=LlmStructuredSchemaResponse,
        prompt=prompt,
        system_prompt=system_prompt,
        model=model,
        api_key=api_key,
    )


def infer_unstructured_fields(
    sample_text: str,
    heuristic_summary: str,
    model: str | None = None,
    api_key: str | None = None,
) -> LlmInvocation[LlmUnstructuredResponse]:
    system_prompt = (
        "You are an expert unstructured log analyst. "
        "Extract robust field candidates that can become table columns with minimal null rates."
    )
    prompt = (
        "Current heuristic fields:\n"
        f"{heuristic_summary}\n\n"
        "Sample unstructured logs:\n"
        f"{sample_text}\n\n"
        "Only suggest stable fields and include clear descriptions."
    )
    return _invoke_structured(
        schema=LlmUnstructuredResponse,
        prompt=prompt,
        system_prompt=system_prompt,
        model=model,
        api_key=api_key,
    )


def extract_semi_structured_fields(
    raw_text: str,
    thinking_level: str = "low",
    context_json: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
) -> LlmInvocation[LlmSemiStructuredResponse]:
    system_prompt = (
        "You extract structured fields from semi-structured logs. "
        "Return normalized key-value fields and section counts."
    )
    prompt = (
        f"Thinking level: {thinking_level}\n"
        f"Context: {context_json or '{}'}\n"
        "Input log:\n"
        f"{raw_text}\n\n"
        "Return stable fields and a detected format type."
    )
    invocation = _invoke_structured(
        schema=LlmSemiStructuredResponse,
        prompt=prompt,
        system_prompt=system_prompt,
        model=model,
        api_key=api_key,
    )

    if invocation.response is None and invocation.warning:
        fallback = _local_semi_structured_fallback(raw_text)
        fallback.warnings.append(invocation.warning)
        return LlmInvocation(response=fallback)

    return invocation


def _local_semi_structured_fallback(raw_text: str) -> LlmSemiStructuredResponse:
    fields: dict[str, Any] = {}
    section_map: dict[str, int] = {}
    current_section = "root"

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("---") and stripped.endswith("---"):
            current_section = stripped.strip("- ").strip() or "section"
            section_map.setdefault(current_section, 0)
            continue

        if "=" in stripped:
            key, _, value = stripped.partition("=")
            key = key.strip().replace(" ", "_")
            fields[key] = _smart_cast(value.strip())
            section_map[current_section] = section_map.get(current_section, 0) + 1
            continue

        if ":" in stripped:
            key, _, value = stripped.partition(":")
            key = key.strip().replace(" ", "_")
            if key:
                fields[key] = _smart_cast(value.strip())
                section_map[current_section] = section_map.get(current_section, 0) + 1

    format_type = "section_delimited" if section_map else "key_value"
    return LlmSemiStructuredResponse(fields=fields, format_type=format_type, section_map=section_map)


def _smart_cast(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"null", "none", ""}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass

    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return value
