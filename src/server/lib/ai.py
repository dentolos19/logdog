import logging
import os
from dataclasses import dataclass
from typing import Generic, TypeVar

from langchain_openrouter import ChatOpenRouter
from pydantic import BaseModel, Field, SecretStr

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "inception/mercury-2")

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
    warnings: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class StructuredInvocationResult(Generic[StructuredResponseT]):
    response: StructuredResponseT | None
    warning: str | None = None


def resolve_openrouter_model(model: str | None = None) -> str:
    return model or OPENROUTER_MODEL


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
        app_title="Logdog",
        app_url="https://logdog.dennise.me",
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
