from typing import TypeVar

from langchain_openrouter import ChatOpenRouter
from pydantic import BaseModel

from environment import OPENROUTER_API_KEY, OPENROUTER_TITLE, OPENROUTER_REFERER

T = TypeVar("T", bound=BaseModel)

DEFAULT_MODEL = "openrouter/auto"
DEFAULT_TEMPERATURE = 0.5
DEFAULT_MAX_TOKENS = 2048


class GenerativeModel:
    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        temperature: float = DEFAULT_TEMPERATURE,
        max_tokens: int | None = DEFAULT_MAX_TOKENS,
    ):
        self.client = ChatOpenRouter(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=str(OPENROUTER_API_KEY),
            app_title=str(OPENROUTER_TITLE),
            app_url=str(OPENROUTER_REFERER),
        )

    def generate(
        self,
        prompt: str,
        system_prompt: str | None = None,
    ) -> str:
        messages = []
        if system_prompt:
            messages.append(("system", system_prompt))
        messages.append(("human", prompt))

        response = self.client.invoke(messages)
        return response.content  # type: ignore

    def generate_structured(
        self,
        prompt: str,
        schema: type[T],
        system_prompt: str | None = None,
    ) -> T:
        messages = []
        if system_prompt:
            messages.append(("system", system_prompt))
        messages.append(("human", prompt))

        structured_model = self.client.withStructuredOutput(schema)  # type: ignore
        return structured_model.invoke(messages)


def get_generative_model(
    model: str = DEFAULT_MODEL,
    temperature: float = DEFAULT_TEMPERATURE,
    max_tokens: int | None = DEFAULT_MAX_TOKENS,
) -> GenerativeModel:
    return GenerativeModel(model=model, temperature=temperature, max_tokens=max_tokens)
