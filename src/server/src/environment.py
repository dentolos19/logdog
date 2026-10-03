import os

from dotenv import find_dotenv, load_dotenv
from pydantic import SecretStr

load_dotenv(find_dotenv())


def _get_env_var(key: str, defaultValue: str | None = None) -> SecretStr:
    value = os.environ.get(key)
    if value is None:
        value = defaultValue
    if value is None:
        raise ValueError(f"Environment variable '{key}' is not defined.")
    value = value.strip().strip("'\"").strip()
    if key in {"DATABASE_URL", "MEGABASE_URL"}:
        value = value.replace("postgres://", "postgresql+psycopg://", 1).replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
    return SecretStr(value)


SECRET_KEY = _get_env_var("SECRET_KEY")

DATABASE_URL = _get_env_var("DATABASE_URL")
MEGABASE_URL = _get_env_var("MEGABASE_URL")

OPENROUTER_API_KEY = _get_env_var("OPENROUTER_API_KEY")
OPENROUTER_TITLE = _get_env_var("OPENROUTER_TITLE", "Logdog")
OPENROUTER_REFERER = _get_env_var("OPENROUTER_REFERER", "https://dennise.me")
OPENROUTER_MODEL = _get_env_var("OPENROUTER_MODEL", "openrouter/auto")

AWS_ACCESS_KEY_ID = _get_env_var("AWS_ACCESS_KEY_ID")
AWS_ENDPOINT_URL_S3 = _get_env_var("AWS_ENDPOINT_URL_S3")
AWS_REGION = _get_env_var("AWS_REGION", "us-east-2")
AWS_SECRET_ACCESS_KEY = _get_env_var("AWS_SECRET_ACCESS_KEY")
