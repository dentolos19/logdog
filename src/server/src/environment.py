import os
from dotenv import load_dotenv, find_dotenv
from pydantic import SecretStr

load_dotenv(find_dotenv())


def _get_env_var(key: str, defaultValue: str | None = None) -> SecretStr:
    value = os.environ.get(key)
    if value is None:
        value = defaultValue
    if value is None:
        raise ValueError(f"Environment variable '{key}' is not defined.")
    value = value.strip().strip("'\"").strip()
    return SecretStr(value)


SECRET_KEY = _get_env_var("SECRET_KEY")

DATABASE_URL = _get_env_var("DATABASE_URL")
MEGABASE_URL = _get_env_var("MEGABASE_URL")

OPENROUTER_API_KEY = _get_env_var("OPENROUTER_API_KEY")
OPENROUTER_TITLE = _get_env_var("OPENROUTER_TITLE", "Logdog")
OPENROUTER_REFERER = _get_env_var("OPENROUTER_REFERER", "https://dennise.me")
OPENROUTER_MODEL = _get_env_var("OPENROUTER_MODEL", "openrouter/auto")

STORAGE_URL = _get_env_var("STORAGE_URL", "http://localhost:3000/assets")
