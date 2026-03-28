import os

from pydantic import SecretStr


def _get_env(key: str) -> SecretStr:
    value = os.environ.get(key)
    if value is None:
        raise ValueError(f"Environment variable '{key}' is not set and no default value provided.")
    return SecretStr(value)


SECRET_KEY = _get_env("SECRET_KEY")

DATABASE_URL = _get_env("MAIN_DATABASE_URL")
MEGABASE_URL = _get_env("SWARM_DATABASE_URL")

BUCKET_ENDPOINT_URL = _get_env("BUCKET_ENDPOINT_URL")
BUCKET_ACCESS_KEY = _get_env("BUCKET_ACCESS_KEY")
BUCKET_SECRET_KEY = _get_env("BUCKET_SECRET_KEY")
BUCKET_NAME = _get_env("BUCKET_NAME")
BUCKET_PREFIX = _get_env("BUCKET_PREFIX")

OPENROUTER_API_KEY = _get_env("OPENROUTER_API_KEY")
OPENROUTER_TITLE = _get_env("OPENROUTER_TITLE")
OPENROUTER_REFERER = _get_env("OPENROUTER_REFERER")
