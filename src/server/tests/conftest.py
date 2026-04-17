from __future__ import annotations

import os
import sys
from pathlib import Path


ENV_DEFAULTS = {
    "SECRET_KEY": "test-secret",
    "MAIN_DATABASE_URL": "sqlite+pysqlite:///:memory:",
    "SWARM_DATABASE_URL": "sqlite+pysqlite:///:memory:",
    "BUCKET_ENDPOINT_URL": "http://localhost",
    "BUCKET_ACCESS_KEY": "test",
    "BUCKET_SECRET_KEY": "test",
    "BUCKET_NAME": "test",
    "BUCKET_PREFIX": "test",
    "OPENROUTER_API_KEY": "test",
    "OPENROUTER_TITLE": "test",
    "OPENROUTER_REFERER": "http://localhost",
}

for env_key, env_value in ENV_DEFAULTS.items():
    os.environ.setdefault(env_key, env_value)


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
