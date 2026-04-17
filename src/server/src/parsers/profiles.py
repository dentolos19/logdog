from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class LogProfileDefinition(BaseModel):
    name: str
    domain: str = "unknown"
    cache_confidence_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    llm_heuristic_fallback_threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    expected_fields: list[str] = Field(default_factory=list)
    expected_types: dict[str, str] = Field(default_factory=dict)
    hints: dict[str, Any] = Field(default_factory=dict)


def _profiles_file() -> Path:
    return Path(__file__).with_name("log_profiles.yaml")


@lru_cache(maxsize=1)
def load_profiles() -> dict[str, LogProfileDefinition]:
    profiles_path = _profiles_file()
    payload = yaml.safe_load(profiles_path.read_text(encoding="utf-8")) if profiles_path.exists() else {}
    raw_profiles = payload.get("profiles", {}) if isinstance(payload, dict) else {}

    profiles: dict[str, LogProfileDefinition] = {}
    for profile_name, raw_profile in raw_profiles.items():
        profile_payload = dict(raw_profile or {})
        profile_payload["name"] = profile_name
        profile = LogProfileDefinition.model_validate(profile_payload)
        profiles[profile_name] = profile

    if "default" not in profiles:
        profiles["default"] = LogProfileDefinition(name="default")

    return profiles


def get_profile(name: str | None) -> LogProfileDefinition:
    profiles = load_profiles()
    resolved_name = (name or "default").strip() or "default"
    return profiles.get(resolved_name, profiles["default"])


def reload_profiles() -> None:
    load_profiles.cache_clear()
