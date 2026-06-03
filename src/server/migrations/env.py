import os
from pathlib import Path

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name:
    load_dotenv(Path(config.config_file_name).parent / ".env")

from lib.megabase_models import metadata as swarm_metadata  # noqa: E402
from lib.models import Base  # noqa: E402


def get_url(key: str) -> str:
    value = os.environ.get(key, "").strip().strip("'\"").strip()
    if not value:
        raise ValueError(f"Environment variable '{key}' is not defined.")
    return value


databases = {
    "main": (get_url("DATABASE_URL"), Base.metadata),
    "swarm": (get_url("MEGABASE_URL"), swarm_metadata),
}


def include_swarm_object(_object, name, type_, reflected, compare_to) -> bool:
    return not (type_ == "table" and reflected and compare_to is None and name not in swarm_metadata.tables)


def run_migrations_offline() -> None:
    for name, (url, metadata) in databases.items():
        context.configure(
            url=url,
            target_metadata=metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
            upgrade_token=f"{name}_upgrades",
            downgrade_token=f"{name}_downgrades",
        )
        with context.begin_transaction():
            context.run_migrations(engine_name=name)


def run_migrations_online() -> None:
    engines = {}
    for name, (url, metadata) in databases.items():
        section = config.get_section(name) or {}
        section["sqlalchemy.url"] = url
        engine = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
        engines[name] = (engine, metadata)

    for name, (engine, metadata) in engines.items():
        with engine.begin() as connection:
            context.configure(
                connection=connection,
                target_metadata=metadata,
                upgrade_token=f"{name}_upgrades",
                downgrade_token=f"{name}_downgrades",
                compare_type=True,
                include_object=include_swarm_object if name == "swarm" else None,
            )
            context.run_migrations(engine_name=name)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
