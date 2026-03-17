import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

_raw_url = os.getenv("DATABASE_URL", "").strip()
_database_token = os.getenv("DATABASE_TOKEN", os.getenv("TURSO_AUTH_TOKEN", "")).strip()
_is_turso_production = bool(_raw_url and _database_token)
_local_store_dir = "store"
_local_database_path = os.path.join(_local_store_dir, "database.db")
_local_database_url = f"sqlite:///./{_local_database_path}"


def _normalize_turso_url(raw_url: str) -> str:
    if raw_url.startswith("libsql://"):
        normalized = raw_url.replace("libsql://", "sqlite+libsql://", 1)
    elif raw_url.startswith("sqlite+libsql://"):
        normalized = raw_url
    else:
        raise ValueError("DATABASE_URL must start with libsql:// or sqlite+libsql:// when DATABASE_TOKEN is set.")

    parsed_url = urlsplit(normalized)
    query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
    query.setdefault("secure", "true")
    return urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, urlencode(query), parsed_url.fragment))


def _has_any_rows(database_engine, table_names: list[str]) -> bool:
    metadata = MetaData()
    metadata.reflect(bind=database_engine, only=table_names)

    with database_engine.connect() as database_connection:
        for table_name in table_names:
            table = metadata.tables.get(table_name)
            if table is None:
                continue
            first_row = database_connection.execute(select(table).limit(1)).first()
            if first_row is not None:
                return True
    return False


def _migrate_local_sqlite_to_turso_if_needed() -> None:
    if not _is_turso_production:
        return

    if not os.path.exists(_local_database_path):
        return

    source_engine = create_engine(_local_database_url, connect_args={"check_same_thread": False})
    try:
        source_tables = set(inspect(source_engine).get_table_names())
        target_tables = set(inspect(engine).get_table_names())
        table_names = [
            table.name
            for table in Base.metadata.sorted_tables
            if table.name in source_tables and table.name in target_tables
        ]

        if not table_names:
            return

        if _has_any_rows(engine, table_names):
            return

        source_metadata = MetaData()
        source_metadata.reflect(bind=source_engine, only=table_names)
        target_metadata = MetaData()
        target_metadata.reflect(bind=engine, only=table_names)

        with source_engine.connect() as source_connection, engine.begin() as target_connection:
            for table_name in table_names:
                source_table = source_metadata.tables.get(table_name)
                target_table: Table | None = target_metadata.tables.get(table_name)

                if source_table is None or target_table is None:
                    continue

                source_rows = source_connection.execute(select(source_table)).mappings().all()
                if source_rows:
                    target_connection.execute(target_table.insert(), [dict(row) for row in source_rows])
    except SQLAlchemyError as error:
        raise RuntimeError(f"Failed to migrate local SQLite data to Turso: {error}") from error
    finally:
        source_engine.dispose()


# Use Turso in production when both URL and token are set.
if _is_turso_production:
    DATABASE_URL = _normalize_turso_url(_raw_url)
    _connect_args = {"auth_token": _database_token}
# Fall back to Postgres if configured without Turso token.
elif _raw_url.startswith("postgresql://") or _raw_url.startswith("postgres://"):
    DATABASE_URL = _raw_url
    _connect_args = {}
else:
    os.makedirs(_local_store_dir, exist_ok=True)
    DATABASE_URL = _local_database_url
    _connect_args = {"check_same_thread": False}

# SQLite local mode requires this flag to allow the same connection across multiple threads,
# which FastAPI uses internally.

engine = create_engine(DATABASE_URL, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_all_tables() -> None:
    Base.metadata.create_all(bind=engine)
    _migrate_local_sqlite_to_turso_if_needed()
