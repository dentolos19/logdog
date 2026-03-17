import json
import os
import re
import sqlite3
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lib.database import SessionLocal
from lib.models import LogGroupSwarmCredential, LogGroupTable
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

STORE_DIR = Path("store")
QUERY_ROW_LIMIT = 200
PREVIEW_ROW_LIMIT = 25
ALLOWED_QUERY_PREFIXES = ("select", "with", "pragma", "explain")
TURSO_API_BASE_URL = "https://api.turso.tech"
DATABASE_NAME_MAX_LENGTH = 64
DATABASE_NAME_PATTERN = re.compile(r"[^a-z0-9-]+")
IS_PRODUCTION = bool(os.getenv("DATABASE_URL", "").strip() and os.getenv("DATABASE_TOKEN", "").strip())

JsonScalar = str | int | float | bool | None
QueryParameters = dict[str, JsonScalar] | list[JsonScalar] | tuple[JsonScalar, ...] | None


class LogDatabaseError(RuntimeError):
    pass


class ReadOnlyQueryError(LogDatabaseError):
    pass


class LogDatabaseSwarm:
    def __init__(self, root_directory: Path = STORE_DIR):
        self.root_directory = root_directory
        self.production_mode = IS_PRODUCTION
        self._engine_cache: dict[str, Engine] = {}

    def database_path(self, log_group_id: str) -> Path:
        return self.root_directory / f"{log_group_id}.sqlite3"

    def ensure_database(
        self,
        log_group_id: str,
        database: Session | None = None,
        provision_if_missing: bool = False,
    ) -> Path:
        if self.production_mode:
            credential = self._get_swarm_credential(log_group_id=log_group_id, database=database)
            if credential is None:
                if not provision_if_missing:
                    raise LogDatabaseError("Swarm database credentials are missing for this log group.")

                if database is None:
                    raise LogDatabaseError("A database session is required to provision a production swarm database.")

                self._create_remote_database_credential(log_group_id=log_group_id, database=database)

            return Path(f"{log_group_id}.turso")

        try:
            self.root_directory.mkdir(parents=True, exist_ok=True)
            database_path = self.database_path(log_group_id)

            with self._connect(database_path) as connection:
                connection.execute("PRAGMA journal_mode=WAL")

            return database_path
        except (OSError, sqlite3.DatabaseError) as exc:
            raise LogDatabaseError(str(exc)) from exc

    def apply_schema(self, log_group_id: str, ddl: str) -> None:
        """Execute a CREATE TABLE DDL statement against the log group's swarm database."""

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.begin() as connection:
                    connection.exec_driver_sql(ddl)
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc
            return

        database_path = self.ensure_database(log_group_id)

        try:
            with self._connect(database_path) as connection:
                connection.execute(ddl)
                connection.commit()
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def insert_rows(
        self,
        log_group_id: str,
        table_name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert rows into a table and return the number inserted.

        The ``id`` column is excluded from the insert so that SQLite's
        AUTOINCREMENT fills it in. Any non-scalar values (dicts / lists)
        are JSON-encoded before insertion.  Returns the count of rows
        actually inserted.
        """
        if not rows:
            return 0

        database_path = self.ensure_database(log_group_id)
        safe_table = self._quote_identifier(table_name)

        # Collect unique column names across all rows (excluding PK).
        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(k for k in row if k != "id")

        columns = sorted(all_keys)
        if not columns:
            return 0

        safe_cols = [self._quote_identifier(c) for c in columns]
        col_list = ", ".join(safe_cols)
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO {safe_table} ({col_list}) VALUES ({placeholders})"

        def _coerce(value: Any) -> JsonScalar:
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            return value  # type: ignore[return-value]

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.begin() as connection:
                    inserted = 0
                    for row in rows:
                        values = tuple(_coerce(row.get(col)) for col in columns)
                        connection.exec_driver_sql(sql, values)
                        inserted += 1
                    return inserted
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        try:
            with self._connect(database_path) as connection:
                inserted = 0
                for row in rows:
                    values = [_coerce(row.get(col)) for col in columns]
                    connection.execute(sql, values)
                    inserted += 1
                connection.commit()
                return inserted
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def apply_schema_and_insert(
        self,
        log_group_id: str,
        ddl: str,
        table_name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Apply a DDL statement and insert rows in a single connection.

        Returns the number of rows inserted.  The DDL is applied first
        (idempotent via ``CREATE TABLE IF NOT EXISTS``), then all rows are
        inserted in a single transaction.
        """
        if not rows:
            self.apply_schema(log_group_id, ddl)
            return 0

        database_path = self.ensure_database(log_group_id)
        safe_table = self._quote_identifier(table_name)

        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(k for k in row if k != "id")

        columns = sorted(all_keys)
        if not columns:
            return 0

        safe_cols = [self._quote_identifier(c) for c in columns]
        col_list = ", ".join(safe_cols)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO {safe_table} ({col_list}) VALUES ({placeholders})"

        def _coerce(value: Any) -> JsonScalar:
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            return value  # type: ignore[return-value]

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.begin() as connection:
                    connection.exec_driver_sql(ddl)
                    inserted = 0
                    for row in rows:
                        values = tuple(_coerce(row.get(col)) for col in columns)
                        connection.exec_driver_sql(insert_sql, values)
                        inserted += 1
                    return inserted
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        try:
            with self._connect(database_path) as connection:
                connection.execute(ddl)
                inserted = 0
                for row in rows:
                    values = [_coerce(row.get(col)) for col in columns]
                    connection.execute(insert_sql, values)
                    inserted += 1
                connection.commit()
                return inserted
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def delete_database(self, log_group_id: str, database: Session | None = None) -> None:
        if self.production_mode:
            credential = self._get_swarm_credential(log_group_id=log_group_id, database=database)
            if credential is None:
                return

            api_key, organization_slug, _, _ = self._require_turso_api_config()

            try:
                self._turso_request(
                    method="DELETE",
                    path=f"/v1/organizations/{organization_slug}/databases/{credential.database_name}",
                    api_key=api_key,
                )
            except LogDatabaseError as exc:
                if "HTTP 404" not in str(exc):
                    raise

            self._dispose_remote_engine(log_group_id)

            if database is not None:
                credential_in_session = (
                    database.query(LogGroupSwarmCredential)
                    .filter(LogGroupSwarmCredential.log_id == log_group_id)
                    .first()
                )
                if credential_in_session is not None:
                    database.delete(credential_in_session)
                    database.flush()
                return

            scoped_session = SessionLocal()
            try:
                credential_in_session = (
                    scoped_session.query(LogGroupSwarmCredential)
                    .filter(LogGroupSwarmCredential.log_id == log_group_id)
                    .first()
                )
                if credential_in_session is not None:
                    scoped_session.delete(credential_in_session)
                    scoped_session.commit()
            except SQLAlchemyError as exc:
                scoped_session.rollback()
                raise LogDatabaseError(str(exc)) from exc
            finally:
                scoped_session.close()

            return

        database_path = self.database_path(log_group_id)

        try:
            if database_path.exists():
                database_path.unlink()

            wal_path = database_path.with_name(f"{database_path.name}-wal")
            shm_path = database_path.with_name(f"{database_path.name}-shm")

            if wal_path.exists():
                wal_path.unlink()
            if shm_path.exists():
                shm_path.unlink()
        except OSError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def sync_table_metadata(self, database: Session, log_group_id: str) -> list[dict[str, Any]]:
        self.ensure_database(log_group_id, database=database)
        tables = self.list_tables(log_group_id)
        existing_tables = {
            table.name: table
            for table in database.query(LogGroupTable).filter(LogGroupTable.log_id == log_group_id).all()
        }
        discovered_names = {table["name"] for table in tables}

        for table_name, table_record in existing_tables.items():
            if table_name not in discovered_names:
                database.delete(table_record)

        for table in tables:
            serialized_columns = json.dumps(table["columns"])
            table_record = existing_tables.get(table["name"])
            is_normalized = 1 if table["name"] == "logs" else 0

            if table_record is None:
                database.add(
                    LogGroupTable(
                        log_id=log_group_id,
                        name=table["name"],
                        columns=serialized_columns,
                        is_normalized=is_normalized,
                    )
                )
                continue

            table_record.columns = serialized_columns
            table_record.is_normalized = is_normalized

        database.flush()
        return tables

    def list_tables(self, log_group_id: str) -> list[dict[str, Any]]:
        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.connect() as connection:
                    table_rows = (
                        connection.exec_driver_sql(
                            """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'table'
                          AND name NOT LIKE 'sqlite_%'
                        ORDER BY name ASC
                        """
                        )
                        .mappings()
                        .all()
                    )

                    return [
                        {
                            "name": str(row["name"]),
                            "columns": self._get_columns_remote(connection, str(row["name"])),
                        }
                        for row in table_rows
                    ]
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        database_path = self.ensure_database(log_group_id)

        try:
            with self._connect(database_path) as connection:
                table_rows = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%'
                    ORDER BY name ASC
                    """
                ).fetchall()

                return [
                    {
                        "name": row["name"],
                        "columns": self._get_columns(connection, row["name"]),
                    }
                    for row in table_rows
                ]
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def summarize_tables(self, log_group_id: str) -> list[dict[str, Any]]:
        """Return each table's name, columns, and row count without loading preview rows."""

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.connect() as connection:
                    tables = self.list_tables(log_group_id)
                    summaries: list[dict[str, Any]] = []

                    for table in tables:
                        safe_table_name = self._quote_identifier(table["name"])
                        row_count_row = (
                            connection.exec_driver_sql(f"SELECT COUNT(*) AS row_count FROM {safe_table_name}")
                            .mappings()
                            .first()
                        )
                        summaries.append(
                            {
                                "name": table["name"],
                                "columns": table["columns"],
                                "row_count": int(row_count_row["row_count"] if row_count_row is not None else 0),
                            }
                        )

                    return summaries
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        database_path = self.ensure_database(log_group_id)

        try:
            with self._connect(database_path) as connection:
                tables = self.list_tables(log_group_id)
                summaries: list[dict[str, Any]] = []

                for table in tables:
                    safe_table_name = self._quote_identifier(table["name"])
                    row_count_row = connection.execute(
                        f"SELECT COUNT(*) AS row_count FROM {safe_table_name}"
                    ).fetchone()
                    summaries.append(
                        {
                            "name": table["name"],
                            "columns": table["columns"],
                            "row_count": int(row_count_row["row_count"] if row_count_row is not None else 0),
                        }
                    )

                return summaries
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def explore_database(self, log_group_id: str, preview_limit: int = PREVIEW_ROW_LIMIT) -> list[dict[str, Any]]:
        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.connect() as connection:
                    tables = self.list_tables(log_group_id)
                    explored_tables: list[dict[str, Any]] = []

                    for table in tables:
                        safe_table_name = self._quote_identifier(table["name"])
                        row_count = (
                            connection.exec_driver_sql(f"SELECT COUNT(*) AS row_count FROM {safe_table_name}")
                            .mappings()
                            .first()
                        )
                        preview_rows = (
                            connection.exec_driver_sql(
                                f"SELECT * FROM {safe_table_name} LIMIT ?",
                                (preview_limit,),
                            )
                            .mappings()
                            .all()
                        )
                        explored_tables.append(
                            {
                                "name": table["name"],
                                "columns": table["columns"],
                                "row_count": int(row_count["row_count"] if row_count is not None else 0),
                                "preview_rows": [dict(row) for row in preview_rows],
                            }
                        )

                    return explored_tables
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        database_path = self.ensure_database(log_group_id)

        try:
            with self._connect(database_path) as connection:
                tables = self.list_tables(log_group_id)
                explored_tables: list[dict[str, Any]] = []

                for table in tables:
                    safe_table_name = self._quote_identifier(table["name"])
                    row_count = connection.execute(f"SELECT COUNT(*) AS row_count FROM {safe_table_name}").fetchone()
                    preview_rows = connection.execute(
                        f"SELECT * FROM {safe_table_name} LIMIT ?",
                        (preview_limit,),
                    ).fetchall()
                    explored_tables.append(
                        {
                            "name": table["name"],
                            "columns": table["columns"],
                            "row_count": int(row_count["row_count"] if row_count is not None else 0),
                            "preview_rows": [dict(row) for row in preview_rows],
                        }
                    )

                return explored_tables
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def fetch_table_rows(
        self,
        log_group_id: str,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """Return paginated rows from a table.

        Returns a dict with keys: columns, rows, total, page, page_size, total_pages.
        Raises LogDatabaseError if the table does not exist or a database error occurs.
        """

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)
            safe_table = self._quote_identifier(table_name)
            offset = (page - 1) * page_size

            try:
                with engine.connect() as connection:
                    exists = connection.exec_driver_sql(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (table_name,),
                    ).first()
                    if exists is None:
                        raise LogDatabaseError(f"Table '{table_name}' does not exist.")

                    total_row = connection.exec_driver_sql(f"SELECT COUNT(*) AS n FROM {safe_table}").mappings().first()
                    total = int(total_row["n"]) if total_row is not None else 0

                    cursor = connection.exec_driver_sql(
                        f"SELECT * FROM {safe_table} LIMIT ? OFFSET ?",
                        (page_size, offset),
                    )
                    rows = [dict(row) for row in cursor.mappings().all()]
                    column_names = list(cursor.keys())

                    return {
                        "columns": column_names,
                        "rows": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                        "total_pages": max(1, (total + page_size - 1) // page_size),
                    }
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        database_path = self.ensure_database(log_group_id)
        safe_table = self._quote_identifier(table_name)
        offset = (page - 1) * page_size

        try:
            with self._connect(database_path) as connection:
                # Verify the table exists to prevent probing arbitrary names.
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                ).fetchone()
                if exists is None:
                    raise LogDatabaseError(f"Table '{table_name}' does not exist.")

                total_row = connection.execute(f"SELECT COUNT(*) AS n FROM {safe_table}").fetchone()
                total = int(total_row["n"]) if total_row is not None else 0

                cursor = connection.execute(
                    f"SELECT * FROM {safe_table} LIMIT ? OFFSET ?",
                    (page_size, offset),
                )
                column_names = [d[0] for d in cursor.description or []]
                rows = [dict(row) for row in cursor.fetchall()]

                return {
                    "columns": column_names,
                    "rows": rows,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": max(1, (total + page_size - 1) // page_size),
                }
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def execute_read_only_query(
        self,
        log_group_id: str,
        sql: str,
        parameters: QueryParameters = None,
        row_limit: int = QUERY_ROW_LIMIT,
    ) -> dict[str, Any]:
        normalized_sql = self._normalize_sql(sql)
        self._validate_query_prefix(normalized_sql)

        if self.production_mode:
            self.ensure_database(log_group_id)
            engine = self._get_remote_engine(log_group_id)

            try:
                with engine.connect() as connection:
                    cursor = connection.exec_driver_sql(normalized_sql, self._coerce_parameters(parameters))
                    column_names = list(cursor.keys())
                    rows = cursor.fetchmany(row_limit)

                    return {
                        "columns": column_names,
                        "rows": [dict(row._mapping) for row in rows],
                        "row_count": len(rows),
                        "truncated": cursor.fetchone() is not None,
                    }
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

        database_path = self.ensure_database(log_group_id)

        with self._connect(database_path) as connection:
            connection.set_authorizer(self._authorizer)

            try:
                cursor = connection.execute(normalized_sql, self._coerce_parameters(parameters))
            except sqlite3.DatabaseError as exc:
                if "not authorized" in str(exc).lower():
                    raise ReadOnlyQueryError("Only read-only SQL statements are allowed.") from exc
                raise LogDatabaseError(str(exc)) from exc
            finally:
                connection.set_authorizer(None)

            column_names = [description[0] for description in cursor.description or []]
            rows = cursor.fetchmany(row_limit)

            return {
                "columns": column_names,
                "rows": [dict(row) for row in rows],
                "row_count": len(rows),
                "truncated": cursor.fetchone() is not None,
            }

    @contextmanager
    def _connect(self, database_path: Path):
        connection = sqlite3.connect(database_path)
        connection.row_factory = sqlite3.Row

        try:
            yield connection
        finally:
            connection.close()

    def _get_columns(self, connection: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
        safe_table_name = self._quote_identifier(table_name)
        column_rows = connection.execute(f"PRAGMA table_info({safe_table_name})").fetchall()

        return [
            {
                "name": row["name"],
                "type": row["type"],
                "not_null": bool(row["notnull"]),
                "default_value": row["dflt_value"],
                "primary_key": bool(row["pk"]),
            }
            for row in column_rows
        ]

    def _get_columns_remote(self, connection: Connection, table_name: str) -> list[dict[str, Any]]:
        safe_table_name = self._quote_identifier(table_name)
        column_rows = connection.exec_driver_sql(f"PRAGMA table_info({safe_table_name})").mappings().all()

        return [
            {
                "name": row["name"],
                "type": row["type"],
                "not_null": bool(row["notnull"]),
                "default_value": row["dflt_value"],
                "primary_key": bool(row["pk"]),
            }
            for row in column_rows
        ]

    def _normalize_sql(self, sql: str) -> str:
        normalized_sql = sql.strip()

        if not normalized_sql:
            raise ReadOnlyQueryError("Query cannot be empty.")

        if ";" in normalized_sql.rstrip(";"):
            raise ReadOnlyQueryError("Only a single SQL statement is allowed.")

        if normalized_sql.endswith(";"):
            normalized_sql = normalized_sql[:-1].rstrip()

        return normalized_sql

    def _validate_query_prefix(self, sql: str) -> None:
        lowered_sql = sql.lstrip().lower()

        if not lowered_sql.startswith(ALLOWED_QUERY_PREFIXES):
            raise ReadOnlyQueryError("Only read-only SELECT, WITH, EXPLAIN, or PRAGMA statements are allowed.")

        if lowered_sql.startswith("pragma") and "=" in lowered_sql:
            raise ReadOnlyQueryError("Writable PRAGMA statements are not allowed.")

    def _coerce_parameters(self, parameters: QueryParameters) -> dict[str, JsonScalar] | Sequence[JsonScalar]:
        if parameters is None:
            return ()

        if isinstance(parameters, dict):
            return parameters

        if isinstance(parameters, list | tuple):
            return tuple(parameters)

        raise ReadOnlyQueryError("Query parameters must be an object or an array.")

    def _quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _authorizer(
        self,
        action_code: int,
        parameter_1: str | None,
        parameter_2: str | None,
        database_name: str | None,
        trigger_name: str | None,
    ) -> int:
        del parameter_1, parameter_2, database_name, trigger_name

        denied_actions = {
            sqlite3.SQLITE_CREATE_INDEX,
            sqlite3.SQLITE_CREATE_TABLE,
            sqlite3.SQLITE_CREATE_TEMP_INDEX,
            sqlite3.SQLITE_CREATE_TEMP_TABLE,
            sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
            sqlite3.SQLITE_CREATE_TEMP_VIEW,
            sqlite3.SQLITE_CREATE_TRIGGER,
            sqlite3.SQLITE_CREATE_VIEW,
            sqlite3.SQLITE_CREATE_VTABLE,
            sqlite3.SQLITE_DELETE,
            sqlite3.SQLITE_DROP_INDEX,
            sqlite3.SQLITE_DROP_TABLE,
            sqlite3.SQLITE_DROP_TEMP_INDEX,
            sqlite3.SQLITE_DROP_TEMP_TABLE,
            sqlite3.SQLITE_DROP_TEMP_TRIGGER,
            sqlite3.SQLITE_DROP_TEMP_VIEW,
            sqlite3.SQLITE_DROP_TRIGGER,
            sqlite3.SQLITE_DROP_VIEW,
            sqlite3.SQLITE_DROP_VTABLE,
            sqlite3.SQLITE_INSERT,
            sqlite3.SQLITE_TRANSACTION,
            sqlite3.SQLITE_UPDATE,
            sqlite3.SQLITE_ATTACH,
            sqlite3.SQLITE_DETACH,
            sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_REINDEX,
            sqlite3.SQLITE_ANALYZE,
            sqlite3.SQLITE_CREATE_TEMP_VIEW,
        }

        if action_code in denied_actions:
            return sqlite3.SQLITE_DENY

        return sqlite3.SQLITE_OK

    def _get_remote_engine(self, log_group_id: str) -> Engine:
        cached = self._engine_cache.get(log_group_id)
        if cached is not None:
            return cached

        credential = self._get_swarm_credential(log_group_id=log_group_id)
        if credential is None:
            raise LogDatabaseError("Swarm database credentials are missing for this log group.")

        engine = create_engine(
            self._normalize_turso_database_url(credential.database_url),
            connect_args={"auth_token": credential.database_token},
        )
        self._engine_cache[log_group_id] = engine
        return engine

    def _dispose_remote_engine(self, log_group_id: str) -> None:
        engine = self._engine_cache.pop(log_group_id, None)
        if engine is not None:
            engine.dispose()

    def _normalize_turso_database_url(self, database_url: str) -> str:
        normalized = database_url.strip()
        if normalized.startswith("libsql://"):
            normalized = normalized.replace("libsql://", "sqlite+libsql://", 1)

        if not normalized.startswith("sqlite+libsql://"):
            raise LogDatabaseError("Invalid Turso database URL for swarm database.")

        if "?" in normalized:
            if "secure=" not in normalized:
                normalized = f"{normalized}&secure=true"
        else:
            normalized = f"{normalized}?secure=true"

        return normalized

    def _get_swarm_credential(
        self,
        log_group_id: str,
        database: Session | None = None,
    ) -> LogGroupSwarmCredential | None:
        if database is not None:
            try:
                return (
                    database.query(LogGroupSwarmCredential)
                    .filter(LogGroupSwarmCredential.log_id == log_group_id)
                    .first()
                )
            except (AssertionError, SQLAlchemyError):
                # Fallback for invalid/detached sessions.
                pass

        scoped_session = SessionLocal()
        try:
            return (
                scoped_session.query(LogGroupSwarmCredential)
                .filter(LogGroupSwarmCredential.log_id == log_group_id)
                .first()
            )
        finally:
            scoped_session.close()

    def _create_remote_database_credential(self, log_group_id: str, database: Session) -> LogGroupSwarmCredential:
        existing = self._get_swarm_credential(log_group_id=log_group_id, database=database)
        if existing is not None:
            return existing

        api_key, organization_slug, group_name, name_prefix = self._require_turso_api_config()
        database_name = self._build_database_name(name_prefix=name_prefix, log_group_id=log_group_id)

        created_payload = self._turso_request(
            method="POST",
            path=f"/v1/organizations/{organization_slug}/databases",
            api_key=api_key,
            payload={"name": database_name, "group": group_name},
        )

        created_database = created_payload.get("database", {})
        resolved_database_name = str(created_database.get("Name") or created_database.get("name") or database_name)
        hostname = created_database.get("Hostname") or created_database.get("hostname")

        if hostname is None:
            database_details_payload = self._turso_request(
                method="GET",
                path=f"/v1/organizations/{organization_slug}/databases/{resolved_database_name}",
                api_key=api_key,
            )
            details_database = database_details_payload.get("database", {})
            hostname = details_database.get("Hostname") or details_database.get("hostname")

        if hostname is None:
            raise LogDatabaseError("Turso API did not return a database hostname.")

        token_payload = self._turso_request(
            method="POST",
            path=(
                f"/v1/organizations/{organization_slug}/databases/{resolved_database_name}"
                "/auth/tokens?authorization=full-access"
            ),
            api_key=api_key,
        )

        database_token = token_payload.get("jwt") or token_payload.get("token")
        if not database_token:
            raise LogDatabaseError("Turso API did not return a database auth token.")

        credential = LogGroupSwarmCredential(
            log_id=log_group_id,
            provider="turso",
            database_name=resolved_database_name,
            database_url=f"libsql://{hostname}",
            database_token=str(database_token),
            group_name=group_name,
        )
        database.add(credential)
        database.flush()

        return credential

    def _require_turso_api_config(self) -> tuple[str, str, str, str]:
        api_key = os.getenv("TURSO_API_KEY", "").strip()
        organization_slug = os.getenv("TURSO_ORGANIZATION_SLUG", "").strip()
        group_name = os.getenv("TURSO_GROUP", "logdog").strip() or "logdog"
        name_prefix = os.getenv("TURSO_SWARM_DATABASE_PREFIX", "logdog-swarm").strip() or "logdog-swarm"

        if not api_key:
            raise LogDatabaseError("Missing TURSO_API_KEY environment variable.")

        if not organization_slug:
            organizations_payload = self._turso_request(
                method="GET",
                path="/v1/organizations",
                api_key=api_key,
            )
            # Handle both dict response (with "organizations" key) and direct list response
            if isinstance(organizations_payload, list):
                organizations = organizations_payload
            else:
                organizations = organizations_payload.get("organizations", [])
            if isinstance(organizations, list) and organizations:
                first_organization = organizations[0]
                if isinstance(first_organization, dict):
                    organization_slug = str(
                        first_organization.get("slug")
                        or first_organization.get("Slug")
                        or first_organization.get("name")
                        or first_organization.get("Name")
                        or ""
                    ).strip()

        if not organization_slug:
            raise LogDatabaseError(
                "Missing TURSO_ORGANIZATION_SLUG and could not discover an organization from Turso API."
            )

        return api_key, organization_slug, group_name, name_prefix

    def _build_database_name(self, name_prefix: str, log_group_id: str) -> str:
        normalized_prefix = self._sanitize_database_name_component(name_prefix)
        normalized_group = self._sanitize_database_name_component(log_group_id)

        if not normalized_prefix:
            normalized_prefix = "swarm"
        if not normalized_group:
            normalized_group = "group"

        database_name = f"{normalized_prefix}-{normalized_group}"
        database_name = database_name[:DATABASE_NAME_MAX_LENGTH].strip("-")
        if not database_name:
            raise LogDatabaseError("Could not build a valid Turso database name for log group.")

        return database_name

    def _sanitize_database_name_component(self, value: str) -> str:
        normalized = DATABASE_NAME_PATTERN.sub("-", value.lower())
        normalized = re.sub(r"-+", "-", normalized).strip("-")
        return normalized

    def _turso_request(
        self,
        method: str,
        path: str,
        api_key: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = None
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(
            url=f"{TURSO_API_BASE_URL}{path}",
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode("utf-8")
                if not response_body:
                    return {}
                return json.loads(response_body)
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise LogDatabaseError(f"Turso API request failed (HTTP {exc.code}): {error_body}") from exc
        except URLError as exc:
            raise LogDatabaseError(f"Turso API request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise LogDatabaseError("Turso API returned malformed JSON.") from exc
