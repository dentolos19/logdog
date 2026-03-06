import json
import sqlite3
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from lib.models import LogGroupTable
from sqlalchemy.orm import Session

STORE_DIR = Path("store")
QUERY_ROW_LIMIT = 200
PREVIEW_ROW_LIMIT = 25
ALLOWED_QUERY_PREFIXES = ("select", "with", "pragma", "explain")

JsonScalar = str | int | float | bool | None
QueryParameters = dict[str, JsonScalar] | list[JsonScalar] | tuple[JsonScalar, ...] | None


class LogDatabaseError(RuntimeError):
    pass


class ReadOnlyQueryError(LogDatabaseError):
    pass


class LogDatabaseSwarm:
    def __init__(self, root_directory: Path = STORE_DIR):
        self.root_directory = root_directory

    def database_path(self, log_group_id: str) -> Path:
        return self.root_directory / f"{log_group_id}.sqlite3"

    def ensure_database(self, log_group_id: str) -> Path:
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

        database_path = self.ensure_database(log_group_id)

        try:
            with self._connect(database_path) as connection:
                connection.execute(ddl)
                connection.commit()
        except sqlite3.DatabaseError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def delete_database(self, log_group_id: str) -> None:
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
        self.ensure_database(log_group_id)
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

            if table_record is None:
                database.add(
                    LogGroupTable(
                        log_id=log_group_id,
                        name=table["name"],
                        columns=serialized_columns,
                        is_normalized=0,
                    )
                )
                continue

            table_record.columns = serialized_columns

        database.flush()
        return tables

    def list_tables(self, log_group_id: str) -> list[dict[str, Any]]:
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

    def explore_database(self, log_group_id: str, preview_limit: int = PREVIEW_ROW_LIMIT) -> list[dict[str, Any]]:
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

    def execute_read_only_query(
        self,
        log_group_id: str,
        sql: str,
        parameters: QueryParameters = None,
        row_limit: int = QUERY_ROW_LIMIT,
    ) -> dict[str, Any]:
        normalized_sql = self._normalize_sql(sql)
        self._validate_query_prefix(normalized_sql)
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
            return parameters

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
