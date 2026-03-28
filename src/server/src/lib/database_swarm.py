import json
import os
import re
import threading
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from lib.models import LogGroupTable
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

QUERY_ROW_LIMIT = 200
PREVIEW_ROW_LIMIT = 25
MESSAGE_HISTORY_LIMIT = 500
ALLOWED_QUERY_PREFIXES = ("select", "with", "explain")
POSTGRES_IDENTIFIER_LIMIT = 63
SWARM_MESSAGES_TABLE = "swarm_messages"
SYSTEM_TABLE_NAMES = frozenset({"messages"})

SWARM_MESSAGES_TABLE_DDL = f'''
CREATE TABLE IF NOT EXISTS "{SWARM_MESSAGES_TABLE}" (
    log_group_id TEXT NOT NULL,
    id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (log_group_id, id)
)
'''.strip()

SWARM_MESSAGES_INDEX_DDL = f'''
CREATE INDEX IF NOT EXISTS "idx_{SWARM_MESSAGES_TABLE}_group_created"
ON "{SWARM_MESSAGES_TABLE}" (log_group_id, created_at, id)
'''.strip()

JsonScalar = str | int | float | bool | None
QueryParameters = dict[str, JsonScalar] | list[JsonScalar] | tuple[JsonScalar, ...] | None


class LogDatabaseError(RuntimeError):
    pass


class ReadOnlyQueryError(LogDatabaseError):
    pass


class LogDatabaseSwarm:
    """Shared Postgres-backed swarm database keyed by log-group table prefixes."""

    def __init__(self, root_directory: Path = Path("store")):
        del root_directory  # Kept for API compatibility with older call sites.

        swarm_database_url = os.getenv("SWARM_DATABASE_URL", "").strip()
        if not swarm_database_url:
            raise LogDatabaseError("Missing SWARM_DATABASE_URL environment variable.")

        self._engine: Engine = create_engine(swarm_database_url, pool_pre_ping=True)
        self._bootstrap_lock = threading.Lock()
        self._bootstrapped = False

    def database_path(self, log_group_id: str) -> Path:
        return Path(f"{log_group_id}.postgres")

    def ensure_database(
        self,
        log_group_id: str,
        database: Session | None = None,
        provision_if_missing: bool = False,
    ) -> Path:
        del database, provision_if_missing
        self._ensure_shared_objects()
        self._validate_log_group_id(log_group_id)
        return self.database_path(log_group_id)

    def apply_schema(self, log_group_id: str, ddl: str) -> None:
        self._ensure_shared_objects()
        rewritten_ddl = self._rewrite_ddl_for_log_group(log_group_id, ddl)

        try:
            with self._engine.begin() as connection:
                connection.exec_driver_sql(rewritten_ddl)
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def insert_rows(
        self,
        log_group_id: str,
        table_name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        self._ensure_shared_objects()
        physical_table_name = self._physical_table_name(log_group_id, table_name)

        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(key for key in row if key != "id")

        columns = sorted(all_keys)
        if not columns:
            return 0

        statement = self._build_insert_statement(physical_table_name, columns)

        try:
            with self._engine.begin() as connection:
                inserted = 0
                for row in rows:
                    parameters = {
                        f"column_{index}": self._coerce_row_value(row.get(column_name))
                        for index, column_name in enumerate(columns)
                    }
                    connection.execute(statement, parameters)
                    inserted += 1
                return inserted
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def apply_schema_and_insert(
        self,
        log_group_id: str,
        ddl: str,
        table_name: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            self.apply_schema(log_group_id, ddl)
            return 0

        self._ensure_shared_objects()
        rewritten_ddl = self._rewrite_ddl_for_log_group(log_group_id, ddl)
        physical_table_name = self._physical_table_name(log_group_id, table_name)

        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(key for key in row if key != "id")

        columns = sorted(all_keys)
        if not columns:
            return 0

        statement = self._build_insert_statement(physical_table_name, columns)

        try:
            with self._engine.begin() as connection:
                connection.exec_driver_sql(rewritten_ddl)
                inserted = 0
                for row in rows:
                    parameters = {
                        f"column_{index}": self._coerce_row_value(row.get(column_name))
                        for index, column_name in enumerate(columns)
                    }
                    connection.execute(statement, parameters)
                    inserted += 1
                return inserted
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def delete_database(self, log_group_id: str, database: Session | None = None) -> None:
        del database
        self._ensure_shared_objects()

        prefix = self._table_prefix(log_group_id)
        pattern = f"{prefix}%"

        try:
            with self._engine.begin() as connection:
                table_names = connection.execute(
                    text(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_type = 'BASE TABLE'
                          AND table_name LIKE :pattern
                        ORDER BY table_name ASC
                        """
                    ),
                    {"pattern": pattern},
                ).scalars()

                for table_name in table_names:
                    connection.exec_driver_sql(f"DROP TABLE IF EXISTS {self._quote_identifier(str(table_name))}")

                connection.execute(
                    text(f'DELETE FROM "{SWARM_MESSAGES_TABLE}" WHERE log_group_id = :log_group_id'),
                    {"log_group_id": log_group_id},
                )
        except SQLAlchemyError as exc:
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
        self._ensure_shared_objects()

        try:
            with self._engine.connect() as connection:
                return self._list_tables(connection, log_group_id)
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def summarize_tables(self, log_group_id: str) -> list[dict[str, Any]]:
        self._ensure_shared_objects()

        try:
            with self._engine.connect() as connection:
                tables = self._list_tables(connection, log_group_id)
                summaries: list[dict[str, Any]] = []

                for table in tables:
                    physical_table_name = self._physical_table_name(log_group_id, table["name"])
                    row_count = (
                        connection.exec_driver_sql(
                            f"SELECT COUNT(*) AS row_count FROM {self._quote_identifier(physical_table_name)}"
                        )
                        .mappings()
                        .first()
                    )
                    summaries.append(
                        {
                            "name": table["name"],
                            "columns": table["columns"],
                            "row_count": int(row_count["row_count"] if row_count is not None else 0),
                        }
                    )

                return summaries
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def explore_database(self, log_group_id: str, preview_limit: int = PREVIEW_ROW_LIMIT) -> list[dict[str, Any]]:
        self._ensure_shared_objects()

        try:
            with self._engine.connect() as connection:
                tables = self._list_tables(connection, log_group_id)
                explored_tables: list[dict[str, Any]] = []

                for table in tables:
                    physical_table_name = self._physical_table_name(log_group_id, table["name"])
                    row_count = (
                        connection.exec_driver_sql(
                            f"SELECT COUNT(*) AS row_count FROM {self._quote_identifier(physical_table_name)}"
                        )
                        .mappings()
                        .first()
                    )
                    preview_rows = (
                        connection.exec_driver_sql(
                            f"SELECT * FROM {self._quote_identifier(physical_table_name)} LIMIT %s",
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

    def list_messages(self, log_group_id: str, limit: int = MESSAGE_HISTORY_LIMIT) -> list[dict[str, Any]]:
        self._ensure_shared_objects()
        bounded_limit = max(1, min(limit, 2000))

        try:
            with self._engine.connect() as connection:
                rows = (
                    connection.execute(
                        text(
                            f'''
                        SELECT id, role, content, payload, created_at
                        FROM "{SWARM_MESSAGES_TABLE}"
                        WHERE log_group_id = :log_group_id
                        ORDER BY created_at ASC, id ASC
                        LIMIT :limit
                        '''
                        ),
                        {"log_group_id": log_group_id, "limit": bounded_limit},
                    )
                    .mappings()
                    .all()
                )

                return [
                    self._deserialize_message_payload(
                        payload=str(row["payload"]),
                        message_id=str(row["id"]),
                        role=str(row["role"]),
                        content=str(row["content"]),
                    )
                    for row in rows
                ]
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def replace_messages(self, log_group_id: str, messages: list[dict[str, Any]]) -> int:
        self._ensure_shared_objects()
        prepared_messages = [self._prepare_message_record(message) for message in messages]

        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text(f'DELETE FROM "{SWARM_MESSAGES_TABLE}" WHERE log_group_id = :log_group_id'),
                    {"log_group_id": log_group_id},
                )

                for message_id, role, content, payload in prepared_messages:
                    connection.execute(
                        text(
                            f'''
                            INSERT INTO "{SWARM_MESSAGES_TABLE}" (log_group_id, id, role, content, payload)
                            VALUES (:log_group_id, :id, :role, :content, :payload)
                            ON CONFLICT (log_group_id, id)
                            DO UPDATE SET role = EXCLUDED.role, content = EXCLUDED.content, payload = EXCLUDED.payload
                            '''
                        ),
                        {
                            "log_group_id": log_group_id,
                            "id": message_id,
                            "role": role,
                            "content": content,
                            "payload": payload,
                        },
                    )

            return len(prepared_messages)
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def fetch_table_rows(
        self,
        log_group_id: str,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        self._ensure_shared_objects()
        physical_table_name = self._physical_table_name(log_group_id, table_name)
        offset = (page - 1) * page_size

        try:
            with self._engine.connect() as connection:
                if not self._table_exists(connection, physical_table_name):
                    raise LogDatabaseError(f"Table '{table_name}' does not exist.")

                total_row = (
                    connection.exec_driver_sql(
                        f"SELECT COUNT(*) AS n FROM {self._quote_identifier(physical_table_name)}"
                    )
                    .mappings()
                    .first()
                )
                total = int(total_row["n"]) if total_row is not None else 0

                cursor = connection.exec_driver_sql(
                    f"SELECT * FROM {self._quote_identifier(physical_table_name)} LIMIT %s OFFSET %s",
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

    def execute_read_only_query(
        self,
        log_group_id: str,
        sql: str,
        parameters: QueryParameters = None,
        row_limit: int = QUERY_ROW_LIMIT,
    ) -> dict[str, Any]:
        self._ensure_shared_objects()

        normalized_sql = self._normalize_sql(sql)
        self._validate_query_prefix(normalized_sql)

        try:
            with self._engine.connect() as connection:
                namespace = self._table_namespace_map(connection, log_group_id)
                rewritten_sql = self._rewrite_query_tables(log_group_id, normalized_sql, namespace)
                cursor = self._execute_with_parameters(connection, rewritten_sql, parameters)
                column_names = list(cursor.keys())
                rows = cursor.fetchmany(row_limit)

                return {
                    "columns": column_names,
                    "rows": [dict(row._mapping) for row in rows],
                    "row_count": len(rows),
                    "truncated": cursor.fetchone() is not None,
                }
        except ReadOnlyQueryError:
            raise
        except SQLAlchemyError as exc:
            raise LogDatabaseError(str(exc)) from exc

    def _ensure_shared_objects(self) -> None:
        if self._bootstrapped:
            return

        with self._bootstrap_lock:
            if self._bootstrapped:
                return

            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(SWARM_MESSAGES_TABLE_DDL)
                    connection.exec_driver_sql(SWARM_MESSAGES_INDEX_DDL)
            except SQLAlchemyError as exc:
                raise LogDatabaseError(str(exc)) from exc

            self._bootstrapped = True

    def _list_tables(self, connection: Connection, log_group_id: str) -> list[dict[str, Any]]:
        prefix = self._table_prefix(log_group_id)
        rows = (
            connection.execute(
                text(
                    """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name LIKE :pattern
                ORDER BY table_name ASC
                """
                ),
                {"pattern": f"{prefix}%"},
            )
            .scalars()
            .all()
        )

        tables: list[dict[str, Any]] = []
        for physical_table_name in rows:
            physical_table_name_str = str(physical_table_name)
            logical_table_name = self._logical_table_name(log_group_id, physical_table_name_str)
            if self._is_system_table(logical_table_name):
                continue

            tables.append(
                {
                    "name": logical_table_name,
                    "columns": self._get_columns(connection, physical_table_name_str),
                }
            )

        return tables

    def _get_columns(self, connection: Connection, physical_table_name: str) -> list[dict[str, Any]]:
        column_rows = (
            connection.execute(
                text(
                    """
                SELECT
                    attribute.attname AS name,
                    pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS type,
                    attribute.attnotnull AS not_null,
                    pg_catalog.pg_get_expr(definition.adbin, definition.adrelid) AS default_value,
                    EXISTS (
                        SELECT 1
                        FROM pg_index index_table
                        WHERE index_table.indrelid = class.oid
                          AND index_table.indisprimary
                          AND attribute.attnum = ANY(index_table.indkey)
                    ) AS primary_key
                FROM pg_attribute attribute
                JOIN pg_class class ON class.oid = attribute.attrelid
                JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                LEFT JOIN pg_attrdef definition
                    ON definition.adrelid = attribute.attrelid
                   AND definition.adnum = attribute.attnum
                WHERE namespace.nspname = 'public'
                  AND class.relname = :table_name
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                ORDER BY attribute.attnum
                """
                ),
                {"table_name": physical_table_name},
            )
            .mappings()
            .all()
        )

        return [
            {
                "name": str(row["name"]),
                "type": str(row["type"]),
                "not_null": bool(row["not_null"]),
                "default_value": row["default_value"],
                "primary_key": bool(row["primary_key"]),
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
            raise ReadOnlyQueryError("Only read-only SELECT, WITH, or EXPLAIN statements are allowed.")

        if re.search(
            r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|vacuum|comment|call|do|copy)\b",
            lowered_sql,
        ):
            raise ReadOnlyQueryError("Only read-only SQL statements are allowed.")

    def _coerce_parameters(self, parameters: QueryParameters) -> dict[str, JsonScalar] | Sequence[JsonScalar]:
        if parameters is None:
            return ()

        if isinstance(parameters, dict):
            return {key: self._coerce_row_value(value) for key, value in parameters.items()}

        if isinstance(parameters, list | tuple):
            return tuple(self._coerce_row_value(value) for value in parameters)

        raise ReadOnlyQueryError("Query parameters must be an object or an array.")

    def _execute_with_parameters(
        self,
        connection: Connection,
        sql: str,
        parameters: QueryParameters,
    ):
        coerced_parameters = self._coerce_parameters(parameters)

        if parameters is None:
            return connection.exec_driver_sql(sql)

        if isinstance(coerced_parameters, dict):
            return connection.execute(text(sql), coerced_parameters)

        sequence_parameters = tuple(coerced_parameters)
        transformed_sql = self._replace_qmark_placeholders(sql, len(sequence_parameters))
        return connection.exec_driver_sql(transformed_sql, sequence_parameters)

    def _replace_qmark_placeholders(self, sql: str, parameter_count: int) -> str:
        qmark_count = sql.count("?")
        if qmark_count == 0:
            return sql

        if qmark_count != parameter_count:
            raise ReadOnlyQueryError("Number of positional parameters does not match placeholders.")

        return "%s".join(sql.split("?"))

    def _table_prefix(self, log_group_id: str) -> str:
        normalized_group_id = re.sub(r"[^a-z0-9]", "", log_group_id.lower())[:24]
        if not normalized_group_id:
            raise LogDatabaseError("Invalid log group id for swarm table namespace.")

        return f"lg_{normalized_group_id}__"

    def _physical_table_name(self, log_group_id: str, logical_table_name: str) -> str:
        self._validate_logical_table_name(logical_table_name)
        physical_table_name = f"{self._table_prefix(log_group_id)}{logical_table_name}"
        if len(physical_table_name) > POSTGRES_IDENTIFIER_LIMIT:
            raise LogDatabaseError(f"Table name '{logical_table_name}' is too long for PostgreSQL after namespacing.")

        return physical_table_name

    def _logical_table_name(self, log_group_id: str, physical_table_name: str) -> str:
        prefix = self._table_prefix(log_group_id)
        if not physical_table_name.startswith(prefix):
            raise LogDatabaseError(f"Table '{physical_table_name}' is outside of the log group namespace.")

        logical_table_name = physical_table_name[len(prefix) :]
        if not logical_table_name:
            raise LogDatabaseError("Encountered an invalid namespaced table name.")

        return logical_table_name

    def _table_namespace_map(self, connection: Connection, log_group_id: str) -> dict[str, str]:
        prefix = self._table_prefix(log_group_id)
        physical_table_names = (
            connection.execute(
                text(
                    """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name LIKE :pattern
                """
                ),
                {"pattern": f"{prefix}%"},
            )
            .scalars()
            .all()
        )

        namespace: dict[str, str] = {}
        for physical_name in physical_table_names:
            physical_name_str = str(physical_name)
            logical_name = self._logical_table_name(log_group_id, physical_name_str)
            namespace[logical_name] = physical_name_str

        return namespace

    def _rewrite_query_tables(self, log_group_id: str, sql: str, namespace: dict[str, str]) -> str:
        prefix = self._table_prefix(log_group_id)

        pattern = re.compile(r'(?i)\b(?P<keyword>from|join)\s+(?P<identifier>"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)')

        def replace(match: re.Match[str]) -> str:
            keyword = match.group("keyword")
            identifier_literal = match.group("identifier")
            identifier = self._unquote_identifier(identifier_literal)

            if identifier in namespace:
                return f"{keyword} {self._quote_identifier(namespace[identifier])}"

            if identifier == SWARM_MESSAGES_TABLE:
                raise ReadOnlyQueryError("Access to system tables is not allowed.")

            if identifier.startswith("lg_") and not identifier.startswith(prefix):
                raise ReadOnlyQueryError("Cross-log-group table access is not allowed.")

            return match.group(0)

        return pattern.sub(replace, sql)

    def _rewrite_ddl_for_log_group(self, log_group_id: str, ddl: str) -> str:
        create_table_pattern = re.compile(
            r'(?is)^\s*create\s+table\s+(?:if\s+not\s+exists\s+)?(?P<identifier>"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)'
        )
        match = create_table_pattern.search(ddl)
        if match is None:
            raise LogDatabaseError("Unsupported DDL format. Expected CREATE TABLE statement.")

        logical_table_name = self._unquote_identifier(match.group("identifier"))
        physical_table_name = self._physical_table_name(log_group_id, logical_table_name)
        remainder = ddl[match.end() :]

        return f"CREATE TABLE IF NOT EXISTS {self._quote_identifier(physical_table_name)}{remainder}"

    def _build_insert_statement(self, physical_table_name: str, columns: list[str]):
        quoted_columns = [self._quote_identifier(column) for column in columns]
        placeholders = [f":column_{index}" for index, _ in enumerate(columns)]
        column_list = ", ".join(quoted_columns)
        placeholder_list = ", ".join(placeholders)

        return text(
            f"INSERT INTO {self._quote_identifier(physical_table_name)} ({column_list}) VALUES ({placeholder_list})"
        )

    def _table_exists(self, connection: Connection, physical_table_name: str) -> bool:
        row = connection.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name = :table_name
                """
            ),
            {"table_name": physical_table_name},
        ).first()

        return row is not None

    def _quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _unquote_identifier(self, identifier: str) -> str:
        if len(identifier) >= 2 and identifier[0] == '"' and identifier[-1] == '"':
            return identifier[1:-1].replace('""', '"')
        return identifier

    def _validate_log_group_id(self, log_group_id: str) -> None:
        if not str(log_group_id).strip():
            raise LogDatabaseError("log_group_id cannot be empty.")

    def _validate_logical_table_name(self, table_name: str) -> None:
        if not table_name or not str(table_name).strip():
            raise LogDatabaseError("Table name cannot be empty.")

    def _is_system_table(self, table_name: str) -> bool:
        return table_name.lower() in SYSTEM_TABLE_NAMES

    def _coerce_row_value(self, value: Any) -> JsonScalar:
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value  # type: ignore[return-value]

    def _prepare_message_record(self, message: dict[str, Any]) -> tuple[str, str, str, str]:
        if not isinstance(message, dict):
            raise LogDatabaseError("Each message must be an object.")

        message_id = str(message.get("id") or f"msg_{uuid.uuid4().hex}")
        role = str(message.get("role") or "assistant")
        content = self._extract_message_content(message)
        payload = json.dumps(message)
        return message_id, role, content, payload

    def _extract_message_content(self, message: dict[str, Any]) -> str:
        parts = message.get("parts")
        if isinstance(parts, list):
            text_parts = [
                part["text"]
                for part in parts
                if isinstance(part, dict)
                and part.get("type") == "text"
                and isinstance(part.get("text"), str)
                and part["text"].strip()
            ]
            if text_parts:
                return "\n".join(text_parts)

        content = message.get("content")
        if isinstance(content, str):
            return content

        return ""

    def _deserialize_message_payload(
        self,
        payload: str,
        message_id: str,
        role: str,
        content: str,
    ) -> dict[str, Any]:
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                parsed.setdefault("id", message_id)
                parsed.setdefault("role", role)
                return parsed
        except json.JSONDecodeError:
            pass

        return {
            "id": message_id,
            "role": role,
            "parts": [{"type": "text", "text": content}],
        }
