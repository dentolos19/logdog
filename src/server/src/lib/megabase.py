import json
import uuid

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    JSON,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    delete,
    func,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.environment import MEGABASE_URL

TYPE_MAP = {
    "uuid": UUID(as_uuid=True),
    "string": String(),
    "text": Text(),
    "integer": Integer(),
    "bigint": BigInteger(),
    "float": Float(),
    "boolean": Boolean(),
    "datetime": DateTime(timezone=True),
    "json": JSON(),
    "bytea": LargeBinary(),
}

metadata = MetaData()

_registry_locked = False


def _get_engine() -> Engine:
    return __import__("sqlalchemy").create_engine(MEGABASE_URL.get_secret_value())


_engine = _get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

_registry_table = Table(
    "megabase_tables",
    metadata,
    Column("table_name", String, primary_key=True),
    Column("schema_json", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)


def _get_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _parse_column(col_def: dict) -> Column:
    col_type = col_def.get("type", "string")
    col_name = col_def["name"]

    sqla_type = TYPE_MAP.get(col_type)
    if not sqla_type:
        raise ValueError(f"Unknown column type: {col_type}")

    kwargs = {}
    if "nullable" in col_def:
        kwargs["nullable"] = col_def["nullable"]
    if "default" in col_def:
        kwargs["default"] = col_def["default"]

    if col_def.get("primary_key"):
        return Column(
            col_name,
            UUID(as_uuid=True) if col_type == "uuid" else sqla_type,
            primary_key=True,
            nullable=False,
            default=uuid.uuid4,
        )

    return Column(col_name, sqla_type, **kwargs)


def _table_from_schema(table_name: str, schema: dict) -> Table:
    columns = [_parse_column(col) for col in schema.get("columns", [])]
    return Table(table_name, metadata, *columns)


def create_table(session: Session, table_name: str, schema: dict) -> Table:
    if table_name in metadata.tables:
        raise ValueError(f"Table '{table_name}' already exists in metadata")

    table = _table_from_schema(table_name, schema)
    table.create(bind=_engine, checkfirst=False)

    existing = session.execute(select(_registry_table).where(_registry_table.c.table_name == table_name)).first()

    if not existing:
        session.execute(
            _registry_table.insert().values(
                table_name=table_name,
                schema_json=json.dumps(schema),
            )
        )
        session.commit()

    return table


def drop_table(session: Session, table_name: str) -> bool:
    if table_name not in metadata.tables:
        table = Table(table_name, metadata)
        table.drop(bind=_engine, checkfirst=True)
    else:
        metadata.tables[table_name].drop(bind=_engine, checkfirst=True)
        del metadata.tables[table_name]

    session.execute(delete(_registry_table).where(_registry_table.c.table_name == table_name))
    session.commit()
    return True


def list_tables(session: Session) -> list[str]:
    result = session.execute(select(_registry_table.c.table_name))
    return [row[0] for row in result.fetchall()]


def describe_table(session: Session, table_name: str) -> dict:
    result = session.execute(select(_registry_table).where(_registry_table.c.table_name == table_name)).first()

    if not result:
        raise ValueError(f"Table '{table_name}' not found in registry")

    return json.loads(result.schema_json)


def add_column(session: Session, table_name: str, column_def: dict) -> Table:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not registered in metadata")

    new_column = _parse_column(column_def)

    try:
        session.execute(
            text(
                f"ALTER TABLE {table_name} ADD COLUMN {new_column.name} {new_column.type.compile(dialect=_engine.dialect)}"
            )
        )
    except Exception:
        pass

    result = session.execute(select(_registry_table).where(_registry_table.c.table_name == table_name)).first()
    schema = json.loads(result.schema_json)  # type: ignore
    schema["columns"].append(column_def)

    session.execute(
        update(_registry_table).where(_registry_table.c.table_name == table_name).values(schema_json=json.dumps(schema))
    )
    session.commit()

    metadata.clear()
    _load_registry(session)

    return metadata.tables[table_name]


def remove_column(session: Session, table_name: str, column_name: str) -> Table:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not registered in metadata")

    try:
        session.execute(text(f"ALTER TABLE {table_name} DROP COLUMN {column_name}"))
    except Exception:
        pass

    result = session.execute(select(_registry_table).where(_registry_table.c.table_name == table_name)).first()
    schema = json.loads(result.schema_json)  # type: ignore
    schema["columns"] = [c for c in schema["columns"] if c["name"] != column_name]

    session.execute(
        update(_registry_table).where(_registry_table.c.table_name == table_name).values(schema_json=json.dumps(schema))
    )
    session.commit()

    metadata.clear()
    _load_registry(session)

    return metadata.tables[table_name]


def _load_registry(session: Session):
    global _registry_locked
    if _registry_locked:
        return

    _registry_locked = True
    try:
        _registry_table.create(bind=_engine, checkfirst=True)

        result = session.execute(select(_registry_table))
        for row in result.fetchall():
            schema = json.loads(row.schema_json)
            table_name = row.table_name
            if table_name not in metadata.tables:
                table = _table_from_schema(table_name, schema)
                metadata._add_table(table_name, None, table)
    finally:
        _registry_locked = False


def insert_record(session: Session, table_name: str, data: dict) -> uuid.UUID:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found. Create it first with create_table().")

    table = metadata.tables[table_name]

    if "id" not in data and "id" in table.c:
        data["id"] = uuid.uuid4()

    session.execute(table.insert().values(**data))
    session.commit()

    return data.get("id", uuid.uuid4())


def get_record(session: Session, table_name: str, record_id: uuid.UUID) -> dict | None:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found")

    table = metadata.tables[table_name]
    stmt = select(table).where(table.c.id == record_id)
    result = session.execute(stmt).first()

    if not result:
        return None

    return dict(result._mapping)


def update_record(session: Session, table_name: str, record_id: uuid.UUID, data: dict) -> dict | None:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found")

    table = metadata.tables[table_name]

    stmt = update(table).where(table.c.id == record_id).values(**data).returning(table)
    result = session.execute(stmt).first()
    session.commit()

    if not result:
        return None

    return dict(result._mapping)


def delete_record(session: Session, table_name: str, record_id: uuid.UUID) -> bool:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found")

    table = metadata.tables[table_name]

    result = session.execute(delete(table).where(table.c.id == record_id))
    session.commit()

    return result.rowcount > 0  # type: ignore


def query_records(
    session: Session,
    table_name: str,
    filters: dict | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    if table_name not in metadata.tables:
        raise ValueError(f"Table '{table_name}' not found")

    table = metadata.tables[table_name]
    stmt = select(table)

    if filters:
        for key, value in filters.items():
            if hasattr(table.c, key):
                stmt = stmt.where(table.c[key] == value)

    stmt = stmt.limit(limit).offset(offset)
    results = session.execute(stmt).fetchall()

    return [dict(row._mapping) for row in results]


def init_megabase(session: Session):
    _load_registry(session)
