from __future__ import annotations

import uuid

from sqlalchemy import select

from lib.megabase import SessionLocal, _table_from_schema, _uuid7, create_table, drop_table, insert_record, metadata


def test_uuid7_helper_generates_rfc_4122_version_7_uuid() -> None:
    value = _uuid7()

    assert isinstance(value, uuid.UUID)
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_megabase_table_schema_injects_indexed_uuid_id_column() -> None:
    table_name = "megabase_regression_table"
    metadata.clear()

    table = _table_from_schema(
        table_name,
        {
            "columns": [
                {"name": "message", "type": "text"},
                {"name": "severity", "type": "string"},
            ]
        },
    )

    assert "id" in table.c
    assert table.c.id.primary_key is True
    assert table.c.id.nullable is False
    assert table.c.id.default is not None
    assert any(index.name == f"ix_{table_name}_id" for index in table.indexes)
    assert list(table.columns.keys())[0] == "id"


def test_megabase_insert_preserves_explicit_uuid_id() -> None:
    table_name = f"megabase_insert_regression_{uuid.uuid4().hex[:8]}"
    metadata.clear()
    session = SessionLocal()

    try:
        create_table(session, table_name, {"columns": [{"name": "message", "type": "text"}]})

        supplied_id = uuid.uuid4()
        returned_id = insert_record(session, table_name, {"id": supplied_id, "message": "hello"})

        assert returned_id == supplied_id

        table = metadata.tables[table_name]
        row = session.execute(select(table)).first()
        assert row is not None
        assert row._mapping["id"] == supplied_id
    finally:
        drop_table(session, table_name)
        session.close()
