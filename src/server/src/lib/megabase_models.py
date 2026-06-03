from sqlalchemy import Column, DateTime, MetaData, String, Table, Text, func

metadata = MetaData()

registry_table = Table(
    "tables",
    metadata,
    Column("table_name", String, primary_key=True),
    Column("schema_json", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
