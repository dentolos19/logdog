from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from environment import DATABASE_URL
from lib.models import Base

engine = create_engine(
    DATABASE_URL.get_secret_value(),
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_database():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    Base.metadata.create_all(bind=engine)
    _ensure_log_process_file_column()
    _ensure_log_group_profile_column()


def _ensure_log_process_file_column() -> None:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "log_processes" not in table_names:
        return

    column_names = {column["name"] for column in inspector.get_columns("log_processes")}
    if "file_id" in column_names:
        return

    dialect = engine.dialect.name
    with engine.begin() as connection:
        if dialect == "postgresql":
            connection.execute(text("ALTER TABLE log_processes ADD COLUMN file_id UUID NULL"))
        else:
            connection.execute(text("ALTER TABLE log_processes ADD COLUMN file_id VARCHAR(36) NULL"))


def _ensure_log_group_profile_column() -> None:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "log_entries" not in table_names:
        return

    column_names = {column["name"] for column in inspector.get_columns("log_entries")}
    if "profile_name" in column_names:
        return

    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE log_entries ADD COLUMN profile_name VARCHAR(255) NULL"))
        connection.execute(text("UPDATE log_entries SET profile_name = 'default' WHERE profile_name IS NULL"))
