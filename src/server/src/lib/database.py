import os

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import text


MAIN_DATABASE_URL = os.getenv("MAIN_DATABASE_URL", "").strip()
if not MAIN_DATABASE_URL:
    raise RuntimeError("Missing MAIN_DATABASE_URL environment variable.")

engine = create_engine(MAIN_DATABASE_URL, pool_pre_ping=True)
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
    _ensure_assets_hash_column()


def _ensure_assets_hash_column() -> None:
    database_inspector = inspect(engine)
    if "assets" not in database_inspector.get_table_names():
        return

    column_names = {column["name"] for column in database_inspector.get_columns("assets")}
    if "hash" in column_names:
        return

    with engine.begin() as database_connection:
        database_connection.execute(text("ALTER TABLE assets ADD COLUMN hash VARCHAR(64)"))
