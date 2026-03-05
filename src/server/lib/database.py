import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

_raw_url = os.getenv("DATABASE_URL", "")

# Use a local SQLite database unless DATABASE_URL points at a real Postgres instance.
if _raw_url.startswith("postgresql://") or _raw_url.startswith("postgres://"):
    DATABASE_URL = _raw_url
else:
    os.makedirs("store", exist_ok=True)
    DATABASE_URL = "sqlite:///./store/database.db"

# SQLite requires this flag to allow the same connection across multiple threads,
# which FastAPI uses internally. Other databases do not need this.
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

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
