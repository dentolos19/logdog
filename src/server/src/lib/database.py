from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from environment import DATABASE_URL
from lib.models import Base

engine = create_engine(DATABASE_URL.get_secret_value())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_database():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    Base.metadata.create_all(bind=engine)
