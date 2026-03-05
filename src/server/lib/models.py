import uuid

from lib.database import Base
from sqlalchemy import Column, String


class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
