import uuid

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    entries = relationship("LogEntry", back_populates="user")
    files = relationship("LogFile", back_populates="user")


class Asset(Base):
    __tablename__ = "assets"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    name = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    hash = Column(String(64), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    files = relationship("LogFile", back_populates="asset")


class LogEntry(Base):
    __tablename__ = "log_entries"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="entries")
    files = relationship("LogFile", back_populates="entry")
    tables = relationship("LogTable", back_populates="entry")
    messages = relationship("LogMessage", back_populates="entry")
    parse_processes = relationship("LogProcess", back_populates="entry")


class LogFile(Base):
    __tablename__ = "log_files"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    asset_id = Column(UUID(as_uuid=True), ForeignKey("assets.id"), nullable=False)
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="files")
    asset = relationship("Asset", back_populates="files")
    entry = relationship("LogEntry", back_populates="files")


class LogTable(Base):
    __tablename__ = "log_tables"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False)
    name = Column(String, nullable=False)
    table = Column(String, nullable=False)  # From the megabase
    schema = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    entry = relationship("LogEntry", back_populates="tables")


class LogMessage(Base):
    __tablename__ = "log_messages"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False)
    role = Column(String, nullable=False)
    content = Column(String, nullable=False)
    payload = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    entry = relationship("LogEntry", back_populates="messages")


class LogProcess(Base):
    __tablename__ = "log_processes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False, index=True)
    status = Column(String, nullable=False, default="queued")
    classification = Column(Text, nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    entry = relationship("LogEntry", back_populates="parse_processes")
