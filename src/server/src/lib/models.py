import uuid

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
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
    profile_name = Column(String, nullable=True, default="default")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="entries")
    files = relationship("LogFile", back_populates="entry")
    tables = relationship("LogTable", back_populates="entry")
    messages = relationship("LogMessage", back_populates="entry")
    processes = relationship("LogProcess", back_populates="entry")
    reports = relationship("LogReport", back_populates="entry", cascade="all, delete-orphan")
    nl_queries = relationship("LogNlQuery", back_populates="entry", cascade="all, delete-orphan")


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
    processes = relationship("LogProcess", back_populates="file")


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
    file_id = Column(UUID(as_uuid=True), ForeignKey("log_files.id"), nullable=True, index=True)
    status = Column(String, nullable=False, default="queued")
    classification = Column(Text, nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    entry = relationship("LogEntry", back_populates="processes")
    file = relationship("LogFile", back_populates="processes")


class LogSchemaCacheEntry(Base):
    __tablename__ = "log_schema_cache_entries"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    cache_key = Column(String(128), nullable=False, unique=True, index=True)
    format_name = Column(String, nullable=False, index=True)
    detected_format = Column(String, nullable=False, index=True)
    structural_class = Column(String, nullable=False)
    domain = Column(String, nullable=False, default="unknown", index=True)
    profile_name = Column(String, nullable=True, index=True)
    parser_key = Column(String, nullable=False, default="unified")
    format_confidence = Column(Float, nullable=False, default=0.0)
    sample_hash = Column(String(64), nullable=False, index=True)
    fingerprint = Column(String(64), nullable=False, index=True)
    columns = Column(Text, nullable=False)
    extraction_strategy = Column(String, nullable=False, default="per_line")
    success_count = Column(Integer, nullable=False, default=0)
    failure_count = Column(Integer, nullable=False, default=0)
    access_count = Column(Integer, nullable=False, default=0)
    last_accessed = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class LogFewShotExample(Base):
    __tablename__ = "log_few_shot_examples"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    signature = Column(String(64), nullable=False, unique=True, index=True)
    format_name = Column(String, nullable=False, index=True)
    domain = Column(String, nullable=False, default="unknown", index=True)
    profile_name = Column(String, nullable=True, index=True)
    fingerprint = Column(String(64), nullable=True, index=True)
    confidence = Column(Float, nullable=False, default=1.0)
    usage_count = Column(Integer, nullable=False, default=0)
    sample_lines = Column(Text, nullable=False)
    schema = Column(Text, nullable=False)
    last_used = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class LogReport(Base):
    __tablename__ = "log_reports"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False, index=True)
    content = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    entry = relationship("LogEntry", back_populates="reports")


class LogNlQuery(Base):
    __tablename__ = "log_nl_queries"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    entry_id = Column(UUID(as_uuid=True), ForeignKey("log_entries.id"), nullable=False, index=True)
    question = Column(Text, nullable=False)
    generated_sql = Column(Text, nullable=False)
    results_json = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    entry = relationship("LogEntry", back_populates="nl_queries")
