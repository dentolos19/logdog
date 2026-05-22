import uuid

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func
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

    log_groups = relationship("LogGroup", back_populates="user")
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


class LogGroup(Base):
    __tablename__ = "log_groups"

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

    user = relationship("User", back_populates="log_groups")
    files = relationship("LogFile", back_populates="group", cascade="all, delete-orphan")
    tables = relationship("LogTable", back_populates="group", cascade="all, delete-orphan")
    messages = relationship("LogMessage", back_populates="group", cascade="all, delete-orphan")
    processes = relationship("LogProcess", back_populates="group", cascade="all, delete-orphan")
    reports = relationship("LogReport", back_populates="group", cascade="all, delete-orphan")


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
    group_id = Column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="files")
    asset = relationship("Asset", back_populates="files")
    group = relationship("LogGroup", back_populates="files")
    processes = relationship("LogProcess", back_populates="file", cascade="all, delete-orphan")


class LogTable(Base):
    __tablename__ = "log_tables"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    group_id = Column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    table = Column(String, nullable=False)  # From the megabase
    schema = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    group = relationship("LogGroup", back_populates="tables")


class LogMessage(Base):
    __tablename__ = "log_messages"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    group_id = Column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), nullable=False)
    role = Column(String, nullable=False)
    content = Column(String, nullable=False)
    payload = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    group = relationship("LogGroup", back_populates="messages")


class LogProcess(Base):
    __tablename__ = "log_processes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    group_id = Column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), nullable=False, index=True)
    file_id = Column(UUID(as_uuid=True), ForeignKey("log_files.id", ondelete="SET NULL"), nullable=True, index=True)
    status = Column(String, nullable=False, default="queued")
    classification = Column(Text, nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    group = relationship("LogGroup", back_populates="processes")
    file = relationship("LogFile", back_populates="processes")


class LogReport(Base):
    __tablename__ = "log_reports"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        index=True,
        nullable=False,
        default=uuid.uuid4,
    )
    group_id = Column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), nullable=False, index=True)
    content = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    group = relationship("LogGroup", back_populates="reports")
