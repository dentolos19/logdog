import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String, unique=True, index=True)
    password: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    log_groups: Mapped[list["LogGroup"]] = relationship(back_populates="user")
    files: Mapped[list["LogFile"]] = relationship(back_populates="user")


class Asset(Base):
    __tablename__ = "assets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String)
    size: Mapped[int]
    type: Mapped[str] = mapped_column(String)
    hash: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    files: Mapped[list["LogFile"]] = relationship(back_populates="asset")


class LogGroup(Base):
    __tablename__ = "log_groups"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(String)
    profile_name: Mapped[str | None] = mapped_column(String, default="default")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[User] = relationship(back_populates="log_groups")
    files: Mapped[list["LogFile"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    tables: Mapped[list["LogTable"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    messages: Mapped[list["LogMessage"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    processes: Mapped[list["LogProcess"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    reports: Mapped[list["LogReport"]] = relationship(back_populates="group", cascade="all, delete-orphan")
    table_summaries: Mapped[list["LogTableSummary"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )


class LogFile(Base):
    __tablename__ = "log_files"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id"))
    group_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[User] = relationship(back_populates="files")
    asset: Mapped[Asset] = relationship(back_populates="files")
    group: Mapped[LogGroup] = relationship(back_populates="files")
    processes: Mapped[list["LogProcess"]] = relationship(back_populates="file", cascade="all, delete-orphan")


class LogTable(Base):
    __tablename__ = "log_tables"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    group_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String)
    table: Mapped[str] = mapped_column(String)  # From the megabase.
    schema: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    group: Mapped[LogGroup] = relationship(back_populates="tables")


class LogMessage(Base):
    __tablename__ = "log_messages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    group_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"))
    role: Mapped[str] = mapped_column(String)
    content: Mapped[str] = mapped_column(String)
    payload: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    group: Mapped[LogGroup] = relationship(back_populates="messages")


class LogProcess(Base):
    __tablename__ = "log_processes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), index=True
    )
    file_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_files.id", ondelete="SET NULL"), index=True
    )
    status: Mapped[str] = mapped_column(String, default="queued")
    classification: Mapped[str | None] = mapped_column(Text)
    result: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    group: Mapped[LogGroup] = relationship(back_populates="processes")
    file: Mapped[LogFile | None] = relationship(back_populates="processes")


class LogReport(Base):
    __tablename__ = "log_reports"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), index=True
    )
    content: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    group: Mapped[LogGroup] = relationship(back_populates="reports")


class LogTableSummary(Base):
    __tablename__ = "log_table_summaries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, index=True, default=uuid.uuid4)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_groups.id", ondelete="CASCADE"), index=True
    )
    table_name: Mapped[str] = mapped_column(String)
    content: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    group: Mapped[LogGroup] = relationship(back_populates="table_summaries")
