import uuid

from lib.database import Base
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import relationship


class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    log_groups = relationship("LogGroup", back_populates="user", cascade="all, delete-orphan")
    log_group_files = relationship("LogGroupFile", back_populates="user", cascade="all, delete-orphan")
    log_group_processes = relationship("LogGroupProcess", back_populates="user", cascade="all, delete-orphan")


class Asset(Base):
    __tablename__ = "assets"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    log_group_files = relationship("LogGroupFile", back_populates="asset", cascade="all, delete-orphan")


class LogGroup(Base):
    __tablename__ = "logs"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    user = relationship("User", back_populates="log_groups")
    files = relationship("LogGroupFile", back_populates="log_group", cascade="all, delete-orphan")
    tables = relationship("LogGroupTable", back_populates="log_group", cascade="all, delete-orphan")
    processes = relationship("LogGroupProcess", back_populates="log_group", cascade="all, delete-orphan")


class LogGroupFile(Base):
    __tablename__ = "log_files"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    log_id = Column(String(36), ForeignKey("logs.id", ondelete="CASCADE"), nullable=False)
    asset_id = Column(String(36), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="log_group_files")
    log_group = relationship("LogGroup", back_populates="files")
    asset = relationship("Asset", back_populates="log_group_files")


class LogGroupTable(Base):
    __tablename__ = "log_tables"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    log_id = Column(String(36), ForeignKey("logs.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    columns = Column(String, nullable=False)  # JSON string of column names and types
    is_normalized = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    log_group = relationship("LogGroup", back_populates="tables")


class LogGroupProcess(Base):
    __tablename__ = "log_processes"

    id = Column(String(36), primary_key=True, index=True, nullable=False, default=lambda: str(uuid.uuid4()))
    log_id = Column(String(36), ForeignKey("logs.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    status = Column(String, nullable=False, default="processing")  # processing | completed | failed
    result = Column(String, nullable=True)  # JSON string of PreprocessorResult
    error = Column(String, nullable=True)
    schema_version = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    log_group = relationship("LogGroup", back_populates="processes")
    user = relationship("User", back_populates="log_group_processes")
