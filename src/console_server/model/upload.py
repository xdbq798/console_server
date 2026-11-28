"""断点续传 + 秒传的数据库模型。

- upload_sessions：上传会话，以整文件 MD5 为主键，未合并会话是续传的状态源。
- upload_chunks：已成功落盘的分片，(file_hash, chunk_index) 去重保证幂等。
- upload_files：秒传索引，merge 成功才写入，独立于会话生命周期（清理会话不影响秒传）。
"""

from sqlalchemy import BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .common import Base


class UploadSession(Base):
    __tablename__ = "upload_sessions"

    file_hash = Column(String(32), primary_key=True)
    file_name = Column(String(512), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    chunk_size = Column(Integer, nullable=False)
    chunk_count = Column(Integer, nullable=False)
    total_hash = Column(String(32), nullable=False)
    created_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=func.now(), onupdate=func.now(), nullable=False
    )
    merged = Column(Boolean, default=False, nullable=False)
    merged_path = Column(String(1024), nullable=True)
    last_error = Column(String(256), nullable=True)

    chunks = relationship(
        "UploadChunk", back_populates="session", cascade="all, delete-orphan"
    )


class UploadChunk(Base):
    __tablename__ = "upload_chunks"

    file_hash = Column(
        String(32),
        ForeignKey("upload_sessions.file_hash", ondelete="CASCADE"),
        primary_key=True,
    )
    chunk_index = Column(Integer, primary_key=True)
    chunk_hash = Column(String(32), nullable=False)
    uploaded_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)

    session = relationship("UploadSession", back_populates="chunks")


class UploadFile(Base):
    __tablename__ = "upload_files"

    file_hash = Column(String(32), primary_key=True)
    path = Column(String(1024), nullable=False)
    file_name = Column(String(512), nullable=False)
    size = Column(BigInteger, nullable=False)
    merged_at = Column(DateTime(timezone=True), default=func.now(), nullable=False)
