"""
RFQ models for product matching
"""

import json
from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from .base import Base


class RFQItem(Base):
    """RFQ items from PDF files"""

    __tablename__ = "rfq_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uploaded_file_id = Column(
        Integer, ForeignKey("uploaded_files.id"), nullable=True, index=True
    )
    rfq_id = Column(String(100), nullable=False, index=True)
    raw_text = Column(Text, nullable=False)
    clean_text = Column(Text, nullable=True, index=True)
    color = Column(String(200), nullable=True)  # Product color (for EML tables)
    qty = Column(String(50), nullable=True)
    uom = Column(String(50), nullable=True)
    source = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Unique constraint to prevent duplicate items within the same RFQ
    # Items are unique by rfq_id, clean_text, and color (to allow same product with different colors)
    __table_args__ = (
        UniqueConstraint(
            "rfq_id", "clean_text", "color", name="uq_rfq_id_clean_text_color"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "uploaded_file_id": self.uploaded_file_id,
            "rfq_id": self.rfq_id,
            "raw_text": self.raw_text,
            "clean_text": self.clean_text,
            "color": self.color,
            "qty": self.qty,
            "uom": self.uom,
            "source": self.source,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class RFQMatch(Base):
    """RFQ to Product Master matching results"""

    __tablename__ = "rfq_matches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rfq_item_id = Column(Integer, nullable=False, index=True)
    product_master_id = Column(Integer, nullable=False, index=True)
    matched_text = Column(String(500), nullable=True)
    score = Column(Float, nullable=False)
    method = Column(String(50), nullable=False)  # 'fuzzy' or 'cosine'
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "rfq_item_id": self.rfq_item_id,
            "product_master_id": self.product_master_id,
            "matched_text": self.matched_text,
            "score": self.score,
            "method": self.method,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class UploadedFile(Base):
    """Track uploaded files"""

    __tablename__ = "uploaded_files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    original_filename = Column(String(500), nullable=False)
    stored_filename = Column(String(500), nullable=False, unique=True)
    file_type = Column(String(50), nullable=False)  # 'excel' or 'pdf'
    file_path = Column(String(1000), nullable=False)
    status = Column(String(50), default="pending")  # pending, parsed, error
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "original_filename": self.original_filename,
            "stored_filename": self.stored_filename,
            "file_type": self.file_type,
            "file_path": self.file_path,
            "status": self.status,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Job(Base):
    """Background job tracking for async processing"""

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(100), nullable=False, unique=True, index=True)
    job_type = Column(
        String(50), nullable=False
    )  # 'rfq_parse_store' or 'iatp_parse_store'
    status = Column(
        String(50), default="pending"
    )  # pending, processing, completed, failed
    file_path = Column(String(1000), nullable=True)
    result = Column(Text, nullable=True)  # JSON string of result/parameters
    error_message = Column(Text, nullable=True)
    progress = Column(Integer, default=0)  # 0-100
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    def to_dict(self):
        """Return job dict with parsed result"""
        result = {
            "id": self.id,
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status,
            "file_path": self.file_path,
            "error_message": self.error_message,
            "progress": self.progress,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
        }

        # Parse JSON result to separate parameters and actual result
        if self.result:
            try:
                result_data = json.loads(self.result)
                # Check if this is parameters (has request_data key)
                if isinstance(result_data, dict) and "request_data" in result_data:
                    result["parameters"] = result_data
                # Check if this is actual result (has success key)
                elif isinstance(result_data, dict) and "success" in result_data:
                    result["result_data"] = result_data
                else:
                    result["result_data"] = result_data
            except Exception:
                result["result_data"] = None

        return result


class NormalizationCache(Base):
    """Cache table for OpenAI normalization results"""

    __tablename__ = "normalization_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_text = Column(String(500), nullable=False, unique=True, index=True)
    normalized_text = Column(String(500), nullable=True)
    normalized_color = Column(String(100), nullable=True)  # Extracted/normalized color
    product_type = Column(String(50), nullable=True)  # 'nippon', 'competitor', None
    match_confidence = Column(Float, default=1.0)
    times_used = Column(Integer, default=1)
    last_used_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Embedding vectors for semantic search (stored as JSON array)
    raw_text_embedding = Column(JSON, nullable=True)  # Embedding of raw_text
    normalized_text_embedding = Column(
        JSON, nullable=True
    )  # Embedding of normalized_text
    embedding_model = Column(String(100), nullable=True)  # Name of embedding model used

    def to_dict(self):
        return {
            "id": self.id,
            "raw_text": self.raw_text,
            "normalized_text": self.normalized_text,
            "normalized_color": self.normalized_color,
            "product_type": self.product_type,
            "match_confidence": self.match_confidence,
            "times_used": self.times_used,
            "last_used_at": self.last_used_at.isoformat()
            if self.last_used_at
            else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "has_embedding": self.raw_text_embedding is not None,
            "embedding_model": self.embedding_model,
        }


class ProductMasterMV(Base):
    """Materialized view for tokenized product names

    This table stores tokenized versions of distinct product names from product_master.
    Used for improved matching and search performance.
    """

    __tablename__ = "product_master_mv"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(Text, nullable=False, index=True)
    tokens = Column(Text, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Unique constraint on product_name
    __table_args__ = (
        UniqueConstraint("product_name", name="uq_product_master_mv_product_name"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "product_name": self.product_name,
            "tokens": self.tokens,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
