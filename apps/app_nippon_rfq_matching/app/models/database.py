"""
Main database models for RFQ Product Matching System
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)

from .base import Base


class ProductMaster(Base):
    """Product master data from IATP Excel files"""

    __tablename__ = "product_master"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uploaded_file_id = Column(
        Integer, ForeignKey("uploaded_files.id"), nullable=True, index=True
    )
    sheet_name = Column(String(255), nullable=True)
    sheet_type = Column(String(50), nullable=False, index=True)
    row_excel = Column(Integer, nullable=True)
    pmc = Column(String(100), nullable=False, index=True)
    product_name = Column(String(500), nullable=False)
    color = Column(String(200), nullable=True)
    clean_product_name = Column(String(500), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Unique constraint to prevent duplicate products
    # A product is uniquely identified by sheet_name, pmc, product_name, and color
    __table_args__ = (
        UniqueConstraint(
            "sheet_name", "pmc", "product_name", "color", name="uq_product_master"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "uploaded_file_id": self.uploaded_file_id,
            "sheet_name": self.sheet_name,
            "sheet_type": self.sheet_type,
            "row_excel": self.row_excel,
            "pmc": self.pmc,
            "product_name": self.product_name,
            "color": self.color,
            "clean_product_name": self.clean_product_name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
