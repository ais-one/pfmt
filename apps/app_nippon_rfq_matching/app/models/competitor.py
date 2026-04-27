"""
Competitor matrix models
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from apps.app_nippon_rfq_matching.app.models.database import Base


class Generic(Base):
    """Generic product categories for competitor matrix"""

    __tablename__ = "generics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Brand(Base):
    """Competitor brands"""

    __tablename__ = "brands"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class CompetitorProduct(Base):
    """Competitor products"""

    __tablename__ = "competitor_products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False, index=True)
    name = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    brand = relationship("Brand", backref="products")

    __table_args__ = (
        UniqueConstraint("brand_id", "name", name="uq_competitor_product_brand_name"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "brand_id": self.brand_id,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ProductEquivalent(Base):
    """Direct mapping between competitor products and Nippon products"""

    __tablename__ = "product_equivalents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    competitor_product_id = Column(
        Integer, ForeignKey("competitor_products.id"), nullable=False, index=True
    )
    nippon_product_name = Column(String(500), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    competitor_product = relationship("CompetitorProduct", backref="nippon_equivalents")

    __table_args__ = (
        UniqueConstraint(
            "competitor_product_id", "nippon_product_name", name="uq_product_equivalent"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "competitor_product_id": self.competitor_product_id,
            "nippon_product_name": self.nippon_product_name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class CompetitorColorComparison(Base):
    """Competitor color comparison data for PDF parsing"""

    __tablename__ = "competitor_color_comparison"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_no = Column(Integer, nullable=False, index=True)
    source_brand = Column(
        String(50), nullable=False, index=True
    )  # "JOTUN" or "INTERNATIONAL"
    source_code = Column(String(200), nullable=False)  # vendor code
    npms_code = Column(String(200), nullable=True)  # mapped result
    raw_text = Column(Text, nullable=True)  # optional for debug
    uploaded_file_id = Column(
        Integer, ForeignKey("uploaded_files.id"), nullable=True, index=True
    )
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "item_no", "source_brand", "source_code", name="uq_competitor_color_item"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "item_no": self.item_no,
            "source_brand": self.source_brand,
            "source_code": self.source_code,
            "npms_code": self.npms_code,
            "raw_text": self.raw_text,
            "uploaded_file_id": self.uploaded_file_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
