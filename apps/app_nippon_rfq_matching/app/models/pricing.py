"""
Pricing models for IATP Excel parser
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from apps.app_nippon_rfq_matching.app.models.database import Base


class Region(Base):
    """Regions for product pricing"""

    __tablename__ = "regions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ProductPrices(Base):
    """Product pricing information across different regions"""

    __tablename__ = "product_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_master_id = Column(
        Integer, ForeignKey("product_master.id"), nullable=False, index=True
    )
    region_id = Column(Integer, ForeignKey("regions.id"), nullable=False, index=True)
    size = Column(Float, nullable=True)
    uom = Column(String(20), nullable=True)
    price = Column(Float, nullable=True)
    price_raw = Column(String(50), nullable=True)  # Original value like "Enquiry"
    created_at = Column(DateTime, default=datetime.utcnow)

    product_master = relationship("ProductMaster", backref="prices")
    region = relationship("Region")

    __table_args__ = (
        UniqueConstraint(
            "product_master_id", "region_id", "size", "uom", name="uq_product_prices"
        ),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "product_master_id": self.product_master_id,
            "region_id": self.region_id,
            "region": self.region.name if self.region else None,
            "size": self.size,
            "uom": self.uom,
            "price": self.price,
            "price_raw": self.price_raw,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
