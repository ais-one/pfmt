"""
Ticket models for non-matching RFQ items
"""

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import relationship

from .base import Base


class Ticket(Base):
    """Ticket for non-matching RFQ items that need manual processing"""

    __tablename__ = "tickets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticket_id = Column(String(50), nullable=False, unique=True, index=True)
    rfq_id = Column(String(100), nullable=False, index=True)
    rfq_item_id = Column(Integer, ForeignKey("rfq_items.id"), nullable=True)
    raw_text = Column(Text, nullable=False)
    clean_text = Column(Text, nullable=True)
    extracted_qty = Column(String(50), nullable=True)
    extracted_uom = Column(String(50), nullable=True)
    extracted_color = Column(String(200), nullable=True)
    priority = Column(String(20), default="medium", index=True)  # low, medium, high
    reason = Column(
        String(100), nullable=False, index=True
    )  # no_match, price_needed, etc.
    status = Column(
        String(20), default="open", index=True
    )  # open, in_progress, resolved, closed
    assignee = Column(String(100), nullable=True)
    ticket_metadata = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship
    rfq_item = relationship("RFQItem", foreign_keys=[rfq_item_id])

    def to_dict(self):
        return {
            "id": self.id,
            "ticket_id": self.ticket_id,
            "rfq_id": self.rfq_id,
            "rfq_item_id": self.rfq_item_id,
            "raw_text": self.raw_text,
            "clean_text": self.clean_text,
            "extracted_qty": self.extracted_qty,
            "extracted_uom": self.extracted_uom,
            "extracted_color": self.extracted_color,
            "priority": self.priority,
            "reason": self.reason,
            "status": self.status,
            "assignee": self.assignee,
            "ticket_metadata": self.ticket_metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
