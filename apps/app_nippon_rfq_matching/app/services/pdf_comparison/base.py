"""
PDF Comparison Export Base Module

This module contains the base class for PDF comparison export service.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.competitor import CompetitorProduct
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem
from apps.app_nippon_rfq_matching.app.services.pdf_service import PDFService

logger = logging.getLogger(__name__)


class PDFComparisonExportBase:
    """
    Base class for PDF comparison export service.

    Handles initialization and database queries.
    """

    def __init__(self):
        """Initialize the PDF comparison export service."""
        self.export_dir = settings.STORAGE_DIR / "pdf_exports"
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_service = PDFService()

    def get_rfq_items_by_id(self, rfq_id: str, db: Session) -> list[dict[str, Any]]:
        """
        Get RFQ items by RFQ ID from database.

        Args:
            rfq_id: RFQ identifier
            db: Database session

        Returns:
            List of RFQ item dictionaries
        """
        items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        return [item.to_dict() for item in items]

    def get_competitor_products_list(self, db: Session) -> list[str]:
        """
        Get list of competitor product names from database.

        Args:
            db: Database session

        Returns:
            List of competitor product names
        """
        products = db.query(CompetitorProduct.name).distinct().all()
        return [p[0] for p in products if p[0]]
