"""
PDF Comparison Export Service with OpenAI Normalization

This service handles the export of PDF comparison reports with OpenAI-based
normalization pipeline for RFQ items.

Split into multiple modules for better maintainability:
- base.py: Base class and database queries
- normalization.py: Normalization logic
- matchers.py: Nippon & Competitor matchers
- no_match.py: No match handling
- matcher.py: Main match orchestration
- exporter.py: PDF generation & export

Backward Compatibility:
This module re-exports the service to maintain the same import path:
    from apps.app_nippon_rfq_matching.app.services.pdf_comparison_export_service import pdf_comparison_export_service
"""

import logging

from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
    PDFExporter,
)

logger = logging.getLogger(__name__)


class PDFComparisonExportService(PDFExporter):
    """
    Service for exporting PDF comparison reports with OpenAI normalization.

    Pipeline:
    1. Load RFQ items from database (by RFQ ID)
    2. Normalize RFQ descriptions using OpenAI
    3. Match normalized items to products
    4. Generate PDF comparison report

    This class inherits from PDFExporter which combines all functionality:
    - PDFComparisonExportBase: Initialization and database queries
    - NormalizationMixin: OpenAI normalization
    - NipponMatcher: Nippon product matching
    - CompetitorMatcher: Competitor product matching
    - NoMatchHandler: No match handling
    - ProductMatcher: Match orchestration
    - PDFExporter: PDF generation and export
    """

    pass


# Singleton instance - maintains backward compatibility
pdf_comparison_export_service = PDFComparisonExportService()


# Export all classes for direct access
__all__ = [
    "PDFComparisonExportService",
    "pdf_comparison_export_service",
]
