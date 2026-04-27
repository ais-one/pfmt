"""
Backward Compatibility Module

This module is kept for backward compatibility. The actual implementation
has been moved to the pdf_comparison package.

Import from the new location:
    from apps.app_nippon_rfq_matching.app.services.pdf_comparison import pdf_comparison_export_service

Or continue using the old import (this module re-exports for compatibility):
    from apps.app_nippon_rfq_matching.app.services.pdf_comparison_export_service import pdf_comparison_export_service
"""

# Re-export from the new location for backward compatibility
from apps.app_nippon_rfq_matching.app.services.pdf_comparison import (
    PDFComparisonExportService,
    pdf_comparison_export_service,
)

__all__ = ["PDFComparisonExportService", "pdf_comparison_export_service"]
