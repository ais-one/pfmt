"""
PDF Comparison Exporter Module

This module contains the PDFExporter class that handles PDF generation
and export functionality.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
    matching_data_service,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.matcher import (
    ProductMatcher,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.normalization import (
    NormalizationMixin,
)
from apps.app_nippon_rfq_matching.app.services.pdf_service import (
    MatchData,
    PDFConfig,
    PDFFormatType,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import _thread_pool

logger = logging.getLogger(__name__)


class PDFExporter(ProductMatcher, NormalizationMixin):
    """
    PDF Exporter class that handles PDF generation and export.

    Inherits from ProductMatcher and NormalizationMixin to provide
    complete export pipeline functionality.
    """

    def generate_pdf_report(
        self,
        rfq_id: str,
        matches: list[dict[str, Any]],
        format_type: PDFFormatType = PDFFormatType.TABLE,
        config: PDFConfig | None = None,
    ) -> str:
        """
        Generate PDF comparison report.

        Args:
            rfq_id: RFQ identifier
            matches: List of match results
            format_type: PDF format type
            config: Optional PDF configuration

        Returns:
            Path to generated PDF file
        """
        # Create match data
        rfq_items = [m["rfq"] for m in matches]
        match_data = MatchData(rfq_id=rfq_id, rfq_items=rfq_items, matches=matches)

        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"comparison_{rfq_id}_{timestamp}.pdf"
        output_path = str(self.export_dir / filename)

        # Generate PDF
        self.pdf_service.generate_pdf(
            data=match_data,
            format_type=format_type,
            config=config,
            output_path=output_path,
        )

        logger.info(f"Generated PDF report: {output_path}")
        return output_path

    def _load_and_validate_rfq_items(
        self, rfq_id: str, db: Session
    ) -> tuple[list, dict | None]:
        """
        Load and validate RFQ items.

        Args:
            rfq_id: RFQ identifier
            db: Database session

        Returns:
            Tuple of (rfq_items list or None, error_dict or None)
        """
        logger.info(f"Step 1: Loading RFQ items for {rfq_id}")
        rfq_items = self.get_rfq_items_by_id(rfq_id, db)

        if not rfq_items:
            return None, {
                "success": False,
                "error": f"No RFQ items found for RFQ ID: {rfq_id}",
            }

        logger.info(f"Found {len(rfq_items)} RFQ items")
        return rfq_items, None

    def _create_pdf_config(self, rfq_id: str) -> PDFConfig:
        """
        Create PDF configuration.

        Args:
            rfq_id: RFQ identifier

        Returns:
            PDFConfig object
        """
        return PDFConfig(
            title=f"RFQ Comparison Report - {rfq_id}",
            show_timestamp=True,
            show_page_numbers=True,
        )

    def _calculate_statistics(
        self, rfq_items: list[dict], matches: list[dict]
    ) -> dict[str, Any]:
        """
        Calculate export statistics.

        Args:
            rfq_items: List of RFQ items
            matches: List of enriched match results

        Returns:
            Statistics dictionary
        """
        matched_count = sum(
            1 for m in matches if m["product_master"] and m["product_master"].get("id")
        )
        normalized_count = sum(1 for m in matches if m["rfq"].get("normalized_name"))

        # Count matches with color information
        color_matches = sum(
            1
            for m in matches
            if m["product_master"] and m["product_master"].get("color")
        )

        # Count matches with pricing information
        price_matches = sum(
            1
            for m in matches
            if m["product_master"] and m["product_master"].get("pricing")
        )

        return {
            "total_items": len(rfq_items),
            "normalized_count": normalized_count,
            "matched_count": matched_count,
            "unmatched_count": len(rfq_items) - matched_count,
            "color_matches": color_matches,
            "price_matches": price_matches,
            "match_rate": round(matched_count / len(rfq_items) * 100, 2)
            if rfq_items
            else 0,
            "color_coverage": round(color_matches / matched_count * 100, 2)
            if matched_count
            else 0,
            "price_coverage": round(price_matches / matched_count * 100, 2)
            if matched_count
            else 0,
        }

    def _build_export_success_result(
        self,
        rfq_id: str,
        pdf_path: str,
        rfq_items: list[dict],
        matches: list[dict],
        format_type: str,
        use_normalization: bool,
    ) -> dict[str, Any]:
        """
        Build export success result dictionary.

        Args:
            rfq_id: RFQ identifier
            pdf_path: Generated PDF path
            rfq_items: List of RFQ items
            matches: List of match results
            format_type: PDF format type
            use_normalization: Whether normalization was enabled

        Returns:
            Success result dictionary
        """
        return {
            "success": True,
            "rfq_id": rfq_id,
            "pdf_path": pdf_path,
            "pdf_filename": Path(pdf_path).name,
            "statistics": self._calculate_statistics(rfq_items, matches),
            "format_type": format_type,
            "normalization_enabled": use_normalization,
        }

    def _build_export_error_result(self, error: Exception) -> dict[str, Any]:
        """
        Build export error result dictionary.

        Args:
            error: Exception object

        Returns:
            Error result dictionary
        """
        logger.error(f"Error exporting comparison report: {error}", exc_info=True)
        return {"success": False, "error": str(error)}

    def export_comparison_report(
        self,
        rfq_id: str,
        db: Session,
        format_type: str = "table",
        use_normalization: bool = True,
        bearer_token: str = "",
    ) -> dict[str, Any]:
        """
        Export PDF comparison report with full pipeline.

        Args:
            rfq_id: RFQ identifier
            db: Database session
            format_type: PDF format type (table, side_by_side, summary)
            use_normalization: Whether to use OpenAI normalization

        Returns:
            Export result dictionary
        """
        try:
            rfq_items, error = self._load_and_validate_rfq_items(rfq_id, db)
            if error:
                return error

            normalized_items = self._get_normalized_items(
                rfq_items, use_normalization, db
            )

            logger.info(f"Step 2: Normalized {len(normalized_items)} items")
            logger.info(f"Normalized items: {normalized_items}")

            logger.info("Step 3: Finding product matches")
            matches = self.find_product_matches(normalized_items, db)

            logger.info("Step 4: Enriching matches with product data")
            enriched_matches = matching_data_service.enrich_matches_with_product_data(
                matches, db
            )

            # Step 5: Send tickets to AIS Manager for unmatched items
            logger.info("Step 5: Processing unmatched items for AIS Manager tickets")
            unmatched_items = []
            for match in enriched_matches:
                if not (match["product_master"] and match["product_master"].get("id")):
                    # Convert to dict format for AIS Manager
                    unmatched_item = {
                        "id": match["rfq_item"]["id"],
                        "raw_text": match["rfq_item"]["raw_text"],
                        "product_name": match["rfq_item"]["clean_text"],
                        "color": match["rfq_item"].get("color", ""),
                        "uom": match["rfq_item"].get("uom", ""),
                        "qty": match["rfq_item"].get("qty", ""),
                        "product_type": match["rfq_item"].get("product_type", ""),
                        "source_brand": match["rfq_item"].get("source_brand", ""),
                        "product_master_id": None,
                    }
                    unmatched_items.append(unmatched_item)

            # Create tickets for unmatched items
            if unmatched_items:
                try:
                    from apps.app_nippon_rfq_matching.app.services.ais_manager_service import (
                        ais_manager_service,
                    )

                    def _send_tickets():
                        try:
                            results = (
                                ais_manager_service.create_tickets_for_unmatched_items(
                                    unmatched_items=unmatched_items,
                                    rfq_id=rfq_id,
                                    user_id="system",
                                    bearer_token=bearer_token,
                                )
                            )
                            successful = len([r for r in results if r is not None])
                            logger.info(
                                f"AIS Manager: Created {successful} tickets for unmatched items in RFQ {rfq_id}"
                            )
                            # Log successful post request
                            if successful > 0:
                                logger.info(
                                    f"POST Request - AIS Manager Tickets: Successfully created {successful} tickets "
                                    f"for unmatched items in RFQ {rfq_id}"
                                )
                        except Exception as e:
                            logger.error(f"AIS Manager: Failed to create tickets: {e}")

                    # Run in thread pool to avoid blocking
                    _thread_pool.submit(_send_tickets).result()

                except Exception as e:
                    logger.error(
                        f"Failed to initialize AIS Manager ticket creation: {e}"
                    )

            logger.info("Step 6: Generating PDF report")
            pdf_format = PDFFormatType(format_type)
            config = self._create_pdf_config(rfq_id)

            pdf_path = self.generate_pdf_report(
                rfq_id=rfq_id,
                matches=enriched_matches,
                format_type=pdf_format,
                config=config,
            )

            return self._build_export_success_result(
                rfq_id,
                pdf_path,
                rfq_items,
                enriched_matches,
                format_type,
                use_normalization,
            )

        except Exception as e:
            return self._build_export_error_result(e)
