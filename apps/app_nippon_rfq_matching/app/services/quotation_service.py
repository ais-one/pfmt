"""
Quotation Generation Service

Service for generating quotations based on RFQ items matched with Product Master
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.pricing import Region
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem
from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
    matching_data_service,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
    PDFExporter,
)
from apps.app_nippon_rfq_matching.app.services.pdf_service import (
    PDFConfig,
    PDFFormatType,
)

logger = logging.getLogger(__name__)


class QuotationService:
    """Service for generating quotations from matched RFQ items"""

    def __init__(self):
        """Initialize quotation service"""
        self.export_dir = Path("storage/quotation_exports")
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def prepare_quotation_data(
        self, rfq_id: str, region: str = "Indonesia", db: Session = None
    ) -> dict[str, Any]:
        """
        Prepare quotation data from matched RFQ items.

        Args:
            rfq_id: RFQ identifier
            region: Region for pricing (default: "Indonesia")
            db: Database session

        Returns:
            Prepared quotation data
        """
        # Get RFQ items
        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        if not rfq_items:
            return {
                "success": False,
                "error": f"No RFQ items found for RFQ ID: {rfq_id}",
            }

        rfq_items_dict = [item.to_dict() for item in rfq_items]

        # Get normalized items
        pdf_exporter = PDFExporter()
        normalized_items = pdf_exporter._get_normalized_items(
            rfq_items_dict, use_normalization=True, db=db
        )

        # Find matches
        matches = pdf_exporter.find_product_matches(normalized_items, db)

        # Enrich with product data
        enriched_matches = matching_data_service.enrich_matches_with_product_data(
            matches, db
        )

        # Separate matched and unmatched items
        matched_items = []
        unmatched_items = []

        for match in enriched_matches:
            if match["product_master"] and match["product_master"].get("id"):
                matched_items.append(match)
            else:
                unmatched_items.append(match)

        # Prepare quotation summary
        summary = self._calculate_quotation_summary(matched_items, region)

        # Prepare items for quotation
        quotation_items = self._prepare_quotation_items(matched_items, region)

        return {
            "success": True,
            "rfq_id": rfq_id,
            "generated_at": datetime.utcnow().isoformat(),
            "region": region,
            "summary": summary,
            "items": quotation_items,
            "unmatched_items": unmatched_items,
            "statistics": {
                "total_items": len(rfq_items),
                "matched_count": len(matched_items),
                "unmatched_count": len(unmatched_items),
                "total_value": summary["total_value"],
                "coverage_rate": round(len(matched_items) / len(rfq_items) * 100, 2)
                if rfq_items
                else 0,
            },
        }

    def _calculate_quotation_summary(
        self, matched_items: list[dict[str, Any]], region: str
    ) -> dict[str, Any]:
        """
        Calculate quotation summary.

        Args:
            matched_items: List of matched items
            region: Region name

        Returns:
            Summary dictionary
        """
        total_value = 0.0
        total_items = 0
        by_product_type = {}

        for item in matched_items:
            product_master = item.get("product_master", {})
            pricing_list = product_master.get("pricing", [])

            # Find pricing for the region
            item_price = 0.0
            product_type = product_master.get("sheet_type", "unknown")

            for pricing in pricing_list:
                if pricing.get("region") == region:
                    item_price = float(pricing.get("price", 0) or 0)
                    pricing.get("uom", "")
                    break

            # Fallback to first available pricing
            if item_price == 0 and pricing_list:
                first_pricing = pricing_list[0]
                item_price = float(first_pricing.get("price", 0) or 0)
                first_pricing.get("uom", "")

            total_value += item_price
            total_items += 1

            # Group by product type
            if product_type not in by_product_type:
                by_product_type[product_type] = {"count": 0, "value": 0.0}

            by_product_type[product_type]["count"] += 1
            by_product_type[product_type]["value"] += item_price

        return {
            "total_items": total_items,
            "total_value": round(total_value, 2),
            "currency": "USD",
            "by_product_type": by_product_type,
            "region": region,
        }

    def _prepare_quotation_items(
        self, matched_items: list[dict[str, Any]], region: str
    ) -> list[dict[str, Any]]:
        """
        Prepare items for quotation display.

        Args:
            matched_items: List of matched items
            region: Region name

        Returns:
            List of prepared quotation items
        """
        quotation_items = []

        for idx, match in enumerate(matched_items, 1):
            rfq_data = match.get("rfq", {})
            product_master = match.get("product_master", {})
            match_info = match.get("match_info", {})

            # Get pricing for the region
            pricing_list = product_master.get("pricing", [])
            selected_pricing = None
            all_pricing = []

            # First, try to get pricing for the specified region
            for pricing in pricing_list:
                all_pricing.append(pricing)  # Collect all pricing
                if pricing.get("region") == region:
                    selected_pricing = pricing
                    break

            # Fallback to first available pricing
            if not selected_pricing and pricing_list:
                selected_pricing = pricing_list[0]

            # Prepare item data
            item = {
                "line_number": idx,
                "rfq_raw_text": rfq_data.get("raw_text", ""),
                "rfq_qty": rfq_data.get("qty"),
                "rfq_uom": rfq_data.get("uom"),
                "product_id": product_master.get("id"),
                "product_code": product_master.get("pmc"),
                "product_name": product_master.get("product_name"),
                "color": product_master.get("color"),
                "matched_name": match_info.get(
                    "competitor_product", product_master.get("clean_product_name")
                ),
                "price": selected_pricing.get("price") if selected_pricing else 0,
                "price_raw": selected_pricing.get("price_raw")
                if selected_pricing
                else None,
                "uom": selected_pricing.get("uom")
                if selected_pricing
                else rfq_data.get("uom"),
                "size": selected_pricing.get("size") if selected_pricing else None,
                "region": selected_pricing.get("region")
                if selected_pricing
                else region,
                "all_pricing": all_pricing,  # Include all available pricing
                "match_score": match_info.get("score", 0),
                "match_method": match_info.get("method", "unknown"),
                "color_match": match_info.get("color_match", False),
                "source_brand": match_info.get("source_brand"),
                "source_color_code": match_info.get("source_color_code"),
                "npms_color_code": match_info.get("npms_color_code"),
                "is_competitor": match_info.get("competitor_product") is not None,
                "normalized_name": rfq_data.get("normalized_name"),
                "normalized_color": rfq_data.get("normalized_color"),
                "sheet_type": product_master.get("sheet_type"),
            }

            # Calculate extended price
            if item["price"] and item["rfq_qty"]:
                try:
                    item["extended_price"] = float(item["price"]) * float(
                        item["rfq_qty"]
                    )
                except (ValueError, TypeError):
                    item["extended_price"] = 0.0
            else:
                item["extended_price"] = 0.0

            quotation_items.append(item)

        return quotation_items

    def generate_quotation_pdf(
        self,
        rfq_id: str,
        region: str = "Indonesia",
        format_type: str = "table",
        db: Session = None,
        config: PDFConfig | None = None,
    ) -> dict[str, Any]:
        """
        Generate quotation PDF.

        Args:
            rfq_id: RFQ identifier
            region: Region for pricing
            format_type: PDF format (table, side_by_side, summary)
            db: Database session
            config: Optional PDF configuration

        Returns:
            PDF generation result
        """
        try:
            # Prepare quotation data
            quotation_data = self.prepare_quotation_data(rfq_id, region, db)

            if not quotation_data["success"]:
                return quotation_data

            # Create PDF config if not provided
            if not config:
                config = PDFConfig(
                    title=f"Quotation - {rfq_id}",
                    show_timestamp=True,
                    show_page_numbers=True,
                )

            # Generate PDF
            pdf_path = self._generate_pdf_file(quotation_data, format_type, config)

            return {
                "success": True,
                "rfq_id": rfq_id,
                "region": region,
                "pdf_path": pdf_path,
                "pdf_filename": Path(pdf_path).name,
                "statistics": quotation_data["statistics"],
                "summary": quotation_data["summary"],
            }

        except Exception as e:
            logger.error(f"Error generating quotation PDF: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _generate_pdf_file(
        self, quotation_data: dict[str, Any], format_type: str, config: PDFConfig
    ) -> str:
        """
        Generate PDF file using the PDF service.

        Args:
            quotation_data: Prepared quotation data
            format_type: PDF format type
            config: PDF configuration

        Returns:
            Path to generated PDF file
        """
        # Import here to avoid circular dependencies
        from apps.app_nippon_rfq_matching.app.services.pdf_service import (
            QuotationPDFService,
        )

        pdf_service = QuotationPDFService()

        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"quotation_{quotation_data['rfq_id']}_{timestamp}.pdf"
        output_path = str(self.export_dir / filename)

        # Generate PDF
        pdf_service.generate_quotation_pdf(
            data=quotation_data,
            format_type=PDFFormatType(format_type),
            config=config,
            output_path=output_path,
        )

        logger.info(f"Generated quotation PDF: {output_path}")
        return output_path

    def get_quotation_pricing_options(
        self, rfq_id: str, db: Session = None
    ) -> dict[str, Any]:
        """
        Get available pricing options for a quotation.

        Args:
            rfq_id: RFQ identifier
            db: Database session

        Returns:
            Available pricing options
        """
        # Get all regions
        regions = db.query(Region).all()
        region_names = [r.name for r in regions]

        # Get matched items
        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        if not rfq_items:
            return {
                "success": False,
                "error": f"No RFQ items found for RFQ ID: {rfq_id}",
            }

        # Calculate summary for each region
        region_summary = {}
        for region in region_names:
            # Prepare data for this region
            rfq_items_dict = [item.to_dict() for item in rfq_items]

            pdf_exporter = PDFExporter()
            normalized_items = pdf_exporter._get_normalized_items(
                rfq_items_dict, use_normalization=True, db=db
            )

            matches = pdf_exporter.find_product_matches(normalized_items, db)
            enriched_matches = matching_data_service.enrich_matches_with_product_data(
                matches, db
            )

            # Filter matched items
            matched_items = [
                m
                for m in enriched_matches
                if m["product_master"] and m["product_master"].get("id")
            ]

            # Calculate summary for this region
            summary = self._calculate_quotation_summary(matched_items, region)
            region_summary[region] = summary

        return {
            "success": True,
            "rfq_id": rfq_id,
            "available_regions": region_names,
            "region_summary": region_summary,
            "default_region": "Indonesia",
        }


# Singleton instance
quotation_service = QuotationService()
