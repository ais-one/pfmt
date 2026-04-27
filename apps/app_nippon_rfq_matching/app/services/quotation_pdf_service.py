"""
Quotation PDF Service

Service for generating quotation PDFs with product master data
"""

import logging
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.services.pdf_service import (
    BasePDFGenerator,
    PDFConfig,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import _thread_pool

logger = logging.getLogger(__name__)


class QuotationPDFService(BasePDFGenerator):
    """Service for generating quotation PDFs"""

    def __init__(self, config=None):
        if config is None:
            config = PDFConfig()
        super().__init__(config)
        self.export_dir = Path("data/output/quotations")
        self.export_dir.mkdir(parents=True, exist_ok=True)


# ============================================================================
# ASYNC QUOTATION PDF GENERATION
# ============================================================================


def generate_quotation_background(
    job_id: str, db: Session, quotation_data: dict[str, Any], bearer_token: str = ""
) -> dict[str, Any]:
    """
    Background function for generating quotation PDF

    Args:
        job_id: Job ID for tracking
        db: Database session
        quotation_data: Quotation data containing RFQ ID and items

    Returns:
        Job result with PDF generation status
    """
    logger.info(f"generate_quotation_background called with job_id: {job_id}")
    logger.info(f"quotation_data: {quotation_data}")

    try:
        # Import required modules

        # Initialize PDF service
        pdf_service = QuotationPDFService()

        # Get RFQ ID from quotation data
        rfq_id = quotation_data.get("rfq_id")
        if not rfq_id:
            raise ValueError("RFQ ID is required")

        # Use items from quotation_data
        items = quotation_data.get("items", [])
        if not items:
            raise ValueError(f"No items found in quotation data for RFQ ID: {rfq_id}")

        # Always perform matching when we have an RFQ ID
        if rfq_id:
            logger.info(f"Performing matching for RFQ: {rfq_id}")
            try:
                # Use pdf_comparison_export_service to get matched items
                from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
                    matching_data_service,
                )
                from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
                    PDFExporter,
                )

                pdf_exporter = PDFExporter()

                # Step 1: Load RFQ items
                rfq_items, error = pdf_exporter._load_and_validate_rfq_items(rfq_id, db)
                if error:
                    logger.error(f"Error loading RFQ items: {error}")
                    # Continue with original items if loading fails
                else:
                    # Step 2: Get normalized items (with normalization like pdf-export)
                    normalized_items = pdf_exporter._get_normalized_items(
                        rfq_items, True, db
                    )

                    # Step 3: Find product matches
                    matches = pdf_exporter.find_product_matches(normalized_items, db)

                    # Step 4: Enrich matches with product data
                    enriched_matches = (
                        matching_data_service.enrich_matches_with_product_data(
                            matches, db
                        )
                    )

                    # Convert enriched matches to items format for quotation
                    items = []
                    logger.info(
                        f"Processing {len(enriched_matches)} enriched matches for conversion"
                    )

                    for match in enriched_matches:
                        # Skip item if color_match is False (from rfq section)
                        rfq_data = match.get("rfq", {})
                        if not rfq_data.get("color_match", True):
                            logger.info(
                                f"Skipping item with color_match=False: {rfq_data}"
                            )
                            continue
                        logger.info(f"Processing match: {match}")
                        product_master = match.get("product_master")
                        if product_master:
                            # Get product code (PMC) from matched product master
                            product_code = product_master.get("pmc")
                            logger.info(f"Product code from master: {product_code}")

                            # Get RFQ item for quantity - match by rfq_item_id from the match
                            rfq_item = None
                            rfq_item_id = match.get("rfq", {}).get("id")
                            if rfq_item_id:
                                for rfq in rfq_items:
                                    if rfq.get("id") == rfq_item_id:
                                        rfq_item = rfq
                                        logger.info(f"Found matching RFQ item: {rfq}")
                                        break
                            if not rfq_item:
                                logger.warning(
                                    f"Could not find RFQ item with id {rfq_item_id}"
                                )

                            # Extract quantity from multiple sources
                            qty_value = rfq_item.get("qty") if rfq_item else None

                            # If qty is None, try to extract from raw_text using regex
                            if qty_value is None or qty_value == "":
                                import re

                                raw_text = rfq_item.get("raw_text", "")
                                # Look for numbers that might be quantities
                                qty_match = re.search(r"\b(\d+)\b", raw_text)
                                if qty_match:
                                    qty_value = qty_match.group(1)
                                    logger.info(
                                        f"Extracted quantity {qty_value} from raw text: {raw_text}"
                                    )

                            quantity = (
                                int(float(qty_value))
                                if qty_value is not None and qty_value != ""
                                else 0
                            )
                            unit_price = (
                                product_master.get("default_price", 0)
                                if product_master
                                else 0
                            )

                            item_code = product_master.get("pmc", "")
                            description = product_master.get("product_name", "")
                            color = product_master.get("color", "")
                            unit = product_master.get("default_uom", "PCS")

                            logger.info(
                                f"Adding item: {item_code} with quantity: {quantity}"
                            )
                            items.append(
                                {
                                    "item_code": item_code,
                                    "description": description,
                                    "color": color,
                                    "unit": unit,
                                    "quantity": quantity,
                                    "unit_price": unit_price,
                                    "line_total": quantity * unit_price,
                                }
                            )
                        else:
                            logger.warning(f"No product_master found in match: {match}")

                    logger.info(f"Generated {len(items)} matched items for quotation")

                    # Send tickets to AIS Manager for unmatched items
                    if rfq_id and rfq_items and items:
                        # Get original RFQ items count for comparison
                        original_items_count = len(rfq_items)
                        matched_items_count = len(items)
                        unmatched_items_count = (
                            original_items_count - matched_items_count
                        )

                        if unmatched_items_count > 0:
                            logger.info(
                                f"Found {unmatched_items_count} unmatched items for RFQ {rfq_id}"
                            )

                            # Prepare unmatched items data for tickets
                            unmatched_items = []
                            matched_item_ids = [
                                item.get("rfq_item_id")
                                for item in items
                                if item.get("rfq_item_id")
                            ]

                            for rfq_item in rfq_items:
                                # Check if this RFQ item was successfully matched
                                if rfq_item.id not in matched_item_ids:
                                    # Convert to dict format for AIS Manager
                                    unmatched_item = {
                                        "id": rfq_item.id,
                                        "raw_text": rfq_item.raw_text,
                                        "product_name": rfq_item.clean_text,
                                        "color": getattr(rfq_item, "color", ""),
                                        "uom": getattr(rfq_item, "uom", ""),
                                        "qty": getattr(rfq_item, "qty", ""),
                                        "product_type": getattr(
                                            rfq_item, "product_type", ""
                                        ),
                                        "source_brand": getattr(
                                            rfq_item, "source_brand", ""
                                        ),
                                        "product_master_id": None,
                                    }
                                    unmatched_items.append(unmatched_item)

                            # Create AIS Manager tickets for unmatched items
                            if unmatched_items:
                                try:
                                    from apps.app_nippon_rfq_matching.app.services.ais_auth_service import (
                                        ais_auth_service,
                                    )
                                    from apps.app_nippon_rfq_matching.app.services.ais_manager_service import (
                                        ais_manager_service,
                                    )

                                    def _send_tickets():
                                        try:
                                            # Extract user_id from token
                                            user_info = ais_auth_service.get_user_info(
                                                bearer_token
                                            )
                                            user_id = (
                                                user_info.get("data", {})
                                                .get("user", {})
                                                .get("id")
                                                if user_info
                                                else None
                                            )

                                            if not user_id:
                                                logger.warning(
                                                    "Could not extract user_id from token, using system"
                                                )
                                                user_id = "system"

                                            results = ais_manager_service.create_tickets_for_unmatched_items(
                                                unmatched_items=unmatched_items,
                                                rfq_id=rfq_id,
                                                user_id=user_id,
                                                bearer_token=bearer_token,
                                            )
                                            successful = len(
                                                [r for r in results if r is not None]
                                            )
                                            logger.info(
                                                f"AIS Manager: Created {successful} tickets for unmatched items in RFQ "
                                                f"{rfq_id}"
                                            )
                                            # Log successful post request
                                            if successful > 0:
                                                logger.info(
                                                    f"POST Request - AIS Manager Tickets: Successfully created "
                                                    f"{successful} tickets for unmatched items in RFQ {rfq_id}"
                                                )
                                        except Exception as e:
                                            logger.error(
                                                f"AIS Manager: Failed to create tickets: {e}"
                                            )

                                    # Run in thread pool to avoid blocking
                                    _thread_pool.submit(_send_tickets).result()

                                except Exception as e:
                                    logger.error(
                                        f"Failed to initialize AIS Manager ticket creation: {e}"
                                    )

            except Exception as e:
                logger.error(f"Error in matching flow: {e}")
                # Fallback to original items if matching fails

        # Check if there are no matched items after all processing
        if not items:
            logger.warning(
                f"No matched items found for RFQ {rfq_id}, skipping PDF generation"
            )
            return {
                "success": False,
                "error": "No matched items found for quotation",
                "job_id": job_id,
                "message": "No matched items found",
            }

        # Add client information if not provided
        if not quotation_data.get("client_info"):
            # Use default client info as RFQ model doesn't exist
            quotation_data["client_info"] = {
                "client_company": "Client Company",
                "client_address_1": "",
                "client_address_2": "",
                "contact_name": "",
                "client_phone": "",
                "client_email": "",
            }

        # Generate PDF
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d")
        quote_number = f"NPM-Q-{timestamp}"
        pdf_filename = (
            f"quotation_{rfq_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        )
        pdf_path = str(pdf_service.export_dir / pdf_filename)

        # Generate the PDF using WeasyPrint (same format as sync mode)
        try:
            from weasyprint import CSS, HTML

            # Update quotation data with processed items for HTML generation
            quotation_data["items"] = items
            quotation_data["quote_number"] = quote_number
            quotation_data["quote_date"] = datetime.now().strftime("%d %B %Y")

            # Calculate totals
            subtotal = sum(item["line_total"] for item in items)
            quotation_data["subtotal"] = subtotal
            quotation_data["total"] = subtotal

            logger.info(f"Quotation data populated {quotation_data}")

            # Generate HTML using the proper function from API
            from apps.app_nippon_rfq_matching.app.api.quotation_pdf import (
                generate_quotation_html,
            )

            html_content = generate_quotation_html(quotation_data)

            # Create PDF
            pdf_data = HTML(string=html_content).write_pdf(
                stylesheets=[CSS(string="@page { size: A4; }")]
            )

            if pdf_data is None:
                raise ValueError("Failed to generate PDF - WeasyPrint returned None")

            # Save PDF
            with open(pdf_path, "wb") as f:
                f.write(pdf_data)

        except Exception as e:
            logger.error(f"Error generating PDF with WeasyPrint: {e}")
            raise

        logger.info(f"Successfully generated PDF: {pdf_path}")

        return {
            "success": True,
            "pdf_path": pdf_path,
            "quote_number": quote_number,
            "job_id": job_id,
            "message": "Quotation PDF generated successfully",
        }

    except Exception as e:
        logger.error(f"Error in background job {job_id}: {e}")
        return {"success": False, "error": str(e), "job_id": job_id}
