"""
Quotation Matching Service

Service for processing RFQ items with matching pipeline to get enriched product data.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def get_pricing_for_region(
    db: Session, product_master_id: int, region_name: str = "Indonesia"
) -> dict[str, Any] | None:
    """
    Get pricing information for a product in a specific region.

    Args:
        db: Database session
        product_master_id: Product master ID
        region_name: Region name (default: "Indonesia")

    Returns:
        Dictionary with pricing information or None
    """
    try:
        from apps.app_nippon_rfq_matching.app.models.pricing import (
            ProductPrices,
            Region,
        )

        # Find region by name
        region = db.query(Region).filter(Region.name == region_name).first()

        if not region:
            # Fallback to default region
            region = db.query(Region).filter(Region.name == "NPMC").first()
            if not region:
                return None

        # Find pricing for this product and region
        pricing = (
            db.query(ProductPrices)
            .filter(
                ProductPrices.product_master_id == product_master_id,
                ProductPrices.region_id == region.id,
            )
            .first()
        )

        if pricing:
            return {
                "unit_price": pricing.price,
                "price_raw": pricing.price_raw,
                "size": pricing.size,
                "uom": pricing.uom,
                "region": pricing.region.name if pricing.region else None,
            }

        return None

    except Exception as e:
        logger.error(
            f"Error getting pricing for product_master_id {product_master_id} in region {region_name}: {e}"
        )
        return None


def process_rfq_items_with_matching(
    rfq_items: list[Any], db: Session, region: str = "Indonesia"
) -> dict[str, Any]:
    """
    Process RFQ items with matching pipeline to get enriched product data.

    Args:
        rfq_items: List of RFQ items
        db: Database session
        region: Region for pricing (default: "Indonesia")

    Returns:
        Dictionary with matching results and quotation items
    """
    from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
        matching_data_service,
    )
    from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
        PDFExporter,
    )

    try:
        # Convert RFQ items to dict format
        rfq_items_dict = [item.to_dict() for item in rfq_items]

        # Initialize PDFExporter
        pdf_exporter = PDFExporter()

        # Step 1: Normalize items
        logger.info("Step 1: Normalizing RFQ items")
        normalized_items = pdf_exporter._get_normalized_items(
            rfq_items_dict, use_normalization=True, db=db
        )

        # Step 2: Find product matches
        logger.info("Step 2: Finding product matches")
        matches = pdf_exporter.find_product_matches(normalized_items, db)

        # Step 3: Enrich matches with product data
        logger.info("Step 3: Enriching matches with product data")
        enriched_matches = matching_data_service.enrich_matches_with_product_data(
            matches, db
        )

        # Step 4: Prepare quotation items from enriched matches
        logger.info("Step 4: Preparing quotation items")
        quotation_items = []

        for match in enriched_matches:
            rfq_data = match.get("rfq", {})
            product_master = match.get("product_master", {})
            match_info = match.get("match_info", {})

            # Skip if no product master found
            if not product_master or not product_master.get("id"):
                logger.warning(
                    f"No product match found for: {rfq_data.get('raw_text', 'Unknown')}"
                )
                continue

            # Get pricing for the specified region
            pricing_info = get_pricing_for_region(db, product_master["id"], region)

            # Prepare quotation item
            quotation_item = {
                "item_code": product_master.get(
                    "pmc", f"ITEM-{rfq_data.get('id', '')}"
                ),
                "description": product_master.get(
                    "product_name", rfq_data.get("raw_text", "")
                ),
                "color": product_master.get("color", rfq_data.get("color", "")),
                "unit": pricing_info.get("uom", rfq_data.get("uom", ""))
                if pricing_info
                else rfq_data.get("uom", ""),
                "quantity": float(rfq_data.get("qty", 1) or 1),
                "unit_price": float(pricing_info.get("unit_price", 0))
                if pricing_info
                else 0.0,
                "product_id": product_master.get("id"),
                "product_code": product_master.get("pmc"),
                "product_name": product_master.get("product_name"),
                "matched_name": match_info.get(
                    "competitor_product", product_master.get("clean_product_name")
                ),
                "match_score": match_info.get("score", 0),
                "match_method": match_info.get("method", "unknown"),
                "region": region,
                "all_pricing": product_master.get("pricing", []),
                "normalized_name": rfq_data.get("normalized_name"),
                "normalized_color": rfq_data.get("normalized_color"),
                "sheet_type": product_master.get("sheet_type"),
            }

            # Add source information
            if match_info.get("competitor_product"):
                quotation_item["source_brand"] = match_info.get("source_brand")
                quotation_item["source_color_code"] = match_info.get(
                    "source_color_code"
                )
                quotation_item["npms_color_code"] = match_info.get("npms_color_code")
                quotation_item["is_competitor"] = True
            else:
                quotation_item["is_competitor"] = False

            quotation_items.append(quotation_item)
            logger.info(
                f"Matched item: {quotation_item['item_code']} - {quotation_item['description']} - "
                f"${quotation_item['unit_price']}"
            )

        return {
            "success": True,
            "total_items": len(rfq_items_dict),
            "matched_count": len(quotation_items),
            "unmatched_count": len(rfq_items_dict) - len(quotation_items),
            "quotation_items": quotation_items,
            "statistics": {
                "total_items": len(rfq_items_dict),
                "matched_count": len(quotation_items),
                "unmatched_count": len(rfq_items_dict) - len(quotation_items),
                "coverage_rate": round(
                    len(quotation_items) / len(rfq_items_dict) * 100, 2
                )
                if rfq_items_dict
                else 0,
            },
        }

    except Exception as e:
        logger.error(f"Error processing RFQ items with matching: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "quotation_items": [],
            "statistics": {
                "total_items": len(rfq_items),
                "matched_count": 0,
                "unmatched_count": len(rfq_items),
                "coverage_rate": 0,
            },
        }
