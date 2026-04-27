"""
Matching Data Service

Service for extracting matching data from Product Master and related tables
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.pricing import ProductPrices, Region

logger = logging.getLogger(__name__)


class MatchingDataService:
    """Service for extracting data from matched Product Master records"""

    def get_product_master_data_with_pricing(
        self,
        product_master_ids: list[int],
        db: Session,
        default_region: str = "Indonesia",
    ) -> list[dict[str, Any]]:
        """
        Get Product Master data with pricing information for multiple product IDs.

        Args:
            product_master_ids: List of ProductMaster IDs
            db: Database session
            default_region: Default region name to get pricing from

        Returns:
            List of dictionaries with complete product and pricing data
        """
        if not product_master_ids:
            return []

        # Get product master data
        products_query = db.query(ProductMaster).filter(
            ProductMaster.id.in_(product_master_ids)
        )
        products = products_query.all()

        # Get available regions
        regions = db.query(Region).all()
        region_names = [r.name for r in regions]

        # Get pricing data for all products
        pricing_data = (
            db.query(
                ProductPrices.product_master_id,
                Region.name.label("region_name"),
                ProductPrices.size,
                ProductPrices.uom,
                ProductPrices.price,
                ProductPrices.price_raw,
            )
            .join(Region, ProductPrices.region_id == Region.id)
            .filter(ProductPrices.product_master_id.in_(product_master_ids))
            .all()
        )

        # Group pricing data by product
        product_pricing = {}
        for pricing in pricing_data:
            if pricing.product_master_id not in product_pricing:
                product_pricing[pricing.product_master_id] = []

            product_pricing[pricing.product_master_id].append(
                {
                    "region": pricing.region_name,
                    "size": pricing.size,
                    "uom": pricing.uom,
                    "price": pricing.price,
                    "price_raw": pricing.price_raw,
                }
            )

        # Build complete product data with pricing
        complete_products = []
        for product in products:
            product_data = {
                "id": product.id,
                "pmc": product.pmc,
                "product_name": product.product_name,
                "color": product.color,
                "clean_product_name": product.clean_product_name,
                "sheet_type": product.sheet_type,
                "pricing": product_pricing.get(product.id, []),
                "available_regions": region_names,
            }

            # Get default pricing (Indonesia region if available)
            default_pricing = None
            if default_region:
                default_pricing = next(
                    (
                        p
                        for p in product_pricing.get(product.id, [])
                        if p["region"] == default_region
                    ),
                    None,
                )

                # If default region not found, use first available pricing
                if not default_pricing and product_pricing.get(product.id):
                    default_pricing = product_pricing.get(product.id)[0]

            if default_pricing:
                product_data.update(
                    {
                        "default_region": default_pricing["region"],
                        "default_size": default_pricing["size"],
                        "default_uom": default_pricing["uom"],
                        "default_price": default_pricing["price"],
                        "default_price_raw": default_pricing["price_raw"],
                    }
                )

            complete_products.append(product_data)

        logger.info(f"Retrieved complete data for {len(complete_products)} products")
        return complete_products

    def enrich_matches_with_product_data(
        self, matches: list[dict[str, Any]], db: Session
    ) -> list[dict[str, Any]]:
        """
        Enrich match results with complete Product Master data including pricing.

        Args:
            matches: List of match results from matching service
            db: Database session

        Returns:
            List of enriched match results with product data
        """
        # Extract ProductMaster IDs from matches
        matched_product_ids = []
        for match in matches:
            product_master = match.get("product_master", {})
            if product_master and product_master.get("id"):
                matched_product_ids.append(product_master["id"])

        if not matched_product_ids:
            logger.warning("No matched ProductMaster IDs found")
            return matches

        # Get complete product data
        product_data_list = self.get_product_master_data_with_pricing(
            matched_product_ids, db
        )

        # Create mapping for quick lookup
        product_data_map = {pd["id"]: pd for pd in product_data_list}

        # Enrich matches with product data
        enriched_matches = []
        for match in matches:
            product_master = match.get("product_master")
            if not product_master:
                logger.debug(f"No product_master found in match: {match}")
                continue

            # Ensure product_master is a dictionary
            if not isinstance(product_master, dict):
                logger.error(
                    f"product_master is not a dict: {product_master} (type: {type(product_master)})"
                )
                logger.error(f"Full match structure: {match}")
                continue

            product_id = product_master.get("id")
            logger.debug(f"Processing product_master with ID: {product_id}")

            if product_id and product_id in product_data_map:
                # Replace product_master with complete data
                complete_product_data = product_data_map[product_id]
                enriched_match = {
                    "rfq": match.get("rfq", {}),
                    "product_master": complete_product_data,
                    "match_info": match.get("match_info", {}),
                }
                enriched_matches.append(enriched_match)
                logger.debug(f"Enriched match for product ID {product_id}")
            else:
                # Keep original match if no product data found
                enriched_matches.append(match)
                logger.debug(f"No product data found for product ID {product_id}")

        logger.info(f"Enriched {len(enriched_matches)} matches with product data")
        return enriched_matches

    def get_product_code_from_match(self, match: dict[str, Any]) -> str | None:
        """
        Get product code (PMC) from a match result.

        Args:
            match: Match result dictionary

        Returns:
            Product code (PMC) or None
        """
        product_master = match.get("product_master", {})
        return product_master.get("pmc")

    def get_color_from_match(self, match: dict[str, Any]) -> str | None:
        """
        Get color from a match result.

        Args:
            match: Match result dictionary

        Returns:
            Color or None
        """
        product_master = match.get("product_master", {})
        return product_master.get("color")

    def get_price_from_match(
        self, match: dict[str, Any], region: str = "Indonesia"
    ) -> dict[str, Any] | None:
        """
        Get price information from a match result for a specific region.

        Args:
            match: Match result dictionary
            region: Region name to get pricing for

        Returns:
            Price dictionary with amount, currency, uom or None
        """
        product_master = match.get("product_master", {})
        pricing_list = product_master.get("pricing", [])

        # Find pricing for the specified region
        for pricing in pricing_list:
            if pricing.get("region") == region:
                return {
                    "amount": pricing.get("price"),
                    "currency": "IDR",  # Default currency
                    "uom": pricing.get("uom"),
                    "price_raw": pricing.get("price_raw"),
                    "region": pricing.get("region"),
                }

        # Return first available pricing if region not found
        if pricing_list:
            first_pricing = pricing_list[0]
            return {
                "amount": first_pricing.get("price"),
                "currency": "IDR",  # Default currency
                "uom": first_pricing.get("uom"),
                "price_raw": first_pricing.get("price_raw"),
                "region": first_pricing.get("region"),
            }

        return None


# Singleton instance
matching_data_service = MatchingDataService()
