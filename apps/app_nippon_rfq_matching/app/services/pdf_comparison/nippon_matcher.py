"""
PDF Comparison Nippon Matcher Module

This module contains the NipponMatcher class for matching Nippon products.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.database import ProductMaster

logger = logging.getLogger(__name__)


class NipponMatcher:
    """
    Matcher class for Nippon products.

    Handles finding and matching Nippon products from the database.
    """

    def _query_products_by_name(self, normalized_name: str, db: Session) -> list[Any]:
        """
        Query products by normalized name.

        Args:
            normalized_name: Normalized product name
            db: Database session

        Returns:
            List of ProductMaster objects
        """
        return (
            db.query(ProductMaster)
            .filter(ProductMaster.product_name.contains(normalized_name))
            .all()
        )

    def _find_product_by_color(
        self, products: list[Any], normalized_color: str
    ) -> Any | None:
        """
        Find product from list by color match.

        Args:
            products: List of ProductMaster objects
            normalized_color: Color to match

        Returns:
            Matched ProductMaster or None
        """
        if not normalized_color:
            return None

        normalized_color_lower = normalized_color.strip().lower()
        for product in products:
            if (
                product.color
                and product.color.strip().lower() == normalized_color_lower
            ):
                return product
        return None

    def _get_default_product(self, products: list[Any]) -> Any:
        """
        Get default product from list (first product).

        Args:
            products: List of ProductMaster objects

        Returns:
            First ProductMaster object
        """
        return products[0]

    def _format_colors_string(self, available_colors: list[str]) -> str:
        """
        Format colors list for logging.

        Args:
            available_colors: List of available colors

        Returns:
            Formatted colors string
        """
        colors_str = ", ".join(available_colors[:5])
        if len(available_colors) > 5:
            colors_str += f" ...+{len(available_colors) - 5}more"
        return colors_str

    def _log_color_match(
        self,
        matched_product: Any,
        normalized_color: str | None,
        available_colors: list[str],
    ) -> None:
        """
        Log color match information.

        Args:
            matched_product: Matched ProductMaster object
            normalized_color: Normalized color from RFQ
            available_colors: List of available colors
        """
        if normalized_color:
            if matched_product and matched_product.color:
                logger.info(f"    → COLOR: {matched_product.color} ✓")
            else:
                colors_str = self._format_colors_string(available_colors)
                logger.info(f"    → COLOR: (no match) → Available: [{colors_str}]")
        elif available_colors:
            colors_str = self._format_colors_string(available_colors)
            logger.info(f"    → Available Colors: [{colors_str}]")

    def _log_nippon_match(
        self,
        idx: int,
        raw_text: str,
        normalized_name: str,
        matched_product: Any,
        normalized_color: str | None,
        available_colors: list[str],
    ) -> None:
        """
        Log Nippon product match details.

        Args:
            idx: Item index
            raw_text: Raw text from RFQ
            normalized_name: Normalized product name
            matched_product: Matched ProductMaster object
            normalized_color: Normalized color from RFQ
            available_colors: List of available colors
        """
        logger.info(f"{idx:2d}. [NIPPON PRODUCT - SCORE: 100%]")
        logger.info(f"    RAW: {raw_text}")
        logger.info(f"    → NORMALIZED: {normalized_name}")
        self._log_color_match(matched_product, normalized_color, available_colors)
        logger.info(f"    → MATCHED: {matched_product.product_name}")
        logger.info("")

    def _build_match_result(
        self, item: dict[str, Any], matched_product: Any
    ) -> dict[str, Any]:
        """
        Build match result dictionary.

        Args:
            item: RFQ item dictionary
            matched_product: Matched ProductMaster object

        Returns:
            Match result dictionary
        """
        return {
            "rfq": item,
            "product_master": matched_product.to_dict(),
            "match_info": {
                "score": 100,
                "method": "nippon_normalized",
                "color_match": self._check_color_match(item, matched_product.to_dict()),
            },
        }

    def _find_nippon_product_match(
        self, item: dict[str, Any], normalized_name: str, idx: int, db: Session
    ) -> dict[str, Any] | None:
        """
        Find product match for Nippon products.

        Args:
            item: RFQ item dictionary
            normalized_name: Normalized product name
            idx: Item index for logging
            db: Database session

        Returns:
            Match result dictionary or None if no match found
        """
        all_products = self._query_products_by_name(normalized_name, db)
        if not all_products:
            return None

        normalized_color = item.get("normalized_color")
        available_colors = item.get("available_colors", [])

        matched_product = self._find_product_by_color(all_products, normalized_color)
        if not matched_product:
            matched_product = self._get_default_product(all_products)

        raw_text = item.get("raw_text", "")
        self._log_nippon_match(
            idx,
            raw_text,
            normalized_name,
            matched_product,
            normalized_color,
            available_colors,
        )

        return self._build_match_result(item, matched_product)

    def _check_color_match(
        self, rfq_item: dict[str, Any], product: dict[str, Any]
    ) -> bool:
        """
        Check if RFQ item color matches product color.

        Args:
            rfq_item: RFQ item dictionary
            product: Product master dictionary

        Returns:
            True if colors match
        """
        # Use normalized_color from OpenAI (if available)
        rfq_color = rfq_item.get("normalized_color") or rfq_item.get("color") or ""
        product_color = product.get("color") or ""

        if not rfq_color or not product_color:
            return False

        return rfq_color.lower() == product_color.lower()
