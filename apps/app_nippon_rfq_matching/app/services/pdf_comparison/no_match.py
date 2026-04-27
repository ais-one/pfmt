"""
PDF Comparison No Match Handler Module

This module contains the no match handler mixin class for handling items
that couldn't be matched to products.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NoMatchHandler:
    """
    Mixin class for handling no match scenarios.

    Provides methods for creating no match results and checking color matches.
    """

    def _format_type_info(self, product_type: str | None) -> str:
        """
        Format product type information for logging.

        Args:
            product_type: Product type string

        Returns:
            Formatted type info string
        """
        return f"Type: {product_type}" if product_type else "Type: Unknown"

    def _log_no_match_found(
        self,
        idx: int,
        raw_text: str,
        normalized_name: str | None,
        normalized_color: str | None,
        product_type: str | None,
    ) -> None:
        """
        Log no match found details.

        Args:
            idx: Item index
            raw_text: Raw text from RFQ
            normalized_name: Normalized product name
            normalized_color: Normalized color from RFQ
            product_type: Product type
        """
        type_info = self._format_type_info(product_type)

        logger.info(f"{idx:2d}. [NO MATCH FOUND]")
        logger.info(f"    RAW: {raw_text}")
        logger.info(f"    → NORMALIZED: {normalized_name}")
        if normalized_color:
            logger.info(f"    → COLOR: {normalized_color}")
        logger.info(f"    → {type_info}")
        logger.info("    → No matching product found in database")
        logger.info("")

    def _build_no_match_result(self, item: dict[str, Any]) -> dict[str, Any]:
        """
        Build no match result dictionary.

        Args:
            item: RFQ item dictionary

        Returns:
            Match result dictionary with no match info
        """
        return {
            "rfq": item,
            "product_master": None,
            "match_info": {"score": 0, "method": "no_match", "color_match": False},
        }

    def _create_no_match_found(
        self,
        item: dict[str, Any],
        normalized_name: str | None,
        product_type: str | None,
        idx: int,
    ) -> dict[str, Any]:
        """
        Create a match result for items with no match found.

        Args:
            item: RFQ item dictionary
            normalized_name: Normalized product name
            product_type: Product type
            idx: Item index for logging

        Returns:
            Match result dictionary
        """
        raw_text = item.get("raw_text", "")
        normalized_color = item.get("normalized_color")

        self._log_no_match_found(
            idx, raw_text, normalized_name, normalized_color, product_type
        )

        return self._build_no_match_result(item)

    def _create_no_normalization_match(
        self, item: dict[str, Any], idx: int
    ) -> dict[str, Any]:
        """
        Create a match result for items that couldn't be normalized.

        Args:
            item: RFQ item dictionary
            idx: Item index for logging

        Returns:
            Match result dictionary
        """
        raw_text = item.get("raw_text", "")
        logger.info(f"{idx:2d}. [NO NORMALIZATION]")
        logger.info(f"    RAW: {raw_text}")
        logger.info("    → Could not normalize with AI")
        logger.info("")

        return {
            "rfq": item,
            "product_master": None,
            "match_info": {"score": 0, "method": "no_match", "color_match": False},
        }

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
