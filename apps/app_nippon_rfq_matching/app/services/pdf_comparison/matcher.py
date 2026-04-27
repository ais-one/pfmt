"""
PDF Comparison Product Matcher Module

This module contains the ProductMatcher class that combines all matcher
functionality for finding product matches.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.services.pdf_comparison.base import (
    PDFComparisonExportBase,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.competitor_matcher import (
    CompetitorMatcher,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.nippon_matcher import (
    NipponMatcher,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison.no_match import (
    NoMatchHandler,
)

logger = logging.getLogger(__name__)


class ProductMatcher(
    PDFComparisonExportBase, NipponMatcher, CompetitorMatcher, NoMatchHandler
):
    """
    Product matcher class that combines all matching functionality.

    Inherits from base class and all matcher mixins to provide complete
    product matching capabilities.
    """

    def find_product_matches(
        self, normalized_items: list[dict[str, Any]], db: Session
    ) -> list[dict[str, Any]]:
        """
        Find product matches for normalized items.

        Args:
            normalized_items: List of normalized RFQ items
            db: Database session

        Returns:
            List of match results
        """
        matches = []

        logger.info("=" * 80)
        logger.info("PRODUCT MATCHING RESULTS")
        logger.info("=" * 80)

        for idx, item in enumerate(normalized_items, 1):
            normalized_name = item.get("normalized_name")
            product_type = item.get("product_type")

            # Case 1: No normalization found
            if not normalized_name:
                matches.append(self._create_no_normalization_match(item, idx))
                continue

            # Case 2: Nippon product matching
            if product_type == "nippon":
                nippon_match = self._find_nippon_product_match(
                    item, normalized_name, idx, db
                )
                if nippon_match:
                    matches.append(nippon_match)
                else:
                    matches.append(
                        self._create_no_match_found(
                            item, normalized_name, product_type, idx
                        )
                    )
                continue

            # Case 3: Competitor product matching
            if product_type == "competitor":
                item["competitor_normalized_product_name"] = normalized_name
                competitor_match = self._find_competitor_product_match(
                    item, normalized_name, idx, db
                )
                if competitor_match:
                    matches.append(competitor_match)
                else:
                    matches.append(
                        self._create_no_match_found(
                            item, normalized_name, product_type, idx
                        )
                    )
                continue

            # Case 4: Unknown product type or no match
            matches.append(
                self._create_no_match_found(item, normalized_name, product_type, idx)
            )

        logger.info("=" * 80)

        # Summary statistics
        matched = sum(1 for m in matches if m["product_master"] is not None)
        logger.info(
            f"Matching Summary: {matched}/{len(normalized_items)} items matched"
        )
        logger.info("=" * 80)

        logger.info(f"Matches structure data : {matches}")

        return matches
