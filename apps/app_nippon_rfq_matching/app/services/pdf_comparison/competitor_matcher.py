"""
PDF Comparison Competitor Matcher Module

This module contains the CompetitorMatcher class for matching competitor products.
"""

import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.competitor import (
    CompetitorColorComparison,
    CompetitorProduct,
    ProductEquivalent,
)
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster

logger = logging.getLogger(__name__)


class CompetitorMatcher:
    """
    Matcher class for Competitor products.

    Handles finding and matching competitor products to their Nippon equivalents.
    """

    def _find_competitor_nippon_equivalent(
        self, normalized_name: str, db: Session
    ) -> str | None:
        """
        Find Nippon equivalent name for a competitor product.

        Args:
            normalized_name: Competitor product name
            db: Database session

        Returns:
            Nippon equivalent name or None
        """
        competitor_product = (
            db.query(CompetitorProduct)
            .filter(CompetitorProduct.name == normalized_name)
            .first()
        )

        if competitor_product:
            equivalents = (
                db.query(ProductEquivalent)
                .filter(
                    ProductEquivalent.competitor_product_id == competitor_product.id
                )
                .first()
            )

            if equivalents:
                logger.info(
                    f"Getting equivalent nippon product name from {normalized_name} : {equivalents.nippon_product_name}"
                )
                return equivalents.nippon_product_name

        return None

    def _find_competitor_products_with_exact_match(
        self, nippon_name: str, npms_color_code: str | None, db: Session
    ) -> tuple[list, bool]:
        """
        Find competitor products with exact match on product name.

        Args:
            nippon_name: Nippon product name
            npms_color_code: NPMS color code
            db: Database session

        Returns:
            Tuple of (list of products, exact_match_used flag)
        """
        query = db.query(ProductMaster).filter(
            ProductMaster.product_name.contains(nippon_name)
        )

        # Filter by color if NPMS code is available
        if npms_color_code:
            # Try exact match first
            products = query.filter(ProductMaster.color == npms_color_code).all()

            if products:
                return products, True

            # If no exact match, try partial match (NPMS code might be part of the color)
            products = query.filter(ProductMaster.color.contains(npms_color_code)).all()

            if products:
                return products, True

            # Log debug info
            logger.info(
                f"No color match found for NPMS code '{npms_color_code}' in product '{nippon_name}'"
            )
            # Get all colors for this product for debugging
            all_colors = query.with_entities(ProductMaster.color).distinct().all()
            logger.info(
                f"Available colors for '{nippon_name}': {[c[0] for c in all_colors if c[0]]}"
            )

        return [], False

    def _find_competitor_products_with_like_match(
        self,
        nippon_name: str,
        npms_color_code: str | None,
        normalized_color: str | None,
        db: Session,
    ) -> list:
        """
        Find competitor products with LIKE match on product name.

        Args:
            nippon_name: Nippon product name
            npms_color_code: NPMS color code
            normalized_color: Normalized color
            db: Database session

        Returns:
            List of products
        """
        # Use LIKE query to find products that contain the nippon_name
        query_like = db.query(ProductMaster).filter(
            ProductMaster.clean_product_name.like(f"%{nippon_name}%")
        )

        if npms_color_code:
            # Try exact color match first
            products_with_color = query_like.filter(
                ProductMaster.color == npms_color_code
            ).all()

            if products_with_color:
                return products_with_color

            # If no exact match, try partial match
            products_partial = query_like.filter(
                ProductMaster.color.contains(npms_color_code)
            ).all()

            if products_partial:
                return products_partial

            # Log debug info
            logger.info(
                f"No color match found for NPMS code '{npms_color_code}' in LIKE search for '{nippon_name}'"
            )
            # Get all available colors for this product pattern
            all_products = query_like.all()
            all_colors = list(set([p.color for p in all_products if p.color]))
            logger.info(f"Available colors for LIKE '{nippon_name}': {all_colors}")

        if normalized_color:
            # Use normalized_color from OpenAI (no NPMS mapping available)
            products = query_like.all()

            # Find exact color match
            normalized_color_lower = normalized_color.strip().lower()
            for p in products:
                if p.color and p.color.strip().lower() == normalized_color_lower:
                    return [p]

            # Fallback: use first product
            if products:
                return [products[0]]

        return query_like.all()

    def _log_competitor_match(
        self,
        idx: int,
        item: dict[str, Any],
        normalized_name: str,
        nippon_name: str,
        matched_product,
        exact_match_used: bool,
        match_method: str,
    ) -> None:
        """
        Log competitor product match information.

        Args:
            idx: Item index
            item: RFQ item dictionary
            normalized_name: Normalized product name
            nippon_name: Nippon equivalent name
            matched_product: Matched product
            exact_match_used: Whether exact match was used
            match_method: Match method type
        """
        raw_text = item.get("raw_text", "")
        npms_color_code = item.get("npms_color_code")
        normalized_color = item.get("normalized_color")
        source_brand = item.get("source_brand")
        source_color_code = item.get("source_color_code")

        logger.info(f"{idx:2d}. [COMPETITOR PRODUCT - SCORE: 95%]")
        logger.info(f"    RAW: {raw_text}")
        logger.info(f"    → NORMALIZED: {normalized_name} (Competitor)")
        logger.info(f"    → NIPPON EQUIVALENT: {nippon_name}")

        if not exact_match_used:
            logger.info(
                f"    → MATCHED BY: LIKE query (product_name LIKE '%{nippon_name}%')"
            )

        # Show color mapping if available
        if npms_color_code:
            logger.info(
                f"    → COLOR MAPPING: {source_brand} {source_color_code} → NPMS {npms_color_code}"
            )
            logger.info(
                f"    → COLOR: {matched_product.color} ✓ (NPMS code match, {match_method} match)"
            )
        elif normalized_color:
            logger.info(
                f"    → COLOR: {matched_product.color} ✓ (OpenAI color match, {match_method} match)"
            )
        else:
            logger.info(f"    → COLOR: {matched_product.color} ({match_method} match)")

        logger.info(f"    → MATCHED: {matched_product.product_name}")
        logger.info("")

    def _log_competitor_no_match(
        self, idx: int, item: dict[str, Any], normalized_name: str, nippon_name: str
    ) -> None:
        """
        Log competitor product no match information.

        Args:
            idx: Item index
            item: RFQ item dictionary
            normalized_name: Normalized product name
            nippon_name: Nippon equivalent name
        """
        raw_text = item.get("raw_text", "")
        npms_color_code = item.get("npms_color_code")
        normalized_color = item.get("normalized_color")
        source_brand = item.get("source_brand")
        source_color_code = item.get("source_color_code")

        logger.info(f"{idx:2d}. [COMPETITOR PRODUCT - NO MATCH]")
        logger.info(f"    RAW: {raw_text}")
        logger.info(f"    → NORMALIZED: {normalized_name} (Competitor)")
        logger.info(f"    → NIPPON EQUIVALENT: {nippon_name}")

        if npms_color_code:
            logger.info(
                f"    → COLOR MAPPING: {source_brand} {source_color_code} → NPMS {npms_color_code}"
            )
            logger.info(f"    → No product found with NPMS color {npms_color_code}")
        elif normalized_color:
            logger.info(f"    → COLOR: {normalized_color}")
            logger.info("    → No product found with this color")
        else:
            logger.info("    → No matching product found in database")
        logger.info("")

    def _get_nippon_equivalent_name(
        self, item: dict[str, Any], normalized_name: str, db: Session
    ) -> str | None:
        """
        Get Nippon equivalent name for competitor product.

        Args:
            item: RFQ item dictionary
            normalized_name: Normalized product name
            db: Database session

        Returns:
            Nippon equivalent name or None
        """
        nippon_name = item.get("nippon_equivalent_name")
        logger.info(f"nippon_name [DEBUG]: {nippon_name}")

        if not nippon_name:
            nippon_name = self._find_competitor_nippon_equivalent(normalized_name, db)

        return nippon_name

    def _log_competitor_debug_info(
        self,
        npms_color_code: str | None,
        raw_text: str,
        normalized_color: str | None,
        source_brand: str | None,
    ) -> None:
        """
        Log competitor debug information.

        Args:
            npms_color_code: NPMS color code
            raw_text: Raw text from RFQ
            normalized_color: Normalized color
            source_brand: Source brand
        """
        logger.info(f"NPMS COLOR CODE[DEBUG]: {npms_color_code}")
        logger.info(f"raw_text[DEBUG]: {raw_text}")
        logger.info(f"normalized_color[DEBUG]: {normalized_color}")
        logger.info(f"source_brand[DEBUG]: {source_brand}")

    def _normalize_nippon_name_for_search(self, nippon_name: str) -> str:
        """
        Normalize nippon name for search by removing hyphens and extra spaces.

        Args:
            nippon_name: Original nippon product name

        Returns:
            Normalized name suitable for searching
        """
        if not nippon_name:
            return nippon_name
        # Replace hyphens with spaces, then normalize multiple spaces
        normalized = nippon_name.replace("-", " ")
        normalized = " ".join(normalized.split())  # Remove multiple spaces
        return normalized

    def _search_nippon_products(
        self,
        nippon_name: str,
        npms_color_code: str | None,
        normalized_color: str | None,
        db: Session,
    ) -> tuple[list, bool]:
        """
        Search for Nippon products using exact and like match strategies.

        Args:
            nippon_name: Nippon product name
            npms_color_code: NPMS color code
            normalized_color: Normalized color
            db: Database session

        Returns:
            Tuple of (list of products, exact_match_used flag)
        """
        # Normalize nippon_name for search
        normalized_nippon_name = self._normalize_nippon_name_for_search(nippon_name)

        all_nippon_products, exact_match_used = (
            self._find_competitor_products_with_exact_match(
                normalized_nippon_name, npms_color_code, db
            )
        )

        if not all_nippon_products:
            all_nippon_products = self._find_competitor_products_with_like_match(
                normalized_nippon_name, npms_color_code, normalized_color, db
            )

        # If still no match, try searching with original name (for backwards compatibility)
        if not all_nippon_products and normalized_nippon_name != nippon_name:
            all_nippon_products, exact_match_used = (
                self._find_competitor_products_with_exact_match(
                    nippon_name, npms_color_code, db
                )
            )
            if not all_nippon_products:
                all_nippon_products = self._find_competitor_products_with_like_match(
                    nippon_name, npms_color_code, normalized_color, db
                )

        return all_nippon_products, exact_match_used

    def _build_competitor_match_result(
        self,
        item: dict[str, Any],
        matched_product: Any,
        match_method: str,
        normalized_name: str,
        nippon_name: str,
        source_brand: str | None,
        source_color_code: str | None,
        npms_color_code: str | None,
    ) -> dict[str, Any]:
        """
        Build competitor match result dictionary.

        Args:
            item: RFQ item dictionary
            matched_product: Matched ProductMaster object
            match_method: Match method type (exact/like)
            normalized_name: Normalized product name
            nippon_name: Nippon equivalent name
            source_brand: Source brand
            source_color_code: Source color code
            npms_color_code: NPMS color code

        Returns:
            Match result dictionary
        """
        # For competitor matches, if NPMS color mapping was used, color_match is True
        # Otherwise, fall back to simple string comparison for OpenAI color match
        color_match = bool(npms_color_code) or self._check_color_match(
            item, matched_product.to_dict()
        )

        return {
            "rfq": item,
            "product_master": matched_product.to_dict(),
            "match_info": {
                "score": 95,
                "method": f"competitor_equivalent_{match_method}",
                "competitor_product": normalized_name,
                "nippon_equivalent": nippon_name,
                "source_brand": source_brand,
                "source_color_code": source_color_code,
                "npms_color_code": npms_color_code,
                "color_match": color_match,
            },
        }

    def _handle_competitor_products_found(
        self,
        item: dict[str, Any],
        all_nippon_products: list,
        exact_match_used: bool,
        normalized_name: str,
        nippon_name: str | None,
        idx: int,
    ) -> dict[str, Any] | None:
        """
        Handle case when competitor products are found.

        Args:
            item: RFQ item dictionary
            all_nippon_products: List of found products
            exact_match_used: Whether exact match was used
            normalized_name: Normalized product name
            nippon_name: Nippon equivalent name
            idx: Item index

        Returns:
            Match result dictionary
        """
        matched_product = all_nippon_products[0]
        match_method = "exact" if exact_match_used else "like"

        self._log_competitor_match(
            idx,
            item,
            normalized_name,
            nippon_name,
            matched_product,
            exact_match_used,
            match_method,
        )

        npms_color_code = item.get("npms_color_code")
        source_brand = item.get("source_brand")
        source_color_code = item.get("source_color_code")

        return self._build_competitor_match_result(
            item,
            matched_product,
            match_method,
            normalized_name,
            nippon_name,
            source_brand,
            source_color_code,
            npms_color_code,
        )

    def _find_competitor_product_match(
        self, item: dict[str, Any], normalized_name: str, idx: int, db: Session
    ) -> dict[str, Any] | None:
        """
        Find product match for Competitor products.

        Args:
            item: RFQ item dictionary
            normalized_name: Normalized product name
            idx: Item index for logging
            db: Database session

        Returns:
            Match result dictionary or None if no match found
        """
        nippon_name = self._get_nippon_equivalent_name(item, normalized_name, db)
        if not nippon_name:
            return None

        logger.info(
            f"Nippon name get from _get_nippon_equivalent_name(item, normalized_name, db) : {nippon_name}"
        )
        npms_color_code = item.get("npms_color_code")
        raw_text = item.get("raw_text", "")
        normalized_color = item.get("normalized_color")
        source_brand = item.get("source_brand")
        competitor_normalized_product_name = item.get(
            "competitor_normalized_product_name"
        )

        self._log_competitor_debug_info(
            npms_color_code, raw_text, normalized_color, source_brand
        )

        competitor_source_brand = (
            db.query(CompetitorProduct)
            .filter(
                func.lower(CompetitorProduct.name)
                == func.lower(competitor_normalized_product_name)
            )
            .first()
        )

        if competitor_source_brand:
            logger.info(
                f"_find_competitor_product_match competitor_source_brand: Found competitor source brand "
                f"{competitor_source_brand.brand.name}"
            )

            # Search normalized_color first from competitor_color_comparison source_code=normalized_color
            competitor_color_match = (
                db.query(CompetitorColorComparison)
                .filter(
                    func.lower(CompetitorColorComparison.source_code)
                    == func.lower(normalized_color),
                    func.lower(CompetitorColorComparison.source_brand)
                    == func.lower(competitor_source_brand.brand.name),
                )
                .first()
            )

            if competitor_color_match:
                logger.info(
                    f"_find_competitor_product_match: Found competitor color match {normalized_color} with NPMS code: "
                    f"{competitor_color_match.npms_code}"
                )
                npms_color_code = competitor_color_match.npms_code

                item["npms_color_code"] = npms_color_code
                item["color_match"] = True

                all_nippon_products, exact_match_used = self._search_nippon_products(
                    nippon_name, competitor_color_match.npms_code, normalized_color, db
                )

                if all_nippon_products:
                    return self._handle_competitor_products_found(
                        item,
                        all_nippon_products,
                        exact_match_used,
                        normalized_name,
                        nippon_name,
                        idx,
                    )

                self._log_competitor_no_match(idx, item, normalized_name, nippon_name)

        # FALLBACK: Jika tidak ada color match, tetap cari produk Nippon tanpa filter warna
        if nippon_name:
            logger.info(
                f"_find_competitor_product_match: No color match found, "
                f"attempting fallback search for nippon_name: {nippon_name}"
            )
            all_nippon_products, exact_match_used = self._search_nippon_products(
                nippon_name, None, normalized_color, db
            )

            if all_nippon_products:
                logger.info(
                    f"_find_competitor_product_match: Found {len(all_nippon_products)} products without color filter "
                    f"(fallback)"
                )
                # Set color_match ke False karena warna tidak match
                item["color_match"] = False
                item["normalized_color_fallback"] = normalized_color
                return self._handle_competitor_products_found(
                    item,
                    all_nippon_products,
                    exact_match_used,
                    normalized_name,
                    nippon_name,
                    idx,
                )
            else:
                # SECONDARY FALLBACK: Jika nippon_name dari ProductEquivalent tidak ditemukan,
                # coba cari dengan nama competitor product secara langsung
                logger.info(
                    f"_find_competitor_product_match: nippon_name '{nippon_name}' not found in ProductMaster, "
                    f"attempting direct search with competitor name: {competitor_normalized_product_name}"
                )
                # Cari produk Nippon yang mirip dengan nama competitor
                direct_products = (
                    db.query(ProductMaster)
                    .filter(
                        ProductMaster.product_name.contains(
                            competitor_normalized_product_name
                        )
                    )
                    .all()
                )

                if direct_products:
                    logger.info(
                        f"_find_competitor_product_match: Found {len(direct_products)} products with direct search"
                    )
                    item["color_match"] = False
                    item["normalized_color_fallback"] = normalized_color
                    return self._handle_competitor_products_found(
                        item,
                        direct_products,
                        False,
                        normalized_name,
                        competitor_normalized_product_name,  # Use original competitor name
                        idx,
                    )
                else:
                    # THIRD FALLBACK: Cari dengan LIKE pada clean_product_name
                    like_products = (
                        db.query(ProductMaster)
                        .filter(
                            ProductMaster.clean_product_name.like(
                                f"%{competitor_normalized_product_name}%"
                            )
                        )
                        .all()
                    )

                    if like_products:
                        logger.info(
                            f"_find_competitor_product_match: Found {len(like_products)} products with LIKE search"
                        )
                        item["color_match"] = False
                        item["normalized_color_fallback"] = normalized_color
                        return self._handle_competitor_products_found(
                            item,
                            like_products,
                            False,
                            normalized_name,
                            competitor_normalized_product_name,
                            idx,
                        )

        return None

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
