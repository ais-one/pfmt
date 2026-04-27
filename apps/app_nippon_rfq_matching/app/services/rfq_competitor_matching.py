"""
RFQ Competitor Matching Service

Service for matching RFQ items with competitor products and Nippon products.
Implements multi-keyword search with scoring across multiple tables.
"""

import logging
from typing import Any

from sqlalchemy import case, or_
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.competitor import (
    Brand,
    CompetitorProduct,
    ProductEquivalent,
)
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

logger = logging.getLogger(__name__)


class RFQCompetitorMatcher:
    """
    Matcher for RFQ items against competitor products and Nippon products.

    Flow:
    1. Get RFQ items by RFQ ID
    2. Extract keywords from item descriptions
    3. Multi-keyword search with scoring:
       - Check competitor_products → get product_id
       - Check product_equivalent → get nippon generic
       - Search product_master with color matching
    """

    def __init__(self, db: Session):
        """
        Initialize matcher with database session.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db

    def get_rfq_items(self, rfq_id: str) -> list[RFQItem]:
        """
        Get all RFQ items for a given RFQ ID.

        Args:
            rfq_id: RFQ identifier

        Returns:
            List of RFQItem objects
        """
        items = self.db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        logger.info(f"Found {len(items)} RFQ items for RFQ ID: {rfq_id}")
        return items

    def extract_keywords(self, text: str) -> list[str]:
        """
        Extract keywords from text for searching.

        Args:
            text: Text to extract keywords from

        Returns:
            List of keywords
        """
        if not text:
            return []

        # Clean and split text
        # Remove special characters, keep alphanumeric and spaces
        import re

        cleaned = re.sub(r"[^\w\s]", " ", text)
        words = cleaned.split()

        # Filter meaningful words (length > 2)
        keywords = [w for w in words if len(w) > 2]

        # Remove duplicates while preserving order
        seen = set()
        unique_keywords = []
        for kw in keywords:
            if kw.lower() not in seen:
                seen.add(kw.lower())
                unique_keywords.append(kw)

        return unique_keywords

    def search_competitor_products(
        self, keywords: list[str], limit: int = 50
    ) -> list[dict[str, Any]]:
        """
        Search competitor products using multi-keyword search with scoring.

        Args:
            keywords: List of keywords to search for
            limit: Maximum results to return

        Returns:
            List of dicts with competitor products and scores (0-100)
        """
        if not keywords:
            return []

        # Build score calculation using case statements
        # Score increases for each keyword match in name or description
        # Max score per keyword: 2 (1 for name + 1 for description)
        max_score_per_keyword = 2
        max_possible_score = len(keywords) * max_score_per_keyword

        score_expr = sum(
            [
                case((CompetitorProduct.name.ilike(f"%{kw}%"), 1), else_=0)
                + case((CompetitorProduct.description.ilike(f"%{kw}%"), 1), else_=0)
                for kw in keywords
            ]
        )

        # Label the score expression for use in ORDER BY
        labeled_score = score_expr.label("raw_score")

        # Query with scoring
        query = (
            self.db.query(CompetitorProduct, Brand, labeled_score)
            .join(Brand, CompetitorProduct.brand_id == Brand.id)
            .filter(score_expr > 0)  # Only items with at least one match
            .order_by(labeled_score.desc())
            .limit(limit)
        )

        results = []
        for product, brand, raw_score in query.all():
            # Normalize to percentage (0-100)
            normalized_score = (
                min(100, int((raw_score / max_possible_score) * 100))
                if max_possible_score > 0
                else 0
            )
            results.append(
                {
                    "product_id": product.id,
                    "product_name": product.name,
                    "description": product.description,
                    "brand": {"id": brand.id, "name": brand.name},
                    "score": normalized_score,
                    "source": "competitor_products",
                }
            )

        logger.info(
            f"Found {len(results)} competitor products for keywords: {keywords}"
        )
        return results

    def get_nippon_products_for_competitor(
        self, competitor_product_id: int, generic_names: list[str] = None
    ) -> list[dict[str, Any]]:
        """
        Get Nippon products for a competitor product.

        Uses direct competitor_product_id -> nippon_product_name mapping.

        Args:
            competitor_product_id: ID of the competitor product
            generic_names: Not used in new schema (kept for compatibility)

        Returns:
            List of Nippon product names (with 'product_name' key for compatibility)
        """
        # Get direct equivalents for this competitor product
        equivalents = (
            self.db.query(ProductEquivalent)
            .filter(ProductEquivalent.competitor_product_id == competitor_product_id)
            .all()
        )

        nippon_products = []
        for eq in equivalents:
            nippon_products.append(
                {
                    "product_name": eq.nippon_product_name  # Use 'product_name' for compatibility
                }
            )

        logger.info(
            f"Found {len(nippon_products)} Nippon products for competitor_product_id: {competitor_product_id}"
        )
        return nippon_products

    def search_product_master_by_nippon_products(
        self,
        nippon_products: list[dict[str, Any]],
        color: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Search product master by NP Marine product names with optional color matching.

        Args:
            nippon_products: List of NP Marine products with product names
            color: Optional color to match
            limit: Maximum results

        Returns:
            List of matching ProductMaster records with scores
        """
        if not nippon_products:
            return []

        # Collect all NP Marine product names
        product_names = [p["product_name"] for p in nippon_products]

        # Build score calculation
        # Score increases for each product name match in various fields
        # Max score per product name: 2+2+1+1 = 6
        # Color bonus: 3 points
        max_score_per_product = 6
        color_bonus_score = 3 if color else 0
        max_possible_score = (
            len(product_names) * max_score_per_product
        ) + color_bonus_score

        score_expr = sum(
            [
                case((ProductMaster.product_name.ilike(f"%{pn}%"), 2), else_=0)
                + case((ProductMaster.clean_product_name.ilike(f"%{pn}%"), 2), else_=0)
                + case((ProductMaster.pmc.ilike(f"%{pn}%"), 1), else_=0)
                + case((ProductMaster.sheet_type.ilike(f"%{pn}%"), 1), else_=0)
                for pn in product_names
            ]
        )

        # Add color bonus if specified (higher weight for color match)
        if color:
            color_bonus = case((ProductMaster.color.ilike(f"%{color}%"), 3), else_=0)
            score_expr = score_expr + color_bonus

        # Label the score expression for use in ORDER BY
        labeled_score = score_expr.label("raw_score")

        # Build query
        query = (
            self.db.query(ProductMaster, labeled_score)
            .filter(score_expr > 0)
            .order_by(labeled_score.desc())
            .limit(limit)
        )

        results = []
        for product, raw_score in query.all():
            # Normalize to percentage (0-100)
            normalized_score = (
                min(100, int((raw_score / max_possible_score) * 100))
                if max_possible_score > 0
                else 0
            )
            results.append(
                {
                    "id": product.id,
                    "product_name": product.product_name,
                    "clean_product_name": product.clean_product_name,
                    "pmc": product.pmc,
                    "sheet_type": product.sheet_type,
                    "color": product.color,
                    "score": normalized_score,
                    "source": "product_master",
                }
            )

        logger.info(
            f"Found {len(results)} product master items for NP Marine products: {product_names}"
        )
        return results

    def get_nippon_generics_for_competitor(
        self, competitor_product_id: int
    ) -> list[dict[str, Any]]:
        """
        Get Nippon product names for a competitor product.

        Args:
            competitor_product_id: ID of the competitor product

        Returns:
            List of Nippon product names
        """
        # Get product equivalents for this competitor product
        equivalents = (
            self.db.query(ProductEquivalent)
            .filter(ProductEquivalent.competitor_product_id == competitor_product_id)
            .all()
        )

        nippon_products = []
        for eq in equivalents:
            nippon_products.append({"nippon_product_name": eq.nippon_product_name})

        return nippon_products

    def search_product_master_by_generics(
        self, generic_names: list[str], color: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """
        Search product master by generic names with optional color matching.

        Args:
            generic_names: List of generic names to search for
            color: Optional color to match
            limit: Maximum results

        Returns:
            List of matching ProductMaster records with scores (0-100)
        """
        if not generic_names:
            return []

        # Build score calculation
        # Score increases for each generic name match in various fields
        # Max score per generic: 1+1+1+1 = 4
        # Color bonus: 2 points
        max_score_per_generic = 4
        color_bonus_score = 2 if color else 0
        max_possible_score = (
            len(generic_names) * max_score_per_generic
        ) + color_bonus_score

        score_expr = sum(
            [
                case((ProductMaster.product_name.ilike(f"%{generic}%"), 1), else_=0)
                + case(
                    (ProductMaster.clean_product_name.ilike(f"%{generic}%"), 1), else_=0
                )
                + case((ProductMaster.sheet_type.ilike(f"%{generic}%"), 1), else_=0)
                + case((ProductMaster.pmc.ilike(f"%{generic}%"), 1), else_=0)
                for generic in generic_names
            ]
        )

        # Add color bonus if specified
        if color:
            color_bonus = case((ProductMaster.color.ilike(f"%{color}%"), 2), else_=0)
            score_expr = score_expr + color_bonus

        # Label the score expression for use in ORDER BY
        labeled_score = score_expr.label("raw_score")

        # Build query
        query = (
            self.db.query(ProductMaster, labeled_score)
            .filter(score_expr > 0)
            .order_by(labeled_score.desc())
            .limit(limit)
        )

        results = []
        for product, raw_score in query.all():
            # Normalize to percentage (0-100)
            normalized_score = (
                min(100, int((raw_score / max_possible_score) * 100))
                if max_possible_score > 0
                else 0
            )
            results.append(
                {
                    "id": product.id,
                    "product_name": product.product_name,
                    "clean_product_name": product.clean_product_name,
                    "pmc": product.pmc,
                    "sheet_type": product.sheet_type,
                    "color": product.color,
                    "score": normalized_score,
                    "source": "product_master",
                }
            )

        logger.info(
            f"Found {len(results)} product master items for generics: {generic_names}"
        )
        return results

    def search_product_master_direct(
        self, keywords: list[str], color: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """
        Direct search in product master using keywords (fallback when no competitor match).

        Args:
            keywords: List of keywords to search for
            color: Optional color to match
            limit: Maximum results

        Returns:
            List of matching ProductMaster records with scores (0-100)
        """
        if not keywords:
            return []

        # Build score calculation
        # Max score per keyword: 1+1+1+1 = 4
        # Color bonus: 2 points
        max_score_per_keyword = 4
        color_bonus_score = 2 if color else 0
        max_possible_score = (len(keywords) * max_score_per_keyword) + color_bonus_score

        score_expr = sum(
            [
                case((ProductMaster.product_name.ilike(f"%{kw}%"), 1), else_=0)
                + case((ProductMaster.clean_product_name.ilike(f"%{kw}%"), 1), else_=0)
                + case((ProductMaster.pmc.ilike(f"%{kw}%"), 1), else_=0)
                + case((ProductMaster.sheet_type.ilike(f"%{kw}%"), 1), else_=0)
                for kw in keywords
            ]
        )

        # Add color bonus if specified
        if color:
            color_bonus = case((ProductMaster.color.ilike(f"%{color}%"), 2), else_=0)
            score_expr = score_expr + color_bonus

        # Label the score expression for use in ORDER BY
        labeled_score = score_expr.label("raw_score")

        # Build query
        query = (
            self.db.query(ProductMaster, labeled_score)
            .filter(score_expr > 0)
            .order_by(labeled_score.desc())
            .limit(limit)
        )

        results = []
        for product, raw_score in query.all():
            # Normalize to percentage (0-100)
            normalized_score = (
                min(100, int((raw_score / max_possible_score) * 100))
                if max_possible_score > 0
                else 0
            )
            results.append(
                {
                    "id": product.id,
                    "product_name": product.product_name,
                    "clean_product_name": product.clean_product_name,
                    "pmc": product.pmc,
                    "sheet_type": product.sheet_type,
                    "color": product.color,
                    "score": normalized_score,
                    "source": "product_master_direct",
                }
            )

        logger.info(
            f"Found {len(results)} product master items (direct) for keywords: {keywords}"
        )
        return results

    def extract_color_from_text(self, text: str) -> str | None:
        """
        Extract color from RFQ description text.

        Args:
            text: RFQ description text

        Returns:
            Extracted color or None
        """
        if not text:
            return None

        # Use existing color extraction from matching service
        try:
            from apps.app_nippon_rfq_matching.app.services.matching import (
                matching_service,
            )

            return matching_service.extract_color_from_text(text)
        except Exception:
            # Fallback: simple regex for common color patterns
            import re

            # Common color names
            common_colors = [
                "white",
                "black",
                "red",
                "blue",
                "green",
                "yellow",
                "grey",
                "gray",
                "orange",
                "brown",
                "purple",
                "pink",
                "beige",
                "cream",
                "ivory",
                "silver",
                "gold",
                "light grey",
                "dark grey",
                "light gray",
                "dark gray",
            ]
            # Case-insensitive search
            pattern = r"\b(" + "|".join(common_colors) + r")\b"
            matches = re.findall(pattern, text, re.IGNORECASE)
            return matches[0] if matches else None

    def match_rfq_item(
        self, rfq_item: RFQItem, max_results: int = 20
    ) -> dict[str, Any]:
        """
        Match a single RFQ item against competitor and Nippon products.

        Flow:
        1. Check if item is a Nippon product (has nippon keyword + exists in product_master)
           - If yes, skip competitor matching (it's already a Nippon product)
        2. Search competitor_products by keywords → get product_id
        3. Get generics AND NP Marine products for each competitor
        4. Search product_master by NP Marine product names + color bonus
        5. No fallback - only return if NP Marine products found

        Args:
            rfq_item: RFQItem to match
            max_results: Maximum number of results to return

        Returns:
            Dict containing match results
        """
        # Extract text for checking
        text = rfq_item.clean_text if rfq_item.clean_text else rfq_item.raw_text
        extracted_color = self.extract_color_from_text(text)

        # Check if has nippon keyword AND exists in product_master
        has_nippon_keyword = any(kw in text.lower() for kw in settings.NIPPON_KEYWORDS)

        if has_nippon_keyword:
            # Query product_master to verify if it's a Nippon product
            # Search by clean_text or product_name in product_master
            pm_match = (
                self.db.query(ProductMaster)
                .filter(
                    or_(
                        ProductMaster.clean_product_name == text,
                        ProductMaster.product_name == text,
                        ProductMaster.clean_product_name.contains(text),
                    )
                )
                .first()
            )

            if pm_match:
                # This is a Nippon product - skip competitor matching
                logger.info(
                    f"RFQ item {rfq_item.id} is a Nippon product (found in product_master), "
                    f"skipping competitor matching"
                )
                return {
                    "rfq_item_id": rfq_item.id,
                    "raw_text": rfq_item.raw_text,
                    "clean_text": rfq_item.clean_text,
                    "qty": rfq_item.qty,
                    "uom": rfq_item.uom,
                    "color": extracted_color,
                    "keywords": [],
                    "competitor_matches": [],
                    "nippon_matches": [],
                    "total_matches": 0,
                    "is_nippon_product": True,
                    "nippon_product_id": pm_match.id,
                }

        # Extract keywords from clean_text (preferred) or raw_text
        text = rfq_item.clean_text if rfq_item.clean_text else rfq_item.raw_text
        keywords = self.extract_keywords(text)

        logger.info(f"Extracted keywords: {keywords} from RFQ item {rfq_item.id}")

        # Extract color from description text
        extracted_color = self.extract_color_from_text(text)

        logger.info(f"Extracted color: {extracted_color} from RFQ item {rfq_item.id}")

        if not keywords:
            return {
                "rfq_item_id": rfq_item.id,
                "raw_text": rfq_item.raw_text,
                "clean_text": rfq_item.clean_text,
                "qty": rfq_item.qty,
                "uom": rfq_item.uom,
                "color": extracted_color,
                "keywords": [],
                "competitor_matches": [],
                "nippon_matches": [],
                "total_matches": 0,
            }

        logger.info(
            f"Processing RFQ item {rfq_item.id} with keywords: {keywords}, color: {extracted_color}"
        )

        # Step 1: Search competitor products
        competitor_matches = self.search_competitor_products(
            keywords, limit=max_results
        )

        if not competitor_matches:
            logger.info(f"No competitor matches found for RFQ item {rfq_item.id}")
            return {
                "rfq_item_id": rfq_item.id,
                "raw_text": rfq_item.raw_text,
                "clean_text": rfq_item.clean_text,
                "qty": rfq_item.qty,
                "uom": rfq_item.uom,
                "color": extracted_color,
                "keywords": keywords,
                "competitor_matches": [],
                "nippon_matches": [],
                "np_marine_products_found": 0,
                "total_competitor_matches": 0,
                "total_nippon_matches": 0,
                "total_matches": 0,
            }

        # Step 2 & 3: For each competitor match, get Nippon product names, then search product master
        nippon_matches = []
        all_nippon_products = []  # Track all Nippon product names found

        for comp_match in competitor_matches:
            # Get Nippon product names for this competitor product (direct mapping)
            nippon_product_list = self.get_nippon_products_for_competitor(
                comp_match["product_id"]
            )

            # Add to collection for product master search
            for np in nippon_product_list:
                product_name = np.get("product_name")
                if product_name and product_name not in [
                    p.get("product_name") for p in all_nippon_products
                ]:
                    all_nippon_products.append(np)

        # Step 3: Search product master by Nippon product names
        if all_nippon_products:
            pm_matches = self.search_product_master_by_nippon_products(
                all_nippon_products, color=extracted_color, limit=max_results
            )

            # Add competitor source info to each match
            for pm_match in pm_matches:
                # Find which Nippon product matched
                matched_np = None
                for np in all_nippon_products:
                    if (
                        np.get("product_name")
                        and np.get("product_name") in pm_match.get("product_name", "")
                    ) or (
                        np.get("product_name")
                        and np.get("product_name")
                        in pm_match.get("clean_product_name", "")
                    ):
                        matched_np = np
                        break

                pm_match["competitor_source"] = {
                    "np_marine_product": matched_np.get("product_name")
                    if matched_np
                    else None,
                    "np_marine_brand": "NP MARINE",
                }
                pm_match[
                    "generic_names"
                ] = []  # Empty since we don't use generics anymore

                nippon_matches.append(pm_match)

        # No fallback - only return results if NP Marine products were found
        if not nippon_matches:
            logger.info(
                f"No Nippon matches found for RFQ item {rfq_item.id} (no NP Marine products in matching generics)"
            )

        return {
            "rfq_item_id": rfq_item.id,
            "raw_text": rfq_item.raw_text,
            "clean_text": rfq_item.clean_text,
            "qty": rfq_item.qty,
            "uom": rfq_item.uom,
            "color": extracted_color,
            "keywords": keywords,
            "competitor_matches": competitor_matches[:10],  # Top 10 competitor matches
            "nippon_matches": nippon_matches[:max_results],  # Top Nippon matches
            "np_marine_products_found": len(all_nippon_products),
            "total_competitor_matches": len(competitor_matches),
            "total_nippon_matches": len(nippon_matches),
            "total_matches": len(competitor_matches) + len(nippon_matches),
        }

    def match_rfq_by_id(
        self, rfq_id: str, max_results_per_item: int = 20
    ) -> dict[str, Any]:
        """
        Match all RFQ items for a given RFQ ID.

        Args:
            rfq_id: RFQ identifier
            max_results_per_item: Maximum results per RFQ item

        Returns:
            Dict containing all match results
        """
        # Get all RFQ items
        rfq_items = self.get_rfq_items(rfq_id)

        if not rfq_items:
            return {"rfq_id": rfq_id, "total_items": 0, "matches": []}

        # Match each item
        all_matches = []
        total_competitor = 0
        total_nippon = 0

        for item in rfq_items:
            match_result = self.match_rfq_item(item, max_results=max_results_per_item)
            all_matches.append(match_result)
            total_competitor += match_result["total_competitor_matches"]
            total_nippon += match_result["total_nippon_matches"]

        return {
            "rfq_id": rfq_id,
            "total_items": len(rfq_items),
            "total_competitor_matches": total_competitor,
            "total_nippon_matches": total_nippon,
            "total_matches": total_competitor + total_nippon,
            "matches": all_matches,
        }


# Convenience function for easy access
def match_rfq_with_competitors(
    db: Session, rfq_id: str, max_results: int = 20
) -> dict[str, Any]:
    """
    Match RFQ items with competitor and Nippon products.

    Args:
        db: Database session
        rfq_id: RFQ identifier
        max_results: Maximum results per item

    Returns:
        Match results dictionary
    """
    matcher = RFQCompetitorMatcher(db)
    return matcher.match_rfq_by_id(rfq_id, max_results_per_item=max_results)
