"""
Enhanced Hybrid Matching Service

Integrates traditional matching (fuzzy/TF-IDF) with semantic matching (OpenAI embeddings).
Provides a robust matching pipeline with multiple fallback strategies.
"""

import logging
from typing import Any

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.services.matching import MatchingService
from apps.app_nippon_rfq_matching.app.services.semantic_matching import (
    semantic_matching_service,
)
from apps.app_nippon_rfq_matching.app.utils.text_normalization import (
    clean_rfq_description,
)

logger = logging.getLogger(__name__)


class EnhancedMatchingService:
    """
    Enhanced matching service with hybrid strategy.

    Pipeline:
    1. Rule-based exact match (fastest, highest confidence)
    2. Traditional fuzzy/cosine match (fast, medium confidence)
    3. Semantic match with OpenAI (robust, handles typos/variants)

    Uses the best result based on confidence scores.
    """

    def __init__(self):
        """Initialize enhanced matching service."""
        self.traditional_service = MatchingService()
        self.semantic_service = semantic_matching_service
        self.hybrid_threshold = settings.HYBRID_MATCH_THRESHOLD
        self.semantic_threshold = settings.SEMANTIC_MATCH_THRESHOLD

        # Color matching keywords
        self.nippon_keywords = set(settings.NIPPON_KEYWORDS)

    def match_rfq_item(
        self,
        rfq_item: dict[str, Any],
        db: Session,
        use_semantic: bool = True,
        use_color: bool = True,
        use_competitor_fallback: bool = True,
    ) -> dict[str, Any]:
        """
        Match a single RFQ item using hybrid strategy.

        Pipeline:
        1. Search in product_master (Nippon products) - ALL methods
        2. If found in product_master → Return Nippon product match
        3. If NOT found → Search competitor products (if enabled)

        Args:
            rfq_item: Dictionary with RFQ item data
            db: Database session
            use_semantic: Whether to use semantic matching
            use_color: Whether to use color-aware matching
            use_competitor_fallback: Whether to search competitor if not in product_master

        Returns:
            Dictionary with comprehensive match results
        """
        # Extract RFQ data
        raw_text = rfq_item.get("raw_text", "")
        clean_text = rfq_item.get("clean_text") or clean_rfq_description(raw_text)

        if not clean_text:
            return self._no_match_result(rfq_item)

        # Extract color
        extracted_color = self.traditional_service.extract_color_from_text(clean_text)

        logger.info("=== Matching RFQ Item ===")
        logger.info(f"Raw text: '{raw_text}'")
        logger.info(f"Clean text: '{clean_text}'")
        logger.info(f"Extracted color: '{extracted_color}'")

        # ========================================================================
        # STEP 1: Search in product_master (Nippon products) using all methods
        # ========================================================================
        logger.info("Step 1: Searching in product_master (Nippon products)...")

        # Try all matching methods for product_master
        nippon_match = self._find_in_product_master(
            clean_text, extracted_color, db, use_semantic
        )

        if (
            nippon_match
            and nippon_match["match_info"]["confidence"] >= self.semantic_threshold
        ):
            logger.info(
                f"✓ Found in product_master: {nippon_match['product_master']['product_name']} (confidence: "
                f"{nippon_match['match_info']['confidence']})"
            )
            return nippon_match

        # ========================================================================
        # STEP 2: Not found in product_master, search competitor products
        # ========================================================================
        if use_competitor_fallback:
            logger.info(
                "Step 2: Not found in product_master, searching competitor products..."
            )
            competitor_match = self._find_in_competitor_products(
                clean_text, extracted_color, db
            )

            if competitor_match:
                comp_name = competitor_match.get("competitor_info", {}).get(
                    "competitor_product", "Unknown"
                )
                nippon_name = competitor_match.get("product_master", {}).get(
                    "product_name", "Unknown"
                )
                logger.info(
                    f"✓ Found competitor match: {comp_name} → Nippon: {nippon_name}"
                )
                return competitor_match
            else:
                logger.info("✗ No match found in product_master or competitor products")
        else:
            logger.info("Step 2: Competitor fallback disabled")

        # No match found anywhere
        return self._no_match_result(rfq_item)

    def _find_in_product_master(
        self,
        clean_text: str,
        extracted_color: str | None,
        db: Session,
        use_semantic: bool,
    ) -> dict[str, Any] | None:
        """
        Search for RFQ item in product_master using all available methods.

        Priority order (rule-based first, embeddings as fallback):
        1. Exact match (confidence: 1.0)
        2. Partial/contains match (confidence: 0.7-0.95)
        3. Fuzzy match (confidence: 0.7-0.95)
        4. Cosine similarity (confidence: 0.7-0.95)
        5. Semantic match (confidence: 0.7-0.95) - ONLY as fallback

        Returns the best match found.
        """
        best_match = None
        best_confidence = 0.0

        # ====================================================================
        # METHOD 1: Try exact match first (highest priority)
        # ====================================================================
        exact_match = self._try_exact_match(clean_text, extracted_color, db)
        if exact_match and exact_match["match_info"]["confidence"] >= 0.95:
            logger.info(
                f"  ✓ Exact match found: {exact_match['product_master']['product_name']} (confidence: 1.0)"
            )
            return exact_match  # Return immediately, no need to try other methods

        if exact_match and exact_match["match_info"]["confidence"] > best_confidence:
            best_match = exact_match
            best_confidence = exact_match["match_info"]["confidence"]

        # ====================================================================
        # METHOD 2: Try extracting core product name (remove color/number)
        # ====================================================================
        core_name = self._extract_core_product_name(clean_text)
        if core_name != clean_text:
            logger.debug(
                f"  Extracted core product name: '{core_name}' from '{clean_text}'"
            )

            # Try exact match with core name (without NIPPON prefix)
            core_exact_match = self._try_exact_match(core_name, extracted_color, db)
            if core_exact_match:
                confidence = core_exact_match["match_info"]["confidence"]
                logger.info(
                    f"  ✓ Core name exact match: {core_exact_match['product_master']['product_name']} (confidence: "
                    f"{confidence:.3f})"
                )
                return core_exact_match

        # ====================================================================
        # METHOD 2.5: Try with core name + "NIPPON" prefix (for databases that keep NIPPON)
        # ====================================================================
        if core_name != clean_text:
            # Try adding "NIPPON " prefix to core name
            nippon_core_name = (
                f"NIPPON {core_name.replace('-', ' ')}"  # U-MARINE → NIPPON U MARINE
            )
            logger.debug(f"  Trying with NIPPON prefix: '{nippon_core_name}'")

            nippon_exact_match = self._try_exact_match(
                nippon_core_name, extracted_color, db
            )
            if nippon_exact_match:
                confidence = nippon_exact_match["match_info"]["confidence"]
                logger.info(
                    f"  ✓ NIPPON prefix match: {nippon_exact_match['product_master']['product_name']} (confidence: "
                    f"{confidence:.3f})"
                )
                # Adjust to indicate matched via prefix addition
                nippon_exact_match["match_info"]["method"] = "exact"
                return nippon_exact_match

        # ====================================================================
        # METHOD 3: Try partial/contains match
        # ====================================================================
        partial_match = self._try_partial_match(
            clean_text, core_name, extracted_color, db
        )
        if partial_match:
            confidence = partial_match["match_info"]["confidence"]
            logger.info(
                f"  ✓ Partial match: {partial_match['product_master']['product_name']} (confidence: {confidence:.3f})"
            )
            if confidence > best_confidence:
                best_match = partial_match
                best_confidence = confidence

        # ====================================================================
        # METHOD 4: Try fuzzy/cosine match (traditional)
        # ====================================================================
        traditional_match = self._try_traditional_match(clean_text, extracted_color, db)
        if traditional_match:
            confidence = traditional_match["match_info"]["confidence"]
            logger.info(
                f"  ✓ Traditional match: {traditional_match['product_master']['product_name']} (confidence: "
                f"{confidence:.3f})"
            )
            if confidence > best_confidence:
                best_match = traditional_match
                best_confidence = confidence

        # Also try with core name for traditional matching
        if core_name != clean_text:
            core_traditional_match = self._try_traditional_match(
                core_name, extracted_color, db
            )
            if core_traditional_match:
                confidence = core_traditional_match["match_info"]["confidence"]
                logger.info(
                    f"  ✓ Traditional match (core): {core_traditional_match['product_master']['product_name']} "
                    f"(confidence: {confidence:.3f})"
                )
                # Boost confidence for core name match
                boosted_confidence = min(0.98, confidence + 0.15)
                core_traditional_match["match_info"]["confidence"] = boosted_confidence
                if boosted_confidence > best_confidence:
                    best_match = core_traditional_match
                    best_confidence = boosted_confidence

        # ====================================================================
        # METHOD 5: Semantic match (ONLY as fallback - lowest priority)
        # ====================================================================
        # Only try semantic if rule-based methods failed
        if use_semantic and self.semantic_service.enabled and best_confidence < 0.85:
            logger.debug(
                f"  Rule-based methods insufficient (best: {best_confidence:.3f}), trying semantic..."
            )
            semantic_match = self._try_semantic_match(clean_text, extracted_color, db)
            if semantic_match:
                confidence = semantic_match["match_info"]["confidence"]
                logger.info(
                    f"  ✓ Semantic match: {semantic_match['product_master']['product_name']} (confidence: "
                    f"{confidence:.3f})"
                )
                if confidence > best_confidence:
                    best_match = semantic_match
                    best_confidence = confidence

            # Also try semantic with core name
            if core_name != clean_text:
                core_semantic_match = self._try_semantic_match(
                    core_name, extracted_color, db
                )
                if core_semantic_match:
                    confidence = core_semantic_match["match_info"]["confidence"]
                    logger.info(
                        f"  ✓ Semantic match (core): {core_semantic_match['product_master']['product_name']} "
                        f"(confidence: {confidence:.3f})"
                    )
                    # Boost confidence for core name match
                    boosted_confidence = min(0.95, confidence + 0.1)
                    core_semantic_match["match_info"]["confidence"] = boosted_confidence
                    if boosted_confidence > best_confidence:
                        best_match = core_semantic_match
                        best_confidence = boosted_confidence

        if best_match:
            logger.info(
                f"  → Best match: {best_match['product_master']['product_name']} (confidence: {best_confidence:.3f})"
            )

        return best_match

    def _extract_core_product_name(self, text: str) -> str:
        """
        Extract core product name by removing prefixes, color codes, and numbers.

        Examples:
            "[LI] Nippon U-Marine Finish 000 White Base" → "U-MARINE FINISH"
            "NIPPON PAINT MARINE A-MARINE FINISH 000 WHITE" → "A-MARINE FINISH"
            "O-MARINE FINISH GREEN No442" → "O-MARINE FINISH"
            "NP MARINE H-MARINE FINISH 060" → "H-MARINE FINISH"

        Args:
            text: Input product name

        Returns:
            Core product name without prefix, color, and number
        """
        import re

        if not text:
            return ""

        original = text

        # ====================================================================
        # STEP 1: Remove common prefixes and noise
        # ====================================================================
        # Remove tags like [LI], [RFQ], etc.
        text = re.sub(r"^\[[A-Z]+\]\s*", "", text)

        # Remove common prefixes
        prefixes = [
            r"^NIPPON PAINT MARINE\s+",
            r"^NIPPON\s+",
            r"^NP PAINT\s+",
            r"^NP MARINE\s+",
            r"^NP\s+",
            r"^NIPPON MARINE\s+",
            r"^NIPPAINT\s+",
            r"^NIPPON PAINT\s+",
        ]

        for prefix in prefixes:
            text = re.sub(prefix, "", text, flags=re.IGNORECASE)

        # ====================================================================
        # STEP 2: Normalize product name format
        # ====================================================================
        # Replace various "Marine" formats with standard "-MARINE"
        # U-Marine → U-MARINE
        # A Marine → A-MARINE
        # O Marine → O-MARINE
        # H Marine → H-MARINE
        text = re.sub(r"([A-Z])\s*-\s*([Mm]arine)", r"\1-MARINE", text)
        text = re.sub(r"([A-Z])\s+([Mm]arine)", r"\1-MARINE", text)

        # Remove "Base", "Coat" suffix
        # But KEEP "FINISH" if it's part of product line name
        # Only remove if NOT part of "-MARINE FINISH" pattern
        text = re.sub(r"\s+Base\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+Coat\s*$", "", text, flags=re.IGNORECASE)

        # Only remove "Finish" if NOT preceded by -MARINE (keep "A-MARINE FINISH", "U-MARINE FINISH", etc.)
        text = re.sub(r"(?<!MARINE)\s+Finish\s*$", "", text, flags=re.IGNORECASE)

        # ====================================================================
        # STEP 3: Remove color patterns at the end
        # ====================================================================
        # Pattern 1: Color + number (e.g., "000 WHITE", "GREEN No442", "060 RED")
        text = re.sub(
            r"\s+\d{3}\s+(?:GREEN|WHITE|BLACK|RED|BLUE|GRAY|GREY|YELLOW|BROWN|LIGHT|DARK)$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            r"\s+(?:GREEN|WHITE|BLACK|RED|BLUE|GRAY|GREY|YELLOW|BROWN|LIGHT|DARK)\s+(?:No\.?\s*)?\d+$",
            "",
            text,
            flags=re.IGNORECASE,
        )

        # Pattern 2: Color code only at end (e.g., "000", "442", "060", "3550")
        text = re.sub(r"\s+\d{3,5}$", "", text)

        # Pattern 3: Color name only (e.g., "GREEN", "WHITE", "BLACK")
        text = re.sub(
            r"\s+(?:GREEN|WHITE|BLACK|RED|BLUE|GRAY|GREY|YELLOW|BROWN|LIGHT|DARK|CREAM|BEIGE)$",
            "",
            text,
            flags=re.IGNORECASE,
        )

        # ====================================================================
        # STEP 4: Clean up extra whitespace and format
        # ====================================================================
        text = re.sub(r"\s+", " ", text).strip()
        text = text.upper()  # Convert to uppercase for consistency

        # If text is too short after extraction, return original
        if len(text) < 3:
            logger.debug(
                f"Core name extraction failed: '{original}' → '{text}' (too short)"
            )
            return original

        logger.debug(f"Core name extraction: '{original}' → '{text}'")
        return text

    def _try_partial_match(
        self,
        clean_text: str,
        core_name: str,
        extracted_color: str | None,
        db: Session,
    ) -> dict[str, Any] | None:
        """
        Try partial/contains matching with multiple strategies.

        Checks:
        1. Direct contains (query in target or target in query)
        2. Word-based overlap
        3. Hyphen-separated component matching
        4. Handles product names with extra text in parentheses

        Returns match result if found.
        """

        # Try with original text first
        products = (
            db.query(ProductMaster)
            .filter(
                or_(
                    ProductMaster.clean_product_name.contains(clean_text),
                    ProductMaster.product_name.contains(clean_text),
                )
            )
            .limit(10)
            .all()
        )

        # If not found, try with core name
        if not products and core_name != clean_text:
            products = (
                db.query(ProductMaster)
                .filter(
                    or_(
                        ProductMaster.clean_product_name.contains(core_name),
                        ProductMaster.product_name.contains(core_name),
                    )
                )
                .limit(10)
                .all()
            )

        # If still not found, try word-based matching
        if not products:
            # Extract key words from core_name (remove common words)
            words = core_name.split()
            key_words = [
                w
                for w in words
                if len(w) > 2
                and w.upper() not in ["MARINE", "PAINT", "NIPPON", "NP", "FINISH"]
            ]

            logger.debug(f"Key words extracted from '{core_name}': {key_words}")

            if key_words:
                # Build query for each key word
                for word in key_words[:3]:  # Limit to first 3 key words
                    word_products = (
                        db.query(ProductMaster)
                        .filter(
                            or_(
                                ProductMaster.clean_product_name.contains(word),
                                ProductMaster.product_name.contains(word),
                            )
                        )
                        .limit(5)
                        .all()
                    )

                    if word_products:
                        products.extend(word_products)
                        logger.debug(
                            f"Found {len(word_products)} products with word '{word}'"
                        )
                        break

        # If still not found, try matching just the product line (A-MARINE, O-MARINE, etc.)
        if not products:
            for prefix in [
                "A-MARINE",
                "O-MARINE",
                "H-MARINE",
                "U-MARINE",
                "NEOGUARD",
                "NEO GUARD",
            ]:
                if prefix in core_name.upper():
                    prefix_products = (
                        db.query(ProductMaster)
                        .filter(
                            or_(
                                ProductMaster.clean_product_name.contains(prefix),
                                ProductMaster.product_name.contains(prefix),
                            )
                        )
                        .limit(10)
                        .all()
                    )

                    if prefix_products:
                        products.extend(prefix_products)
                        logger.debug(
                            f"Found {len(prefix_products)} products with prefix '{prefix}'"
                        )
                        break

        if not products:
            return None

        # Find best partial match
        best_product = None
        best_score = 0.0

        for product in products:
            # Calculate match score based on multiple factors
            target = product.clean_product_name or product.product_name or ""

            # Normalize both for comparison
            query_norm = self._normalize_for_matching(core_name)
            target_norm = self._normalize_for_matching(target)

            # Calculate scores
            score1 = self._calculate_partial_match_score(clean_text, target)
            score2 = self._calculate_partial_match_score(core_name, target)
            score3 = self._calculate_partial_match_score(query_norm, target_norm)
            score = max(score1, score2, score3)

            # Boost score for exact word matches
            if (
                core_name.lower() in target.lower()
                or target.lower() in core_name.lower()
            ):
                score = min(1.0, score + 0.2)

            # Boost score for matching main product line (A-MARINE, O-MARINE, H-MARINE)
            if any(
                prefix in core_name.upper()
                for prefix in ["A-MARINE", "O-MARINE", "H-MARINE"]
            ):
                if any(
                    prefix in target.upper()
                    for prefix in ["A-MARINE", "O-MARINE", "H-MARINE"]
                ):
                    score = min(1.0, score + 0.15)

            if score > best_score:
                best_score = score
                best_product = product

        if best_product and best_score >= 0.4:  # Lower threshold for partial match
            # Higher confidence for better scores
            confidence = min(0.95, best_score + 0.35)
            return self._build_match_result(
                rfq_text=clean_text,
                product=best_product,
                method="partial",
                score=best_score,
                confidence=confidence,
                extracted_color=extracted_color,
                color_match=False,
            )

        return None

    def _normalize_for_matching(self, text: str) -> str:
        """
        Normalize text for matching by removing common variations.

        Removes:
        - Parentheses content: "(Anti-Fouling)" → ""
        - Extra whitespace
        - Special characters except hyphens

        Example:
            "A-MARINE FINISH (Anti-Fouling)" → "A-MARINE FINISH"
        """
        import re

        if not text:
            return ""

        # Remove content in parentheses
        text = re.sub(r"\s*\([^)]*\)", "", text)

        # Remove special characters except hyphens
        text = re.sub(r"[^\w\s\-]", " ", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text.upper()

    def _calculate_partial_match_score(self, query: str, target: str) -> float:
        """
        Calculate partial match score between query and target.

        Returns score between 0 and 1.
        """
        if not query or not target:
            return 0.0

        query_lower = query.lower().strip()
        target_lower = target.lower().strip()

        # Exact match
        if query_lower == target_lower:
            return 1.0

        # Contains match
        if query_lower in target_lower:
            # Score based on coverage
            return len(query_lower) / len(target_lower)

        if target_lower in query_lower:
            # Score based on coverage
            return len(target_lower) / len(query_lower)

        # Word overlap
        query_words = set(query_lower.split())
        target_words = set(target_lower.split())

        if not query_words or not target_words:
            return 0.0

        intersection = query_words & target_words
        union = query_words | target_words

        return len(intersection) / len(union) if union else 0.0

    def _find_in_competitor_products(
        self, clean_text: str, extracted_color: str | None, db: Session
    ) -> dict[str, Any] | None:
        """
        Search for RFQ item in competitor products.

        Uses the competitor matching API to find:
        1. Competitor product
        2. Generic type
        3. Nippon equivalent

        Returns competitor match result with Nippon product mapping.
        """
        try:
            from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem
            from apps.app_nippon_rfq_matching.app.services.rfq_competitor_matching import (
                RFQCompetitorMatcher,
            )

            # Create a temporary RFQItem for matching
            temp_rfq = RFQItem(
                raw_text=clean_text,
                clean_text=clean_text,
                qty=None,
                uom=None,
                source="semantic_matching",
            )

            # Use competitor matcher
            competitor_matcher = RFQCompetitorMatcher(db)
            result = competitor_matcher.match_rfq_item(temp_rfq)

            # Check if any Nippon products found
            if result.get("nippon_matches") and len(result["nippon_matches"]) > 0:
                nippon_product = result["nippon_matches"][0]

                # Get full product details from database
                product = (
                    db.query(ProductMaster)
                    .filter(ProductMaster.id == nippon_product.get("id"))
                    .first()
                )

                if product:
                    # Build competitor match result
                    return {
                        "rfq": {
                            "raw_text": clean_text,
                            "clean_text": clean_text,
                            "color": extracted_color,
                        },
                        "product_master": {
                            "id": product.id,
                            "clean_product_name": product.clean_product_name,
                            "pmc": product.pmc,
                            "product_name": product.product_name,
                            "color": product.color,
                            "sheet_type": product.sheet_type,
                        },
                        "competitor_info": {
                            "competitor_product": result.get(
                                "competitor_matches", [{}]
                            )[0].get("product_name")
                            if result.get("competitor_matches")
                            else None,
                            "competitor_brand": result.get("competitor_matches", [{}])[
                                0
                            ].get("brand")
                            if result.get("competitor_matches")
                            else None,
                            "generic_names": result.get("nippon_matches", [{}])[0].get(
                                "generic_names", []
                            )
                            if result.get("nippon_matches")
                            else [],
                            "matched_via": "competitor_matrix",
                        },
                        "match_info": {
                            "method": "competitor_matrix",
                            "score": 0.8,  # Default score for competitor match
                            "confidence": 0.8,
                            "extracted_color": extracted_color,
                            "color_match": False,
                            "is_competitor_product": True,
                        },
                    }

        except Exception as e:
            logger.warning(f"Competitor product search failed: {e}")

        return None

    def match_rfq_items(
        self, rfq_items: list[dict[str, Any]], db: Session, use_semantic: bool = True
    ) -> list[dict[str, Any]]:
        """
        Match multiple RFQ items using hybrid strategy.

        Args:
            rfq_items: List of RFQ item dictionaries
            db: Database session
            use_semantic: Whether to use semantic matching

        Returns:
            List of match results
        """
        results = []

        for item in rfq_items:
            result = self.match_rfq_item(item, db, use_semantic)
            results.append(result)

        # Log statistics
        matched_count = sum(1 for r in results if r["product_master"].get("id"))
        methods = [
            r["match_info"]["method"] for r in results if r["product_master"].get("id")
        ]

        logger.info(f"Matched {matched_count}/{len(rfq_items)} RFQ items")
        if methods:
            from collections import Counter

            method_counts = Counter(methods)
            logger.info(f"Match methods: {dict(method_counts)}")

        return results

    def _try_exact_match(
        self, clean_text: str, extracted_color: str | None, db: Session
    ) -> dict[str, Any] | None:
        """
        Try exact/rule-based matching.

        Tries multiple exact match patterns:
        1. Exact match on clean_product_name
        2. Exact match on product_name
        3. Case-insensitive exact match
        4. Contains match (both directions)

        Args:
            clean_text: Cleaned RFQ text
            extracted_color: Extracted color
            db: Database session

        Returns:
            Match result or None
        """
        # Try exact match on clean_product_name
        product = (
            db.query(ProductMaster)
            .filter(ProductMaster.clean_product_name == clean_text)
            .first()
        )

        if product:
            return self._build_match_result(
                rfq_text=clean_text,
                product=product,
                method="exact",
                score=1.0,
                confidence=1.0,
                extracted_color=extracted_color,
                color_match=(
                    extracted_color == product.color
                    if extracted_color and product.color
                    else False
                ),
            )

        # Try exact match on product_name (original, not cleaned)
        product = (
            db.query(ProductMaster)
            .filter(ProductMaster.product_name == clean_text)
            .first()
        )

        if product:
            return self._build_match_result(
                rfq_text=clean_text,
                product=product,
                method="exact",
                score=1.0,
                confidence=1.0,
                extracted_color=extracted_color,
                color_match=(
                    extracted_color == product.color
                    if extracted_color and product.color
                    else False
                ),
            )

        # Try case-insensitive exact match
        product = (
            db.query(ProductMaster)
            .filter(ProductMaster.clean_product_name.ilike(clean_text))
            .first()
        )

        if product:
            return self._build_match_result(
                rfq_text=clean_text,
                product=product,
                method="exact",
                score=1.0,
                confidence=0.98,  # Slightly lower confidence for case-insensitive
                extracted_color=extracted_color,
                color_match=(
                    extracted_color == product.color
                    if extracted_color and product.color
                    else False
                ),
            )

        return None

    def _try_traditional_match(
        self, clean_text: str, extracted_color: str | None, db: Session
    ) -> dict[str, Any] | None:
        """
        Try traditional fuzzy/cosine matching.

        Args:
            clean_text: Cleaned RFQ text
            extracted_color: Extracted color
            db: Database session

        Returns:
            Match result or None
        """
        try:
            # Use existing match_with_color method
            match_result = self.traditional_service.match_with_color(clean_text)

            if not match_result or not match_result.get("matched"):
                return None

            # Get product from database
            matched_text = match_result.get("matched", "")
            product = (
                db.query(ProductMaster)
                .filter(ProductMaster.clean_product_name == matched_text)
                .first()
            )

            if not product:
                return None

            score = match_result.get("score", 0.0) / 100.0  # Convert to 0-1 scale
            method = match_result.get("method", "fuzzy")

            # Determine color match
            color_match = match_result.get("color_match", False)

            return self._build_match_result(
                rfq_text=clean_text,
                product=product,
                method=method,
                score=score,
                confidence=score,  # Traditional score is already a confidence measure
                extracted_color=extracted_color,
                color_match=color_match,
            )

        except Exception as e:
            logger.warning(f"Traditional matching failed: {e}")
            return None

    def _try_semantic_match(
        self, clean_text: str, extracted_color: str | None, db: Session
    ) -> dict[str, Any] | None:
        """
        Try semantic matching using OpenAI embeddings.

        Args:
            clean_text: Cleaned RFQ text
            extracted_color: Extracted color
            db: Database session

        Returns:
            Match result or None
        """
        try:
            result = self.semantic_service.match_rfq_item(clean_text, db)

            if not result.get("matched"):
                return None

            # Get product from database
            product = (
                db.query(ProductMaster)
                .filter(ProductMaster.id == result["product_id"])
                .first()
            )

            if not product:
                return None

            return self._build_match_result(
                rfq_text=clean_text,
                product=product,
                method="semantic",
                score=result["similarity"],
                confidence=result["confidence"],
                extracted_color=extracted_color,
                color_match=(
                    extracted_color == product.color
                    if extracted_color and product.color
                    else False
                ),
            )

        except Exception as e:
            logger.warning(f"Semantic matching failed: {e}")
            return None

    def _build_match_result(
        self,
        rfq_text: str,
        product: ProductMaster,
        method: str,
        score: float,
        confidence: float,
        extracted_color: str | None,
        color_match: bool,
    ) -> dict[str, Any]:
        """Build standardized match result."""
        return {
            "rfq": {
                "raw_text": rfq_text,
                "clean_text": rfq_text,
                "color": extracted_color,
            },
            "product_master": {
                "id": product.id,
                "clean_product_name": product.clean_product_name,
                "pmc": product.pmc,
                "product_name": product.product_name,
                "color": product.color,
                "sheet_type": product.sheet_type,
            },
            "match_info": {
                "method": method,
                "score": score,
                "confidence": confidence,
                "extracted_color": extracted_color,
                "color_match": color_match,
            },
        }

    def _no_match_result(self, rfq_item: dict[str, Any]) -> dict[str, Any]:
        """Build result for no match found."""
        raw_text = rfq_item.get("raw_text", "")
        clean_text = rfq_item.get("clean_text", "")

        # Try to extract color anyway
        extracted_color = self.traditional_service.extract_color_from_text(
            clean_text or raw_text
        )

        return {
            "rfq": {
                "raw_text": raw_text,
                "clean_text": clean_text,
                "color": extracted_color,
            },
            "product_master": {
                "id": None,
                "clean_product_name": None,
                "pmc": None,
                "product_name": None,
                "color": None,
                "sheet_type": None,
            },
            "match_info": {
                "method": "none",
                "score": 0.0,
                "confidence": 0.0,
                "extracted_color": extracted_color,
                "color_match": False,
            },
        }

    def initialize_services(self, db: Session):
        """
        Initialize all matching services.

        Args:
            db: Database session
        """
        logger.info("Initializing enhanced matching services...")

        # Initialize traditional service
        if not self.traditional_service.is_loaded:
            try:
                # Load from database
                products = db.query(ProductMaster).all()
                if products:
                    products_data = [p.to_dict() for p in products]
                    df = pd.DataFrame(products_data)
                    self.traditional_service.load_product_master(df)
                    logger.info(
                        f"Loaded {len(products)} products into traditional matching service"
                    )
            except Exception as e:
                logger.warning(f"Failed to initialize traditional service: {e}")

        # Initialize semantic service
        if self.semantic_service.enabled:
            try:
                self.semantic_service.initialize_vector_store(db)
                logger.info("Initialized semantic matching service")
            except Exception as e:
                logger.warning(f"Failed to initialize semantic service: {e}")

        stats = self.get_stats()
        logger.info(f"Enhanced matching service initialized: {stats}")

    def get_stats(self) -> dict[str, Any]:
        """Get service statistics."""
        from apps.app_nippon_rfq_matching.app.services.vector_store import vector_store

        return {
            "traditional_loaded": self.traditional_service.is_loaded,
            "semantic_enabled": self.semantic_service.enabled,
            "vector_store": vector_store.get_stats(),
        }


# Singleton instance
enhanced_matching_service = EnhancedMatchingService()
