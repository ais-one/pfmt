"""
Semantic Matching Service

Service for semantic matching of RFQ items to product master using OpenAI embeddings.
Implements hybrid strategy: rule-based matching with semantic fallback.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.services.embedding_service import (
    embedding_service,
)
from apps.app_nippon_rfq_matching.app.services.vector_store import vector_store
from apps.app_nippon_rfq_matching.app.utils.text_normalization import (
    clean_rfq_description,
)

logger = logging.getLogger(__name__)


class SemanticMatchingService:
    """
    Semantic matching service using OpenAI embeddings.

    Strategy:
    1. Try rule-based matching first (fast, exact matches)
    2. If confidence < threshold, use semantic search (robust, handles typos)
    3. Combine results with confidence scores
    """

    def __init__(self):
        """Initialize semantic matching service."""
        self.semantic_threshold = settings.SEMANTIC_MATCH_THRESHOLD
        self.hybrid_threshold = settings.HYBRID_MATCH_THRESHOLD
        self.enabled = settings.ENABLE_SEMANTIC_SEARCH and embedding_service.enabled

        if not self.enabled:
            logger.warning("Semantic matching is disabled (check OPENAI_API_KEY)")

    def match_rfq_item(
        self, rfq_text: str, db: Session, top_k: int = 5, use_color: bool = True
    ) -> dict[str, Any]:
        """
        Match a single RFQ item to product master.

        Args:
            rfq_text: RFQ item description text
            db: Database session
            top_k: Number of top results to return
            use_color: Whether to consider color in matching

        Returns:
            Dictionary with match results:
            {
                "matched": bool,
                "product_id": int or None,
                "product_name": str or None,
                "similarity": float,
                "method": "rule-based" or "semantic",
                "color": str or None,
                "confidence": float
            }

        Example:
            >>> service = SemanticMatchingService()
            >>> result = service.match_rfq_item("Nippon Paint Marine", db)
            >>> result["matched"]
            True
            >>> result["method"]
            'semantic'
        """
        if not rfq_text or not rfq_text.strip():
            return {
                "matched": False,
                "product_id": None,
                "product_name": None,
                "similarity": 0.0,
                "method": "none",
                "color": None,
                "confidence": 0.0,
            }

        # Clean and normalize text
        clean_text = clean_rfq_description(rfq_text)

        logger.info(f"Matching RFQ: '{rfq_text}' -> '{clean_text}'")

        # ========================================================================
        # STEP 1: Search in product_master (Nippon products) using all methods
        # ========================================================================
        logger.info("Step 1: Searching in product_master...")

        # Try all methods for product_master
        best_match = None
        best_confidence = 0.0

        # Try exact match
        rule_result = self._rule_based_match(clean_text, db, use_color)
        if rule_result["matched"]:
            if rule_result["confidence"] > best_confidence:
                best_match = rule_result
                best_confidence = rule_result["confidence"]
            logger.debug(f"  Rule-based match: {rule_result['confidence']:.3f}")

        # Try semantic match (if enabled)
        if self.enabled:
            semantic_result = self._semantic_match(clean_text, db, top_k, use_color)
            if semantic_result["matched"]:
                if semantic_result["confidence"] > best_confidence:
                    best_match = semantic_result
                    best_confidence = semantic_result["confidence"]
                logger.debug(f"  Semantic match: {semantic_result['confidence']:.3f}")

        # If found in product_master with good confidence, return it
        if best_match and best_confidence >= self.semantic_threshold:
            logger.info(
                f"✓ Found in product_master: {best_match['product_name']} (confidence: {best_confidence:.3f})"
            )
            return best_match

        # ========================================================================
        # STEP 2: Not found in product_master - this might be a competitor product
        # ========================================================================
        logger.info("Step 2: Not found in product_master with sufficient confidence")
        logger.info(
            "  → This might be a competitor product (use competitor matrix search)"
        )

        # Return the best match even if below threshold (caller can decide what to do)
        if best_match:
            return best_match

        return {
            "matched": False,
            "product_id": None,
            "product_name": None,
            "similarity": 0.0,
            "method": "none",
            "color": None,
            "confidence": 0.0,
        }

    def _rule_based_match(
        self, clean_text: str, db: Session, use_color: bool
    ) -> dict[str, Any]:
        """
        Rule-based matching using exact string matching.

        Args:
            clean_text: Cleaned RFQ text
            db: Database session
            use_color: Whether to consider color

        Returns:
            Match result dictionary
        """
        # Try exact match first
        product = (
            db.query(ProductMaster)
            .filter(ProductMaster.clean_product_name == clean_text)
            .first()
        )

        if product:
            return {
                "matched": True,
                "product_id": product.id,
                "product_name": product.product_name,
                "similarity": 1.0,
                "method": "rule-based",
                "color": product.color,
                "confidence": 1.0,
            }

        # Try partial match (contains)
        products = (
            db.query(ProductMaster)
            .filter(ProductMaster.clean_product_name.contains(clean_text))
            .limit(5)
            .all()
        )

        if products:
            # Calculate confidence based on match quality
            best_product = products[0]
            confidence = self._calculate_rule_confidence(
                clean_text, best_product.clean_product_name
            )

            return {
                "matched": True,
                "product_id": best_product.id,
                "product_name": best_product.product_name,
                "similarity": confidence,
                "method": "rule-based",
                "color": best_product.color,
                "confidence": confidence,
            }

        return {
            "matched": False,
            "product_id": None,
            "product_name": None,
            "similarity": 0.0,
            "method": "rule-based",
            "color": None,
            "confidence": 0.0,
        }

    def _semantic_match(
        self, clean_text: str, db: Session, top_k: int, use_color: bool
    ) -> dict[str, Any]:
        """
        Semantic matching using OpenAI embeddings.

        Args:
            clean_text: Cleaned RFQ text
            db: Database session
            top_k: Number of results
            use_color: Whether to consider color

        Returns:
            Match result dictionary
        """
        # Generate embedding for query
        query_embedding = embedding_service.embed_text(clean_text)

        if query_embedding is None:
            logger.warning("Failed to generate embedding for semantic match")
            return {
                "matched": False,
                "product_id": None,
                "product_name": None,
                "similarity": 0.0,
                "method": "semantic",
                "color": None,
                "confidence": 0.0,
            }

        # Search vector store
        results = vector_store.search_product_master(
            query_embedding, top_k=top_k, threshold=self.semantic_threshold
        )

        if not results:
            logger.debug(f"No semantic matches found for '{clean_text}'")
            return {
                "matched": False,
                "product_id": None,
                "product_name": None,
                "similarity": 0.0,
                "method": "semantic",
                "color": None,
                "confidence": 0.0,
            }

        # Get best match
        best_match = results[0]
        product_id = best_match["product_id"]
        similarity = best_match["similarity"]

        # Get product details from DB
        product = db.query(ProductMaster).filter(ProductMaster.id == product_id).first()

        if not product:
            logger.warning(f"Product {product_id} found in vector store but not in DB")
            return {
                "matched": False,
                "product_id": None,
                "product_name": None,
                "similarity": 0.0,
                "method": "semantic",
                "color": None,
                "confidence": 0.0,
            }

        logger.debug(
            f"Semantic match found: '{product.product_name}' (similarity: {similarity:.3f})"
        )

        return {
            "matched": True,
            "product_id": product.id,
            "product_name": product.product_name,
            "similarity": similarity,
            "method": "semantic",
            "color": product.color,
            "confidence": similarity,
        }

    def _calculate_rule_confidence(self, query: str, match: str) -> float:
        """
        Calculate confidence score for rule-based match.

        Args:
            query: Query text
            match: Matched text

        Returns:
            Confidence score between 0 and 1
        """
        if not query or not match:
            return 0.0

        # Exact match
        if query == match:
            return 1.0

        # Contains match
        if query in match or match in query:
            # Calculate overlap ratio
            shorter, longer = (
                (query, match) if len(query) < len(match) else (match, query)
            )
            overlap = len(shorter) / len(longer)
            return 0.7 + (0.3 * overlap)

        # Word overlap
        query_words = set(query.split())
        match_words = set(match.split())

        if not query_words or not match_words:
            return 0.0

        intersection = query_words & match_words
        union = query_words | match_words

        jaccard = len(intersection) / len(union) if union else 0.0

        return jaccard

    def match_rfq_items_batch(
        self, rfq_items: list[dict[str, Any]], db: Session, top_k: int = 5
    ) -> list[dict[str, Any]]:
        """
        Match multiple RFQ items in batch.

        Args:
            rfq_items: List of RFQ item dictionaries
            db: Database session
            top_k: Number of top results per item

        Returns:
            List of match results (same order as input)

        Example:
            >>> items = [
            ...     {"raw_text": "Nippon Paint Marine", "clean_text": "nippon paint marine"},
            ...     {"raw_text": "International Thinner", "clean_text": "international thinner"}
            ... ]
            >>> service = SemanticMatchingService()
            >>> results = service.match_rfq_items_batch(items, db)
            >>> len(results)
            2
        """
        results = []

        for item in rfq_items:
            clean_text = item.get("clean_text") or item.get("raw_text", "")
            result = self.match_rfq_item(clean_text, db, top_k)
            results.append(result)

        matched_count = sum(1 for r in results if r["matched"])
        logger.info(f"Matched {matched_count}/{len(rfq_items)} RFQ items")

        return results

    def initialize_vector_store(self, db: Session):
        """
        Initialize or rebuild the vector store with current product master.

        Args:
            db: Database session
        """
        if not self.enabled:
            logger.warning("Cannot initialize vector store: semantic matching disabled")
            return

        logger.info("Initializing vector store with product master data...")

        # Get all products from database
        products = db.query(ProductMaster).all()

        if not products:
            logger.warning("No products found in database to initialize vector store")
            return

        # Convert to list of dictionaries
        product_dicts = []
        for p in products:
            product_dicts.append(
                {
                    "id": p.id,
                    "clean_product_name": p.clean_product_name or "",
                    "product_name": p.product_name or "",
                    "color": p.color,
                }
            )

        # Rebuild vector store
        vector_store.rebuild_product_master_index(product_dicts)

        stats = vector_store.get_stats()
        logger.info(f"Vector store initialized: {stats}")


# Singleton instance
semantic_matching_service = SemanticMatchingService()
