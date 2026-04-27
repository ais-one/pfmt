"""
Product Tokenization Service

Service for tokenizing product names from product_master.
Stores tokenized versions in product_master_mv table for improved matching.
"""

import logging
from typing import Any

from sqlalchemy import distinct, text
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models import ProductMaster
from apps.app_nippon_rfq_matching.app.models.rfq import ProductMasterMV
from apps.app_nippon_rfq_matching.app.utils.text_normalization import normalize_text

logger = logging.getLogger(__name__)

# Common stopwords to remove from tokens
STOPWORDS = {
    # Colors
    "white",
    "black",
    "red",
    "blue",
    "green",
    "yellow",
    "brown",
    "gray",
    "grey",
    "orange",
    "purple",
    "pink",
    "cyan",
    "magenta",
    "lime",
    "olive",
    "maroon",
    "navy",
    "teal",
    "azure",
    "beige",
    "cream",
    "ivory",
    "lavender",
    "gold",
    "silver",
    "bronze",
    "platinum",
    "pearl",
    "champagne",
    "chocolate",
    "coffee",
    # Product types
    "paint",
    "coating",
    "finish",
    "primer",
    "thinner",
    "hardener",
    "activator",
    "anti",
    "fouling",
    "epoxy",
    "polyurethane",
    "alkyd",
    "acrylic",
    # Common words
    "marine",
    "nippon",
    "np",
    "nippont",
    "product",
    "system",
    "type",
    "kit",
    "a",
    "an",
    "the",
    "for",
    "with",
    "and",
    "or",
    "of",
    "in",
    "at",
    "no",
    "sku",
    "item",
    "number",
    "code",
    "ref",
}


class TokenizationService:
    """Service for tokenizing product names"""

    def tokenize_product_name(self, product_name: str) -> str:
        """
        Tokenize a product name into searchable tokens.

        Steps:
        1. Normalize text (lowercase, remove special chars)
        2. Split into words
        3. Remove stopwords
        4. Sort and deduplicate
        5. Join with spaces

        Examples:
            "NIPPON U-MARINE FINISH 000 WHITE" → "marine finish"
            "A-MARINE FINISH" → "marine finish"
            "NEOGUARD 100" → "neoguard"

        Args:
            product_name: Original product name

        Returns:
            Tokenized string
        """
        if not product_name:
            return ""

        # Normalize
        text = normalize_text(product_name)

        # Split into words
        words = text.split()

        # Remove stopwords
        filtered_words = [w for w in words if w.lower() not in STOPWORDS and len(w) > 2]

        # Remove duplicates and sort
        unique_words = sorted(set(filtered_words))

        # Join with spaces
        tokens = " ".join(unique_words)

        return tokens

    def get_distinct_product_names(self, db: Session) -> list[str]:
        """
        Get all distinct product names from product_master.

        Args:
            db: Database session

        Returns:
            List of distinct product names
        """
        try:
            # Get distinct product_name values
            products = db.query(distinct(ProductMaster.product_name)).all()

            product_names = [p[0] for p in products if p[0]]

            logger.info(f"Found {len(product_names)} distinct product names")
            return product_names

        except Exception as e:
            logger.error(f"Error getting distinct product names: {e}")
            return []

    def tokenize_and_store(self, db: Session, batch_size: int = 100) -> dict[str, Any]:
        """
        Tokenize all distinct product names and store in product_master_mv.

        Args:
            db: Database session
            batch_size: Number of records to process at once

        Returns:
            Dictionary with statistics
        """
        logger.info("Starting product name tokenization...")

        # Get distinct product names
        product_names = self.get_distinct_product_names(db)

        if not product_names:
            logger.warning("No product names found to tokenize")
            return {"total": 0, "created": 0, "updated": 0, "skipped": 0}

        # Get existing tokens
        existing_tokens = db.query(ProductMasterMV.product_name).all()
        existing_set = {p.product_name for p in existing_tokens}

        # Clear existing table (for rebuild)
        if existing_set:
            logger.info(f"Clearing existing {len(existing_set)} tokenized products")
            db.query(ProductMasterMV).delete()
            db.commit()

        # Process and insert
        created_count = 0
        skipped_count = 0

        for product_name in product_names:
            try:
                # Tokenize
                tokens = self.tokenize_product_name(product_name)

                if not tokens:
                    skipped_count += 1
                    continue

                # Create record
                mv_record = ProductMasterMV(product_name=product_name, tokens=tokens)
                db.add(mv_record)
                created_count += 1

            except Exception as e:
                logger.warning(f"Error tokenizing '{product_name}': {e}")
                skipped_count += 1
                continue

            # Commit in batches
            if (created_count + skipped_count) % batch_size == 0:
                db.commit()
                logger.info(
                    f"Processed {created_count + skipped_count}/{len(product_names)} products..."
                )

        # Final commit
        db.commit()

        logger.info(
            f"Tokenization complete: {created_count} created, {skipped_count} skipped"
        )

        return {
            "total": len(product_names),
            "created": created_count,
            "updated": 0,
            "skipped": skipped_count,
        }

    def search_by_tokens(
        self, query: str, db: Session, limit: int = 20
    ) -> list[dict[str, Any]]:
        """
        Search for products using tokenized names.

        Tokenizes the query and searches against stored tokens.

        Args:
            query: Search query
            db: Database session
            limit: Maximum number of results

        Returns:
            List of matching products with scores
        """
        # Tokenize the query
        query_tokens = self.tokenize_product_name(query)

        if not query_tokens:
            return []

        # Split into individual tokens
        query_token_list = query_tokens.split()

        if not query_token_list:
            return []

        # Build SQL query to find products with matching tokens
        # We want products that contain ALL query tokens
        sql_query = """
            SELECT product_name, tokens,
                   COUNT(*) as match_count
            FROM product_master_mv
        WHERE 1=1
        """

        # Add token matching conditions
        for i, token in enumerate(query_token_list):
            sql_query += f" AND tokens LIKE :token{i} "

        sql_query += """
            GROUP BY product_name, tokens
            ORDER BY match_count DESC, product_name
            LIMIT :limit
        """

        try:
            # Execute query
            params = {
                f"token{i}": f"%{token}%" for i, token in enumerate(query_token_list)
            }
            params["limit"] = limit
            result = db.execute(text(sql_query), params)

            rows = result.fetchall()

            # Format results
            matches = []
            for row in rows:
                matches.append(
                    {"product_name": row[0], "tokens": row[1], "match_count": row[2]}
                )

            logger.info(
                f"Token search found {len(matches)} matches for query: '{query}'"
            )
            return matches

        except Exception as e:
            logger.error(f"Error searching by tokens: {e}")
            return []

    def get_stats(self, db: Session) -> dict[str, Any]:
        """
        Get statistics about the tokenized products.

        Args:
            db: Database session

        Returns:
            Statistics dictionary
        """
        try:
            total = db.query(ProductMasterMV).count()

            # Get sample of tokens
            samples = db.query(ProductMasterMV).limit(5).all()

            return {
                "total_tokenized_products": total,
                "sample_tokens": [
                    {"product_name": p.product_name, "tokens": p.tokens}
                    for p in samples
                ],
            }

        except Exception as e:
            logger.error(f"Error getting tokenization stats: {e}")
            return {"total_tokenized_products": 0, "sample_tokens": []}


# Singleton instance
tokenization_service = TokenizationService()
