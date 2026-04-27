"""
Script to flush all competitor matrix data

This will delete all data from:
- product_equivalents
- competitor_products
- brands
- generics

Run this script to clean competitor data:
    python migrations/flush_competitor_data.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def flush():
    """Flush all competitor matrix data"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print("Starting flush: Remove all competitor matrix data")

            # Get counts before deletion
            result = conn.execute(text("SELECT COUNT(*) FROM product_equivalents"))
            equivalents_count = result.scalar()
            print(f"  Product equivalents: {equivalents_count}")

            result = conn.execute(text("SELECT COUNT(*) FROM competitor_products"))
            products_count = result.scalar()
            print(f"  Competitor products: {products_count}")

            result = conn.execute(text("SELECT COUNT(*) FROM brands"))
            brands_count = result.scalar()
            print(f"  Brands: {brands_count}")

            result = conn.execute(text("SELECT COUNT(*) FROM generics"))
            generics_count = result.scalar()
            print(f"  Generics: {generics_count}")

            # Delete in correct order (respecting foreign keys)
            print("\nDeleting data...")

            conn.execute(text("DELETE FROM product_equivalents"))
            print("  Deleted product_equivalents")

            conn.execute(text("DELETE FROM competitor_products"))
            print("  Deleted competitor_products")

            conn.execute(text("DELETE FROM brands"))
            print("  Deleted brands")

            conn.execute(text("DELETE FROM generics"))
            print("  Deleted generics")

            # Commit transaction
            trans.commit()

            print("\nFlush completed successfully!")

        except Exception as e:
            # Rollback on error
            trans.rollback()
            print(f"Flush failed: {e}")
            raise


if __name__ == "__main__":
    flush()
