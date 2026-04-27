"""
Migration script to recreate product_equivalents with direct competitor->nippon mapping

New schema:
- competitor_product_id (FK to competitor_products.id) - NOT NULL
- nippon_product_name (VARCHAR) - NOT NULL
- Removed: generic_id, product_id

This creates a direct 1-to-1 mapping between competitor products and Nippon products.

Run this script to update existing database:
    python migrations/recreate_product_equivalents_direct.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Recreate product_equivalents table with direct mapping"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print(
                "Starting migration: Recreate product_equivalents with direct mapping"
            )

            # For SQLite, recreate the table
            if "sqlite" in settings.DATABASE_URL:
                print("  Detected SQLite, recreating table...")

                # Step 1: Drop old table
                print("Step 1: Dropping old table...")
                conn.execute(text("DROP TABLE IF EXISTS product_equivalents"))

                # Step 2: Create new table with direct mapping
                print("Step 2: Creating new table with direct mapping...")

                create_table_sql = text("""
                    CREATE TABLE product_equivalents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        competitor_product_id INTEGER NOT NULL,
                        nippon_product_name VARCHAR(500) NOT NULL,
                        created_at DATETIME,
                        FOREIGN KEY(competitor_product_id) REFERENCES competitor_products (id),
                        UNIQUE (competitor_product_id, nippon_product_name)
                    )
                """)

                conn.execute(create_table_sql)

                # Step 3: Create indexes
                print("Step 3: Creating indexes...")
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_competitor_product_id ON product_equivalents (competitor_product_id)"  # noqa: E501
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_nippon_product_name ON product_equivalents (nippon_product_name)"  # noqa: E501
                    )
                )

                print("  Migration completed successfully!")

            else:
                # For PostgreSQL/MySQL - similar approach
                print("  Detected PostgreSQL/MySQL...")
                conn.execute(text("DROP TABLE IF EXISTS product_equivalents CASCADE"))

                create_table_sql = text("""
                    CREATE TABLE product_equivalents (
                        id SERIAL PRIMARY KEY,
                        competitor_product_id INTEGER NOT NULL,
                        nippon_product_name VARCHAR(500) NOT NULL,
                        created_at TIMESTAMP,
                        FOREIGN KEY(competitor_product_id) REFERENCES competitor_products (id),
                        UNIQUE (competitor_product_id, nippon_product_name)
                    )
                """)

                conn.execute(create_table_sql)
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_competitor_product_id ON product_equivalents (competitor_product_id)"  # noqa: E501
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_nippon_product_name ON product_equivalents (nippon_product_name)"  # noqa: E501
                    )
                )

            # Commit transaction
            trans.commit()

            print("Migration completed successfully!")
            print(
                "\nNote: Please re-upload your competitor matrix to populate the new table."
            )

        except Exception as e:
            # Rollback on error
            trans.rollback()
            print(f"Migration failed: {e}")
            raise


if __name__ == "__main__":
    migrate()
