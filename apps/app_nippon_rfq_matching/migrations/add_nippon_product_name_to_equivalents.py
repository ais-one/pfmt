"""
Migration script to add nippon_product_name column to product_equivalents table

This script:
1. Adds nippon_product_name column (VARCHAR) - stores Nippon product name as string
2. Makes product_id column nullable (to support either competitor products OR Nippon products)
3. Adds unique constraint for (generic_id, nippon_product_name)

Run this script to update existing database:
    python migrations/add_nippon_product_name_to_equivalents.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add nippon_product_name column to product_equivalents table"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print(
                "Starting migration: Add nippon_product_name to product_equivalents table"
            )

            # Check if column already exists
            check_column_sql = text("""
                PRAGMA table_info(product_equivalents)
            """)
            result = conn.execute(check_column_sql)
            columns = [row[1] for row in result.fetchall()]

            if "nippon_product_name" in columns:
                print("  Column nippon_product_name already exists, skipping migration")
                return

            # For SQLite, we need to recreate the table to modify columns
            if "sqlite" in settings.DATABASE_URL:
                print("  Detected SQLite, recreating table with new columns...")

                # Step 1: Create new table with updated schema
                print("Step 1: Creating new table with nippon_product_name column...")

                create_new_table_sql = text("""
                    CREATE TABLE product_equivalents_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        generic_id INTEGER NOT NULL,
                        product_id INTEGER,
                        nippon_product_name VARCHAR(500),
                        created_at DATETIME,
                        FOREIGN KEY(generic_id) REFERENCES generics (id),
                        FOREIGN KEY(product_id) REFERENCES competitor_products (id),
                        UNIQUE (generic_id, product_id),
                        UNIQUE (generic_id, nippon_product_name)
                    )
                """)

                conn.execute(create_new_table_sql)

                # Step 2: Copy existing data from old table to new table
                print("Step 2: Copying existing data to new table...")

                copy_data_sql = text("""
                    INSERT INTO product_equivalents_new (id, generic_id, product_id, nippon_product_name, created_at)
                    SELECT id, generic_id, product_id, NULL, created_at
                    FROM product_equivalents
                """)

                result = conn.execute(copy_data_sql)
                copied_count = result.rowcount
                print(f"  Copied {copied_count} existing records to new table")

                # Step 3: Drop old table
                print("Step 3: Dropping old table...")
                conn.execute(text("DROP TABLE product_equivalents"))

                # Step 4: Rename new table to original name
                print("Step 4: Renaming new table...")
                conn.execute(
                    text(
                        "ALTER TABLE product_equivalents_new RENAME TO product_equivalents"
                    )
                )

                # Step 5: Recreate indexes
                print("Step 5: Recreating indexes...")
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_generic_id ON product_equivalents (generic_id)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_product_id ON product_equivalents (product_id)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_nippon_product_name ON product_equivalents (nippon_product_name)"  # noqa: E501
                    )
                )

                print("  Migration completed successfully!")

            else:
                # For PostgreSQL/MySQL
                print("  Detected PostgreSQL/MySQL...")

                # Add nippon_product_name column
                alter_table_sql = text("""
                    ALTER TABLE product_equivalents
                    ADD COLUMN nippon_product_name VARCHAR(500)
                """)
                conn.execute(alter_table_sql)
                print("  Added nippon_product_name column")

                # Make product_id nullable
                alter_nullable_sql = text("""
                    ALTER TABLE product_equivalents
                    ALTER COLUMN product_id DROP NOT NULL
                """)
                conn.execute(alter_nullable_sql)
                print("  Made product_id nullable")

                # Add unique constraint
                unique_sql = text("""
                    ALTER TABLE product_equivalents
                    ADD CONSTRAINT uq_product_equivalent_nippon
                    UNIQUE (generic_id, nippon_product_name)
                """)
                conn.execute(unique_sql)
                print("  Added unique constraint")

                # Create index
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_nippon_product_name ON product_equivalents (nippon_product_name)"  # noqa: E501
                    )
                )
                print("  Created index on nippon_product_name")

            # Commit transaction
            trans.commit()

            print("Migration completed successfully!")
            print(
                "\nNote: After running this migration, you should re-upload your competitor matrix"
            )
            print("      to store NP product names in the nippon_product_name column.")

        except Exception as e:
            # Rollback on error
            trans.rollback()
            print(f"Migration failed: {e}")
            raise


if __name__ == "__main__":
    migrate()
