"""
Migration script to add product_nippon_id column to product_equivalents table

This script:
1. Adds product_nippon_id column (FK to product_master.id)
2. Makes product_id column nullable (to support either competitor products OR Nippon products)
3. Adds unique constraint for (generic_id, product_nippon_id)

Run this script to update existing database:
    python migrations/add_product_nippon_id_to_equivalents.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add product_nippon_id column to product_equivalents table"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print(
                "Starting migration: Add product_nippon_id to product_equivalents table"
            )

            # Check if column already exists
            check_column_sql = text("""
                PRAGMA table_info(product_equivalents)
            """)
            result = conn.execute(check_column_sql)
            columns = [row[1] for row in result.fetchall()]

            if "product_nippon_id" in columns:
                print("  Column product_nippon_id already exists, skipping migration")
                return

            # For SQLite, we need to recreate the table to modify columns
            if "sqlite" in settings.DATABASE_URL:
                print("  Detected SQLite, recreating table with new columns...")

                # Step 1: Create new table with updated schema
                print("Step 1: Creating new table with product_nippon_id column...")

                create_new_table_sql = text("""
                    CREATE TABLE product_equivalents_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        generic_id INTEGER NOT NULL,
                        product_id INTEGER,
                        product_nippon_id INTEGER,
                        created_at DATETIME,
                        FOREIGN KEY(generic_id) REFERENCES generics (id),
                        FOREIGN KEY(product_id) REFERENCES competitor_products (id),
                        FOREIGN KEY(product_nippon_id) REFERENCES product_master (id),
                        UNIQUE (generic_id, product_id),
                        UNIQUE (generic_id, product_nippon_id)
                    )
                """)

                conn.execute(create_new_table_sql)

                # Step 2: Copy existing data from old table to new table
                print("Step 2: Copying existing data to new table...")

                copy_data_sql = text("""
                    INSERT INTO product_equivalents_new (id, generic_id, product_id, product_nippon_id, created_at)
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
                        "CREATE INDEX ix_product_equivalents_product_nippon_id ON product_equivalents (product_nippon_id)"  # noqa: E501
                    )
                )

                print("  Migration completed successfully!")

            else:
                # For PostgreSQL/MySQL
                print("  Detected PostgreSQL/MySQL...")

                # Add product_nippon_id column
                alter_table_sql = text("""
                    ALTER TABLE product_equivalents
                    ADD COLUMN product_nippon_id INTEGER
                """)
                conn.execute(alter_table_sql)
                print("  Added product_nippon_id column")

                # Make product_id nullable
                alter_nullable_sql = text("""
                    ALTER TABLE product_equivalents
                    ALTER COLUMN product_id DROP NOT NULL
                """)
                conn.execute(alter_nullable_sql)
                print("  Made product_id nullable")

                # Add foreign key constraint
                fk_sql = text("""
                    ALTER TABLE product_equivalents
                    ADD CONSTRAINT fk_product_equivalents_product_nippon_id
                    FOREIGN KEY (product_nippon_id) REFERENCES product_master (id)
                """)
                conn.execute(fk_sql)
                print("  Added foreign key constraint")

                # Add unique constraint
                unique_sql = text("""
                    ALTER TABLE product_equivalents
                    ADD CONSTRAINT uq_product_equivalent_nippon
                    UNIQUE (generic_id, product_nippon_id)
                """)
                conn.execute(unique_sql)
                print("  Added unique constraint")

                # Create index
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_equivalents_product_nippon_id ON product_equivalents (product_nippon_id)"  # noqa: E501
                    )
                )
                print("  Created index on product_nippon_id")

            # Commit transaction
            trans.commit()

            print("Migration completed successfully!")
            print(
                "\nNote: After running this migration, you should re-upload your competitor matrix"
            )
            print("      to link NP products to the actual product_master entries.")

        except Exception as e:
            # Rollback on error
            trans.rollback()
            print(f"Migration failed: {e}")
            raise


if __name__ == "__main__":
    migrate()
