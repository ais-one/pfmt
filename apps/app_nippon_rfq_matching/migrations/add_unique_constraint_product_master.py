"""
Migration script to add unique constraint to product_master table

This script adds a unique constraint on (sheet_type, pmc, product_name, color)
to prevent duplicate products in the product master.

Run this script to update existing database:
    python migrations/add_unique_constraint_product_master.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add unique constraint to product_master table"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print("Starting migration: Add unique constraint to product_master table")

            # First, remove any duplicates that might exist
            print("Step 1: Removing duplicate products...")

            # Find and remove duplicates (keep the first occurrence based on id)
            delete_duplicate_sql = text("""
                DELETE FROM product_master
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM product_master
                    GROUP BY sheet_type, pmc, product_name, color
                )
            """)

            result = conn.execute(delete_duplicate_sql)
            deleted_count = result.rowcount
            print(f"  Deleted {deleted_count} duplicate products")

            # Add the unique constraint
            print("Step 2: Adding unique constraint...")

            # For SQLite, we need to recreate the table
            if "sqlite" in settings.DATABASE_URL:
                # Get existing table schema
                print("  Detected SQLite, recreating table with unique constraint...")

                # Create new table with unique constraint
                create_new_table_sql = text("""
                    CREATE TABLE product_master_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uploaded_file_id INTEGER,
                        sheet_name VARCHAR(255),
                        sheet_type VARCHAR(50) NOT NULL,
                        row_excel INTEGER,
                        pmc VARCHAR(100) NOT NULL,
                        product_name VARCHAR(500) NOT NULL,
                        color VARCHAR(200),
                        clean_product_name VARCHAR(500),
                        created_at DATETIME,
                        FOREIGN KEY(uploaded_file_id) REFERENCES uploaded_files (id),
                        UNIQUE (sheet_type, pmc, product_name, color)
                    )
                """)

                conn.execute(create_new_table_sql)

                # Copy data from old table to new table
                copy_data_sql = text("""
                    INSERT INTO product_master_new (id, uploaded_file_id, sheet_name, sheet_type, row_excel, pmc, product_name, color, clean_product_name, created_at)
                    SELECT id, uploaded_file_id, sheet_name, sheet_type, row_excel, pmc, product_name, color, clean_product_name, created_at
                    FROM product_master
                """)  # noqa: E501

                conn.execute(copy_data_sql)
                print("  Copied existing data to new table")

                # Drop old table
                conn.execute(text("DROP TABLE product_master"))

                # Rename new table to original name
                conn.execute(
                    text("ALTER TABLE product_master_new RENAME TO product_master")
                )

                # Recreate indexes
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_master_uploaded_file_id ON product_master (uploaded_file_id)"
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_master_sheet_type ON product_master (sheet_type)"
                    )
                )
                conn.execute(
                    text("CREATE INDEX ix_product_master_pmc ON product_master (pmc)")
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_product_master_clean_product_name ON product_master (clean_product_name)"
                    )
                )

                print("  Table recreated with unique constraint")

            else:
                # For PostgreSQL/MySQL, just add the constraint
                alter_table_sql = text("""
                    ALTER TABLE product_master
                    ADD CONSTRAINT uq_product_master UNIQUE (sheet_type, pmc, product_name, color)
                """)

                conn.execute(alter_table_sql)
                print("  Unique constraint added")

            # Commit transaction
            trans.commit()

            print("Migration completed successfully!")

        except Exception as e:
            # Rollback on error
            trans.rollback()
            print(f"Migration failed: {e}")
            raise


if __name__ == "__main__":
    migrate()
