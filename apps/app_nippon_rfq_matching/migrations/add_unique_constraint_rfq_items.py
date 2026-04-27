"""
Migration script to add unique constraint to rfq_items table

This script adds a unique constraint on (rfq_id, raw_text) to prevent
duplicate RFQ items within the same RFQ.

Run this script to update existing database:
    python migrations/add_unique_constraint_rfq_items.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add unique constraint to rfq_items table"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Start transaction
        trans = conn.begin()

        try:
            print("Starting migration: Add unique constraint to rfq_items table")

            # First, remove any duplicates that might exist
            print("Step 1: Removing duplicate RFQ items...")

            # Find and remove duplicates (keep the first occurrence)
            delete_duplicate_sql = text("""
                DELETE FROM rfq_items
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM rfq_items
                    GROUP BY rfq_id, raw_text
                )
            """)

            result = conn.execute(delete_duplicate_sql)
            deleted_count = result.rowcount
            print(f"  Deleted {deleted_count} duplicate RFQ items")

            # Add the unique constraint
            print("Step 2: Adding unique constraint...")

            # For SQLite, we need to recreate the table
            # Check if we're using SQLite
            if "sqlite" in settings.DATABASE_URL:
                # Get existing table schema
                print("  Detected SQLite, recreating table with unique constraint...")

                # Create new table with unique constraint
                create_new_table_sql = text("""
                    CREATE TABLE rfq_items_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        uploaded_file_id INTEGER,
                        rfq_id VARCHAR(100) NOT NULL,
                        raw_text TEXT NOT NULL,
                        clean_text TEXT,
                        qty VARCHAR(50),
                        uom VARCHAR(50),
                        source VARCHAR(100) NOT NULL,
                        created_at DATETIME,
                        FOREIGN KEY(uploaded_file_id) REFERENCES uploaded_files (id),
                        UNIQUE (rfq_id, raw_text)
                    )
                """)

                conn.execute(create_new_table_sql)

                # Copy data from old table to new table
                copy_data_sql = text("""
                    INSERT INTO rfq_items_new (id, uploaded_file_id, rfq_id, raw_text, clean_text, qty, uom, source, created_at)
                    SELECT id, uploaded_file_id, rfq_id, raw_text, clean_text, qty, uom, source, created_at
                    FROM rfq_items
                """)  # noqa: E501

                conn.execute(copy_data_sql)
                print("  Copied existing data to new table")

                # Drop old table
                conn.execute(text("DROP TABLE rfq_items"))

                # Rename new table to original name
                conn.execute(text("ALTER TABLE rfq_items_new RENAME TO rfq_items"))

                # Recreate indexes
                conn.execute(
                    text(
                        "CREATE INDEX ix_rfq_items_uploaded_file_id ON rfq_items (uploaded_file_id)"
                    )
                )
                conn.execute(
                    text("CREATE INDEX ix_rfq_items_rfq_id ON rfq_items (rfq_id)")
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_rfq_items_clean_text ON rfq_items (clean_text)"
                    )
                )

                print("  Table recreated with unique constraint")

            else:
                # For PostgreSQL/MySQL, just add the constraint
                alter_table_sql = text("""
                    ALTER TABLE rfq_items
                    ADD CONSTRAINT uq_rfq_id_raw_text UNIQUE (rfq_id, raw_text)
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
