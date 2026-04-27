"""
Migration script to add color column to rfq_items table
and update unique constraint to (rfq_id, clean_text, color)

Run this script to update the database schema:
    python migrations/add_color_to_rfq_items.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add color column to rfq_items table and update unique constraint"""
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Check if rfq_items table exists
        result = conn.execute(
            text("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='rfq_items'
        """)
        )
        table_exists = result.fetchone()

        if not table_exists:
            print(
                "Table 'rfq_items' does not exist yet. It will be created with the updated schema when the app initializes."  # noqa: E501
            )
            return

        # Check if color column already exists
        result = conn.execute(
            text("""
            SELECT COUNT(*) as count
            FROM pragma_table_info('rfq_items')
            WHERE name = 'color'
        """)
        )
        row = result.fetchone()

        if row and row[0] > 0:
            print(
                "Column 'color' already exists in rfq_items table. Skipping migration."
            )
            return

        print("Starting migration: Add color column and update unique constraint...")

        # SQLite doesn't support ALTER TABLE to drop constraints directly
        # We need to recreate the table

        # Step 1: Create new table with updated schema
        print("Creating new table with updated schema...")
        conn.execute(
            text("""
            CREATE TABLE rfq_items_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uploaded_file_id INTEGER,
                rfq_id VARCHAR(100) NOT NULL,
                raw_text TEXT NOT NULL,
                clean_text TEXT,
                color VARCHAR(200),
                qty VARCHAR(50),
                uom VARCHAR(50),
                source VARCHAR(100) NOT NULL,
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY(uploaded_file_id) REFERENCES uploaded_files (id),
                UNIQUE (rfq_id, clean_text, color)
            )
        """)
        )

        # Step 2: Copy data from old table to new table
        print("Copying data from old table to new table...")
        conn.execute(
            text("""
            INSERT INTO rfq_items_new (
                id, uploaded_file_id, rfq_id, raw_text, clean_text,
                qty, uom, source, created_at, updated_at
            )
            SELECT
                id, uploaded_file_id, rfq_id, raw_text, clean_text,
                qty, uom, source, created_at, updated_at
            FROM rfq_items
        """)
        )

        # Step 3: Drop old table
        print("Dropping old table...")
        conn.execute(text("DROP TABLE rfq_items"))

        # Step 4: Rename new table to original name
        print("Renaming new table to rfq_items...")
        conn.execute(text("ALTER TABLE rfq_items_new RENAME TO rfq_items"))

        # Step 5: Recreate indexes
        print("Recreating indexes...")
        conn.execute(
            text(
                "CREATE INDEX ix_rfq_items_uploaded_file_id ON rfq_items (uploaded_file_id)"
            )
        )
        conn.execute(text("CREATE INDEX ix_rfq_items_rfq_id ON rfq_items (rfq_id)"))
        conn.execute(
            text("CREATE INDEX ix_rfq_items_clean_text ON rfq_items (clean_text)")
        )

        conn.commit()
        print("Migration completed successfully!")
        print("- Added column 'color' to rfq_items")
        print("- Updated unique constraint to (rfq_id, clean_text, color)")


if __name__ == "__main__":
    migrate()
