"""
Migration script to add updated_at column to rfq_items table

Run this script to update the database schema:
    python migrations/add_updated_at_to_rfq_items.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def migrate():
    """Add updated_at column to rfq_items table"""
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

        # Check if column already exists
        result = conn.execute(
            text("""
            SELECT COUNT(*) as count
            FROM pragma_table_info('rfq_items')
            WHERE name = 'updated_at'
        """)
        )
        row = result.fetchone()

        if row and row[0] > 0:
            print(
                "Column 'updated_at' already exists in rfq_items table. Skipping migration."
            )
            return

        # Add updated_at column
        print("Adding 'updated_at' column to rfq_items table...")
        conn.execute(
            text("""
            ALTER TABLE rfq_items
            ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        """)
        )

        # Update existing rows to have updated_at = created_at
        print("Updating existing rows...")
        conn.execute(
            text("""
            UPDATE rfq_items
            SET updated_at = created_at
            WHERE updated_at IS NULL
        """)
        )

        conn.commit()
        print("Migration completed successfully!")


if __name__ == "__main__":
    migrate()
