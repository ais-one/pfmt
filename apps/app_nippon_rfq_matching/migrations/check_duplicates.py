"""
Script to check for duplicate RFQ items in the database

Usage:
    python migrations/check_duplicates.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def check_duplicates():
    """Check for duplicate RFQ items"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        print("Checking for duplicate RFQ items...")
        print("=" * 60)

        # Find duplicates
        duplicate_sql = text("""
            SELECT rfq_id, raw_text, COUNT(*) as count
            FROM rfq_items
            GROUP BY rfq_id, raw_text
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)

        result = conn.execute(duplicate_sql)
        duplicates = result.fetchall()

        if duplicates:
            print(f"\nFound {len(duplicates)} duplicate items:\n")

            for i, (rfq_id, raw_text, count) in enumerate(duplicates, 1):
                print(f"{i}. RFQ ID: {rfq_id}")
                print(
                    f"   Raw Text: {raw_text[:100]}{'...' if len(raw_text) > 100 else ''}"
                )
                print(f"   Count: {count}")
                print()

            # Show details of first duplicate
            if duplicates:
                first_rfq_id, first_raw_text, _ = duplicates[0]

                detail_sql = text("""
                    SELECT id, uploaded_file_id, qty, uom, source, created_at
                    FROM rfq_items
                    WHERE rfq_id = :rfq_id AND raw_text = :raw_text
                    ORDER BY id
                """)

                detail_result = conn.execute(
                    detail_sql, {"rfq_id": first_rfq_id, "raw_text": first_raw_text}
                )

                print(f"Details of first duplicate (RFQ: {first_rfq_id}):")
                print("-" * 60)
                for row in detail_result:
                    print(
                        f"  ID: {row[0]}, File ID: {row[1]}, Qty: {row[2]}, UoM: {row[3]}, Source: {row[4]}, "
                        f"Created: {row[5]}"
                    )
        else:
            print("No duplicate RFQ items found!")

        # Summary
        print("\n" + "=" * 60)
        print("Summary:")

        # Total items
        total_sql = text("SELECT COUNT(*) FROM rfq_items")
        total_result = conn.execute(total_sql)
        total_count = total_result.scalar()

        # Unique items
        unique_sql = text(
            "SELECT COUNT(DISTINCT rfq_id || '|' || raw_text) FROM rfq_items"
        )
        unique_result = conn.execute(unique_sql)
        unique_count = unique_result.scalar()

        # RFQ count
        rfq_sql = text("SELECT COUNT(DISTINCT rfq_id) FROM rfq_items")
        rfq_result = conn.execute(rfq_sql)
        rfq_count = rfq_result.scalar()

        print(f"  Total RFQ Items: {total_count}")
        print(f"  Unique Items: {unique_count}")
        print(f"  Duplicate Items: {total_count - unique_count}")
        print(f"  Number of RFQs: {rfq_count}")


if __name__ == "__main__":
    check_duplicates()
