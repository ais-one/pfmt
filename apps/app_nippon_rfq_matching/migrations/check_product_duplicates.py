"""
Script to check for duplicate products in the product_master table

Usage:
    python migrations/check_product_duplicates.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def check_duplicates():
    """Check for duplicate products"""

    # Connect to database
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        print("Checking for duplicate products...")
        print("=" * 80)

        # Find duplicates
        duplicate_sql = text("""
            SELECT sheet_type, pmc, product_name, color, COUNT(*) as count
            FROM product_master
            GROUP BY sheet_type, pmc, product_name, color
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)

        result = conn.execute(duplicate_sql)
        duplicates = result.fetchall()

        if duplicates:
            print(f"\nFound {len(duplicates)} duplicate products:\n")

            for i, (sheet_type, pmc, product_name, color, count) in enumerate(
                duplicates, 1
            ):
                color_display = color if color else "NULL"
                print(f"{i}. Sheet Type: {sheet_type}")
                print(f"   PMC: {pmc}")
                print(
                    f"   Product Name: {product_name[:80]}{'...' if len(product_name) > 80 else ''}"
                )
                print(f"   Color: {color_display}")
                print(f"   Count: {count}")
                print()

            # Show details of first duplicate
            if duplicates:
                first_sheet_type, first_pmc, first_product_name, first_color, _ = (
                    duplicates[0]
                )

                detail_sql = text("""
                    SELECT id, uploaded_file_id, sheet_name, row_excel, clean_product_name, created_at
                    FROM product_master
                    WHERE sheet_type = :sheet_type AND pmc = :pmc AND product_name = :product_name AND color = :color
                    ORDER BY id
                """)

                detail_result = conn.execute(
                    detail_sql,
                    {
                        "sheet_type": first_sheet_type,
                        "pmc": first_pmc,
                        "product_name": first_product_name,
                        "color": first_color,
                    },
                )

                print(f"Details of first duplicate (PMC: {first_pmc}):")
                print("-" * 80)
                for row in detail_result:
                    print(
                        f"  ID: {row[0]}, File ID: {row[1]}, Sheet: {row[2]}, Row: {row[3]}, Created: {row[5]}"
                    )
        else:
            print("No duplicate products found!")

        # Summary
        print("\n" + "=" * 80)
        print("Summary:")

        # Total products
        total_sql = text("SELECT COUNT(*) FROM product_master")
        total_result = conn.execute(total_sql)
        total_count = total_result.scalar()

        # Unique products
        unique_sql = text("""
            SELECT COUNT(DISTINCT sheet_type || '|' || pmc || '|' || product_name || '|' || COALESCE(color, ''))
            FROM product_master
        """)
        unique_result = conn.execute(unique_sql)
        unique_count = unique_result.scalar()

        # Sheet type breakdown
        sheet_type_sql = text("""
            SELECT sheet_type, COUNT(*) as count
            FROM product_master
            GROUP BY sheet_type
            ORDER BY count DESC
        """)
        sheet_type_result = conn.execute(sheet_type_sql)
        sheet_types = sheet_type_result.fetchall()

        print(f"  Total Products: {total_count}")
        print(f"  Unique Products: {unique_count}")
        print(f"  Duplicate Products: {total_count - unique_count}")
        print("\n  Breakdown by Sheet Type:")
        for sheet_type, count in sheet_types:
            print(f"    - {sheet_type}: {count}")


if __name__ == "__main__":
    check_duplicates()
