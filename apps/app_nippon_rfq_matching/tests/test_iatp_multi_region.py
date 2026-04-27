#!/usr/bin/env python3
"""
Test script for IATP multi-region parsing functionality
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))


from apps.app_nippon_rfq_matching.app.core.database import get_db  # noqa: E402
from apps.app_nippon_rfq_matching.app.utils.parsers import (  # noqa: E402
    insert_product_prices_to_database,
    insert_regions_to_database,
    parse_iatp_excel_with_multi_region,
    process_iatp_excel_with_database_insertion,
)


def test_multi_region_parsing():
    """Test the multi-region parsing functionality"""
    # Test file path

    # Check if we have a test Excel file
    excel_file = "/home/arifrahmanrhm/rfq-parser-matching-be-fastapi/202508 IATP AF&GEN - no pass iatp2508.xlsx"

    if not os.path.exists(excel_file):
        print(f"Test Excel file not found at: {excel_file}")
        return

    print(f"Testing IATP multi-region parsing with file: {excel_file}")

    try:
        # Test parsing
        print("\n1. Testing parsing...")
        parsed_data = parse_iatp_excel_with_multi_region(excel_file)

        print(f"   - Products found: {len(parsed_data['products'])}")
        print(f"   - Regions detected: {parsed_data['regions']}")
        print(
            f"   - Products with pricing: {parsed_data['summary']['products_with_pricing']}"
        )

        # Test database insertion
        print("\n2. Testing database insertion...")

        # Initialize database
        db = next(get_db())

        # Test region insertion
        print("   a) Inserting regions...")
        region_map = insert_regions_to_database(parsed_data["regions"], db)
        print(f"      - Regions inserted: {len(region_map)}")
        print(f"      - Region map: {region_map}")

        # Test product price insertion
        print("   b) Inserting product prices...")
        pricing_count = insert_product_prices_to_database(
            parsed_data["products"], region_map, db
        )
        print(f"      - Pricing records inserted: {pricing_count}")

        # Test complete pipeline
        print("\n3. Testing complete pipeline...")
        db_results = process_iatp_excel_with_database_insertion(excel_file, db)
        print(f"   - Status: {db_results['status']}")
        print(f"   - Products inserted: {db_results['products_inserted']}")
        print(f"   - Regions inserted: {db_results['regions_inserted']}")
        print(
            f"   - Pricing records inserted: {db_results['pricing_records_inserted']}"
        )

        db.close()

        print("\n✅ All tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_multi_region_parsing()
