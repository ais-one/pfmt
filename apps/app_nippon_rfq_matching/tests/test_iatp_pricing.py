#!/usr/bin/env python3
"""
Test script for IATP Excel parsing with pricing support

This script demonstrates how to use the new pricing functionality.
"""

import pandas as pd

from apps.app_nippon_rfq_matching.app.utils.parsers import parse_iatp_excel_with_pricing


def test_parsing():
    """
    Test the IATP parsing with pricing functionality
    """
    print("Testing IATP Excel parsing with pricing...")

    # Create a sample Excel file for testing
    # This simulates the IATP format with pricing columns

    # Sample data structure:
    # Row 0: Empty
    # Row 1: Empty
    # Row 2: Empty
    # Row 3: Regions: NPMC, NPMS, NPMK
    # Row 4: Field types: IATP, IATP, IATP
    # Row 5: Pack size, UoM, Price
    # Row 6: Product code, Product name, Color
    # Row 7: Data rows

    sample_data = {
        # Column 0 (Product codes) — rows 8..10 hold data to align with region data rows
        0: [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "AF9AL11X001",
            "AF9AL11X002",
            "AF9AL11X003",
        ],
        # Column 1 (Product names)
        1: [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "A-LF-SEA 150",
            "A-LF-SEA 600",
            "RUST REMOVER",
        ],
        # Column 2 (Color)
        2: ["", "", "", "", "", "", "", "", "", "", ""],
        # Column 3 (NPMC region)
        3: ["", "", "", "NPMC", "IATP", "Pack size", "UoM", "Price", 20, 20, 16],
        # Column 4 (NPMS region)
        4: ["", "", "", "NPMS", "IATP", "Pack size", "UoM", "Price", 20, 20, 16],
        # Column 5 (NPMK region)
        5: ["", "", "", "NPMK", "IATP", "Pack size", "UoM", "Price", 20, 20, 16],
        # Column 6 (NPMEU region)
        6: ["", "", "", "NPMEU", "IATP", "Pack size", "UoM", "Price", 20, 20, 16],
    }

    # Create DataFrame
    df = pd.DataFrame(sample_data)

    # Save to Excel
    output_file = "test_iatp_pricing.xlsx"
    df.to_excel(output_file, index=False, header=False)

    print(f"Created test file: {output_file}")

    # Test parsing
    try:
        result = parse_iatp_excel_with_pricing(output_file)

        print("\n=== Parse Results ===")
        print(f"Products found: {len(result['products'])}")
        print(f"Pricing records found: {len(result['pricing'])}")

        print("\n=== Products ===")
        for product in result["products"]:
            print(f"  - {product['pmc']}: {product['product_name']}")

        print("\n=== Pricing ===")
        for pricing in result["pricing"]:
            print(
                f"  - Region: {pricing['region']}, Size: {pricing['size']}, "
                f"UoM: {pricing['uom']}, Price: {pricing['price']}, "
                f"Raw: {pricing['price_raw']}"
            )

        return True

    except Exception as e:
        print(f"Error during parsing: {e}")
        return False
    finally:
        # Clean up test file
        import os

        if os.path.exists(output_file):
            os.remove(output_file)


if __name__ == "__main__":
    test_parsing()
