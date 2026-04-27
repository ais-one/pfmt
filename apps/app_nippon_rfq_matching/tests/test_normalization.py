"""
Test script for OpenAI Normalization Service

This script tests the normalization service with sample RFQ descriptions.
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
    openai_normalization_service,
)

# Sample RFQ descriptions for testing (original test data)
ORIGINAL_SAMPLE_RFQ_DESCRIPTIONS = [
    "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L",
    "环氧红底漆 JOTAMASTIC 80 RED A 16L",
    "环氧灰底漆 JOTAFIX EPOXY PRIMER GREY A 15L",
    "环氧绿面漆 PENGUARD FC STD 257 GREEN 16L",
    "环氧蓝绿面漆 PENGUARD FC STD 5100 NEU BLUE GREEN， 16L",
    "环氧黄面漆 PENGUARD FC STD 258 YELLOW A 16L",
    "环氧灰面漆 PENGUARD FC GREY 16L",
    "环氧橙面漆 PENGUARD FC ORANGE 16L",
    "信号红 PILOT II RAL3000 5L",
    "稀释剂THINNER 17号",
    "稀释剂THINNER 2号",
    "环氧456固化剂",
    "环氧350固化剂",
    "环氧200固化剂",
    "稀释剂91-92",
    "醇酸灰面漆 PILOT II STD 4218 GREY 20L",
    "醇酸黄面漆 PILOT II STD BASE 5 YELLOW STD 8130 20L",
    "醇酸黑面漆 PILOT II BLACK 20L",
    "醇酸白面漆 PILOT II WHITE 20L",
    "醇酸苹果绿 PILOT II BASE 2 GREEN STD 7132 20L",
    "高温银粉漆 SOLVAVITT ALUMINIUM 5L",
]

# Sample RFQ descriptions for testing (problematic cases from user)
PROBLEMATIC_SAMPLE_RFQ_DESCRIPTIONS = [
    "[LI] Nippon U-Marine Finish 000 White Base",
    "[LI] Nippon U-Marine Finish Gray 060 Base",
    "[LI] Nippon O-Marine Finish 442 Green",
    "[LI] NIPPON O-MARINE FINISH 355 SIGNAL YELLOW -",
    "[LI] Nippon Marine Thinner 700 POLYURE MIGHTYLAC",
    "[LI] Nippon Marine Thinner 100",
    "[LI] Nippon O-Marine Finish 060 Gray",
    "[LI] Nippon O-Marine Finish 000 White FAE095 (N9.5)",
    "[LI] NIPPON PYLOX SPRAY PAINT BLACK",
    "[LI] Tetzsol 500 Eco Silver FHE320",
    "[LI] Nippon O-Marine Finish 632 Green (7.5 BG 7/2) ( Machinery Green )",
]

# Test cases for brand prefix normalization
BRAND_PREFIX_TEST_CASES = [
    # (RFQ Input, Expected Normalized Output, Test Description)
    ("NIPPON NEO GUARD", "NEO GUARD", "Brand prefix stripping - NIPPON prefix"),
    (
        "NIPPON U-MARINE FINISH 000 WHITE",
        "U-MARINE FINISH",
        "Brand prefix stripping with color",
    ),
    (
        "NP MARINE THINNER 700",
        "MARINE THINNER 700",
        "Brand prefix stripping - NP prefix",
    ),
    (
        "NIPPON PAINT MARINE A-MARINE",
        "A-MARINE",
        "Brand prefix stripping - NIPPON PAINT prefix",
    ),
    (
        "NP U-MARINE FINISH",
        "U-MARINE FINISH",
        "Brand prefix stripping - NP prefix alone",
    ),
    ("TETZSOL 500 ECO", "TETZSOL 500 ECO", "No brand prefix - unchanged"),
    (
        "PENGUARD FC STD 2244 REDBROEN",
        "PENGUARD",
        "Competitor product - no prefix stripping",
    ),
]


def test_brand_prefix_stripping():
    """Test the brand prefix stripping functionality."""
    print("=" * 80)
    print("Brand Prefix Stripping Test")
    print("=" * 80)
    print()

    # Import the helper function
    from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
        _strip_brand_prefix,
    )

    print("Testing _strip_brand_prefix() function:")
    print("-" * 80)

    # Test the helper function directly
    for rfq_input, expected_output, description in BRAND_PREFIX_TEST_CASES:
        actual_output = _strip_brand_prefix(rfq_input)
        status = "✓ PASS" if actual_output == expected_output else "✗ FAIL"

        print(f"{status} {description}")
        print(f"  Input:    '{rfq_input}'")
        print(f"  Expected: '{expected_output}'")
        print(f"  Actual:   '{actual_output}'")
        print()

    print("Testing brand prefix stripping in full normalization:")
    print("-" * 80)

    # Check if service is enabled
    if not openai_normalization_service.enabled:
        print("WARNING: OpenAI service disabled, skipping full normalization test")
        print(
            "Please set OPENAI_API_KEY in your .env file to test actual normalization."
        )
        return

    # Get database session
    db = next(get_db())

    try:
        # Test only the brand prefix cases
        print("Testing batch normalization with brand prefix cases...")
        result = openai_normalization_service.normalize_rfq_items(
            rfq_descriptions=[case[0] for case in BRAND_PREFIX_TEST_CASES], db=db
        )

        print("\nResults:")
        print("-" * 80)
        for i, (case, (before, after)) in enumerate(
            zip(BRAND_PREFIX_TEST_CASES, zip(result["before"], result["after"])), 1
        ):
            expected = case[1]
            status = "✓ MATCHED" if after == expected else "✗ FAIL/NO MATCH"
            print(f"{i}. [{status}] {case[2]}")
            print(f"   Input:    '{case[0]}'")
            print(f"   Expected: '{expected}'")
            print(f"   Actual:   '{after}'")
            print()

        # Statistics
        matched_count = sum(
            1
            for i, case in enumerate(BRAND_PREFIX_TEST_CASES)
            if result["after"][i] == case[1]
        )
        total_count = len(BRAND_PREFIX_TEST_CASES)
        match_rate = matched_count / total_count if total_count > 0 else 0

        print("=" * 80)
        print("Brand Prefix Normalization Statistics:")
        print(f"  - Total Items: {total_count}")
        print(f"  - Correctly Normalized: {matched_count}")
        print(f"  - Success Rate: {match_rate:.1%}")
        print("=" * 80)

    except Exception as e:
        print(f"ERROR in normalization test: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


# Use both sample sets
SAMPLE_RFQ_DESCRIPTIONS = ORIGINAL_SAMPLE_RFQ_DESCRIPTIONS


def test_normalization():
    """Test the normalization service with sample data."""
    print("=" * 80)
    print("OpenAI Normalization Service Test")
    print("=" * 80)
    print()

    # Check if service is enabled
    if not openai_normalization_service.enabled:
        print("ERROR: OpenAI normalization service is disabled!")
        print("Please set OPENAI_API_KEY in your .env file.")
        return

    print("Service Status:")
    print(f"  - Enabled: {openai_normalization_service.enabled}")
    print(f"  - Model: {openai_normalization_service.model}")
    print(f"  - Temperature: {openai_normalization_service.temperature}")
    print(f"  - Max Tokens: {openai_normalization_service.max_tokens}")
    print()

    # Get database session
    db = next(get_db())

    try:
        # Test batch normalization
        print("Testing batch normalization...")
        print("-" * 80)

        result = openai_normalization_service.normalize_rfq_items(
            rfq_descriptions=SAMPLE_RFQ_DESCRIPTIONS, db=db
        )

        # Display results
        print(f"\nResults: {result.get('usage', {})}")
        print("-" * 80)

        for i, (before, after) in enumerate(zip(result["before"], result["after"]), 1):
            status = "✓ MATCHED" if after else "✗ NO MATCH"
            print(f"{i:2}. [{status}]")
            print(f"    Before: {before}")
            if after:
                print(f"    After:  {after}")
            else:
                print("    After:  (null)")
            print()

        # Statistics
        matched_count = sum(1 for item in result["after"] if item is not None)
        total_count = len(result["after"])
        match_rate = matched_count / total_count if total_count > 0 else 0

        print("=" * 80)
        print("Statistics:")
        print(f"  - Total Items: {total_count}")
        print(f"  - Matched: {matched_count}")
        print(f"  - Unmatched: {total_count - matched_count}")
        print(f"  - Match Rate: {match_rate:.1%}")
        print("=" * 80)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


def test_separate_normalization():
    """Test the separate normalization methods (product names and colors)."""
    print("=" * 80)
    print("OpenAI Separate Normalization Test (Product Names + Colors)")
    print("=" * 80)
    print()

    # Check if service is enabled
    if not openai_normalization_service.enabled:
        print("ERROR: OpenAI normalization service is disabled!")
        print("Please set OPENAI_API_KEY in your .env file.")
        return

    print("Service Status:")
    print(f"  - Enabled: {openai_normalization_service.enabled}")
    print(f"  - Model: {openai_normalization_service.model}")
    print(f"  - Temperature: {openai_normalization_service.temperature}")
    print(f"  - Max Tokens: {openai_normalization_service.max_tokens}")
    print()

    # Get database session
    db = next(get_db())

    try:
        # Test with problematic samples
        print("Testing with PROBLEMATIC samples (model numbers preserved)...")
        print("-" * 80)

        # Step 1: Normalize product names
        print("\n[STEP 1] Normalizing Product Names (preserving model numbers)...")
        print("-" * 80)

        product_name_result = openai_normalization_service.normalize_product_names_only(
            rfq_descriptions=PROBLEMATIC_SAMPLE_RFQ_DESCRIPTIONS, db=db
        )

        for i, (before, after, prod_type) in enumerate(
            zip(
                product_name_result["before"],
                product_name_result["after"],
                product_name_result["types"],
            ),
            1,
        ):
            status = "✓ MATCHED" if after else "✗ NO MATCH"
            type_display = f" [{prod_type.upper()}]" if prod_type else ""
            print(f"{i:2}. [{status}{type_display}]")
            print(f"    Before: {before}")
            if after:
                print(f"    After:  {after}")
            else:
                print("    After:  (null)")
            print()

        # Step 2: Extract colors
        print("\n[STEP 2] Extracting Colors (separate process)...")
        print("-" * 80)

        color_result = openai_normalization_service.extract_colors_only(
            rfq_descriptions=PROBLEMATIC_SAMPLE_RFQ_DESCRIPTIONS, db=db
        )

        for i, (before, color) in enumerate(
            zip(color_result["before"], color_result["colors"]), 1
        ):
            status = "✓ COLOR" if color else "✗ NO COLOR"
            print(f"{i:2}. [{status}]")
            print(f"    Before: {before}")
            print(f"    Color:  {color}")
            print()

        # Step 3: Combined results
        print("\n[STEP 3] Combined Results (Product Name + Color)...")
        print("-" * 80)

        for i, (before, after, color, prod_type) in enumerate(
            zip(
                product_name_result["before"],
                product_name_result["after"],
                color_result["colors"],
                product_name_result["types"],
            ),
            1,
        ):
            if after and prod_type:
                type_display = prod_type.upper()
                if prod_type == "nippon":
                    type_display = "🔵 NIPPON"
                elif prod_type == "competitor":
                    type_display = "🔴 COMPETITOR"
                status = f"✓ MATCHED [{type_display}]"
            elif after:
                status = "⚠ MATCHED [UNKNOWN TYPE]"
            else:
                status = "✗ NO MATCH"

            print(f"{i:2}. [{status}]")
            print(f"    Before: {before}")
            if after:
                print(f"    After:  {after}")
                if color:
                    print(f"    Color:  {color}")
                if prod_type:
                    print(f"    Type:   {prod_type}")
            else:
                print("    After:  (null)")
            print()

        # Statistics
        matched_count = sum(
            1 for item in product_name_result["after"] if item is not None
        )
        total_count = len(product_name_result["after"])
        match_rate = matched_count / total_count if total_count > 0 else 0
        nippon_count = sum(1 for t in product_name_result["types"] if t == "nippon")
        competitor_count = sum(
            1 for t in product_name_result["types"] if t == "competitor"
        )
        color_count = sum(1 for c in color_result["colors"] if c is not None)

        print("=" * 80)
        print("Statistics:")
        print(f"  - Total Items: {total_count}")
        print(f"  - Products Matched: {matched_count}")
        print(f"  - Nippon Products: {nippon_count}")
        print(f"  - Competitor Products: {competitor_count}")
        print(f"  - Colors Extracted: {color_count}")
        print(f"  - Product Match Rate: {match_rate:.1%}")
        print(f"  - Color Extraction Rate: {color_count / total_count:.1%}")
        print("=" * 80)

        # Token usage
        total_tokens = (
            product_name_result["usage"]["total_tokens"]
            + color_result["usage"]["total_tokens"]
        )
        print("\nToken Usage:")
        print(
            f"  - Product Names: {product_name_result['usage']['total_tokens']} tokens"
        )
        print(f"  - Colors: {color_result['usage']['total_tokens']} tokens")
        print(f"  - Total: {total_tokens} tokens")
        print(f"  - Estimated Cost: ${total_tokens / 1000000 * 0.15:.4f}")
        print("=" * 80)

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    # Test both the original and new separate normalization
    print("\n\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print(
        "║" + "  RUNNING COMBINED NORMALIZATION TEST (Original Method)".center(78) + "║"
    )
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    test_normalization()

    print("\n\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  RUNNING SEPARATE NORMALIZATION TEST (New Method)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    test_separate_normalization()

    print("\n\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  RUNNING BRAND PREFIX STRIPPING TEST".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    test_brand_prefix_stripping()
