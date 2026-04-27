"""
Test script for Competitor Color Mapping

This script tests the new competitor color mapping methods without requiring OpenAI API.
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.app_nippon_rfq_matching.app.core.database import _ensure_db_initialized
from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
    OpenAINormalizationService,
)

# Ensure database is initialized
_ensure_db_initialized()

# Import SessionLocal after initialization
from apps.app_nippon_rfq_matching.app.core.database import SessionLocal  # noqa: E402


def test_extract_color_code_from_text():
    """Test the _extract_color_code_from_text method."""
    print("=" * 80)
    print("Testing _extract_color_code_from_text method")
    print("=" * 80)

    service = OpenAINormalizationService()

    test_cases = [
        ("JOTUN GALVOSIL 157 JOTUN COLOR 12345", "12345"),
        ("INTERNATIONAL PAINT NCS-001", "NCS-001"),
        ("JOTUN 12345 RED", "12345"),
        ("SIGMA COLOR ABC-123", "ABC-123"),
        ("HEMPEL 9876 BLUE", "9876"),
        ("PPG COLOR 456", "456"),
        ("No color code here", None),
        ("", None),
    ]

    all_passed = True
    for text, expected in test_cases:
        result = service._extract_color_code_from_text(text)
        passed = result == expected
        all_passed = all_passed and passed
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: '{text}' -> '{result}' (expected: '{expected}')")

    print("\n" + ("All tests passed!" if all_passed else "Some tests failed!"))
    print("=" * 80)
    return all_passed


def test_get_competitor_brand():
    """Test the _get_competitor_brand method."""
    print("\n" + "=" * 80)
    print("Testing _get_competitor_brand method")
    print("=" * 80)

    service = OpenAINormalizationService()
    db = SessionLocal()

    try:
        # Get all competitor products from database
        from apps.app_nippon_rfq_matching.app.models.competitor import CompetitorProduct

        competitor_products = db.query(CompetitorProduct).limit(5).all()

        if not competitor_products:
            print("No competitor products found in database!")
            return False

        all_passed = True
        for product in competitor_products:
            brand = service._get_competitor_brand(product.name, db)
            expected_brand = product.brand.name.upper() if product.brand else None
            passed = brand == expected_brand
            all_passed = all_passed and passed
            status = "✓ PASS" if passed else "✗ FAIL"
            print(
                f"{status}: '{product.name}' -> '{brand}' (expected: '{expected_brand}')"
            )

        print("\n" + ("All tests passed!" if all_passed else "Some tests failed!"))
        print("=" * 80)
        return all_passed

    finally:
        db.close()


def test_get_nippon_equivalent():
    """Test the _get_nippon_equivalent method."""
    print("\n" + "=" * 80)
    print("Testing _get_nippon_equivalent method")
    print("=" * 80)

    service = OpenAINormalizationService()
    db = SessionLocal()

    try:
        # Get competitor products that have equivalents
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorProduct,
            ProductEquivalent,
        )

        # Find a competitor product with an equivalent
        competitor_with_equivalent = (
            db.query(CompetitorProduct)
            .join(
                ProductEquivalent,
                CompetitorProduct.id == ProductEquivalent.competitor_product_id,
            )
            .first()
        )

        if not competitor_with_equivalent:
            print("No competitor products with equivalents found in database!")
            return False

        all_passed = True
        # Test a few competitor products
        competitor_products = db.query(CompetitorProduct).limit(5).all()

        for product in competitor_products:
            nippon_equiv = service._get_nippon_equivalent(product.name, db)

            # Get expected equivalent from database
            equivalent = (
                db.query(ProductEquivalent)
                .filter(ProductEquivalent.competitor_product_id == product.id)
                .first()
            )

            expected = equivalent.nippon_product_name if equivalent else None
            passed = nippon_equiv == expected
            all_passed = all_passed and passed
            status = "✓ PASS" if passed else "✗ FAIL"
            print(
                f"{status}: '{product.name}' -> '{nippon_equiv}' (expected: '{expected}')"
            )

        print("\n" + ("All tests passed!" if all_passed else "Some tests failed!"))
        print("=" * 80)
        return all_passed

    finally:
        db.close()


def test_get_npms_color_code():
    """Test the _get_npms_color_code method."""
    print("\n" + "=" * 80)
    print("Testing _get_npms_color_code method")
    print("=" * 80)

    service = OpenAINormalizationService()
    db = SessionLocal()

    try:
        # Get color mappings from database
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorColorComparison,
        )

        color_mappings = db.query(CompetitorColorComparison).limit(5).all()

        if not color_mappings:
            print("No color mappings found in database!")
            return False

        all_passed = True
        for mapping in color_mappings:
            npms_code = service._get_npms_color_code(
                mapping.source_brand, mapping.source_code, db
            )

            expected = mapping.npms_code
            passed = npms_code == expected
            all_passed = all_passed and passed
            status = "✓ PASS" if passed else "✗ FAIL"
            print(
                f"{status}: {mapping.source_brand} {mapping.source_code} -> '{npms_code}' (expected: '{expected}')"
            )

        print("\n" + ("All tests passed!" if all_passed else "Some tests failed!"))
        print("=" * 80)
        return all_passed

    finally:
        db.close()


def test_integration():
    """Test the integration of all competitor color mapping methods."""
    print("\n" + "=" * 80)
    print("Testing Integration: Competitor Product Color Matching Flow")
    print("=" * 80)

    service = OpenAINormalizationService()
    db = SessionLocal()

    try:
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorColorComparison,
            CompetitorProduct,
            ProductEquivalent,
        )

        # Find a competitor product with both equivalent and color mapping
        competitor_product = (
            db.query(CompetitorProduct)
            .join(
                ProductEquivalent,
                CompetitorProduct.id == ProductEquivalent.competitor_product_id,
            )
            .first()
        )

        if not competitor_product:
            print("No competitor product with equivalent found in database!")
            return False

        print(f"\nTest Case: '{competitor_product.name}'")

        # Step 1: Get brand
        brand = service._get_competitor_brand(competitor_product.name, db)
        print(f"  1. Brand: {brand}")

        # Step 2: Get Nippon equivalent
        nippon_equiv = service._get_nippon_equivalent(competitor_product.name, db)
        print(f"  2. Nippon Equivalent: {nippon_equiv}")

        # Step 3: Get available colors for Nippon equivalent
        if nippon_equiv:
            colors = service._get_colors_for_product(nippon_equiv, db)
            print(
                f"  3. Available Colors: {colors[:5]}{'...' if len(colors) > 5 else ''}"
            )

        # Step 4: Test color code extraction (with sample text)
        sample_text = f"{competitor_product.name} JOTUN COLOR 12345"
        color_code = service._extract_color_code_from_text(sample_text)
        print(f"  4. Extracted Color Code from '{sample_text}': {color_code}")

        # Step 5: Test NPMS color mapping (if there are mappings in DB)
        color_mapping = db.query(CompetitorColorComparison).first()
        if color_mapping and brand:
            npms_code = service._get_npms_color_code(
                color_mapping.source_brand, color_mapping.source_code, db
            )
            print(
                f"  5. NPMS Color Mapping ({color_mapping.source_brand} {color_mapping.source_code}): {npms_code}"
            )

        print("\n✓ Integration test completed successfully!")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\n✗ Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        db.close()


if __name__ == "__main__":
    print("\n" + "╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  COMPETITOR COLOR MAPPING UNIT TESTS".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝\n")

    results = []
    results.append(
        ("_extract_color_code_from_text", test_extract_color_code_from_text())
    )
    results.append(("_get_competitor_brand", test_get_competitor_brand()))
    results.append(("_get_nippon_equivalent", test_get_nippon_equivalent()))
    results.append(("_get_npms_color_code", test_get_npms_color_code()))
    results.append(("Integration", test_integration()))

    # Summary
    print("\n" + "╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  TEST SUMMARY".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")

    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")

    all_passed = all(result[1] for result in results)
    print("\n" + ("=" * 80))
    print("ALL TESTS PASSED!" if all_passed else "SOME TESTS FAILED!")
    print("=" * 80)

    sys.exit(0 if all_passed else 1)
