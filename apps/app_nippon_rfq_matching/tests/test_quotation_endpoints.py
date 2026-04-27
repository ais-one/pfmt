#!/usr/bin/env python3
"""
Test script for quotation endpoints with RFQ ID support
"""

import requests

BASE_URL = "http://103.103.22.118:8000/api/v1/quotation-pdf"


def test_quotation_with_rfq_id():
    """Test quotation generation with RFQ ID"""

    print("=== Testing Quotation Endpoints with RFQ ID ===\n")

    # Test data with RFQ ID
    test_rfq_data = {"rfq_id": "TEST-RFQ-001"}

    # Test 1: Generate preview with RFQ ID (POST)
    print("1. Testing preview with RFQ ID (POST)...")
    try:
        response = requests.post(
            f"{BASE_URL}/preview",
            headers={"Content-Type": "application/json"},
            json=test_rfq_data,
        )
        if response.status_code == 200:
            result = response.json()
            print("✓ Preview generated successfully")
            print(f"  Quote Number: {result.get('quote_number')}")
            print(f"  RFQ ID: {result.get('rfq_id')}")
            print("✓ HTML preview available")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Request failed: {e}")

    print()

    # Test 2: Generate preview with RFQ ID (GET)
    print("2. Testing preview with RFQ ID (GET)...")
    try:
        response = requests.get(f"{BASE_URL}/preview?rfq_id=TEST-RFQ-001")
        if response.status_code == 200:
            result = response.json()
            print("✓ Preview generated successfully")
            print(f"  Quote Number: {result.get('quote_number')}")
            print(f"  RFQ ID: {result.get('rfq_id')}")
            print("✓ HTML preview available")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Request failed: {e}")

    print()

    # Test 3: Generate PDF with RFQ ID (POST)
    print("3. Testing PDF generation with RFQ ID (POST)...")
    try:
        response = requests.post(
            f"{BASE_URL}/generate",
            headers={"Content-Type": "application/json"},
            json=test_rfq_data,
        )
        if response.status_code == 200:
            result = response.json()
            print("✓ PDF generation initiated")
            print(f"  Success: {result.get('success')}")
            print(f"  Job ID: {result.get('job_id')}")
            print(f"  RFQ ID: {result.get('rfq_id')}")
            if result.get("pdf_path"):
                print("✓ PDF file generated successfully")
            elif result.get("html"):
                print("⚠ WeasyPrint not installed, returning HTML preview")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Request failed: {e}")

    print()


def test_traditional_quotation():
    """Test traditional quotation generation with full data"""

    print("=== Testing Traditional Quotation Endpoints ===\n")

    # Test data with full details
    test_full_data = {
        "client_company": "ABC Marine Services Ltd",
        "client_address_1": "123 Harbor Road, Singapore 123456",
        "client_address_2": "Marine Industrial Park",
        "contact_name": "John Smith",
        "client_phone": "+65 1234 5678",
        "client_email": "john.smith@abcmarine.com",
        "items": [
            {
                "item_code": "A-611",
                "description": "Self polishing copolymer type antifouling paint",
                "color": "Red",
                "unit": "Litre",
                "quantity": 200,
                "unit_price": 45.50,
            },
            {
                "item_code": "B-332",
                "description": "Epoxy primer for steel surfaces",
                "color": "Grey",
                "unit": "Litre",
                "quantity": 150,
                "unit_price": 32.00,
            },
        ],
        "quotation_number": "Q-2024-002",
        "sales_representative": "Jane Doe",
        "validity_days": 30,
    }

    # Test 4: Generate preview with full data
    print("4. Testing preview with full data (POST)...")
    try:
        response = requests.post(
            f"{BASE_URL}/preview",
            headers={"Content-Type": "application/json"},
            json=test_full_data,
        )
        if response.status_code == 200:
            result = response.json()
            print("✓ Preview generated successfully")
            print(f"  Quote Number: {result.get('quote_number')}")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Request failed: {e}")

    print()

    # Test 5: Generate PDF with full data
    print("5. Testing PDF generation with full data (POST)...")
    try:
        response = requests.post(
            f"{BASE_URL}/generate",
            headers={"Content-Type": "application/json"},
            json=test_full_data,
        )
        if response.status_code == 200:
            result = response.json()
            print("✓ PDF generation initiated")
            print(f"  Success: {result.get('success')}")
            print(f"  Job ID: {result.get('job_id')}")
            if result.get("pdf_path"):
                print("✓ PDF file generated successfully")
            elif result.get("html"):
                print("⚠ WeasyPrint not installed, returning HTML preview")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"✗ Request failed: {e}")


def main() -> None:
    # First, check available RFQ IDs
    print("Checking available RFQ IDs...")
    try:
        response = requests.get("http://103.103.22.118:8000/api/v1/files/rfq-ids")
        rfq_data = response.json()
        if rfq_data.get("rfq_ids"):
            print(f"Available RFQ IDs: {rfq_data['rfq_ids']}")
        else:
            print("No RFQ IDs found. Please upload some RFQ files first.")
            return
    except Exception as e:
        print(f"Error checking RFQ IDs: {e}")
        return

    print()
    test_quotation_with_rfq_id()
    print()
    test_traditional_quotation()


if __name__ == "__main__":
    main()
