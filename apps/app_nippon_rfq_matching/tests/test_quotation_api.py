#!/usr/bin/env python3
"""
Test script for quotation generation API endpoints with async mode
"""

import time
from datetime import datetime

import requests

# Base URL
BASE_URL = "http://103.103.22.118:8000/api/v1"


def test_quotation_pdf_generation():
    """Test the quotation PDF generation endpoint"""
    print("Testing quotation PDF generation...")

    # Test data with RFQ ID
    test_data = {"rfq_id": "RFQ-RFQ-000000709025"}

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/generate",
            json=test_data,
            headers={"Content-Type": "application/json"},
        )

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Message: {result.get('message', 'No message')}")

            if "data" in result:
                data = result["data"]
                print(f"PDF Path: {data.get('pdf_path', 'N/A')}")
                print(f"PDF Filename: {data.get('pdf_filename', 'N/A')}")
                print(f"Statistics: {data.get('statistics', {})}")
        else:
            print("❌ Failed!")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")


def test_check_matching_status():
    """Test the check matching status endpoint"""
    print("\nTesting matching status check...")

    rfq_id = "RFQ-RFQ-000000709025"

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/check-matching-status",
            json={"rfq_id": rfq_id},
            headers={"Content-Type": "application/json"},
        )

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"RFQ ID: {result.get('rfq_id', 'N/A')}")
            print(f"Matching Status: {result.get('matching_status', 'N/A')}")
            print(f"Matched Count: {result.get('matched_count', 0)}")
            print(f"Total Items: {result.get('total_items', 0)}")
            print(f"Match Rate: {result.get('match_rate', 0)}%")
            print(
                f"Can Generate Quotation: {result.get('can_generate_quotation', False)}"
            )

            if result.get("recommendation"):
                print(f"Recommendation: {result.get('recommendation')}")
        else:
            print("❌ Failed!")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")


def test_generate_with_matching():
    """Test generate quotation with matching"""
    print("\nTesting generate with matching...")

    rfq_id = "RFQ-RFQ-000000709025"

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/generate-from-rfq-with-matching",
            json={"rfq_id": rfq_id, "region": "Indonesia", "format_type": "table"},
            headers={"Content-Type": "application/json"},
        )

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Message: {result.get('message', 'No message')}")

            if "job_id" in result:
                print(f"Job ID: {result.get('job_id', 'N/A')}")
                print("⏳ Use this job ID to check status")

                # Check job status
                time.sleep(2)  # Wait a bit
                check_job_status(result["job_id"])
            elif "data" in result:
                data = result["data"]
                print(f"PDF Path: {data.get('pdf_path', 'N/A')}")
                print(f"Matched Items: {data.get('matched_items_count', 0)}")
        else:
            print("❌ Failed!")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")


def check_job_status(job_id):
    """Check job status"""
    print(f"\nChecking job status for job_id: {job_id}")

    try:
        response = requests.get(f"{BASE_URL}/jobs/{job_id}")

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Job Status:")
            print(f"Job ID: {result.get('job_id', 'N/A')}")
            print(f"Status: {result.get('status', 'N/A')}")
            print(f"Progress: {result.get('progress', 0)}%")

            if result.get("completed_at"):
                print(f"Completed: {result.get('completed_at', 'N/A')}")

            if result.get("result_data"):
                print("Result Data Available")
            if result.get("error_message"):
                print(f"Error: {result.get('error_message', 'N/A')}")
        else:
            print("❌ Failed to get job status")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error checking job status: {e}")


def test_preview_quotation():
    """Test preview quotation"""
    print("\nTesting preview quotation...")

    rfq_id = "RFQ-RFQ-000000709025"

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/preview",
            json={"rfq_id": rfq_id},
            headers={"Content-Type": "application/json"},
        )

        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Quote Number: {result.get('quote_number', 'N/A')}")
            print(f"RFQ ID: {result.get('rfq_id', 'N/A')}")

            # Show first part of HTML
            if "html" in result:
                html = result["html"]
                print(f"HTML Length: {len(html)} characters")
                print("HTML Preview (first 500 chars):")
                print(html[:500] + "..." if len(html) > 500 else html)
        else:
            print("❌ Failed!")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Error: {e}")


def test_async_quotation():
    """Test asynchronous quotation PDF generation"""
    print("\n=== Testing Async Mode ===")

    payload = {
        "rfq_id": "RFQ-RFQ-000000709025",
        "client_company": "ABC Marine Services Ltd",
        "contact_name": "John Smith",
        "client_phone": "+65 1234 5678",
        "client_email": "john.smith@abcmarine.com",
        "sales_representative": "Jane Doe",
        "validity_days": 30,
    }

    response = requests.post(
        f"{BASE_URL}/quotation-pdf/generate-async",
        headers={"Content-Type": "application/json"},
        json=payload,
    )

    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")

    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job ID: {job_id}")
        print("✅ Async mode test started - checking status...")

        # Check job status
        time.sleep(5)  # Wait for background job to complete

        status_response = requests.get(f"{BASE_URL}/quotation-pdf/status/{job_id}")
        print(f"Status: {status_response.json()}")

        if status_response.json().get("status") == "completed":
            print("✅ Async mode test completed successfully")
        else:
            print("⏳ Async mode still processing")
    else:
        print("❌ Async mode test failed")


def test_generate_with_async_flag():
    """Test generate endpoint with async mode flag"""
    print("\n=== Testing Generate with Async Flag ===")

    payload = {
        "rfq_id": "RFQ-RFQ-000000709025",
        "async_mode": True,
        "client_company": "Test Company",
        "contact_name": "Test User",
        "client_phone": "+1234567890",
        "client_email": "test@example.com",
    }

    response = requests.post(
        f"{BASE_URL}/quotation-pdf/generate",
        headers={"Content-Type": "application/json"},
        json=payload,
    )

    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")

    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job ID: {job_id}")
        print("✅ Generate with async flag test started")
    else:
        print("❌ Generate with async flag test failed")


if __name__ == "__main__":
    print("=== Testing Quotation Generation API (with Async Mode) ===")
    print(f"Time: {datetime.now().isoformat()}")
    print()

    # Run basic tests
    test_check_matching_status()
    test_quotation_pdf_generation()
    test_preview_quotation()
    test_generate_with_matching()

    # Run async mode tests
    test_async_quotation()
    test_generate_with_async_flag()

    print("\n=== Tests Complete ===")
