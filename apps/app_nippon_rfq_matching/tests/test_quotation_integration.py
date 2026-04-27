#!/usr/bin/env python3
"""
Integration test for quotation PDF generation with background job
"""

import json
import time

import requests

# Base URL
BASE_URL = "http://103.103.22.118:8000/api/v1"


def test_background_quotation_generation():
    """Test background quotation generation with curl equivalent"""
    print("\n=== Testing Background Quotation Generation ===")
    print("Equivalent to:")
    print("curl -X 'POST' \\")
    print("  'http://103.103.22.118:8000/api/v1/quotation-pdf/generate' \\")
    print("  -H 'accept: application/json' \\")
    print("  -H 'Content-Type: application/json' \\")
    print("  -d '{\\")
    print('  "rfq_id": "RFQ-RFQ-000000709025"\\')
    print("}'")

    # Test with async mode
    payload = {"rfq_id": "RFQ-RFQ-000000709025", "async_mode": True}

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/generate",
            headers={"accept": "application/json", "Content-Type": "application/json"},
            json=payload,
        )

        print(f"\nStatus Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("\n✅ Background job started successfully!")
            print(f"Response: {json.dumps(result, indent=2)}")

            # Extract job_id
            job_id = result.get("job_id")
            if job_id:
                print(f"\n📋 Job ID: {job_id}")
                print("\n⏳ Checking job status...")

                # Wait a bit for processing
                time.sleep(3)

                # Check status
                status_url = f"{BASE_URL}/quotation-pdf/status/{job_id}"
                status_response = requests.get(status_url)

                print(f"\nStatus Response ({status_url}):")
                print(json.dumps(status_response.json(), indent=2))

                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data.get("status")

                    if status == "completed":
                        print("\n✅ Quotation PDF generation completed!")
                        if status_data.get("pdf_path"):
                            print(f"📄 PDF Path: {status_data['pdf_path']}")
                        else:
                            print("📄 PDF Path: Not available yet")
                    elif status == "processing":
                        print("\n⏳ Still processing...")
                    else:
                        print(f"\n❌ Status: {status}")
                        if status_data.get("error"):
                            print(f"Error: {status_data['error']}")
                else:
                    print(f"\n❌ Failed to get status: {status_response.text}")
            else:
                print("\n❌ No job_id in response")
        else:
            print(f"\n❌ Failed: {response.text}")

    except Exception as e:
        print(f"\n❌ Error: {e}")


def test_direct_async_endpoint():
    """Test the direct async endpoint"""
    print("\n=== Testing Direct Async Endpoint ===")
    print("Using: POST /api/v1/quotation-pdf/generate-async")

    payload = {
        "rfq_id": "RFQ-RFQ-000000709025",
        "client_company": "Test Company",
        "contact_name": "John Doe",
        "client_phone": "+1234567890",
        "client_email": "john.doe@example.com",
    }

    try:
        response = requests.post(
            f"{BASE_URL}/quotation-pdf/generate-async",
            headers={"Content-Type": "application/json"},
            json=payload,
        )

        print(f"\nStatus Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("\n✅ Async job started successfully!")
            print(f"Response: {json.dumps(result, indent=2)}")

            # Check status
            job_id = result.get("job_id")
            if job_id:
                print(f"\n📋 Job ID: {job_id}")
                time.sleep(2)

                status_response = requests.get(
                    f"{BASE_URL}/quotation-pdf/status/{job_id}"
                )
                print(f"\nStatus: {json.dumps(status_response.json(), indent=2)}")
        else:
            print(f"\n❌ Failed: {response.text}")

    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    print("=== Background Job Integration Test ===")
    print("This test demonstrates the async quotation PDF generation feature")
    print()

    test_background_quotation_generation()
    test_direct_async_endpoint()

    print("\n=== Integration Test Complete ===")
