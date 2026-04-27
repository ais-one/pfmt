"""
Quotation PDF Generation API Endpoints

API endpoints for generating quotation PDFs from HTML templates.
"""

import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

router = APIRouter(prefix="/quotation-pdf", tags=["Quotation PDF"])
logger = logging.getLogger(__name__)


# Request Schemas
class QuotationRequest(BaseModel):
    """Request schema for quotation PDF generation - always async"""

    rfq_id: str = Field(
        ..., description="RFQ ID to fetch data from database (required)"
    )


class QuotationResponse(BaseModel):
    """Response schema for quotation PDF generation"""

    success: bool
    message: str
    job_id: str | None = None
    file_path: str | None = None
    pdf_filename: str | None = None


def generate_quotation_html(data: dict[str, Any]) -> str:
    """Generate HTML template for quotation PDF"""
    # Format items table
    items_html = ""
    subtotal = 0

    for index, item in enumerate(data["items"], 1):
        item_total = item.get("quantity", 0) * float(item.get("unit_price", 0))
        subtotal += item_total

        items_html += f"""
        <tr>
            <td style="text-align: center; width: 25px;">{index}</td>
            <td class="item-code">{item.get("item_code", "")}</td>
            <td class="description" title="{item.get("description", "")}">{item.get("description", "")}</td>
            <td class="color">{item.get("color", "")}</td>
            <td class="unit">{item.get("unit", "")}</td>
            <td class="quantity">{float(item.get("quantity", 0))}</td>
            <td class="unit-price">${float(item.get("unit_price", 0)):.2f}</td>
            <td class="total-price">${item_total:.2f}</td>
        </tr>
        """

    # Calculate tax and total
    tax_rate = 0.08  # 8% tax
    tax_amount = subtotal * tax_rate
    total_amount = subtotal + tax_amount

    # Generate quotation number if not provided
    quote_number = data.get(
        "quote_number",
        f"NPM-Q-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}",
    )

    # Calculate validity date
    validity_days = data.get("validity_days", 30)
    valid_until = (datetime.now() + timedelta(days=validity_days)).strftime("%B %d, %Y")

    # Client info
    client_info = data.get("client_info", {})

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Nippon Paint Marine Quotation</title>
<style>
  {get_quotation_css()}
</style>
</head>

<body>

<div class="container">
  <div class="main-content">
    <div class="content-wrapper">
      <!-- Header -->
      <div class="header">
        <h1>NIPPON PAINT MARINE</h1>
        <div class="badge">QUOTATION</div>
      </div>

  <!-- Meta -->
  <div class="meta">
    <div># {quote_number}</div>
    <div>Date: {datetime.now().strftime("%B %d, %Y")} | Valid Until: {valid_until}</div>
  </div>

  <!-- From & To -->
  <div class="section flex">
    <div class="box">
      <h3>From</h3>
      Nippon Paint Marine Coatings Co., Ltd.<br>
      1-2-3 Marine Tower, Harbor District<br>
      Tokyo 135-8234, Japan<br>
      +81 3 5566 7788<br>
      marine.sales@nipponpaint.com
    </div>

    <div class="box">
      <h3>To</h3>
      {client_info.get("client_company", "")}<br>
      {client_info.get("client_address_1", "")}<br>
      {client_info.get("client_address_2", "")}<br>
      Attn: {client_info.get("contact_name", "")}<br>
      {client_info.get("client_phone", "")}<br>
      {client_info.get("client_email", "")}
    </div>
      </div>

      <!-- Table -->
      <div class="section">
        <h3>Quotation Details</h3>

    <table>
      <thead>
        <tr>
          <th style="width: 25px; text-align: center;">No.</th>
          <th style="width: 60px;">Item Code</th>
          <th style="width: 180px;">Description</th>
          <th style="width: 40px;">Color</th>
          <th style="width: 25px;">Unit</th>
          <th style="width: 40px;">Qty</th>
          <th style="width: 45px;">Unit Price</th>
          <th style="width: 50px; text-align: right;">Amount</th>
        </tr>
      </thead>
      <tbody>
        {items_html}
      </tbody>
    </table>

    <!-- Summary -->
    <div class="summary">
      <div><span>Subtotal</span><span>${subtotal:.2f}</span></div>
      <div><span>Tax (8%)</span><span>${tax_amount:.2f}</span></div>
      <div class="total"><span>Total Amount</span><span>${total_amount:.2f}</span></div>
    </div>
  </div>
    </div>

    <!-- Terms -->
    <div class="section terms">
      <h3>Terms & Conditions</h3>
      • Payment terms: Net 30 days from invoice date<br>
      • Delivery: 14-21 business days from order confirmation<br>
      • Prices are in USD and exclude shipping costs<br>
      • All products comply with IMO PSPC and relevant standards<br>
      • Technical data sheets available upon request<br>
      • Quotation valid for {validity_days} days
    </div>

    <!-- Signature -->
    <div class="footer">
    <div class="signature">
      <div>
        Prepared by
        <div class="line"></div>
        {client_info.get("sales_representative", "Sales Representative Name")}
      </div>

      <div>
        Client Acceptance
        <div class="line"></div>
        Signature & Date
      </div>
    </div>
  </div>

  <!-- Bottom -->
  <div class="bottom-bar">
    Thank you for considering Nippon Paint Marine.
  </div>
    </div>
  </div>
</body>
</html>"""


def get_quotation_css() -> str:
    """Get CSS for quotation PDF with Nippon Paint Marine styling"""
    return """
  body {
    font-family: Arial, sans-serif;
    margin: 0;
    background: #f4f6f9;
    display: flex;
    flex-direction: column;
    min-height: 100vh;
  }

  .container {
    width: 178mm;
    margin: 3mm auto;
    background: #fff;
    border: 1px solid #ddd;
    display: flex;
    flex-direction: column;
    flex-grow: 1;
  }

  .header {
    background: #1f3fa3;
    color: #fff;
    padding: 10px 15px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .header h1 {
    margin: 0;
    font-size: 16px;
  }

  .badge {
    background: #3b5bdb;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 9px;
  }

  .meta {
    padding: 4px 10px;
    font-size: 9px;
    color: #666;
    display: flex;
    justify-content: space-between;
  }

  .section {
    padding: 6px 10px;
  }

  .flex {
    display: flex;
    justify-content: space-between;
  }

  .box {
    width: 48%;
    font-size: 9px;
    line-height: 1.4;
    padding: 4px;
  }

  h3 {
    margin-bottom: 4px;
    color: #333;
    font-size: 9px;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 4px;
    font-size: 7px;
    table-layout: fixed;
    border: 1px solid #ddd;
  }

  th {
    background: #f0f2f5;
    text-align: left;
    padding: 2px 1px;
    border-bottom: 1px solid #ddd;
    border-top: 1px solid #ddd;
    border-left: 1px solid #ddd;
    border-right: 1px solid #ddd;
    font-size: 7px;
    font-weight: bold;
  }

  td {
    padding: 1px;
    border-bottom: 1px solid #eee;
    border-left: 1px solid #eee;
    border-right: 1px solid #eee;
  }

  /* Zebra row pattern */
  tr:nth-child(even) {
    background-color: #fafbfc;
  }

  tr:nth-child(odd) {
    background-color: #ffffff;
  }

  /* Hover effect */
  tr:hover {
    background-color: #f5f7fa;
  }

  /* Column specific alignments */
  .item-code {
    width: 60px;
    text-align: left;
  }

  .description {
    width: 180px;
    text-align: left;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    word-break: break-word;
  }

  .color {
    width: 40px;
    text-align: center;
  }

  .unit {
    width: 25px;
    text-align: center;
  }

  .quantity {
    width: 40px;
    text-align: right;
  }

  .unit-price {
    width: 45px;
    text-align: right;
  }

  .total-price {
    width: 50px;
    text-align: right;
    font-weight: bold;
  }

  .text-right {
    text-align: right;
  }

  .main-content {
    flex-grow: 1;
    display: flex;
    flex-direction: column;
  }

  .content-wrapper {
    flex-grow: 1;
  }

  .summary {
    width: 200px;
    margin-left: auto;
    margin-top: 6px;
    font-size: 9px;
    padding: 4px;
  }

  .summary div {
    display: flex;
    justify-content: space-between;
    margin: 4px 0;
  }

  .total {
    font-weight: bold;
    color: #1f3fa3;
    font-size: 12px;
  }

  .terms {
    font-size: 8px;
    color: #555;
    padding: 4px 8px;
    margin-top: auto;
  }

  .footer {
    padding: 6px 10px;
    font-size: 8px;
    margin-top: 0;
  }

  .signature {
    display: flex;
    justify-content: space-between;
    margin-top: 10px;
    padding: 0 10px;
  }

  .line {
    margin-top: 10px;
    border-top: 1px solid #ccc;
    width: 120px;
  }

  .bottom-bar {
    background: #1f3fa3;
    color: #fff;
    text-align: center;
    padding: 4px 6px;
    font-size: 9px;
  }
    """


# Endpoints
@router.post("/generate", response_model=QuotationResponse)
async def generate_quotation_pdf(
    request: QuotationRequest, db: Session = Depends(get_db)
):
    """
    Generate quotation PDF using the same matching flow as pdf-export.
    Only shows matched items with product master data including item code, color, unit price.
    Quantity is taken from RFQ data.

    Args:
        request: Quotation request with rfq_id
        background_tasks: FastAPI background tasks for async processing
        db: Database session

    Returns:
        PDF generation result with file path

    Example:
        POST /api/v1/quotation-pdf/generate
        {
            "rfq_id": "TEST-RFQ-001",
            "async_mode": true
        }
    """

    try:
        # Use only RFQ ID to fetch data (same as pdf-export)
        rfq_id = (
            request.rfq_id
            if hasattr(request, "rfq_id")
            else (request.get("rfq_id") if isinstance(request, dict) else None)
        )

        if not rfq_id:
            raise HTTPException(status_code=400, detail="rfq_id is required")

        logger.info(f"Generating quotation PDF for RFQ: {rfq_id}")

        # Validate RFQ exists (same as pdf-export)
        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()
        if not rfq_items:
            raise HTTPException(
                status_code=404,
                detail=f"RFQ ID '{rfq_id}' not found. Please upload RFQ first.",
            )

        logger.info(f"Found {len(rfq_items)} RFQ items")

        # Always use async mode
        from apps.app_nippon_rfq_matching.app.services.job_service import job_service

        job = job_service.create_job("quotation_export", "", db)
        job_id = str(job.job_id)
        logger.info(f"Created job with ID: {job_id}")

        # Start background job with RFQ ID and request data
        from apps.app_nippon_rfq_matching.app.services.job_service import (
            run_job_background,
        )

        request_data = {"rfq_id": rfq_id, "client_info": {}}
        run_job_background(job_id, rfq_id=rfq_id, request_data=request_data)

        return QuotationResponse(
            success=True,
            message="Quotation PDF generation started in background",
            job_id=job_id,
            file_path=None,
            pdf_filename=None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating quotation PDF: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate quotation PDF: {str(e)}"
        )


@router.get("/download/{job_id}")
async def download_quotation_pdf(job_id: str, db: Session = Depends(get_db)):
    """
    Download generated quotation PDF file for a completed job.
    """
    try:
        from apps.app_nippon_rfq_matching.app.services.job_service import job_service

        job_dict = job_service.get_job_dict(job_id, db)

        if not job_dict:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        if job_dict["status"] != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"Job {job_id} is not completed. Current status: {job_dict['status']}",
            )

        result = job_dict.get("result_data")
        if not result or not result.get("success"):
            raise HTTPException(
                status_code=400, detail=f"Job {job_id} did not complete successfully"
            )

        pdf_path = result.get("pdf_path")
        if not pdf_path or not Path(pdf_path).exists():
            raise HTTPException(
                status_code=404, detail=f"PDF file not found at {pdf_path}"
            )

        # Return file
        filename = result.get("pdf_filename", f"quotation_{job_id}.pdf")
        return FileResponse(
            path=pdf_path, filename=filename, media_type="application/pdf"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to download PDF: {str(e)}")


@router.get("/job/{job_id}", response_model=dict[str, Any])
async def get_quotation_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get quotation PDF job status and result.
    """
    try:
        from apps.app_nippon_rfq_matching.app.services.job_service import job_service

        job_dict = job_service.get_job_dict(job_id, db)

        if not job_dict:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return {
            "job_id": job_dict["job_id"],
            "status": job_dict["status"],
            "progress": job_dict.get("progress", 0),
            "result": job_dict.get("result_data"),
            "error": job_dict.get("error_message"),
            "created_at": job_dict.get("created_at"),
            "completed_at": job_dict.get("completed_at"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get job status: {str(e)}"
        )
