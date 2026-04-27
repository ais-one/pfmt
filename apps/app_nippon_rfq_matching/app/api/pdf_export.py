"""
PDF Comparison Export API Endpoints

API endpoints for exporting PDF comparison reports with OpenAI normalization.
"""

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.services.job_service import (
    job_service,
    run_pdf_comparison_export_job,
)
from apps.app_nippon_rfq_matching.app.services.pdf_comparison_export_service import (
    pdf_comparison_export_service,
)

router = APIRouter(prefix="/pdf-export", tags=["PDF Export"])
logger = logging.getLogger(__name__)


# Request Schemas
class PDFExportRequest(BaseModel):
    """Request schema for PDF comparison export"""

    rfq_id: str = Field(..., description="RFQ identifier to export")
    format_type: str = Field(
        "table", description="PDF format type: table, side_by_side, summary"
    )
    use_normalization: bool = Field(
        True, description="Whether to use OpenAI normalization"
    )
    async_mode: bool = Field(
        True, description="Whether to process asynchronously (background job)"
    )


class PDFExportBatchRequest(BaseModel):
    """Request schema for batch PDF export"""

    rfq_ids: list[str] = Field(..., description="List of RFQ identifiers to export")
    format_type: str = Field(
        "table", description="PDF format type: table, side_by_side, summary"
    )
    use_normalization: bool = Field(
        True, description="Whether to use OpenAI normalization"
    )


# Response Schemas
class PDFExportResponse(BaseModel):
    """Response schema for PDF export job creation"""

    job_id: str
    status: str
    message: str
    rfq_id: str
    format_type: str
    normalization_enabled: bool


class PDFExportSyncResponse(BaseModel):
    """Response schema for synchronous PDF export"""

    success: bool
    rfq_id: str
    pdf_path: str | None = None
    pdf_filename: str | None = None
    statistics: dict[str, Any] | None = None
    error: str | None = None
    format_type: str
    normalization_enabled: bool


class PDFExportFormatsResponse(BaseModel):
    """Response schema for available PDF formats"""

    formats: list[str]
    default: str


# Endpoints
@router.post("/export", response_model=dict[str, Any])
async def export_pdf_comparison(
    request: PDFExportRequest, db: Session = Depends(get_db)
):
    """
    Export PDF comparison report for an RFQ.

    This endpoint creates a PDF comparison report with optional OpenAI normalization.
    By default, it runs asynchronously as a background job.

    Args:
        request: PDF export request
        db: Database session

    Returns:
        Job information for tracking progress

    Example:
        POST /api/v1/pdf-export/export
        {
            "rfq_id": "RFQ001",
            "format_type": "table",
            "use_normalization": true,
            "async_mode": true
        }

        Response:
        {
            "job_id": "uuid-here",
            "status": "pending",
            "message": "PDF export job created",
            "rfq_id": "RFQ001",
            "format_type": "table",
            "normalization_enabled": true
        }
    """
    try:
        # Validate RFQ exists
        from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == request.rfq_id).all()
        if not rfq_items:
            raise HTTPException(
                status_code=404,
                detail=f"RFQ ID '{request.rfq_id}' not found. Please upload RFQ first.",
            )

        # Validate format type
        valid_formats = ["table", "side_by_side", "summary"]
        if request.format_type not in valid_formats:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid format_type '{request.format_type}'. Must be one of: {valid_formats}",
            )

        # Check if OpenAI is enabled for normalization
        if request.use_normalization and not pdf_comparison_export_service:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        if request.async_mode:
            # Create background job
            job = job_service.create_job(
                job_type="pdf_comparison_export",
                file_path="",  # No file path for this job type
                db=db,
            )

            # Start background job with parameters
            run_pdf_comparison_export_job(
                job_id=job.job_id,
                rfq_id=request.rfq_id,
                format_type=request.format_type,
                use_normalization=request.use_normalization,
            )

            logger.info(f"Created PDF export job {job.job_id} for RFQ {request.rfq_id}")

            return {
                "job_id": job.job_id,
                "status": "pending",
                "message": "PDF export job created successfully",
                "rfq_id": request.rfq_id,
                "format_type": request.format_type,
                "normalization_enabled": request.use_normalization,
                "async": True,
            }
        else:
            # Synchronous processing (not recommended for large datasets)
            result = pdf_comparison_export_service.export_comparison_report(
                rfq_id=request.rfq_id,
                db=db,
                format_type=request.format_type,
                use_normalization=request.use_normalization,
                bearer_token="",
            )

            if result.get("success"):
                return {
                    "success": True,
                    "rfq_id": request.rfq_id,
                    "pdf_path": result.get("pdf_path"),
                    "pdf_filename": result.get("pdf_filename"),
                    "statistics": result.get("statistics"),
                    "format_type": request.format_type,
                    "normalization_enabled": request.use_normalization,
                    "async": False,
                }
            else:
                raise HTTPException(
                    status_code=500, detail=f"PDF export failed: {result.get('error')}"
                )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating PDF export job: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to create PDF export job: {str(e)}"
        )


@router.get("/job/{job_id}", response_model=dict[str, Any])
async def get_pdf_export_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get PDF export job status and result.

    Args:
        job_id: Job identifier
        db: Database session

    Returns:
        Job status and result information

    Example:
        GET /api/v1/pdf-export/job/{job_id}

        Response (processing):
        {
            "job_id": "uuid",
            "status": "processing",
            "progress": 60,
            "result": null
        }

        Response (completed):
        {
            "job_id": "uuid",
            "status": "completed",
            "progress": 100,
            "result": {
                "success": true,
                "pdf_path": "/path/to/file.pdf",
                "statistics": {...}
            }
        }
    """
    try:
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


@router.get("/download/{job_id}")
async def download_pdf_export(job_id: str, db: Session = Depends(get_db)):
    """
    Download generated PDF file for a completed export job.

    Args:
        job_id: Job identifier
        db: Database session

    Returns:
        PDF file as attachment

    Example:
        GET /api/v1/pdf-export/download/{job_id}

        Response: PDF file download
    """
    try:
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
        filename = result.get("pdf_filename", f"export_{job_id}.pdf")
        return FileResponse(
            path=pdf_path, filename=filename, media_type="application/pdf"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to download PDF: {str(e)}")


@router.get("/formats", response_model=PDFExportFormatsResponse)
async def get_pdf_formats():
    """
    Get available PDF export formats.

    Returns:
        List of available formats and default

    Example:
        GET /api/v1/pdf-export/formats

        Response:
        {
            "formats": ["table", "side_by_side", "summary"],
            "default": "table"
        }
    """
    return {"formats": ["table", "side_by_side", "summary"], "default": "table"}


@router.get("/rfq-list", response_model=list[str])
async def get_available_rfqs(limit: int = 100, db: Session = Depends(get_db)):
    """
    Get list of available RFQ IDs for export.

    Args:
        limit: Maximum number of RFQ IDs to return
        db: Database session

    Returns:
        List of RFQ IDs

    Example:
        GET /api/v1/pdf-export/rfq-list?limit=50

        Response:
        ["RFQ001", "RFQ002", "RFQ003"]
    """
    try:
        from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

        rfq_ids = (
            db.query(RFQItem.rfq_id)
            .distinct()
            .order_by(RFQItem.rfq_id.desc())
            .limit(limit)
            .all()
        )

        return [rfq[0] for rfq in rfq_ids]

    except Exception as e:
        logger.error(f"Error getting RFQ list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get RFQ list: {str(e)}")


@router.get("/status/service")
async def get_service_status():
    """
    Get PDF export service status.

    Returns:
        Service status information

    Example:
        GET /api/v1/pdf-export/status/service

        Response:
        {
            "enabled": true,
            "openai_normalization_enabled": true,
            "export_dir": "/path/to/exports"
        }
    """
    return {
        "enabled": True,
        "openai_normalization_enabled": pdf_comparison_export_service is not None,
        "export_dir": str(pdf_comparison_export_service.export_dir)
        if pdf_comparison_export_service
        else None,
    }


@router.get("/health")
async def health_check():
    """
    Health check endpoint for PDF export service.

    Returns:
        Health status

    Example:
        GET /api/v1/pdf-export/health

        Response:
        {
            "status": "healthy",
            "openai_enabled": true
        }
    """
    return {
        "status": "healthy",
        "openai_enabled": pdf_comparison_export_service is not None,
    }
