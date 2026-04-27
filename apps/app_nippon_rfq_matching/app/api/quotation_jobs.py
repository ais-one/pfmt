"""
API endpoints for quotation export async jobs
"""

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.schemas import JobStatusResponse
from apps.app_nippon_rfq_matching.app.services.job_service import job_service

router = APIRouter(prefix="/quotation-jobs", tags=["quotation-jobs"])
logger = logging.getLogger(__name__)


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_quotation_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get quotation export job status and results

    - **job_id**: Job ID from quotation export response

    Returns job status, progress, and results if completed
    """
    job_dict = job_service.get_job_dict(job_id, db)

    if not job_dict:
        raise HTTPException(status_code=404, detail=f"Quotation job {job_id} not found")

    return JobStatusResponse(**job_dict)


@router.get("/{job_id}/download")
async def download_quotation_by_job_id(job_id: str, db: Session = Depends(get_db)):
    """
    Download quotation PDF by job ID

    - **job_id**: Job ID from quotation export response

    Returns PDF file if job completed successfully
    """
    job_dict = job_service.get_job_dict(job_id, db)

    if not job_dict:
        raise HTTPException(status_code=404, detail=f"Quotation job {job_id} not found")

    if job_dict["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job {job_id} is not completed yet. Current status: {job_dict['status']}",
        )

    if not job_dict.get("result_data"):
        raise HTTPException(status_code=404, detail=f"No result found for job {job_id}")

    # result_data should already be parsed by Job.to_dict()
    result = job_dict["result_data"]
    if not result.get("success") or not result.get("pdf_path"):
        error_msg = result.get("error", "Unknown error")
        raise HTTPException(
            status_code=500, detail=f"Quotation generation failed: {error_msg}"
        )

    pdf_path = result["pdf_path"]

    # Check if file exists
    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found: {pdf_path}")
        raise HTTPException(status_code=404, detail="PDF file not found")

    # Get quote number from result
    quote_number = result.get("quote_number", "quotation")

    # Return file response
    return FileResponse(
        path=pdf_path,
        filename=f"NPM-Quotation-{quote_number}.pdf",
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename=NPM-Quotation-{quote_number}.pdf"
        },
    )


@router.get("", response_model=list[JobStatusResponse])
async def list_quotation_jobs(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all quotation export jobs with optional filters

    - **status**: Filter by status (pending, processing, completed, failed)
    - **limit**: Maximum number of jobs to return
    - **offset**: Number of jobs to skip
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import Job

    query = db.query(Job).filter(Job.job_type == "quotation_export")

    if status:
        query = query.filter(Job.status == status)

    jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()

    return [
        JobStatusResponse(**job_service.get_job_dict(job.job_id, db)) for job in jobs
    ]
