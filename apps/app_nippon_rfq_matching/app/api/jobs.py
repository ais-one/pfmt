"""
API endpoints for async job-based file uploads
"""

import logging

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.schemas import (
    JobStatusResponse,
    JobUploadResponse,
)
from apps.app_nippon_rfq_matching.app.services.job_service import (
    job_service,
    run_job_background,
    sequential_job_queue,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service

router = APIRouter(prefix="/jobs", tags=["jobs"])
logger = logging.getLogger(__name__)


@router.post("/upload/pdf", response_model=JobUploadResponse)
async def upload_pdf_async(
    rfq_id: str | None = Query(None, description="RFQ identifier (optional)"),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
    Upload RFQ PDF file and create async parsing job

    - **rfq_id**: Optional RFQ identifier (will be generated if not provided)
    - **file**: PDF file to upload

    Returns job_id for tracking processing status
    """
    try:
        # Read file content
        file_content = await file.read()

        # Save file
        uploaded_file = rfq_service.save_uploaded_file(file_content, file.filename)

        # Create job
        job = job_service.create_job("rfq_parse_store", uploaded_file.file_path, db)

        # Start background processing (using wrapper to avoid blocking)
        run_job_background(job.job_id)

        logger.info(f"Created RFQ parsing job {job.job_id} for file {file.filename}")

        return JobUploadResponse(
            status="success",
            job_id=job.job_id,
            job_type="rfq_parse_store",
            message=f"RFQ PDF uploaded successfully. Job ID: {job.job_id}. Use GET /jobs/{job.job_id} to check status.",
        )

    except Exception as e:
        logger.error(f"Error uploading PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error uploading PDF: {str(e)}")


@router.post("/upload/excel", response_model=JobUploadResponse)
async def upload_excel_async(
    file: UploadFile = File(...), db: Session = Depends(get_db)
):
    """
    Upload IATP Excel file and create async parsing job

    - **file**: Excel file to upload

    Returns job_id for tracking processing status
    """
    try:
        # Read file content
        file_content = await file.read()

        # Save file
        uploaded_file = rfq_service.save_uploaded_file(file_content, file.filename)

        # Create job
        job = job_service.create_job("iatp_parse_store", uploaded_file.file_path, db)

        # Start background processing (using wrapper to avoid blocking)
        run_job_background(job.job_id)

        logger.info(f"Created IATP parsing job {job.job_id} for file {file.filename}")

        return JobUploadResponse(
            status="success",
            job_id=job.job_id,
            job_type="iatp_parse_store",
            message=f"IATP Excel uploaded successfully. Job ID: {job.job_id}. Use GET /jobs/{job.job_id} to check "
            f"status.",
        )

    except Exception as e:
        logger.error(f"Error uploading Excel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error uploading Excel: {str(e)}")


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get job status and results

    - **job_id**: Job ID from upload response

    Returns job status, progress, and results if completed
    """
    job_dict = job_service.get_job_dict(job_id, db)

    if not job_dict:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return JobStatusResponse(**job_dict)


@router.get("", response_model=list[JobStatusResponse])
async def list_jobs(
    status: str | None = Query(None, description="Filter by status"),
    job_type: str | None = Query(None, description="Filter by job type"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all jobs with optional filters

    - **status**: Filter by status (pending, processing, completed, failed)
    - **job_type**: Filter by job type (rfq_parse_store, iatp_parse_store)
    - **limit**: Maximum number of jobs to return
    - **offset**: Number of jobs to skip
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import Job

    query = db.query(Job)

    if status:
        query = query.filter(Job.status == status)
    if job_type:
        query = query.filter(Job.job_type == job_type)

    jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()

    return [
        JobStatusResponse(**job_service.get_job_dict(job.job_id, db)) for job in jobs
    ]


@router.get("/queue/status")
async def get_queue_status():
    """
    Get sequential job queue status

    Returns information about the job queue:
    - queue_size: Number of jobs waiting to be processed
    - is_processing: Whether a job is currently being processed
    """
    return {
        "queue_size": sequential_job_queue.get_queue_size(),
        "is_processing": sequential_job_queue.is_processing(),
        "processing_mode": "sequential",
    }
