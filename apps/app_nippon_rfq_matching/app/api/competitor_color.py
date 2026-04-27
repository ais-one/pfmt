"""
API endpoints for competitor color comparison PDF parsing
"""

import logging

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.competitor import CompetitorColorComparison
from apps.app_nippon_rfq_matching.app.models.schemas import (
    APIResponse,
    CompetitorColorJobStatusResponse,
    CompetitorColorUploadResponse,
)
from apps.app_nippon_rfq_matching.app.services.job_service import (
    job_service,
    run_job_background,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service

router = APIRouter(prefix="/competitor-color", tags=["competitor-color"])
logger = logging.getLogger(__name__)


@router.post("/upload", response_model=CompetitorColorUploadResponse)
async def upload_competitor_color(
    file: UploadFile = File(...), db: Session = Depends(get_db)
):
    """
    Upload competitor color comparison file (PDF or Excel) and create async parsing job

    This endpoint supports:
    - PDF files: Uses pdfplumber to parse color comparison tables
    - Excel files (.xlsx): Parses multisheet Excel files with color comparison data

    - **file**: PDF or Excel file to upload

    Returns job_id for tracking processing status

    Example PDF structure:
    - Table 1: Item no., Jotun code, Recommended NPMS code
    - Table 2: Item no., International Paint code, Recommended NPMS code

    Example Excel structure:
    - Sheet names like "JOTUN_vs_HEMPEL", "HEMPEL_vs_RAL", "CMP_vs_PPG"
    - Row 3 onwards contains: Item no, Source code, NPMS code
    """
    try:
        # Validate file type
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name is required")

        file_lower = file.filename.lower()
        if not (file_lower.endswith(".pdf") or file_lower.endswith(".xlsx")):
            raise HTTPException(
                status_code=400,
                detail="Only PDF (.pdf) or Excel (.xlsx) files are allowed",
            )

        # Determine job type based on file extension
        job_type = (
            "competitor_color_excel_parse"
            if file_lower.endswith(".xlsx")
            else "competitor_color_parse"
        )

        # Read file content
        file_content = await file.read()

        # Save file
        uploaded_file = rfq_service.save_uploaded_file(file_content, file.filename)

        # Create job
        job = job_service.create_job(job_type, uploaded_file.file_path, db)

        # Start background processing (using wrapper to avoid blocking)
        run_job_background(job.job_id)

        file_type = "PDF" if job_type == "competitor_color_parse" else "Excel"
        logger.info(
            f"Created competitor color parsing job {job.job_id} for file {file.filename}"
        )

        return CompetitorColorUploadResponse(
            status="success",
            job_id=job.job_id,
            job_type=job_type,
            message=f"Competitor color comparison {file_type} uploaded successfully. Job ID: {job.job_id}. Use GET "
            f"/competitor-color/jobs/{job.job_id} to check status.",
        )

    except HTTPException:
        raise
    except Exception as e:
        file_type = "PDF" if job_type == "competitor_color_parse" else "Excel"
        logger.error(
            f"Error uploading competitor color {file_type}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Error uploading {file_type}: {str(e)}"
        )


@router.get("/jobs/{job_id}", response_model=CompetitorColorJobStatusResponse)
async def get_competitor_color_job_status(job_id: str, db: Session = Depends(get_db)):
    """
    Get competitor color comparison job status and results

    - **job_id**: Job ID from upload response

    Returns job status, progress, and parsed results if completed
    """
    job_dict = job_service.get_job_dict(job_id, db)

    if not job_dict:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return CompetitorColorJobStatusResponse(**job_dict)


@router.get("/jobs", response_model=list[CompetitorColorJobStatusResponse])
async def list_competitor_color_jobs(
    status: str | None = Query(
        None, description="Filter by status (pending, processing, completed, failed)"
    ),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all competitor color comparison jobs with optional filters

    - **status**: Filter by job status
    - **limit**: Maximum number of jobs to return (default: 50)
    - **offset**: Number of jobs to skip (default: 0)
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import Job

    query = db.query(Job).filter(Job.job_type == "competitor_color_parse")

    if status:
        query = query.filter(Job.status == status)

    jobs = query.order_by(Job.created_at.desc()).offset(offset).limit(limit).all()

    return [
        CompetitorColorJobStatusResponse(**job_service.get_job_dict(job.job_id, db))
        for job in jobs
    ]


@router.get("/comparisons", response_model=APIResponse)
async def get_competitor_color_comparisons(
    source_brand: str | None = Query(
        None, description="Filter by source brand (JOTUN or INTERNATIONAL)"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=500, description="Number of items per page"),
    db: Session = Depends(get_db),
):
    """
    Get competitor color comparison data from database

    - **source_brand**: Optional filter by source brand (JOTUN or INTERNATIONAL)
    - **page**: Page number (starts from 1)
    - **page_size**: Number of items per page (max 500)

    Returns paginated list of competitor color comparison records
    """
    try:
        # Calculate offset
        offset = (page - 1) * page_size

        query = db.query(CompetitorColorComparison)

        if source_brand:
            # Normalize brand name
            source_brand = source_brand.upper()
            if source_brand not in ["JOTUN", "INTERNATIONAL"]:
                raise HTTPException(
                    status_code=400,
                    detail="source_brand must be either 'JOTUN' or 'INTERNATIONAL'",
                )
            query = query.filter(CompetitorColorComparison.source_brand == source_brand)

        # Get total count
        total_count = query.count()

        # Get paginated results
        records = (
            query.order_by(
                CompetitorColorComparison.source_brand,
                CompetitorColorComparison.item_no,
            )
            .offset(offset)
            .limit(page_size)
            .all()
        )

        # Calculate pagination metadata
        total_pages = (total_count + page_size - 1) // page_size

        # Convert records to dictionaries for serialization
        records_dict = []
        for record in records:
            record_dict = {
                "id": record.id,
                "item_no": record.item_no,
                "source_brand": record.source_brand,
                "source_code": record.source_code,
                "npms_code": record.npms_code,
                "raw_text": record.raw_text,
                "uploaded_file_id": record.uploaded_file_id,
                "created_at": record.created_at.isoformat()
                if record.created_at
                else None,
            }
            records_dict.append(record_dict)

        return APIResponse(
            success=True,
            message="Competitor color comparisons retrieved successfully",
            data=records_dict,
            meta={
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": offset + page_size < total_count,
                "has_prev": page > 1,
                "source_brand": source_brand,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting competitor color comparisons: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")


@router.get("/comparisons/stats")
async def get_competitor_color_stats(db: Session = Depends(get_db)):
    """
    Get statistics for competitor color comparison data

    Returns:
        - total_records: Total number of records
        - jotun_count: Number of JOTUN records
        - international_count: Number of INTERNATIONAL records
    """
    try:
        total_records = db.query(CompetitorColorComparison).count()

        jotun_count = (
            db.query(CompetitorColorComparison)
            .filter(CompetitorColorComparison.source_brand == "JOTUN")
            .count()
        )

        international_count = (
            db.query(CompetitorColorComparison)
            .filter(CompetitorColorComparison.source_brand == "INTERNATIONAL")
            .count()
        )

        return {
            "total_records": total_records,
            "jotun_count": jotun_count,
            "international_count": international_count,
        }

    except Exception as e:
        logger.error(f"Error getting competitor color stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving stats: {str(e)}")


@router.get("/search/equivalent")
async def search_equivalent_color(
    search: str = Query(
        ...,
        description="Color code or name to search (e.g., 'RAL3000', '80 RED', 'CS-625 SIGNAL RED')",
    ),
    source_brand: str | None = Query(
        None,
        description="Filter by source brand (JOTUN, INTERNATIONAL, SIGMA, HEMPEL, RAL)",
    ),
    db: Session = Depends(get_db),
):
    """
    Search for equivalent Nippon colors from competitor color codes

    This endpoint searches the competitor_color_comparison table to find NPMS color
    equivalents for competitor color codes.

    Args:
        - search: Color code or name to search (partial match supported)
        - source_brand: Optional filter by source brand

    Returns:
        - search_term: The search term used
        - matches: List of matching color mappings with NPMS equivalents
        - available_nippon_colors: Available Nippon colors for each match

    Examples:
        GET /competitor-color/search/equivalent?search=RAL3000
        GET /competitor-color/search/equivalent?search=80+RED&source_brand=JOTUN
        GET /competitor-color/search/equivalent?search=CS-625
    """
    try:
        from apps.app_nippon_rfq_matching.app.models.database import ProductMaster

        search_term = search.strip()
        if not search_term:
            raise HTTPException(status_code=400, detail="Search term is required")

        # Build query for competitor color comparison
        query = db.query(CompetitorColorComparison)

        # Apply source brand filter if provided
        if source_brand:
            source_brand = source_brand.upper()
            query = query.filter(CompetitorColorComparison.source_brand == source_brand)

        # Search in source_code field (case-insensitive partial match)
        query = query.filter(
            CompetitorColorComparison.source_code.ilike(f"%{search_term}%")
        )

        # Get matches
        matches = (
            query.order_by(
                CompetitorColorComparison.source_brand,
                CompetitorColorComparison.item_no,
            )
            .limit(50)
            .all()
        )

        if not matches:
            return {
                "search_term": search_term,
                "matches": [],
                "message": f"No matches found for '{search_term}'",
            }

        # Get available Nippon colors for each match
        results = []
        for match in matches:
            # Get available Nippon colors that have this NPMS code
            nippon_colors = (
                db.query(ProductMaster.color)
                .filter(ProductMaster.color.ilike(f"%{match.npms_code}%"))
                .distinct()
                .all()
            )

            # Also check for exact color match
            exact_colors = (
                db.query(ProductMaster.color)
                .filter(ProductMaster.color == match.npms_code)
                .distinct()
                .all()
            )

            # Count products with this color
            color_count = (
                db.query(ProductMaster)
                .filter(ProductMaster.color.ilike(f"%{match.npms_code}%"))
                .count()
            )

            results.append(
                {
                    "source_brand": match.source_brand,
                    "source_code": match.source_code,
                    "npms_code": match.npms_code,
                    "item_no": match.item_no,
                    "exact_match": len(exact_colors) > 0,
                    "nippon_color_count": color_count,
                    "available_nippon_colors": [c[0] for c in nippon_colors if c[0]],
                }
            )

        return {
            "search_term": search_term,
            "matches": results,
            "total_matches": len(results),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching equivalent colors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching colors: {str(e)}")


@router.get("/search/nippon-colors")
async def search_nippon_colors(
    search: str = Query(
        ...,
        description="Color name or code to search (e.g., 'SIGNAL RED', '060 GRAY', 'GREEN')",
    ),
    product_name: str | None = Query(
        None, description="Filter by product name (e.g., 'O-MARINE FINISH', 'A-MARINE')"
    ),
    db: Session = Depends(get_db),
):
    """
    Search for available Nippon colors

    This endpoint searches for Nippon colors in the product_master table,
    optionally filtered by product name.

    Args:
        - search: Color name or code to search (partial match supported)
        - product_name: Optional filter by product name

    Returns:
        - search_term: The search term used
        - colors: List of matching Nippon colors
        - product_filter: Product name filter applied (if any)

    Examples:
        GET /competitor-color/search/nippon-colors?search=SIGNAL+RED
        GET /competitor-color/search/nippon-colors?search=GREEN&product_name=O-MARINE
        GET /competitor-color/search/nippon-colors?search=060
    """
    try:
        from apps.app_nippon_rfq_matching.app.models.database import ProductMaster

        search_term = search.strip()
        if not search_term:
            raise HTTPException(status_code=400, detail="Search term is required")

        # Build query for colors
        color_query = db.query(ProductMaster.color).filter(
            ProductMaster.color.isnot(None),
            ProductMaster.color != "",
            ProductMaster.color.ilike(f"%{search_term}%"),
        )

        # Apply product name filter if provided
        if product_name:
            # Support both exact and LIKE match for product name
            # Use subquery to get colors for specific product

            subquery = (
                db.query(ProductMaster.color)
                .filter(
                    ProductMaster.product_name.ilike(f"%{product_name}%"),
                    ProductMaster.color.ilike(f"%{search_term}%"),
                )
                .distinct()
            )

            colors = subquery.order_by(ProductMaster.color).limit(50).all()
        else:
            # Get distinct colors
            colors = (
                color_query.distinct().order_by(ProductMaster.color).limit(50).all()
            )

        if not colors:
            return {
                "search_term": search_term,
                "product_filter": product_name,
                "colors": [],
                "message": f"No colors found matching '{search_term}'",
            }

        # Get color details with product count
        color_details = []
        for color in colors:
            color_name = color[0]

            # Count products with this color
            count_query = db.query(ProductMaster).filter(
                ProductMaster.color == color_name
            )

            if product_name:
                count_query = count_query.filter(
                    ProductMaster.product_name.ilike(f"%{product_name}%")
                )

            product_count = count_query.count()

            color_details.append({"color": color_name, "product_count": product_count})

        return {
            "search_term": search_term,
            "product_filter": product_name,
            "colors": color_details,
            "total_colors": len(color_details),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching Nippon colors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching colors: {str(e)}")
