"""
API endpoints for file upload and processing
"""

from datetime import datetime
from typing import Any

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    Request,
    UploadFile,
)
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.api.security import get_current_user
from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.core.logging_config import get_logger
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.rfq import UploadedFile
from apps.app_nippon_rfq_matching.app.models.schemas import (
    APIErrorResponse,
    APIResponse,
)
from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
    matching_data_service,
)
from apps.app_nippon_rfq_matching.app.services.quotation_service import (
    quotation_service,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service
from apps.app_nippon_rfq_matching.app.services.ticket_generation_service import (
    ticket_generation_service,
)

router = APIRouter(prefix="/upload", tags=["upload"])

logger = get_logger(__name__)


def handle_upload_error(
    error: Exception, error_code: str = "UPLOAD_ERROR", message: str = "Upload failed"
):
    """Handle upload errors with proper logging and response format"""
    logger.error(f"Upload Error [{error_code}]: {str(error)}", exc_info=True)

    error_details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "error_code": error_code,
        "timestamp": datetime.now().isoformat(),
    }

    raise HTTPException(
        status_code=500,
        detail=APIErrorResponse(
            message=message,
            error=str(error),
            error_code=error_code,
            details=error_details,
        ).dict(exclude_none=True),
    )


@router.post("/iatp-product-master", response_model=APIResponse)
async def upload_iatp_product_master(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Upload and parse IATP Excel file (Product Master)

    - **file**: Excel file (.xlsx or .xls) containing IATP product data
    """
    try:
        # Check if database is available
        if db is None:
            raise HTTPException(
                status_code=503,
                detail=APIErrorResponse(
                    message="Database is not available",
                    error="SERVICE_UNAVAILABLE",
                    error_code="DATABASE_NOT_AVAILABLE",
                ).dict(exclude_none=True),
            )

        # Validate file extension
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=400,
                detail=APIErrorResponse(
                    message="Only Excel files (.xlsx, .xls) are allowed",
                    error="INVALID_FILE_TYPE",
                    error_code="INVALID_FILE_TYPE",
                ).dict(exclude_none=True),
            )

        # Read file content
        content = await file.read()

        # Save uploaded file
        uploaded_file = rfq_service.save_uploaded_file(content, file.filename)
        db.add(uploaded_file)
        db.commit()
        db.refresh(uploaded_file)

        # Process Excel file (async - matching reload runs in background)
        try:
            result = await rfq_service.process_excel_upload(
                uploaded_file.file_path, uploaded_file.id, db
            )

            # Update file status
            uploaded_file.status = "parsed"
            db.commit()

            # Format response
            response = APIResponse(
                success=True,
                message="Excel file processed successfully",
                data=result,
                meta={
                    "uploaded_file_id": uploaded_file.id,
                    "filename": uploaded_file.original_filename,
                    "processed_at": datetime.now().isoformat(),
                },
            )

            return response

        except Exception as e:
            uploaded_file.status = "error"
            uploaded_file.error_message = str(e)
            db.commit()
            handle_upload_error(
                e, "EXCEL_PROCESSING_ERROR", "Error processing Excel file"
            )

    except HTTPException:
        raise
    except Exception as e:
        handle_upload_error(e, "UPLOAD_ERROR", "Error uploading file")


@router.post("/iatp-product-master/multi-region", response_model=APIResponse)
async def upload_iatp_product_master_multi_region(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Upload and parse IATP Excel file with multi-region pricing support

    - **file**: Excel file (.xlsx or .xls) containing IATP product data with multi-region pricing
    """
    try:
        # Check if database is available
        if db is None:
            raise HTTPException(
                status_code=503,
                detail=APIErrorResponse(
                    message="Database is not available",
                    error="SERVICE_UNAVAILABLE",
                    error_code="DATABASE_NOT_AVAILABLE",
                ).dict(exclude_none=True),
            )

        # Validate file extension
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=400,
                detail=APIErrorResponse(
                    message="Only Excel files (.xlsx, .xls) are allowed",
                    error="INVALID_FILE_TYPE",
                    error_code="INVALID_FILE_TYPE",
                ).dict(exclude_none=True),
            )

        # Read file content
        content = await file.read()

        # Save uploaded file
        uploaded_file = rfq_service.save_uploaded_file(content, file.filename)
        db.add(uploaded_file)
        db.commit()
        db.refresh(uploaded_file)

        # Process Excel file with multi-region pricing
        try:
            result = await rfq_service.process_iatp_excel_with_multi_region_and_pricing(
                uploaded_file.file_path, uploaded_file.id, db
            )

            # Update file status
            uploaded_file.status = "parsed"
            db.commit()

            # Convert the result to proper response format
            # Extract product master records that were created/updated
            product_master_records = []
            for product in result["products"]:
                product_master = (
                    db.query(ProductMaster)
                    .filter(ProductMaster.pmc == product["product_code"])
                    .first()
                )
                if product_master:
                    product_master_records.append(product_master)

            # Convert records to dict and handle datetime serialization
            product_responses = []
            for p in product_master_records:
                product_dict = {
                    "id": p.id,
                    "sheet_name": p.sheet_name,
                    "sheet_type": p.sheet_type,
                    "row_excel": p.row_excel,
                    "pmc": p.pmc,
                    "product_name": p.product_name,
                    "color": p.color,
                    "clean_product_name": p.clean_product_name,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                }
                product_responses.append(product_dict)

            # Format response
            response = APIResponse(
                success=True,
                message="Excel file with multi-region pricing processed successfully",
                data={
                    "status": result["status"],
                    "products_count": len(product_master_records),
                    "products": product_responses,
                    "summary": result.get("summary", {}),
                },
                meta={
                    "uploaded_file_id": uploaded_file.id,
                    "filename": uploaded_file.original_filename,
                    "processed_at": datetime.now().isoformat(),
                    "regions_inserted": result.get("regions_inserted", 0),
                    "pricing_records_inserted": result.get(
                        "pricing_records_inserted", 0
                    ),
                },
            )

            return response

        except Exception as e:
            uploaded_file.status = "error"
            uploaded_file.error_message = str(e)
            db.commit()
            handle_upload_error(
                e,
                "MULTI_REGION_EXCEL_PROCESSING_ERROR",
                "Error processing Excel file with multi-region pricing",
            )

    except HTTPException:
        raise
    except Exception as e:
        handle_upload_error(e, "UPLOAD_ERROR", "Error uploading file")


@router.post("/rfq", response_model=APIResponse)
async def upload_rfq(
    file: UploadFile = File(...),
    rfq_id: str | None = None,
    db: Session = Depends(get_db),
    request: Request = None,  # For security dependency
    user: dict = Depends(get_current_user),
):
    """
    Upload and parse RFQ file (PDF, Excel, or EML)

    - **file**: PDF file, Excel file (CERU/ZO Paint/Indonesia formats), or EML email file containing RFQ data
    - **rfq_id**: Optional RFQ identifier (extracted from filename/email if not provided)

    The RFQ ID is extracted from the filename by default.
    For PDF: "RFQ 000000709025.pdf" → "RFQ-000000709025"
    For Excel: Auto-detects format (CERU, ZO Paint, Indonesia) → "RFQ-{filename}"
    For EML: Extracted from email subject (e.g., "NPM250005566" → "RFQ-NPM250005566")

    Supported Excel formats:
    - CERU: Cerulean vessel RFQ format
    - ZO Paint: Store request form with paint items
    - Indonesia: Indonesian vessel RFQ with Part Number and Description
    """
    is_pdf = file.filename.endswith(".pdf")
    is_excel = file.filename.endswith((".xlsx", ".xls"))
    is_eml = file.filename.endswith(".eml")

    # Generate RFQ ID from filename if not provided
    if not rfq_id:
        # Extract RFQ ID from filename
        # Remove extension and clean up the name
        if is_pdf:
            name_without_ext = file.filename.replace(".pdf", "").strip()
        elif is_eml:
            # For EML, don't generate from filename - let the parser extract from email subject
            name_without_ext = None
        else:
            name_without_ext = (
                file.filename.replace(".xlsx", "").replace(".xls", "").strip()
            )

        # Add RFQ- prefix if not present (except for EML which will be extracted from email)
        if name_without_ext and not name_without_ext.upper().startswith("RFQ-"):
            name_without_ext = f"RFQ-{name_without_ext}"

        # Replace spaces with hyphens
        if name_without_ext:
            rfq_id = name_without_ext.replace(" ", "-").upper()

    try:
        # Read file content
        content = await file.read()

        # Log authenticated user
        user_data = user.get("data", {}).get("user", {})
        logger.info(
            f"RFQ file upload initiated by user: {user_data.get('email', 'Unknown')} (ID: "
            f"{user_data.get('id', 'Unknown')}) - File: {file.filename}"
        )

        # Save uploaded file
        uploaded_file = rfq_service.save_uploaded_file(content, file.filename)
        db.add(uploaded_file)
        db.commit()
        db.refresh(uploaded_file)

        # Process file based on type
        try:
            if is_excel:
                # Process Excel with auto-detection (CERU, ZO Paint, Indonesia)
                result = await rfq_service.process_routed_excel_upload(
                    uploaded_file.file_path, rfq_id, uploaded_file.id, db
                )
            elif is_eml:
                # Process EML email file
                result = await rfq_service.process_eml_upload(
                    uploaded_file.file_path, rfq_id, uploaded_file.id, db
                )
            else:
                # Process PDF RFQ file
                result = await rfq_service.process_pdf_upload(
                    uploaded_file.file_path, rfq_id, uploaded_file.id, db
                )

            # Update file status
            uploaded_file.status = "parsed"
            db.commit()

            # Format response using enterprise format
            response = APIResponse(
                success=True,
                message="RFQ file processed successfully",
                data=result,
                meta={
                    "uploaded_file_id": uploaded_file.id,
                    "filename": uploaded_file.original_filename,
                    "processed_at": datetime.now().isoformat(),
                },
            )
            return response

        except Exception as e:
            uploaded_file.status = "error"
            uploaded_file.error_message = str(e)
            db.commit()
            raise HTTPException(
                status_code=500, detail=f"Error processing file: {str(e)}"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/files")
async def list_uploaded_files(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db)
):
    """
    List all uploaded files
    """
    files = db.query(UploadedFile).offset(skip).limit(limit).all()
    return APIResponse(
        success=True,
        message=f"Retrieved {len(files)} uploaded files",
        data={"count": len(files), "files": [f.to_dict() for f in files]},
        meta={"skip": skip, "limit": limit, "total_returned": len(files)},
    )


@router.get("/files/{file_id}")
async def get_uploaded_file(file_id: int, db: Session = Depends(get_db)):
    """
    Get details of a specific uploaded file
    """
    file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not file:
        raise HTTPException(
            status_code=404,
            detail=APIErrorResponse(
                message="File not found", error="NOT_FOUND", error_code="FILE_NOT_FOUND"
            ).dict(exclude_none=True),
        )
    return APIResponse(
        success=True,
        message="File details retrieved successfully",
        data=file.to_dict(),
        meta={"file_id": file_id},
    )


@router.post("/rfq/parse-only", response_model=APIResponse)
async def upload_rfq_parse_only(
    file: UploadFile = File(...),
    rfq_id: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Upload RFQ file for parsing and insertion only (NO MATCHING)

    This endpoint creates a background job that:
    1. Parses the RFQ file (PDF, Excel, or EML)
    2. Inserts RFQ items into database
    3. Saves to CSV

    Does NOT perform product matching. Use this when you only want to
    store RFQ data without matching.

    Supported formats:
    - PDF files
    - Excel files: CERU format, ZO Paint format, Indonesia format
    - EML email files with embedded product tables

    - **file**: PDF file, Excel file (.xlsx, .xls), or EML file containing RFQ data
    - **rfq_id**: Optional RFQ identifier (extracted from filename/email if not provided)

    Returns job_id for tracking processing status.

    The RFQ ID is extracted from the filename by default.
    For PDF: "RFQ 000000709025.pdf" → "RFQ-000000709025"
    For Excel: "CERU-D-25-096.xlsx" → "RFQ-CERU-D-25-096"
    For EML: Extracted from email subject (e.g., "NPM250005566" → "RFQ-NPM250005566")

    Use GET /jobs/{job_id} to check job status and results.
    """
    from apps.app_nippon_rfq_matching.app.services.job_service import (
        job_service,
        run_rfq_parse_only_job,
    )
    from apps.app_nippon_rfq_matching.app.utils.eml_parser import (
        process_eml_for_products,
    )

    is_pdf = file.filename.endswith(".pdf")
    is_excel = file.filename.endswith((".xlsx", ".xls"))
    is_eml = file.filename.endswith(".eml")

    # Validate file extension
    if not is_pdf and not is_excel and not is_eml:
        raise HTTPException(
            status_code=400,
            detail="Only PDF files, Excel files (.xlsx, .xls), and EML files are allowed",
        )

    # Initialize name_without_ext
    name_without_ext = None

    # Read file content first
    content = await file.read()

    # Generate RFQ ID from filename if not provided
    if not rfq_id:
        # Extract RFQ ID from filename
        # Remove extension and clean up the name
        if is_pdf:
            name_without_ext = file.filename.replace(".pdf", "").strip()
        elif is_eml:
            # For EML, try to extract RFQ ID from email subject first
            try:
                # Process EML to extract RFQ ID from subject
                import os
                import tempfile

                # Save file temporarily to parse it
                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=".eml"
                ) as tmp_file:
                    tmp_file.write(content)
                    tmp_file_path = tmp_file.name

                try:
                    eml_result = process_eml_for_products(tmp_file_path)
                    extracted_rfq_id = eml_result.get("rfq_id")
                    if extracted_rfq_id:
                        rfq_id = extracted_rfq_id
                    else:
                        # If no RFQ ID found in email, use filename as fallback
                        name_without_ext = file.filename.replace(".eml", "").strip()
                        rfq_id = name_without_ext.replace(" ", "-").upper()
                finally:
                    # Clean up temporary file
                    os.unlink(tmp_file_path)
            except Exception:
                # If parsing fails, use filename as fallback
                name_without_ext = file.filename.replace(".eml", "").strip()
                rfq_id = name_without_ext.replace(" ", "-").upper()
        else:
            name_without_ext = (
                file.filename.replace(".xlsx", "").replace(".xls", "").strip()
            )

        # Add RFQ- prefix if not present (except for EML which might already have RFQ- from extraction)
        if name_without_ext and not name_without_ext.upper().startswith("RFQ-"):
            name_without_ext = f"RFQ-{name_without_ext}"

        # Replace spaces with hyphens (only if we're using filename-based generation)
        if name_without_ext and not is_eml:
            rfq_id = name_without_ext.replace(" ", "-").upper()
    else:
        # If rfq_id was provided, generate name_without_ext for consistency
        if is_pdf:
            name_without_ext = file.filename.replace(".pdf", "").strip()
        elif is_excel:
            name_without_ext = (
                file.filename.replace(".xlsx", "").replace(".xls", "").strip()
            )
        elif is_eml:
            name_without_ext = file.filename.replace(".eml", "").strip()

        # Add RFQ- prefix if not present
        if name_without_ext and not name_without_ext.upper().startswith("RFQ-"):
            name_without_ext = f"RFQ-{name_without_ext}"

    try:
        # Save uploaded file (we already have content)

        # Save uploaded file
        uploaded_file = rfq_service.save_uploaded_file(content, file.filename)
        db.add(uploaded_file)
        db.commit()
        db.refresh(uploaded_file)

        # Create job for parse-only processing
        job = job_service.create_job("rfq_parse_only", uploaded_file.file_path, db)

        # Start background processing with rfq_id parameter
        run_rfq_parse_only_job(job.job_id, rfq_id)

        response = APIResponse(
            success=True,
            message=f"RFQ file uploaded successfully. Parse and insert job created. Job ID: {job.job_id}. Use GET "
            f"/jobs/{job.job_id} to check status.",
            data={
                "status": "success",
                "job_id": job.job_id,
                "job_type": "rfq_parse_only",
                "rfq_id": rfq_id,
            },
            meta={"job_id": job.job_id, "job_type": "rfq_parse_only", "rfq_id": rfq_id},
        )
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/rfq/{rfq_id}/matching-data", response_model=dict[str, Any])
async def get_rfq_matching_data(
    rfq_id: str, region: str = "Indonesia", db: Session = Depends(get_db)
):
    """
    Get complete matching data for an RFQ including product details and pricing.

    This endpoint extracts matching data from RFQ items and enriches it with
    product master data including color, price, and product code from ProductMaster.

    Args:
        rfq_id: RFQ identifier
        region: Region name for pricing (default: "Indonesia")
        db: Database session

    Returns:
        Complete matching data with product details
    """
    try:
        # Get RFQ items
        from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem

        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        if not rfq_items:
            raise HTTPException(status_code=404, detail=f"RFQ ID '{rfq_id}' not found")

        # Convert to dict
        rfq_items_dict = [item.to_dict() for item in rfq_items]

        # Get normalized items using the PDF comparison service
        from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
            PDFExporter,
        )

        pdf_exporter = PDFExporter()

        # Normalize items
        normalized_items = pdf_exporter._get_normalized_items(
            rfq_items_dict, use_normalization=True, db=db
        )

        # Find matches
        matches = pdf_exporter.find_product_matches(normalized_items, db)

        # Enrich with product data
        enriched_matches = matching_data_service.enrich_matches_with_product_data(
            matches, db
        )

        # Separate matched and unmatched items
        matched_items = []
        unmatched_items = []

        for match in enriched_matches:
            if match["product_master"] and match["product_master"].get("id"):
                matched_items.append(match)
            else:
                unmatched_items.append(match)

        # Get statistics
        stats = {
            "total_items": len(rfq_items),
            "matched_count": len(matched_items),
            "unmatched_count": len(unmatched_items),
            "match_rate": round(len(matched_items) / len(rfq_items) * 100, 2)
            if rfq_items
            else 0,
            "color_matches": sum(
                1 for m in matched_items if m["product_master"].get("color")
            ),
            "price_matches": sum(
                1 for m in matched_items if m["product_master"].get("pricing")
            ),
        }

        return {
            "success": True,
            "rfq_id": rfq_id,
            "region": region,
            "statistics": stats,
            "matched_items": matched_items,
            "unmatched_items": unmatched_items,
            "has_enhanced_data": True,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error getting matching data for RFQ {rfq_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Error retrieving matching data: {str(e)}"
        )


@router.post("/rfq/{rfq_id}/generate-tickets", response_model=dict[str, Any])
async def generate_rfq_tickets(
    rfq_id: str,
    region: str = "Indonesia",
    priority: str = "medium",
    assignee: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Generate tickets for unmatched items in an RFQ.

    This endpoint creates tickets for all RFQ items that couldn't be matched
    with products in the Product Master, allowing manual review and processing.

    Args:
        rfq_id: RFQ identifier
        region: Region name for context (default: "Indonesia")
        priority: Default priority for tickets (default: "medium")
        assignee: Person to assign tickets to
        db: Database session

    Returns:
        Generation result with statistics and created tickets
    """
    try:
        # Generate and save tickets
        result = ticket_generation_service.generate_and_save_tickets(
            rfq_id=rfq_id, db=db, region=region
        )

        if not result["success"]:
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Failed to generate tickets"),
            )

        return {
            "success": True,
            "message": f"Generated {len(result['tickets'])} tickets for RFQ {rfq_id}",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating tickets for RFQ {rfq_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error generating tickets: {str(e)}"
        )


@router.post("/rfq/{rfq_id}/quotation", response_model=dict[str, Any])
async def generate_quotation(
    rfq_id: str,
    region: str = "Indonesia",
    format_type: str = "table",
    db: Session = Depends(get_db),
):
    """
    Generate quotation PDF for an RFQ using matched Product Master data.

    This endpoint creates a quotation with pricing, item codes, and colors
    from the Product Master for matched items.

    Args:
        rfq_id: RFQ identifier
        region: Region for pricing (default: "Indonesia")
        format_type: PDF format type (table, side_by_side, summary)
        db: Database session

    Returns:
        Quotation generation result
    """
    try:
        # Validate format type
        valid_formats = ["table", "side_by_side", "summary"]
        if format_type not in valid_formats:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid format_type '{format_type}'. Must be one of: {valid_formats}",
            )

        # Generate quotation PDF
        result = quotation_service.generate_quotation_pdf(
            rfq_id=rfq_id, region=region, format_type=format_type, db=db
        )

        if not result["success"]:
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Failed to generate quotation"),
            )

        return {
            "success": True,
            "message": f"Quotation generated successfully for RFQ {rfq_id}",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating quotation for RFQ {rfq_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error generating quotation: {str(e)}"
        )


@router.get("/rfq/{rfq_id}/quotation", response_model=dict[str, Any])
async def get_quotation_data(
    rfq_id: str, region: str = "Indonesia", db: Session = Depends(get_db)
):
    """
    Get quotation data for an RFQ.

    Args:
        rfq_id: RFQ identifier
        region: Region for pricing (default: "Indonesia")
        db: Database session

    Returns:
        Quotation data with matched items from Product Master
    """
    try:
        # Prepare quotation data
        quotation_data = quotation_service.prepare_quotation_data(
            rfq_id=rfq_id, region=region, db=db
        )

        if not quotation_data["success"]:
            raise HTTPException(
                status_code=404,
                detail=quotation_data.get("error", "Failed to get quotation data"),
            )

        return {
            "success": True,
            "message": f"Quotation data retrieved successfully for RFQ {rfq_id}",
            "data": quotation_data,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error getting quotation data for RFQ {rfq_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Error getting quotation data: {str(e)}"
        )


@router.get("/rfq/{rfq_id}/quotation/pricing-options", response_model=dict[str, Any])
async def get_quotation_pricing_options(rfq_id: str, db: Session = Depends(get_db)):
    """
    Get available pricing options for an RFQ quotation.

    Args:
        rfq_id: RFQ identifier
        db: Database session

    Returns:
        Available pricing options by region
    """
    try:
        # Get pricing options
        result = quotation_service.get_quotation_pricing_options(rfq_id=rfq_id, db=db)

        if not result["success"]:
            raise HTTPException(
                status_code=404,
                detail=result.get("error", "Failed to get pricing options"),
            )

        return {
            "success": True,
            "message": f"Pricing options retrieved successfully for RFQ {rfq_id}",
            "data": result,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error getting pricing options for RFQ {rfq_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Error getting pricing options: {str(e)}"
        )
