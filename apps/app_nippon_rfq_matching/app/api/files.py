"""
API endpoints for uploaded files management and result retrieval
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem, RFQMatch, UploadedFile
from apps.app_nippon_rfq_matching.app.models.schemas import (
    ProductMasterUsageResponse,
    RFQItemsWithMatchesResponse,
    UploadedFileResponse,
)

router = APIRouter(prefix="/files", tags=["files"])
logger = logging.getLogger(__name__)


# ============== UPLOAD FILES MANAGEMENT ==============


@router.get("/uploaded/rfq", response_model=list[UploadedFileResponse])
async def list_uploaded_rfq_files(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all uploaded RFQ (PDF) files with relationship info

    - **status**: Filter by status (pending, parsed, error)
    - **limit**: Maximum number of files to return
    - **offset**: Number of files to skip
    """
    query = db.query(UploadedFile).filter(UploadedFile.file_type == "pdf")

    if status:
        query = query.filter(UploadedFile.status == status)

    files = (
        query.order_by(UploadedFile.created_at.desc()).offset(offset).limit(limit).all()
    )

    # Convert to response format with relationship info
    result = []
    for f in files:
        # Count RFQ items for this file
        rfq_items_count = (
            db.query(RFQItem).filter(RFQItem.uploaded_file_id == f.id).count()
        )

        # Get unique RFQ IDs
        rfq_ids = (
            db.query(RFQItem.rfq_id)
            .filter(RFQItem.uploaded_file_id == f.id)
            .distinct()
            .all()
        )
        rfq_ids_list = [r[0] for r in rfq_ids]

        result.append(
            {
                "id": f.id,
                "original_filename": f.original_filename,
                "stored_filename": f.stored_filename,
                "file_type": f.file_type,
                "status": f.status,
                "error_message": f.error_message,
                "created_at": f.created_at.isoformat() if f.created_at else None,
                "stats": {"rfq_items_count": rfq_items_count, "rfq_ids": rfq_ids_list},
            }
        )

    return result


@router.get("/rfq-ids")
async def list_all_rfq_ids(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all unique RFQ IDs available for PDF generation

    Returns all RFQ IDs that have been processed and can be used
    for generating PDF comparison reports.

    - **limit**: Maximum number of RFQ IDs to return
    - **offset**: Number of RFQ IDs to skip (for pagination)
    """
    # Get unique RFQ IDs

    rfq_ids = (
        db.query(RFQItem.rfq_id)
        .distinct()
        .order_by(RFQItem.rfq_id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    rfq_ids_list = [r[0] for r in rfq_ids]

    # Get total count for pagination info
    total_count = db.query(RFQItem.rfq_id).distinct().count()

    # Get additional info for each RFQ ID
    result = []
    for rfq_id in rfq_ids_list:
        # Count items
        items_count = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).count()

        # Count matches
        matches_count = (
            db.query(RFQMatch)
            .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
            .filter(RFQItem.rfq_id == rfq_id)
            .count()
        )

        # Get average score
        matches = (
            db.query(RFQMatch)
            .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
            .filter(RFQItem.rfq_id == rfq_id)
            .all()
        )
        avg_score = sum(m.score for m in matches) / len(matches) if matches else 0

        # Get file info
        file_info = (
            db.query(UploadedFile)
            .join(RFQItem, RFQItem.uploaded_file_id == UploadedFile.id)
            .filter(RFQItem.rfq_id == rfq_id)
            .first()
        )

        result.append(
            {
                "rfq_id": rfq_id,
                "items_count": items_count,
                "matches_count": matches_count,
                "average_score": round(avg_score, 2),
                "source_file": file_info.original_filename if file_info else None,
            }
        )

    # Calculate next page offset
    next_offset = offset + limit if offset + limit < total_count else None

    return {
        "total": total_count,
        "count": len(result),
        "offset": offset,
        "limit": limit,
        "next_offset": next_offset,
        "rfq_ids": result,
    }


@router.get("/uploaded/product-master", response_model=list[dict])
async def list_uploaded_product_master_files(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    List all uploaded Product Master (Excel) files with relationship info

    - **status**: Filter by status (pending, parsed, error)
    - **limit**: Maximum number of files to return
    - **offset**: Number of files to skip
    """
    query = db.query(UploadedFile).filter(UploadedFile.file_type == "excel")

    if status:
        query = query.filter(UploadedFile.status == status)

    files = (
        query.order_by(UploadedFile.created_at.desc()).offset(offset).limit(limit).all()
    )

    # Convert to response format with relationship info
    result = []
    for f in files:
        # Count products for this file
        products_count = (
            db.query(ProductMaster)
            .filter(ProductMaster.uploaded_file_id == f.id)
            .count()
        )

        # Get RFQs that matched with products from this file
        matched_rfqs = (
            db.query(RFQItem.rfq_id)
            .join(RFQMatch, RFQMatch.rfq_item_id == RFQItem.id)
            .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
            .filter(ProductMaster.uploaded_file_id == f.id)
            .distinct()
            .all()
        )

        rfq_ids_list = [r[0] for r in matched_rfqs]

        result.append(
            {
                "id": f.id,
                "original_filename": f.original_filename,
                "stored_filename": f.stored_filename,
                "file_type": f.file_type,
                "status": f.status,
                "error_message": f.error_message,
                "created_at": f.created_at.isoformat() if f.created_at else None,
                "stats": {
                    "products_count": products_count,
                    "matched_rfqs": rfq_ids_list,
                    "matched_rfqs_count": len(rfq_ids_list),
                },
            }
        )

    return result


@router.post("/match-rfq/{rfq_id}")
async def trigger_matching_for_rfq(rfq_id: str, db: Session = Depends(get_db)):
    """
    Trigger matching process for an existing RFQ

    This endpoint performs matching for RFQ items that don't have matches yet.
    Useful for re-running matching after uploading new Product Master data.

    - **rfq_id**: RFQ identifier to match
    """
    from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service

    # Get RFQ items
    rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

    if not rfq_items:
        raise HTTPException(status_code=404, detail=f"RFQ {rfq_id} not found")

    # Check if matches already exist
    existing_matches = (
        db.query(RFQMatch)
        .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
        .filter(RFQItem.rfq_id == rfq_id)
        .count()
    )

    # Delete existing matches if any (to re-match)
    if existing_matches > 0:
        db.query(RFQMatch).filter(
            RFQMatch.rfq_item_id.in_([item.id for item in rfq_items])
        ).delete(synchronize_session=False)
        db.commit()
        logger.info(f"Deleted {existing_matches} existing matches for RFQ {rfq_id}")

    # Convert RFQItem to dict format for matching
    rfq_items_dict = [
        {
            "raw_text": item.raw_text,
            "clean_text": item.clean_text,
            "qty": item.qty,
            "uom": item.uom,
            "source": item.source,
        }
        for item in rfq_items
    ]

    # Perform matching
    try:
        matches = rfq_service.perform_matching(rfq_items_dict, db)

        # Save matches to database
        if matches:
            rfq_service.save_matches_to_db(matches, rfq_items, db)
            logger.info(f"Created {len(matches)} matches for RFQ {rfq_id}")

        return {
            "status": "success",
            "rfq_id": rfq_id,
            "rfq_items_count": len(rfq_items),
            "matches_created": len(matches),
            "message": f"Successfully matched {len(rfq_items)} RFQ items, created {len(matches)} matches",
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error during matching: {str(e)}")


@router.post("/fix-rfq-ids")
async def fix_rfq_ids_from_filenames(db: Session = Depends(get_db)):
    """
    Update RFQ IDs to match their filenames

    This endpoint updates all RFQ items to use RFQ IDs based on
    their source filenames instead of randomly generated IDs.

    Example: "RFQ 000000709025.pdf" → RFQ ID: "RFQ-000000709025"
    """

    # Get all uploaded PDF files
    files = db.query(UploadedFile).filter(UploadedFile.file_type == "pdf").all()

    updated_count = 0
    errors = []

    for f in files:
        try:
            # Extract RFQ ID from filename
            name_without_ext = f.original_filename.replace(".pdf", "").strip().upper()

            # Ensure RFQ- prefix
            if not name_without_ext.startswith("RFQ"):
                name_without_ext = f"RFQ-{name_without_ext}"

            # Replace spaces with hyphens
            new_rfq_id = name_without_ext.replace(" ", "-")

            # Ensure hyphen after RFQ
            if new_rfq_id.startswith("RFQ-") is False:
                new_rfq_id = new_rfq_id.replace("RFQ", "RFQ-")

            # Get current RFQ IDs for this file
            current_rfqs = (
                db.query(RFQItem.rfq_id)
                .filter(RFQItem.uploaded_file_id == f.id)
                .distinct()
                .all()
            )
            current_rfq_ids = [r[0] for r in current_rfqs]

            if current_rfq_ids:
                old_rfq_id = current_rfq_ids[0]

                # Skip if already correct
                if old_rfq_id == new_rfq_id:
                    continue

                # Update all RFQ items with the new RFQ ID
                db.query(RFQItem).filter(
                    RFQItem.uploaded_file_id == f.id, RFQItem.rfq_id == old_rfq_id
                ).update({"rfq_id": new_rfq_id})

                updated_count += 1
                logger.info(
                    f"Updated RFQ ID from '{old_rfq_id}' to '{new_rfq_id}' for file {f.original_filename}"
                )

        except Exception as e:
            errors.append({"file": f.original_filename, "error": str(e)})
            logger.error(f"Error updating RFQ ID for file {f.original_filename}: {e}")

    db.commit()

    return {
        "status": "completed",
        "updated_count": updated_count,
        "errors": errors,
        "message": f"Updated {updated_count} RFQ IDs to match filenames",
    }


@router.get("/uploaded/{file_id}", response_model=UploadedFileResponse)
async def get_uploaded_file(file_id: int, db: Session = Depends(get_db)):
    """
    Get details of a specific uploaded file

    - **file_id**: File ID
    """
    file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()

    if not file:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    return {
        "id": file.id,
        "original_filename": file.original_filename,
        "stored_filename": file.stored_filename,
        "file_type": file.file_type,
        "status": file.status,
        "error_message": file.error_message,
        "created_at": file.created_at.isoformat() if file.created_at else None,
    }


# ============== RFQ RESULTS ==============


@router.get("/rfq/{rfq_id}/results", response_model=RFQItemsWithMatchesResponse)
async def get_rfq_results(
    rfq_id: str,
    include_competitor: bool = Query(
        False, description="Include competitor matching results"
    ),
    max_results: int = Query(20, ge=1, le=100, description="Maximum results per item"),
    db: Session = Depends(get_db),
):
    """
    Get RFQ items and matches for a specific RFQ ID

    - **rfq_id**: RFQ identifier
    - **include_competitor**: If True, include competitor product matching results
    - **max_results**: Maximum number of results per RFQ item

    Returns RFQ items with their matched products
    """
    # Get RFQ items
    rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

    if not rfq_items:
        raise HTTPException(status_code=404, detail=f"RFQ {rfq_id} not found")

    # If competitor matching requested, use the new service
    if include_competitor:
        from apps.app_nippon_rfq_matching.app.services.rfq_competitor_matching import (
            RFQCompetitorMatcher,
        )

        matcher = RFQCompetitorMatcher(db)
        competitor_results = matcher.match_rfq_by_id(
            rfq_id, max_results_per_item=max_results
        )

        # Build results in the expected format
        results = []
        for item_match in competitor_results["matches"]:
            # Build match list from nippon_matches
            match_list = []
            for nippon_match in item_match["nippon_matches"]:
                match_list.append(
                    {
                        "rfq": {
                            "raw_text": item_match["raw_text"],
                            "clean_text": item_match["clean_text"],
                            "qty": item_match["qty"],
                            "uom": item_match["uom"],
                            "source": "",  # Not available in competitor match result
                        },
                        "product_master": {
                            "id": nippon_match["id"],
                            "clean_product_name": nippon_match["clean_product_name"],
                            "pmc": nippon_match["pmc"],
                            "product_name": nippon_match["product_name"],
                            "color": nippon_match["color"],
                            "sheet_type": nippon_match["sheet_type"],
                        },
                        "match_info": {
                            "score": nippon_match["score"],
                            "method": "competitor_multi_keyword",
                            "extracted_color": item_match.get("color"),
                            "color_match": item_match.get("color")
                            is not None,  # Color was extracted and matched
                        },
                        # Additional competitor info
                        "competitor_info": {
                            "competitor_matches": item_match.get(
                                "competitor_matches", []
                            ),
                            "keywords": item_match.get("keywords", []),
                            "np_marine_product": nippon_match.get(
                                "competitor_source", {}
                            ).get("np_marine_product"),
                            "generic_names": nippon_match.get("generic_names", []),
                        },
                    }
                )

            results.append(
                {"rfq_item_id": item_match["rfq_item_id"], "matches": match_list}
            )

        return {
            "rfq_id": rfq_id,
            "total_items": competitor_results["total_items"],
            "results": results,
        }

    # Original logic without competitor matching
    results = []
    for item in rfq_items:
        # Get matches for this RFQ item
        matches = (
            db.query(RFQMatch, ProductMaster)
            .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
            .filter(RFQMatch.rfq_item_id == item.id)
            .all()
        )

        match_list = []
        for match, product in matches:
            match_list.append(
                {
                    "rfq": {
                        "raw_text": item.raw_text,
                        "clean_text": item.clean_text,
                        "qty": item.qty,
                        "uom": item.uom,
                        "source": item.source,
                    },
                    "product_master": {
                        "id": product.id,
                        "clean_product_name": product.clean_product_name,
                        "pmc": product.pmc,
                        "product_name": product.product_name,
                        "color": product.color,
                        "sheet_type": product.sheet_type,
                    },
                    "match_info": {
                        "score": match.score,
                        "method": match.method,
                        "extracted_color": None,
                        "color_match": False,
                    },
                }
            )

        results.append({"rfq_item_id": item.id, "matches": match_list})

    return {"rfq_id": rfq_id, "total_items": len(rfq_items), "results": results}


@router.get("/rfq/{rfq_id}/summary")
async def get_rfq_summary(rfq_id: str, db: Session = Depends(get_db)):
    """
    Get summary of RFQ processing

    - **rfq_id**: RFQ identifier

    Returns summary statistics
    """
    # Get RFQ items count
    items_count = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).count()

    # Get matches count
    matches_query = db.query(RFQMatch).join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
    matches_count = matches_query.filter(RFQItem.rfq_id == rfq_id).count()

    # Get average score
    matches = matches_query.filter(RFQItem.rfq_id == rfq_id).all()
    avg_score = sum(m.score for m in matches) / len(matches) if matches else 0

    # Get unique products matched
    unique_products = (
        db.query(ProductMaster)
        .join(RFQMatch, RFQMatch.product_master_id == ProductMaster.id)
        .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
        .filter(RFQItem.rfq_id == rfq_id)
        .distinct()
        .count()
    )

    return {
        "rfq_id": rfq_id,
        "rfq_items_count": items_count,
        "matches_count": matches_count,
        "unique_products_matched": unique_products,
        "average_score": round(avg_score, 2),
        "status": "completed" if items_count > 0 else "pending",
    }


# ============== PRODUCT MASTER RESULTS ==============


@router.get(
    "/product-master/{file_id}/usage", response_model=ProductMasterUsageResponse
)
async def get_product_master_usage(file_id: int, db: Session = Depends(get_db)):
    """
    Get usage statistics for a specific Product Master file

    - **file_id**: Uploaded file ID

    Shows which RFQs have matched with products from this file
    """
    # Get file info
    uploaded_file = db.query(UploadedFile).filter(UploadedFile.id == file_id).first()
    if not uploaded_file:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    if uploaded_file.file_type != "excel":
        raise HTTPException(
            status_code=400, detail="File must be an Excel file (Product Master)"
        )

    # Get products from this file using the relationship
    products_count = (
        db.query(ProductMaster)
        .filter(ProductMaster.uploaded_file_id == file_id)
        .count()
    )

    # Get RFQs that matched with products from THIS specific file
    matched_rfqs = (
        db.query(RFQItem.rfq_id)
        .join(RFQMatch, RFQMatch.rfq_item_id == RFQItem.id)
        .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
        .filter(ProductMaster.uploaded_file_id == file_id)
        .distinct()
        .all()
    )

    rfq_list = [r[0] for r in matched_rfqs]

    # Get detailed match info
    match_details = []
    for rfq_id in rfq_list:
        # Count matches for this RFQ from this file
        match_count = (
            db.query(RFQMatch)
            .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
            .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
            .filter(RFQItem.rfq_id == rfq_id, ProductMaster.uploaded_file_id == file_id)
            .count()
        )

        match_details.append({"rfq_id": rfq_id, "matches_count": match_count})

    return {
        "file_id": file_id,
        "filename": uploaded_file.original_filename,
        "products_count": products_count,
        "matched_rfqs": rfq_list,
        "total_rfqs_using": len(rfq_list),
        "match_details": match_details,
        "status": uploaded_file.status,
    }


@router.get("/product-master/stats")
async def get_product_master_stats(db: Session = Depends(get_db)):
    """
    Get overall Product Master statistics
    """
    total_products = db.query(ProductMaster).count()

    # Count by sheet type
    from sqlalchemy import func

    sheet_type_counts = (
        db.query(ProductMaster.sheet_type, func.count(ProductMaster.id))
        .group_by(ProductMaster.sheet_type)
        .all()
    )

    sheet_stats = {st: count for st, count in sheet_type_counts}

    # Count total matches
    total_matches = db.query(RFQMatch).count()

    return {
        "total_products": total_products,
        "by_sheet_type": sheet_stats,
        "total_matches_made": total_matches,
    }


# ============== COMBINED RESULTS ==============


@router.get("/match-analysis")
async def get_match_analysis(
    rfq_id: str | None = Query(None, description="Filter by RFQ ID"),
    min_score: float | None = Query(
        None, ge=0, le=100, description="Minimum match score"
    ),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """
    Get detailed match analysis

    - **rfq_id**: Filter by specific RFQ
    - **min_score**: Filter by minimum score
    - **limit**: Maximum results

    Returns detailed match information with both RFQ and Product Master data
    """
    # Build query
    query = (
        db.query(RFQMatch, RFQItem, ProductMaster)
        .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
        .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
    )

    # Apply filters
    if rfq_id:
        query = query.filter(RFQItem.rfq_id == rfq_id)

    if min_score is not None:
        query = query.filter(RFQMatch.score >= min_score)

    # Order by score descending
    query = query.order_by(RFQMatch.score.desc())

    results = query.limit(limit).all()

    # Build response
    analysis = []
    for match, rfq_item, product in results:
        analysis.append(
            {
                "match": {
                    "id": match.id,
                    "score": match.score,
                    "method": match.method,
                },
                "rfq": {
                    "rfq_id": rfq_item.rfq_id,
                    "raw_text": rfq_item.raw_text,
                    "clean_text": rfq_item.clean_text,
                    "qty": rfq_item.qty,
                    "uom": rfq_item.uom,
                    "source": rfq_item.source,
                },
                "product_master": {
                    "id": product.id,
                    "pmc": product.pmc,
                    "product_name": product.product_name,
                    "color": product.color,
                    "sheet_type": product.sheet_type,
                    "clean_product_name": product.clean_product_name,
                },
            }
        )

    return {
        "total_results": len(analysis),
        "filters": {"rfq_id": rfq_id, "min_score": min_score},
        "data": analysis,
    }
