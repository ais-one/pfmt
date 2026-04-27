"""
API endpoints for RFQ table results storage and retrieval
"""

import json
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.api.security import get_current_user
from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem, RFQMatch
from apps.app_nippon_rfq_matching.app.services.pdf_comparison import (
    pdf_comparison_export_service,
)

router = APIRouter(prefix="/table-results", tags=["Table Results"])
logger = logging.getLogger(__name__)

# Global status tracking for async operations
processing_status = {}
status_lock = threading.Lock()


def update_processing_status(
    job_id: str, status: str, progress: float = None, message: str = None
):
    """Update processing status for a job"""
    with status_lock:
        processing_status[job_id] = {
            "status": status,
            "progress": progress,
            "message": message or f"Job {status}",
            "updated_at": datetime.utcnow().isoformat(),
        }


# Request/Response Schemas
class TableResultRequest(BaseModel):
    """Request schema for table result export"""

    rfq_id: str = Field(..., description="RFQ identifier to export")
    format_type: str = Field(
        "table", description="Result format type: table, detailed, compact"
    )
    use_normalization: bool = Field(
        True, description="Whether to use OpenAI normalization"
    )
    async_mode: bool = Field(True, description="Whether to process asynchronously")


class TableResultBatchRequest(BaseModel):
    """Request schema for batch table result export"""

    rfq_ids: list[str] = Field(..., description="List of RFQ identifiers to export")
    format_type: str = Field(
        "table", description="Result format type: table, detailed, compact"
    )
    use_normalization: bool = Field(
        True, description="Whether to use OpenAI normalization"
    )


class TableResultResponse(BaseModel):
    """Response schema for table result job creation"""

    job_id: str
    status: str
    message: str
    rfq_id: str
    format_type: str
    normalization_enabled: bool
    storage_key: str | None = None
    created_at: datetime


class TableResultSaved(BaseModel):
    """Response schema for saved table result"""

    storage_key: str
    rfq_id: str
    format_type: str
    normalization_enabled: bool
    created_at: datetime
    size_bytes: int
    result_count: int
    message: str


class TableResultFormatsResponse(BaseModel):
    """Response schema for available table result formats"""

    formats: list[str]
    default: str


class JobStatusResponse(BaseModel):
    """Response schema for job status"""

    job_id: str
    status: str  # pending, processing, completed, failed
    message: str
    progress: float | None = None
    error: str | None = None
    created_at: datetime
    completed_at: datetime | None = None
    updated_at: str | None = None


class ProcessingStatusResponse(BaseModel):
    """Response schema for processing status check"""

    job_id: str
    status: str
    progress: float | None = None
    message: str
    is_completed: bool
    estimated_time_left: str | None = None
    updated_at: str


# Storage directory setup
STORAGE_DIR = Path(settings.STORAGE_DIR) / "table_results"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


# Helper functions
def _check_competitor_data_exists(db: Session, rfq_id: str) -> bool:
    """Check if competitor data exists for the given RFQ"""
    try:
        # Check if competitor matching has been done for this RFQ
        from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
        from apps.app_nippon_rfq_matching.app.models.rfq import RFQMatch

        # Check if there are competitor matches
        competitor_matches = (
            db.query(RFQMatch)
            .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
            .join(
                # Check for competitor-specific products
                # This is a simplified check - in real implementation,
                # you might want to check specific competitor fields
                ProductMaster
            )
            .filter(
                RFQMatch.rfq_item_id.in_(
                    db.query(RFQItem.id).filter(RFQItem.rfq_id == rfq_id)
                )
            )
            .all()
        )

        # If we have matches, check if any have competitor info
        if competitor_matches:
            # Check if products have competitor source or specific competitor identifiers
            # This is a simplified check - adjust based on your actual data structure
            return True

        return False
    except Exception as e:
        logger.warning(f"Error checking competitor data: {e}")
        return False


# Helper functions
def save_table_result_to_storage(result: dict[str, Any], storage_key: str) -> str:
    """Save table result to storage directory"""
    file_path = STORAGE_DIR / f"{storage_key}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"Saved table result to {file_path}")
    return str(file_path)


def load_table_result_from_storage(storage_key: str) -> dict[str, Any]:
    """Load table result from storage directory"""
    file_path = STORAGE_DIR / f"{storage_key}.json"

    if not file_path.exists():
        raise FileNotFoundError(f"Table result not found: {storage_key}")

    with open(file_path, encoding="utf-8") as f:
        result = json.load(f)

    logger.info(f"Loaded table result from {file_path}")
    return result


def delete_table_result_from_storage(storage_key: str) -> bool:
    """Delete table result from storage directory"""
    file_path = STORAGE_DIR / f"{storage_key}.json"

    if file_path.exists():
        file_path.unlink()
        logger.info(f"Deleted table result {storage_key}")
        return True

    return False


# Endpoints
@router.post("/matching", response_model=TableResultResponse)
async def export_table_result(
    request: TableResultRequest,
    request_obj: Request,
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """
    Export table result for an RFQ and save to storage.

    This endpoint generates table result for an RFQ and saves it to storage
    for later retrieval through API endpoints.

    Args:
        request: Table result export request
        db: Database session

    Returns:
        Job information and storage key for tracking

    Example:
        POST /api/v1/table-results/export
        {
            "rfq_id": "RFQ001",
            "format_type": "table",
            "include_competitor": false,
            "use_normalization": true,
            "async_mode": true,
            "max_results": 20
        }
    """
    try:
        # Validate RFQ exists
        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == request.rfq_id).all()
        if not rfq_items:
            raise HTTPException(
                status_code=404,
                detail=f"RFQ ID '{request.rfq_id}' not found. Please upload RFQ first.",
            )

        # Validate format type
        valid_formats = ["table", "detailed", "compact"]
        if request.format_type not in valid_formats:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid format_type '{request.format_type}'. Must be one of: {valid_formats}",
            )

        # Generate storage key
        storage_key = f"table_{request.rfq_id}_{uuid.uuid4().hex[:8]}"
        # No expiration - persistent storage

        if request.async_mode:
            # Process asynchronously using background thread with status tracking

            # Initialize processing status
            update_processing_status(storage_key, "started", 0, "Processing started")

            def background_table_result_processor():
                """Process table result in background thread with detailed status updates"""
                try:
                    # Import here to avoid potential issues with imports in threads
                    import logging

                    from apps.app_nippon_rfq_matching.app.core.database import (
                        SessionLocal,
                    )

                    # Update status: processing
                    update_processing_status(
                        storage_key, "processing", 10, "Loading RFQ items"
                    )
                    bg_logger = logging.getLogger(f"background.{request.rfq_id}")

                    # Create new database session for background processing
                    background_db = SessionLocal()

                    try:
                        # Step 1: Load RFQ items
                        update_processing_status(
                            storage_key, "processing", 20, "Loading RFQ items"
                        )
                        result = _process_table_result_sync(
                            request.rfq_id,
                            request.format_type,
                            request.use_normalization,
                            background_db,
                            None,  # bearer_token not available in background
                            None,  # user not available in background
                            209,  # Default category_id for background processing
                        )

                        # Step 2: Save to storage
                        update_processing_status(
                            storage_key, "processing", 90, "Saving to storage"
                        )
                        save_table_result_to_storage(result, storage_key)

                        # Step 3: Complete
                        update_processing_status(
                            storage_key,
                            "completed",
                            100,
                            "Processing completed successfully",
                        )

                        # Log success
                        bg_logger.info(
                            f"Background processing completed successfully for RFQ {request.rfq_id}"
                        )
                        bg_logger.info(f"Result saved with storage_key: {storage_key}")

                    except Exception as e:
                        # Update status to failed
                        error_msg = f"Error in background processing: {str(e)}"
                        update_processing_status(storage_key, "failed", None, error_msg)
                        # Log error with details
                        bg_logger.error(error_msg, exc_info=True)
                    finally:
                        # Ensure database session is closed
                        background_db.close()

                except Exception as e:
                    # Update status to failed
                    error_msg = f"Failed to create database session: {str(e)}"
                    update_processing_status(storage_key, "failed", None, error_msg)
                    # Log critical errors
                    bg_logger = logging.getLogger(f"background.{request.rfq_id}")
                    bg_logger.error(error_msg, exc_info=True)

            # Start background thread
            import threading

            background_thread = threading.Thread(
                target=background_table_result_processor,
                daemon=True,
                name=f"TableResult-{request.rfq_id}-{storage_key[:8]}",
            )
            background_thread.start()

            # Give the thread a moment to start
            background_thread.join(timeout=0.1)

            # Log authenticated user
            user_data = user.get("data", {}).get("user", {})
            logger.info(
                f"Table result processing initiated by user: {user_data.get('email', 'Unknown')} (ID: "
                f"{user_data.get('id', 'Unknown')}) for RFQ {request.rfq_id}"
            )

            logger.info(
                f"Started background processing for RFQ {request.rfq_id} with storage_key {storage_key}"
            )

            # Return immediately with processing status
            return TableResultResponse(
                job_id=storage_key,
                status="processing",
                message="Table result export processing in background (async)",
                rfq_id=request.rfq_id,
                format_type=request.format_type,
                normalization_enabled=request.use_normalization,
                storage_key=storage_key,
                created_at=datetime.utcnow(),
            )
        else:
            # Process synchronously
            try:
                result = _process_table_result_sync(
                    request.rfq_id,
                    request.format_type,
                    request.use_normalization,
                    db,
                    None,  # bearer_token not available in synchronous mode
                    None,  # user not available in synchronous mode
                    209,  # Default category_id for synchronous processing
                )
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
            except Exception as e:
                raise HTTPException(
                    status_code=500, detail=f"Error processing table result: {str(e)}"
                )

            # Save to storage
            save_table_result_to_storage(result, storage_key)

            return TableResultResponse(
                job_id=storage_key,
                status="completed",
                message="Table result export completed",
                rfq_id=request.rfq_id,
                format_type=request.format_type,
                normalization_enabled=request.use_normalization,
                storage_key=storage_key,
                created_at=datetime.utcnow(),
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating table result export job: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create table result export job: {str(e)}",
        )


def _process_table_result_sync(
    rfq_id: str,
    format_type: str,
    use_normalization: bool,
    db: Session,
    bearer_token: str = None,
    user: dict = None,
    request_obj: Request = None,
    category_id: int = 209,
) -> dict[str, Any]:
    """Process table result synchronously using PDF export service pipeline"""
    # Log bearer token availability
    logger.info(f"Processing table result for RFQ {rfq_id}")
    logger.info(f"Bearer token available: {'Yes' if bearer_token else 'No'}")

    # Step 1: Load RFQ items (same as PDF export)
    rfq_items = pdf_comparison_export_service.get_rfq_items_by_id(rfq_id, db)
    if not rfq_items:
        raise ValueError(f"RFQ {rfq_id} not found")

    logger.info(f"Step 1: Loaded {len(rfq_items)} RFQ items")

    # Step 2: Normalize items (same as PDF export)
    normalized_items = pdf_comparison_export_service._get_normalized_items(
        rfq_items, use_normalization, db
    )

    logger.info(f"Step 2: Normalized {len(normalized_items)} items")
    logger.info(
        f"DEBUG: Items lost in normalization: {len(rfq_items) - len(normalized_items)}"
    )

    # Step 3: Find product matches (same as PDF export)
    logger.info("Step 3: Finding product matches")
    matches = pdf_comparison_export_service.find_product_matches(normalized_items, db)

    # Step 4: Enrich matches with product data (same as PDF export)
    logger.info("Step 4: Enriching matches with product data")
    from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
        matching_data_service,
    )

    enriched_matches = matching_data_service.enrich_matches_with_product_data(
        matches, db
    )

    # DEBUG: Log RFQ items count vs enriched_matches count
    logger.info(f"DEBUG: RFQ items count: {len(normalized_items)}")
    logger.info(f"DEBUG: Enriched matches count: {len(enriched_matches)}")
    logger.info(
        f"DEBUG: Missing items: {len(normalized_items) - len(enriched_matches)}"
    )

    # Step 5: Send tickets to AIS Manager for unmatched items
    logger.info("Step 5: Processing unmatched items for AIS Manager tickets")
    unmatched_items = []
    matched_item_ids = []

    for match in enriched_matches:
        # Extract RFQ item ID from the match
        rfq_item_id = match["rfq"]["id"]
        matched_item_ids.append(rfq_item_id)

        # Check if this item is unmatched (consistent with matched_count logic)
        if match["product_master"] is None:
            # Convert to dict format for AIS Manager
            unmatched_item = {
                "id": rfq_item_id,
                "raw_text": match["rfq"]["raw_text"],
                "product_name": match["rfq"]["clean_text"],
                "color": match["rfq"].get("color", ""),
                "uom": match["rfq"].get("uom", ""),
                "qty": match["rfq"].get("qty", ""),
                "product_type": match["rfq"].get("product_type", ""),
                "source_brand": match["rfq"].get("source_brand", ""),
                "product_master_id": None,
            }
            unmatched_items.append(unmatched_item)

    # Find items that are NOT in enriched_matches at all (no match found)
    # Get all RFQ item IDs from normalized_items
    all_normalized_ids = {item["id"] for item in normalized_items}

    # Find items that are not in matched_item_ids
    unmatched_ids = all_normalized_ids - set(matched_item_ids)

    # Create unmatched items for those that have no match at all
    for item_id in unmatched_ids:
        normalized_item = next(
            (item for item in normalized_items if item["id"] == item_id), None
        )
        if normalized_item:
            unmatched_item = {
                "id": item_id,
                "raw_text": normalized_item.get("raw_text", ""),
                "product_name": normalized_item.get("clean_text", ""),
                "color": normalized_item.get("color", ""),
                "uom": normalized_item.get("uom", ""),
                "qty": normalized_item.get("qty", ""),
                "product_type": normalized_item.get("product_type", ""),
                "source_brand": normalized_item.get("source_brand", ""),
                "product_master_id": None,
            }
            unmatched_items.append(unmatched_item)

    # Create tickets for unmatched items
    logger.info(
        f"Checking unmatched items for ticket creation: {len(unmatched_items)} items"
    )
    logger.debug(
        f"Unmatched items details: {json.dumps(unmatched_items[:2], indent=2)}"
    )

    # DEBUG: Log matched_item_ids and unmatched_ids
    logger.info(f"DEBUG: matched_item_ids count: {len(matched_item_ids)}")
    logger.info(f"DEBUG: unmatched_ids count: {len(unmatched_ids)}")
    logger.info(f"DEBUG: all_normalized_ids count: {len(all_normalized_ids)}")

    # Log first few unmatched items for debugging
    if unmatched_items:
        logger.info(
            f"First unmatched item example: {json.dumps(unmatched_items[0], indent=2)}"
        )
    else:
        logger.warning("No unmatched items found - check data structures!")
    logger.info(f"User object from dependency: {user}")

    # Get user_id from user object if available
    user_id = None
    if user and isinstance(user, dict):
        # Check if user has nested structure
        if "data" in user and "user" in user["data"]:
            user_id = user["data"]["user"].get("id")
        elif "user" in user:
            user_id = user["user"].get("id")
        else:
            user_id = user.get("id")
    logger.info(f"User ID extracted: {user_id}")

    if unmatched_items:
        logger.info(
            f"Found {len(unmatched_items)} unmatched items in RFQ {rfq_id}, attempting to create AIS Manager tickets"
        )

        try:
            if not user_id:
                logger.warning(
                    "User ID not found in user object, skipping AIS Manager ticket creation"
                )
                logger.debug(f"User object structure: {json.dumps(user, indent=2)}")
                logger.info(
                    f"Request object available: {'Yes' if request_obj else 'No'}"
                )
                if request_obj:
                    # Try to get user_id from request state
                    user_id = getattr(request_obj.state, "user_id", None)
                    logger.info(f"User ID from request state: {user_id}")
            else:
                logger.info(
                    f"User ID found: {user_id}, "
                    f"proceeding with ticket creation for {len(unmatched_items)} unmatched items"
                )
            import asyncio

            from apps.app_nippon_rfq_matching.app.services.ais_manager_service import (
                ais_manager_service,
            )

            # Define category_id for ticket creation
            ticket_category_id = category_id

            def _send_tickets():
                try:
                    logger.info(
                        f"Starting asynchronous ticket creation for {len(unmatched_items)} unmatched items in RFQ "
                        f"{rfq_id}"
                    )

                    results = ais_manager_service.create_tickets_for_unmatched_items(
                        unmatched_items=unmatched_items,
                        rfq_id=rfq_id,
                        user_id=user_id,
                        bearer_token=bearer_token,
                        category_id=ticket_category_id,
                    )

                    successful = len([r for r in results if r is not None])
                    failed = len(unmatched_items) - successful

                    logger.info(
                        f"AIS Manager: Ticket creation summary for RFQ {rfq_id}:"
                    )
                    logger.info(f"  - Total unmatched items: {len(unmatched_items)}")
                    logger.info(f"  - Successfully created tickets: {successful}")
                    logger.info(f"  - Failed to create tickets: {failed}")
                    logger.info(
                        f"  - Success rate: {successful / len(unmatched_items) * 100:.1f}%"
                    )

                    if failed > 0:
                        failed_items_indices = [
                            i for i, r in enumerate(results) if r is None
                        ]
                        logger.warning(
                            f"Failed to create tickets for item indices: {failed_items_indices}"
                        )

                    # Extract ticket IDs from successful results
                    successful_ticket_ids = [
                        r.get("id") for r in results if r and "id" in r
                    ]
                    if successful_ticket_ids:
                        logger.info(
                            f"Successfully created ticket IDs: {successful_ticket_ids}"
                        )

                except Exception as e:
                    logger.error(
                        f"AIS Manager: Critical error during ticket creation: {e}"
                    )
                    logger.error(f"Exception type: {type(e).__name__}")
                    logger.error(
                        f"RFQ ID: {rfq_id}, User: {user_id}, Unmatched items count: {len(unmatched_items)}"
                    )
                    import traceback

                    logger.error(f"Full traceback: {traceback.format_exc()}")

            # Run in thread pool to avoid blocking
            logger.info("Submitting ticket creation to thread pool")
            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, _send_tickets)

        except Exception as e:
            logger.error(f"Failed to initialize AIS Manager ticket creation: {e}")
            logger.error(f"RFQ ID: {rfq_id}, Exception type: {type(e).__name__}")
            import traceback

            logger.error(f"Full traceback: {traceback.format_exc()}")

    # Build table result
    table_items = []
    for match in enriched_matches:
        # Extract RFQ item ID from the match
        rfq_item_id = match["rfq"]["id"]

        table_items.append({"rfq_item_id": rfq_item_id, "matches": [match]})

    # Calculate statistics
    matched_count = sum(1 for m in enriched_matches if m["product_master"] is not None)
    unmatched_count = len(rfq_items) - matched_count
    match_rate = round(matched_count / len(rfq_items) * 100, 2) if rfq_items else 0

    # Log debug info for unmatched items calculation
    logger.info(f"Debug - RFQ items count: {len(rfq_items)}")
    logger.info(f"Debug - Matched count from enriched_matches: {matched_count}")
    logger.info(f"Debug - Unmatched items in list: {len(unmatched_items)}")
    logger.info(f"Debug - Unmatched count from calculation: {unmatched_count}")

    # Prepare ticket statistics
    tickets_created = len(unmatched_items) > 0
    tickets_count = len(unmatched_items)
    tickets_pending = (
        tickets_created and not user_id
    )  # If no user ID, tickets weren't created
    tickets_queued = (
        tickets_created and user_id
    )  # If we have user ID, tickets were queued for creation

    # Debug log for ticket statistics
    logger.info("Ticket statistics calculation:")
    logger.info(f"  - unmatched_items list length: {len(unmatched_items)}")
    logger.info(f"  - tickets_created flag: {tickets_created}")
    logger.info(f"  - tickets_count: {tickets_count}")
    logger.info(f"  - user_id available: {bool(user_id)}")
    logger.info(f"  - tickets_pending: {tickets_pending}")
    logger.info(f"  - tickets_queued: {tickets_queued}")

    # Log final statistics
    logger.info(f"Table result generation completed for RFQ {rfq_id}")
    logger.info("Final statistics:")
    logger.info(f"  - Total items: {len(rfq_items)}")
    logger.info(f"  - Matched items: {matched_count}")
    logger.info(f"  - Unmatched items: {unmatched_count}")
    logger.info(f"  - Match rate: {match_rate:.1f}%")
    logger.info(f"  - Tickets to create: {tickets_count}")
    logger.info(f"  - Tickets created: {'Yes' if tickets_count > 0 else 'No'}")
    logger.info(f"  - Tickets queued: {'Yes' if tickets_queued else 'No'}")

    if unmatched_items:
        # Log details of unmatched items for debugging
        logger.info("Unmatched items details:")
        for i, item in enumerate(
            unmatched_items[:5]
        ):  # Log first 5 to avoid too much output
            logger.info(
                f"  Item {i + 1}: ID={item.get('id')}, Product='{item.get('product_name')[:50]}...', "
                f"Color={item.get('color')}, Raw='{item.get('raw_text')[:30]}...'"
            )
        if len(unmatched_items) > 5:
            logger.info(f"  ... and {len(unmatched_items) - 5} more unmatched items")
    else:
        logger.info("No unmatched items found for ticket creation")

    statistics = {
        "total_items": len(rfq_items),
        "normalized_count": sum(
            1 for m in enriched_matches if m["rfq"].get("normalized_name")
        ),
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "match_rate": match_rate,
        "tickets_created": tickets_created,
        "tickets_count": tickets_count,
        "tickets_pending": tickets_pending,
        "tickets_queued": tickets_queued,
    }

    return {
        "rfq_id": rfq_id,
        "format_type": format_type,
        "normalization_enabled": use_normalization,
        "total_items": len(rfq_items),
        "items": table_items,
        "statistics": statistics,
    }


@router.get("/get/{storage_key}", response_model=dict[str, Any])
async def get_table_result(storage_key: str, db: Session = Depends(get_db)):
    """
    Get saved table result by storage key.

    Args:
        storage_key: Storage key of the saved result
        db: Database session

    Returns:
        Saved table result data

    Example:
        GET /api/v1/table-results/get/{storage_key}
    """
    try:
        result = load_table_result_from_storage(storage_key)
        return result

    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail=f"Table result not found: {storage_key}"
        )
    except Exception as e:
        logger.error(f"Error retrieving table result: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve table result: {str(e)}"
        )


@router.get("/rfq/{rfq_id}/latest", response_model=dict[str, Any])
async def get_latest_table_result(
    rfq_id: str,
    request_obj: Request,
    format_type: str = Query("table", description="Result format type"),
    include_competitor: bool = Query(
        False, description="Whether to include competitor matching"
    ),
    max_results: int = Query(20, ge=1, le=100, description="Maximum results per item"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """
    Get the latest table result for an RFQ.

    This endpoint generates a fresh table result for the RFQ.

    Args:
        rfq_id: RFQ identifier
        format_type: Result format type
        include_competitor: Whether to include competitor matching
        max_results: Maximum results per item
        db: Database session
        user: Authenticated user

    Returns:
        Freshly generated table result

    Example:
        GET /api/v1/table-results/rfq/{rfq_id}/latest?format_type=detailed&include_competitor=true
    """
    try:
        # Get bearer token from middleware
        bearer_token = getattr(request_obj.state, "bearer_token", None)
        if not bearer_token:
            logger.error("Bearer token not found in request state - middleware issue")
            raise HTTPException(
                status_code=401, detail="Authentication token not available"
            )

        # Validate RFQ exists
        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()
        if not rfq_items:
            raise HTTPException(status_code=404, detail=f"RFQ {rfq_id} not found")

        # Process the result
        result = _process_table_result_sync(
            rfq_id,
            format_type,
            True,  # Always use normalization for fresh results
            db,
            bearer_token,
            user,
            request_obj,
            209,  # Default category_id for fresh results
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating latest table result: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to generate table result: {str(e)}"
        )


@router.delete("/delete/{storage_key}")
async def delete_table_result(storage_key: str, db: Session = Depends(get_db)):
    """
    Delete saved table result.

    Args:
        storage_key: Storage key of the result to delete
        db: Database session

    Returns:
        Deletion status

    Example:
        DELETE /api/v1/table-results/delete/{storage_key}
    """
    try:
        success = delete_table_result_from_storage(storage_key)

        if success:
            return {
                "status": "success",
                "message": f"Table result {storage_key} deleted successfully",
            }
        else:
            raise HTTPException(
                status_code=404, detail=f"Table result not found: {storage_key}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting table result: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to delete table result: {str(e)}"
        )


@router.get("/formats", response_model=TableResultFormatsResponse)
async def get_table_formats():
    """
    Get available table result formats.

    Returns:
        List of available formats and default

    Example:
        GET /api/v1/table-results/formats
    """
    return {"formats": ["table", "detailed", "compact"], "default": "table"}


@router.get("/match-status")
async def get_items_by_match_status(
    rfq_id: str | None = Query(None, description="Filter by specific RFQ ID"),
    has_match: bool | None = Query(
        None,
        description="Filter by match status: true for items with matches, false for no matches",
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get RFQ items filtered by match status.

    Args:
        rfq_id: Filter by specific RFQ ID (optional)
        has_match: Filter by match status - true for items with matches, false for no matches
        limit: Maximum number of results to return
        offset: Number of results to skip (for pagination)
        db: Database session

    Example:
        GET /api/v1/table-results/match-status?has_match=false&limit=50
        GET /api/v1/table-results/match-status?rfq_id=RFQ001&has_match=true
    """
    try:
        # Build base query for RFQ items
        query = db.query(RFQItem)

        if rfq_id:
            query = query.filter(RFQItem.rfq_id == rfq_id)

        rfq_items = query.all()

        # Filter by match status if specified
        if has_match is not None:
            filtered_items = []
            for item in rfq_items:
                matches_count = (
                    db.query(RFQMatch).filter(RFQMatch.rfq_item_id == item.id).count()
                )

                if (has_match and matches_count > 0) or (
                    not has_match and matches_count == 0
                ):
                    filtered_items.append(
                        {
                            "rfq_item_id": item.id,
                            "rfq_id": item.rfq_id,
                            "raw_text": item.raw_text,
                            "clean_text": item.clean_text,
                            "qty": item.qty,
                            "uom": item.uom,
                            "source": item.source,
                            "match_count": matches_count,
                            "created_at": item.created_at.isoformat()
                            if item.created_at
                            else None,
                        }
                    )
        else:
            # Return all items with match counts
            filtered_items = []
            for item in rfq_items:
                matches_count = (
                    db.query(RFQMatch).filter(RFQMatch.rfq_item_id == item.id).count()
                )
                filtered_items.append(
                    {
                        "rfq_item_id": item.id,
                        "rfq_id": item.rfq_id,
                        "raw_text": item.raw_text,
                        "clean_text": item.clean_text,
                        "qty": item.qty,
                        "uom": item.uom,
                        "source": item.source,
                        "match_count": matches_count,
                        "created_at": item.created_at.isoformat()
                        if item.created_at
                        else None,
                    }
                )

        # Apply pagination
        total_count = len(filtered_items)
        paginated_items = filtered_items[offset : offset + limit]

        # Calculate next page offset
        next_offset = offset + limit if offset + limit < total_count else None

        return {
            "total": total_count,
            "count": len(paginated_items),
            "offset": offset,
            "limit": limit,
            "next_offset": next_offset,
            "filters": {"rfq_id": rfq_id, "has_match": has_match},
            "items": paginated_items,
        }

    except Exception as e:
        logger.error(f"Error getting items by match status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get items by match status: {str(e)}"
        )


@router.get("/rfq-list", response_model=list[str])
async def get_available_rfqs_for_table_results(
    limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)
):
    """
    Get list of RFQ IDs available for table result generation.

    Returns:
        List of RFQ IDs that have been processed and can be used
        for generating table results.

    Example:
        GET /api/v1/table-results/rfq-list?limit=50
    """
    try:
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


@router.get("/no-matches")
async def get_no_match_results(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Get RFQ items that have no matches.

    Returns RFQ items that don't have any matches found.

    Args:
        limit: Maximum number of results to return
        offset: Number of results to skip (for pagination)
        db: Database session

    Example:
        GET /api/v1/table-results/no-matches?limit=50&offset=0
    """
    try:
        # Get all RFQ items
        rfq_items = db.query(RFQItem).all()

        # Find items with no matches
        no_match_items = []
        for item in rfq_items:
            # Check if this item has any matches
            matches_count = (
                db.query(RFQMatch).filter(RFQMatch.rfq_item_id == item.id).count()
            )

            if matches_count == 0:
                no_match_items.append(
                    {
                        "rfq_item_id": item.id,
                        "rfq_id": item.rfq_id,
                        "raw_text": item.raw_text,
                        "clean_text": item.clean_text,
                        "qty": item.qty,
                        "uom": item.uom,
                        "source": item.source,
                        "created_at": item.created_at.isoformat()
                        if item.created_at
                        else None,
                    }
                )

        # Apply pagination
        total_count = len(no_match_items)
        paginated_items = no_match_items[offset : offset + limit]

        # Calculate next page offset
        next_offset = offset + limit if offset + limit < total_count else None

        return {
            "total": total_count,
            "count": len(paginated_items),
            "offset": offset,
            "limit": limit,
            "next_offset": next_offset,
            "items": paginated_items,
        }

    except Exception as e:
        logger.error(f"Error getting no match results: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get no match results: {str(e)}"
        )


@router.get("/stats")
async def get_table_results_stats(db: Session = Depends(get_db)):
    """
    Get statistics about stored table results.

    Returns:
        Statistics about stored table results

    Example:
        GET /api/v1/table-results/stats
    """
    try:
        # Count stored files
        files = list(STORAGE_DIR.glob("*.json"))
        total_files = len(files)

        # Calculate total size
        total_size = sum(f.stat().st_size for f in files if f.exists())

        # Count RFQs with stored results
        rfqs_with_results = set()
        for file_path in files:
            try:
                with open(file_path, encoding="utf-8") as f:
                    result = json.load(f)
                    rfqs_with_results.add(result.get("rfq_id"))
            except Exception:
                pass

        return {
            "total_stored_files": total_files,
            "total_size_bytes": total_size,
            "rfqs_with_results": len(rfqs_with_results),
            "storage_directory": str(STORAGE_DIR),
            "formats_available": ["table", "detailed", "compact"],
        }

    except Exception as e:
        logger.error(f"Error getting table results stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get statistics: {str(e)}"
        )


@router.get("/job/{job_id}/status")
async def get_job_status(
    job_id: str, db: Session = Depends(get_db)
) -> JobStatusResponse:
    """
    Get job status by job ID.

    Args:
        job_id: Job identifier from the initial request
        db: Database session

    Returns:
        Job status information
    """
    try:
        # Check if file exists in storage
        file_path = STORAGE_DIR / f"{job_id}.json"

        if file_path.exists():
            # Job is completed
            from datetime import datetime

            created_time = datetime.fromtimestamp(file_path.stat().st_ctime)
            completed_time = datetime.fromtimestamp(file_path.stat().st_mtime)

            return JobStatusResponse(
                job_id=job_id,
                status="completed",
                message="Table result export completed successfully",
                progress=100.0,
                created_at=created_time,
                completed_at=completed_time,
            )
        else:
            # Check if job is still processing
            # This is a simple implementation - in production, you'd want proper job tracking
            from datetime import datetime

            created_time = datetime.now()

            return JobStatusResponse(
                job_id=job_id,
                status="processing",
                message="Table result export is still in progress",
                progress=None,
                created_at=created_time,
                completed_at=None,
            )

    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {e}", exc_info=True)
        return JobStatusResponse(
            job_id=job_id,
            status="failed",
            message=f"Error checking job status: {str(e)}",
            progress=None,
            error=str(e),
            created_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
        )


@router.get("/status/{storage_key}", response_model=ProcessingStatusResponse)
async def get_processing_status(
    storage_key: str,
) -> ProcessingStatusResponse:
    """
    Get processing status for an async table results export.

    This endpoint allows checking the current status of an async processing job
    without having to check if the result file exists.

    Args:
        storage_key: Storage key from the initial request

    Returns:
        Current processing status information

    Example:
        GET /api/v1/table-results/status/{storage_key}

        Response (processing):
        {
            "storage_key": "abc123",
            "status": "processing",
            "progress": 45.0,
            "message": "Processing matches...",
            "updated_at": "2024-01-01T10:00:00Z"
        }

        Response (completed):
        {
            "storage_key": "abc123",
            "status": "completed",
            "progress": 100.0,
            "message": "Processing completed successfully",
            "updated_at": "2024-01-01T10:01:00Z"
        }

        Response (failed):
        {
            "storage_key": "abc123",
            "status": "failed",
            "progress": 30.0,
            "message": "Error processing data",
            "error": "Detailed error message",
            "updated_at": "2024-01-01T10:00:30Z"
        }
    """
    try:
        # Check global status tracking
        with status_lock:
            status_info = processing_status.get(storage_key)

        if status_info:
            # Return tracked status
            return ProcessingStatusResponse(
                storage_key=storage_key,
                status=status_info["status"],
                progress=status_info.get("progress"),
                message=status_info.get("message"),
                updated_at=status_info["updated_at"],
            )
        else:
            # Check if result file exists (fallback)
            file_path = STORAGE_DIR / f"{storage_key}.json"
            if file_path.exists():
                # Job completed but not tracked in status (fallback)
                from datetime import datetime

                completed_time = datetime.fromtimestamp(file_path.stat().st_mtime)

                return ProcessingStatusResponse(
                    storage_key=storage_key,
                    status="completed",
                    progress=100.0,
                    message="Processing completed successfully",
                    updated_at=completed_time.isoformat(),
                )
            else:
                # No status found - job might not exist
                return ProcessingStatusResponse(
                    storage_key=storage_key,
                    status="not_found",
                    progress=None,
                    message="Processing job not found",
                    updated_at=datetime.utcnow().isoformat(),
                )

    except Exception as e:
        logger.error(
            f"Error getting processing status for {storage_key}: {e}", exc_info=True
        )
        return ProcessingStatusResponse(
            storage_key=storage_key,
            status="error",
            progress=None,
            message=f"Error checking status: {str(e)}",
            error=str(e),
            updated_at=datetime.utcnow().isoformat(),
        )


@router.delete("/cleanup")
async def cleanup_table_results(
    storage_keys: list[str] | None = Query(
        None, description="Specific storage keys to delete"
    ),
    db: Session = Depends(get_db),
):
    """
    Clean up table results from storage.

    Args:
        storage_keys: Specific storage keys to delete (optional). If not provided, shows cleanup info.

    Returns:
        Cleanup status

    Example:
        DELETE /api/v1/table-results/cleanup?storage_keys=key1&storage_keys=key2
    """
    try:
        if storage_keys:
            # Delete specific storage keys
            deleted_count = 0
            errors = []

            for storage_key in storage_keys:
                try:
                    success = delete_table_result_from_storage(storage_key)
                    if success:
                        deleted_count += 1
                        logger.info(f"Deleted table result: {storage_key}")
                    else:
                        errors.append(
                            {"storage_key": storage_key, "error": "Not found"}
                        )
                except Exception as e:
                    errors.append({"storage_key": storage_key, "error": str(e)})
                    logger.error(f"Error deleting table result {storage_key}: {e}")

            return {
                "status": "completed",
                "deleted_count": deleted_count,
                "errors": errors,
                "message": f"Cleanup completed. Deleted {deleted_count} results.",
            }
        else:
            # Show info about cleanup without actually deleting
            files = list(STORAGE_DIR.glob("*.json"))
            total_size = sum(f.stat().st_size for f in files if f.exists())

            return {
                "status": "info",
                "total_files": len(files),
                "total_size_bytes": total_size,
                "storage_directory": str(STORAGE_DIR),
                "message": "Use storage_keys parameter to specify which files to delete",
            }

    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cleanup: {str(e)}")
