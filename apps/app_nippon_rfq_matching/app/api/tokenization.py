"""
Tokenization API Endpoints

API endpoints for tokenizing product names and managing product_master_mv table.
"""

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import ProductMasterMV
from apps.app_nippon_rfq_matching.app.services.tokenization_service import (
    tokenization_service,
)

router = APIRouter(prefix="/tokenize", tags=["Tokenization"])


# Request/Response Schemas
class TokenizeProductsRequest(BaseModel):
    """Request schema for tokenizing products"""

    batch_size: int = Field(
        100, description="Number of records to process per batch", ge=1, le=1000
    )
    force_rebuild: bool = Field(False, description="Clear and rebuild existing tokens")


class TokenizeProductsResponse(BaseModel):
    """Response schema for tokenization"""

    status: str
    message: str
    stats: dict[str, Any]


class TokenSearchRequest(BaseModel):
    """Request schema for token search"""

    query: str = Field(..., description="Search query")
    limit: int = Field(20, description="Maximum number of results", ge=1, le=100)


class TokenSearchResponse(BaseModel):
    """Response schema for token search results"""

    query: str
    matches: list
    total_found: int


# Endpoints
@router.post("/products", response_model=TokenizeProductsResponse)
async def tokenize_products(
    request: TokenizeProductsRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Tokenize all distinct product names from product_master and store in product_master_mv.

    This endpoint:
    1. Gets all distinct product names from product_master
    2. Tokenizes each product name (removes stopwords, normalizes, etc.)
    3. Stores results in product_master_mv table
    4. Returns statistics

    Args:
        request: Tokenization request with batch_size and force_rebuild options
        background_tasks: FastAPI background tasks for async processing
        db: Database session

    Returns:
        Tokenization results with statistics

    Example:
        POST /api/v1/tokenize/products
        {
          "batch_size": 100,
          "force_rebuild": true
        }
    """
    try:
        # Run tokenization (could be slow for large datasets)
        stats = tokenization_service.tokenize_and_store(
            db=db, batch_size=request.batch_size
        )

        return TokenizeProductsResponse(
            status="success",
            message=f"Tokenization completed: {stats['created']} products tokenized",
            stats=stats,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tokenization failed: {str(e)}")


@router.post("/products/async")
async def tokenize_products_async(
    request: TokenizeProductsRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Tokenize all distinct product names from product_master (async background task).

    This endpoint starts the tokenization process in the background
    and returns immediately with a job ID for tracking.

    Args:
        request: Tokenization request
        background_tasks: FastAPI background tasks
        db: Database session

    Returns:
        Job information

    Example:
        POST /api/v1/tokenize/products/async
        {
          "batch_size": 100,
          "force_rebuild": true
        }
    """
    try:
        import uuid

        from apps.app_nippon_rfq_matching.app.models.rfq import Job

        # Create job record
        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id, job_type="tokenize_products", status="processing", progress=0
        )
        db.add(job)
        db.commit()

        # Define background task
        def run_tokenization():
            try:
                db = SessionLocal()
                stats = tokenization_service.tokenize_and_store(
                    db=db, batch_size=request.batch_size
                )

                # Update job as completed
                job.status = "completed"
                job.progress = 100
                job.result = str(stats)
                db.commit()

            except Exception as e:
                job.status = "failed"
                job.error_message = str(e)
                db.commit()

        # Start background task

        from apps.app_nippon_rfq_matching.app.core.database import SessionLocal

        def run_in_thread():
            from sqlalchemy.orm import sessionmaker

            from apps.app_nippon_rfq_matching.app.core.database import engine

            SessionLocal = sessionmaker(bind=engine)
            db_bg = SessionLocal()
            run_tokenization()
            db_bg.close()

        # Run in thread pool
        import threading

        thread = threading.Thread(target=run_in_thread)
        thread.start()

        return {
            "status": "started",
            "job_id": job_id,
            "message": "Tokenization started in background",
            "check_url": f"/api/v1/jobs/{job_id}",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to start tokenization: {str(e)}"
        )


@router.post("/search", response_model=TokenSearchResponse)
async def search_by_tokens(request: TokenSearchRequest, db: Session = Depends(get_db)):
    """
    Search for products using tokenized names.

    This endpoint:
    1. Tokenizes the search query
    2. Searches product_master_mv for matching tokens
    3. Returns products sorted by match quality

    Args:
        request: Search request with query and limit
        db: Database session

    Returns:
        Search results with product names and match counts

    Example:
        POST /api/v1/tokenize/search
        {
          "query": "marine finish white",
          "limit": 10
        }
    """
    try:
        matches = tokenization_service.search_by_tokens(
            query=request.query, db=db, limit=request.limit
        )

        return TokenSearchResponse(
            query=request.query, matches=matches, total_found=len(matches)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Token search failed: {str(e)}")


@router.get("/stats")
async def get_tokenization_stats(db: Session = Depends(get_db)):
    """
    Get statistics about tokenized products.

    Returns:
        Statistics including total count and sample tokens

    Example:
        GET /api/v1/tokenize/stats
    """
    try:
        stats = tokenization_service.get_stats(db)
        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@router.delete("/products")
async def clear_tokenized_products(db: Session = Depends(get_db)):
    """
    Clear all tokenized products from product_master_mv.

    Use with caution - this will delete all tokenized products.

    Returns:
        Status message

    Example:
        DELETE /api/v1/tokenize/products
    """
    try:
        # Delete all records
        deleted_count = db.query(ProductMasterMV).delete()
        db.commit()

        return {
            "status": "success",
            "message": f"Cleared {deleted_count} tokenized products",
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to clear tokenized products: {str(e)}"
        )
