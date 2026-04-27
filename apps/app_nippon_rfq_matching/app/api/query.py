"""
API endpoints for querying data as dataframe
"""

import logging
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.pricing import ProductPrices, Region
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem, RFQMatch
from apps.app_nippon_rfq_matching.app.models.schemas import (
    APIErrorResponse,
    APIResponse,
    CSVSummaryResponse,
    DataframeQueryResponse,
    ProductMasterQueryResponse,
    RFQItemsQueryResponse,
    RFQMatchesQueryResponse,
)
from apps.app_nippon_rfq_matching.app.services.rfq_service import rfq_service
from apps.app_nippon_rfq_matching.app.utils.csv_storage import csv_storage

router = APIRouter(prefix="/query", tags=["query"])

logger = logging.getLogger(__name__)


def handle_error(
    error: Exception,
    error_code: str = "INTERNAL_ERROR",
    message: str = "Internal server error",
):
    """Handle errors with proper logging and response format"""
    logger.error(f"Error [{error_code}]: {str(error)}", exc_info=True)

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


def handle_validation_error(error: Exception, message: str = "Validation failed"):
    """Handle validation errors"""
    logger.warning(f"Validation Error: {str(error)}")

    raise HTTPException(
        status_code=422,
        detail=APIErrorResponse(
            message=message,
            error="VALIDATION_ERROR",
            error_code="VALIDATION_ERROR",
            details={"validation_errors": str(error)},
        ).dict(exclude_none=True),
    )


@router.get("/product-master", response_model=APIResponse)
async def query_product_master(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sheet_type: str | None = None,
    search: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Query product master data with pagination

    - **page**: Page number (starts from 1)
    - **page_size**: Number of items per page
    - **sheet_type**: Filter by sheet type (IATP_AF, IATP_SW, IATP_GEN)
    - **search**: Search in product_name or pmc
    """
    query = db.query(ProductMaster)

    # Apply filters
    if sheet_type:
        query = query.filter(ProductMaster.sheet_type == sheet_type)

    if search:
        search_pattern = f"%{search}%"
        query = query.filter(
            (ProductMaster.product_name.ilike(search_pattern))
            | (ProductMaster.pmc.ilike(search_pattern))
        )

    # Get total count
    total_count = query.count()

    # Apply pagination
    offset = (page - 1) * page_size
    products = query.offset(offset).limit(page_size).all()

    # Calculate pagination metadata
    total_pages = (total_count + page_size - 1) // page_size

    return APIResponse(
        success=True,
        message="Product master data retrieved successfully",
        data={"products": [p.to_dict() for p in products], "count": total_count},
        meta={
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_next": offset + page_size < total_count,
            "has_prev": page > 1,
        },
    )


@router.get("/rfq-items", response_model=RFQItemsQueryResponse)
async def query_rfq_items(
    rfq_id: str | None = None,
    source: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Query RFQ items

    - **rfq_id**: Filter by RFQ ID
    - **source**: Filter by source (rfq_1_table_0, rfq_1_table_1, rfq_2)
    """
    query = db.query(RFQItem)

    if rfq_id:
        query = query.filter(RFQItem.rfq_id == rfq_id)

    if source:
        query = query.filter(RFQItem.source == source)

    items = query.all()

    return RFQItemsQueryResponse(
        data=[item.to_dict() for item in items], count=len(items), rfq_id=rfq_id
    )


@router.get("/rfq-matches", response_model=RFQMatchesQueryResponse)
async def query_rfq_matches(
    rfq_id: str | None = None,
    min_score: float | None = Query(None, ge=0, le=100),
    method: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Query RFQ match results

    - **rfq_id**: Filter by RFQ ID
    - **min_score**: Minimum match score
    - **method**: Filter by matching method (fuzzy, cosine)
    """
    # Build query with joins
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

    if method:
        query = query.filter(RFQMatch.method == method)

    results = query.all()

    # Build response
    matches = []
    for match, rfq_item, product in results:
        matches.append(
            {
                "rfq": {
                    "raw_text": rfq_item.raw_text,
                    "clean_text": rfq_item.clean_text,
                    "qty": rfq_item.qty,
                    "uom": rfq_item.uom,
                    "source": rfq_item.source,
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

    return RFQMatchesQueryResponse(data=matches, count=len(matches), rfq_id=rfq_id)


@router.get("/dataframe/product-master", response_model=DataframeQueryResponse)
async def get_product_master_dataframe(
    format: str = Query("json", pattern="^(json|csv)$"), db: Session = Depends(get_db)
):
    """
    Get product master as dataframe

    - **format**: Response format (json or csv)
    """
    products = db.query(ProductMaster).all()
    data = [p.to_dict() for p in products]

    if not data:
        return DataframeQueryResponse(data=[], columns=[], shape=[0, 0])

    df = pd.DataFrame(data)

    if format == "csv":
        csv_data = df.to_csv(index=False)
        return Response(content=csv_data, media_type="text/csv")

    return DataframeQueryResponse(
        data=data, columns=df.columns.tolist(), shape=df.shape
    )


@router.get("/dataframe/rfq-items/{rfq_id}", response_model=DataframeQueryResponse)
async def get_rfq_items_dataframe(
    rfq_id: str,
    format: str = Query("json", pattern="^(json|csv)$"),
    db: Session = Depends(get_db),
):
    """
    Get RFQ items as dataframe

    - **rfq_id**: RFQ identifier
    - **format**: Response format (json or csv)
    """
    items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

    if not items:
        return DataframeQueryResponse(data=[], columns=[], shape=[0, 0], rfq_id=rfq_id)

    data = [item.to_dict() for item in items]
    df = pd.DataFrame(data)

    if format == "csv":
        from fastapi.responses import Response

        csv_data = df.to_csv(index=False)
        return Response(content=csv_data, media_type="text/csv")

    return DataframeQueryResponse(
        data=data, columns=df.columns.tolist(), shape=df.shape, rfq_id=rfq_id
    )


@router.get("/csv/summary", response_model=CSVSummaryResponse)
async def get_csv_summary():
    """
    Get summary of CSV storage
    """
    summary = csv_storage.get_summary()
    return CSVSummaryResponse(**summary)


@router.get("/csv/product-master")
async def get_csv_product_master():
    """
    Get product master from CSV storage
    """
    df = csv_storage.load_product_master()

    if df.empty:
        return {"data": [], "count": 0}

    return {
        "data": df.to_dict(orient="records"),
        "count": len(df),
        "columns": df.columns.tolist(),
        "shape": df.shape,
    }


@router.get("/csv/rfq-items")
async def get_csv_rfq_items(rfq_id: str | None = None):
    """
    Get RFQ items from CSV storage

    - **rfq_id**: Optional RFQ ID filter
    """
    df = csv_storage.load_rfq_items(rfq_id)

    if df.empty:
        return {"data": [], "count": 0, "rfq_id": rfq_id}

    return {
        "data": df.to_dict(orient="records"),
        "count": len(df),
        "columns": df.columns.tolist(),
        "shape": df.shape,
        "rfq_id": rfq_id,
    }


@router.get("/match/top")
async def get_top_matches(
    query: str = Query(..., min_length=1),
    top_n: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db),
):
    """
    Get top N matches for a query text

    - **query**: Text to match
    - **top_n**: Number of top matches to return
    """
    # Ensure matching service is loaded
    try:
        rfq_service.reload_matching_service(db)
    except Exception:
        pass

    from apps.app_nippon_rfq_matching.app.models.schemas import (
        TopMatchesResponse,
        TopMatchResult,
    )
    from apps.app_nippon_rfq_matching.app.services.matching import matching_service

    matches = matching_service.get_top_matches(query, top_n)

    return TopMatchesResponse(
        query=query, matches=[TopMatchResult(**m) for m in matches]
    )


@router.get("/product-master/search", response_model=ProductMasterQueryResponse)
async def search_product_master(
    keyword: str = Query(..., min_length=1, description="Keyword to search for"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sheet_type: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Search product master by keyword

    Searches across multiple fields:
    - product_name
    - clean_product_name
    - pmc
    - sheet_type
    - color

    - **keyword**: Keyword to search for (e.g., "A-MARINE", "Anti-Fouling")
    - **page**: Page number (starts from 1)
    - **page_size**: Number of items per page
    - **sheet_type**: Optional filter by sheet type
    """
    from sqlalchemy import or_

    search_pattern = f"%{keyword}%"

    # Build query with multiple field search
    query = db.query(ProductMaster).filter(
        or_(
            ProductMaster.product_name.ilike(search_pattern),
            ProductMaster.clean_product_name.ilike(search_pattern),
            ProductMaster.pmc.ilike(search_pattern),
            ProductMaster.sheet_type.ilike(search_pattern),
            ProductMaster.color.ilike(search_pattern),
        )
    )

    # Apply additional filter if specified
    if sheet_type:
        query = query.filter(ProductMaster.sheet_type == sheet_type)

    # Get total count
    total_count = query.count()

    # Apply pagination
    offset = (page - 1) * page_size
    products = query.offset(offset).limit(page_size).all()

    return ProductMasterQueryResponse(
        data=[p.to_dict() for p in products],
        count=total_count,
        page=page,
        page_size=page_size,
    )


@router.get("/product-master/lookup")
async def lookup_product_master(
    keyword: str = Query(..., min_length=1, description="Keyword to lookup"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Quick lookup for product master by keyword

    Returns a simplified list for quick lookup/reference.
    Searches in product_name, clean_product_name, and pmc.

    - **keyword**: Keyword to search for
    - **limit**: Maximum number of results (default: 20)
    """
    from sqlalchemy import or_

    search_pattern = f"%{keyword}%"

    # Build query
    products = (
        db.query(ProductMaster)
        .filter(
            or_(
                ProductMaster.product_name.ilike(search_pattern),
                ProductMaster.clean_product_name.ilike(search_pattern),
                ProductMaster.pmc.ilike(search_pattern),
            )
        )
        .limit(limit)
        .all()
    )

    # Return simplified response
    return {
        "keyword": keyword,
        "count": len(products),
        "results": [
            {
                "id": p.id,
                "product_name": p.product_name,
                "clean_product_name": p.clean_product_name,
                "pmc": p.pmc,
                "sheet_type": p.sheet_type,
                "color": p.color,
            }
            for p in products
        ],
    }


@router.get("/product-master/{id}", response_model=APIResponse)
async def get_product_master_detail(id: int, db: Session = Depends(get_db)):
    """
    Get product master detail by ID

    - **id**: Product master ID
    """
    try:
        product_master = db.query(ProductMaster).filter(ProductMaster.id == id).first()

        if not product_master:
            raise HTTPException(
                status_code=404,
                detail=APIErrorResponse(
                    message="Product master not found",
                    error="NOT_FOUND",
                    error_code="PRODUCT_MASTER_NOT_FOUND",
                ).dict(exclude_none=True),
            )

        # Convert to dict and handle datetime
        product_dict = {
            "id": product_master.id,
            "sheet_name": product_master.sheet_name,
            "sheet_type": product_master.sheet_type,
            "row_excel": product_master.row_excel,
            "pmc": product_master.pmc,
            "product_name": product_master.product_name,
            "color": product_master.color,
            "clean_product_name": product_master.clean_product_name,
            "created_at": product_master.created_at.isoformat()
            if product_master.created_at
            else None,
            "regions": [],
        }

        # Get regions with pricing
        pricing_records = (
            db.query(ProductPrices).filter(ProductPrices.product_master_id == id).all()
        )

        for pricing in pricing_records:
            region = db.query(Region).filter(Region.id == pricing.region_id).first()
            if region:
                product_dict["regions"].append(
                    {
                        "region_id": pricing.region_id,
                        "region_name": region.name,
                        "size": pricing.size,
                        "uom": pricing.uom,
                        "price": pricing.price,
                        "price_raw": pricing.price_raw,
                        "created_at": pricing.created_at.isoformat()
                        if pricing.created_at
                        else None,
                    }
                )

        response = APIResponse(
            success=True,
            message="Product master detail retrieved successfully",
            data=product_dict,
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        handle_error(
            e, "PRODUCT_MASTER_DETAIL_ERROR", "Failed to get product master detail"
        )


@router.get("/product-master/{id}/regions", response_model=APIResponse)
async def get_product_master_regions(
    id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    region_filter: str | None = Query(
        None, description="Filter by region name (comma separated)"
    ),
    db: Session = Depends(get_db),
):
    """
    Get product master with paginated regions

    - **id**: Product master ID
    - **page**: Page number (starts from 1)
    - **page_size**: Number of items per page (max 100)
    - **region_filter**: Filter by region names, comma separated (e.g., 'NPMC,NPMS')
    """
    try:
        # Get product master
        product_master = db.query(ProductMaster).filter(ProductMaster.id == id).first()

        if not product_master:
            raise HTTPException(
                status_code=404,
                detail=APIErrorResponse(
                    message="Product master not found",
                    error="NOT_FOUND",
                    error_code="PRODUCT_MASTER_NOT_FOUND",
                ).dict(exclude_none=True),
            )

        # Build product master response
        product_dict = {
            "id": product_master.id,
            "sheet_name": product_master.sheet_name,
            "sheet_type": product_master.sheet_type,
            "row_excel": product_master.row_excel,
            "pmc": product_master.pmc,
            "product_name": product_master.product_name,
            "color": product_master.color,
            "clean_product_name": product_master.clean_product_name,
            "created_at": product_master.created_at.isoformat()
            if product_master.created_at
            else None,
        }

        # Get base query for pricing
        query = db.query(ProductPrices).filter(ProductPrices.product_master_id == id)

        # Apply region filter if provided
        if region_filter:
            region_names = [r.strip() for r in region_filter.split(",")]
            if not region_names:
                handle_validation_error(
                    ValueError("Empty region filter"), "Region filter cannot be empty"
                )
            query = query.join(Region).filter(Region.name.in_(region_names))

        # Get total count
        total_count = query.count()

        # Apply pagination
        offset = (page - 1) * page_size
        pricing_records = query.offset(offset).limit(page_size).all()

        # Get regions data
        regions_data = []
        for pricing in pricing_records:
            region = db.query(Region).filter(Region.id == pricing.region_id).first()
            if region:
                region_data = {
                    "region_id": pricing.region_id,
                    "region_name": region.name,
                    "size": pricing.size,
                    "uom": pricing.uom,
                    "price": pricing.price,
                    "price_raw": pricing.price_raw,
                    "created_at": pricing.created_at.isoformat()
                    if pricing.created_at
                    else None,
                }
                regions_data.append(region_data)

        # Calculate pagination metadata
        total_pages = (total_count + page_size - 1) // page_size

        response = APIResponse(
            success=True,
            message="Product master regions retrieved successfully",
            data={"product_master": product_dict, "regions": regions_data},
            meta={
                "total_regions": total_count,
                "regions_count": len(regions_data),
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": offset + page_size < total_count,
                "has_prev": page > 1,
            },
        )

        return response

    except HTTPException:
        raise
    except ValueError as e:
        handle_validation_error(e)
    except Exception as e:
        handle_error(
            e, "PRODUCT_MASTER_REGIONS_ERROR", "Failed to get product master regions"
        )
