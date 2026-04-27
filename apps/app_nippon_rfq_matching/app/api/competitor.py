"""
API endpoints for Competitor Matrix

Handles competitor matrix Excel file uploads and queries.
"""

import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import UploadedFile
from apps.app_nippon_rfq_matching.app.models.schemas import (
    BrandResponse,
    CompetitorMatrixUploadResponse,
    CompetitorProductResponse,
    GenericResponse,
)
from apps.app_nippon_rfq_matching.app.services.competitor_service import (
    CompetitorService,
)

router = APIRouter(prefix="/competitor", tags=["competitor"])


def save_uploaded_file(content: bytes, filename: str) -> UploadedFile:
    """
    Save uploaded file to disk and create database record.

    Args:
        content: File content as bytes
        filename: Original filename

    Returns:
        UploadedFile database record
    """
    # Create upload directory if not exists
    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    # Generate unique filename
    file_extension = Path(filename).suffix
    stored_filename = f"{uuid.uuid4()}{file_extension}"
    file_path = upload_dir / stored_filename

    # Save file
    with open(file_path, "wb") as f:
        f.write(content)

    # Create database record
    uploaded_file = UploadedFile(
        original_filename=filename,
        stored_filename=stored_filename,
        file_type="excel",
        file_path=str(file_path),
        status="pending",
    )

    return uploaded_file


@router.post("/upload", response_model=CompetitorMatrixUploadResponse)
async def upload_competitor_matrix(
    file: UploadFile = File(...),
    sheet_name: str | None = Query(
        None, description="Sheet name to parse (default: first sheet)"
    ),
    header_row: int | None = Query(5, description="Header row number (default: 5)"),
    db: Session = Depends(get_db),
):
    """
    Upload and parse Competitor Matrix Excel file

    Expected Excel format:
    - First column: GENERIC (generic product names)
    - Other columns: Brand names with their equivalent products
    - Header row: Specified row number (default: row 5)

    - **file**: Excel file (.xlsx or .xls) containing competitor matrix
    - **sheet_name**: Optional sheet name to parse (default: first sheet)
    - **header_row**: Header row number (default: 5)

    Returns summary of imported data including:
    - Number of generics, brands, products, and equivalents
    - List of imported generics and brands
    """
    # Validate file extension
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed"
        )

    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        # Read file content
        content = await file.read()

        # Save uploaded file
        uploaded_file = save_uploaded_file(content, file.filename)
        db.add(uploaded_file)
        db.commit()
        db.refresh(uploaded_file)

        # Process Excel file
        try:
            service = CompetitorService(db)
            result = service.import_competitor_matrix(
                file_path=uploaded_file.file_path,
                sheet_name=sheet_name,
                header_row=header_row,
            )

            # Update file status
            uploaded_file.status = "parsed"
            db.commit()

            return result

        except Exception as e:
            uploaded_file.status = "error"
            uploaded_file.error_message = str(e)
            db.commit()
            raise HTTPException(
                status_code=500, detail=f"Error processing Excel file: {str(e)}"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")


@router.get("/generics", response_model=list[GenericResponse])
async def get_generics(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum records to return"),
    db: Session = Depends(get_db),
):
    """
    Get all generics

    Returns a list of all generic product categories.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    generics = service.get_all_generics(skip=skip, limit=limit)
    return generics


@router.get("/generics/{generic_name}/products", response_model=list[dict])
async def get_equivalent_products(generic_name: str, db: Session = Depends(get_db)):
    """
    Get all equivalent products for a generic

    Returns all competitor products that are equivalent to the specified generic.

    - **generic_name**: Name of the generic product
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    products = service.get_equivalent_products(generic_name)

    if not products:
        raise HTTPException(
            status_code=404, detail=f"No products found for generic: {generic_name}"
        )

    return products


@router.get("/brands", response_model=list[BrandResponse])
async def get_brands(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=500, description="Maximum records to return"),
    db: Session = Depends(get_db),
):
    """
    Get all brands

    Returns a list of all competitor brands.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    brands = service.get_all_brands(skip=skip, limit=limit)
    return brands


@router.get(
    "/brands/{brand_id}/products", response_model=list[CompetitorProductResponse]
)
async def get_products_by_brand(brand_id: int, db: Session = Depends(get_db)):
    """
    Get all products for a brand

    Returns all products for the specified brand.

    - **brand_id**: ID of the brand
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    products = service.get_products_by_brand(brand_id)

    if not products:
        raise HTTPException(
            status_code=404, detail=f"No products found for brand ID: {brand_id}"
        )

    return products


@router.get("/products/search", response_model=list[CompetitorProductResponse])
async def search_products(
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db),
):
    """
    Search products by name

    Returns products matching the search query.

    - **query**: Search term (searches in product name)
    - **limit**: Maximum number of results
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    products = service.search_products(query, limit=limit)

    if not products:
        raise HTTPException(
            status_code=404, detail=f"No products found matching: {query}"
        )

    return products


@router.get("/nippon-equivalents/{competitor_product}", response_model=list[dict])
async def find_nippon_equivalents(
    competitor_product: str,
    exact_match: bool = Query(False, description="Use exact match for product name"),
    db: Session = Depends(get_db),
):
    """
    Find Nippon products equivalent to a competitor product

    Searches for competitor products matching the given name,
    finds products in the same generic categories that have brand "NP MARINE" (Nippon Paint Marine).

    - **competitor_product**: Competitor product name to find Nippon equivalents for
    - **exact_match**: If True, only exact product matches; if False, uses partial matching

    Example:
    - /nippon-equivalents/Interspeed - Finds Nippon products (NP MARINE) equivalent to Interspeed
    - /nippon-equivalents/PIONEER - Finds Nippon products (NP MARINE) like A-MARINE that are in the same category

    Response includes:
    - competitor_product: Competitor product name (e.g., "PIONEER")
    - brand: Competitor brand info (e.g., "JOTUN")
    - generics: Generic categories linking competitor to Nippon products
    - nippon_products: Nippon products from NP MARINE brand in the same categories
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    equivalents = service.find_nippon_equivalents_by_competitor(
        competitor_product, exact_match
    )

    if not equivalents:
        raise HTTPException(
            status_code=404,
            detail=f"No Nippon equivalents found for competitor product: {competitor_product}",
        )

    return equivalents


@router.get("/competitor-equivalents/{nippon_product}", response_model=list[dict])
async def find_competitor_equivalents(
    nippon_product: str,
    exact_match: bool = Query(False, description="Use exact match for generic name"),
    db: Session = Depends(get_db),
):
    """
    Find competitor products equivalent to a Nippon product

    Searches for generics matching the Nippon product name/category
    and returns all competitor products equivalent to those generics.

    - **nippon_product**: Nippon product name or category to find competitor equivalents for
    - **exact_match**: If True, only exact generic matches; if False, uses partial matching

    Example:
    - /competitor-equivalents/Anti-Fouling - Finds all competitor products for Anti-Fouling category
    - /competitor-equivalents/Interspeed - Finds competitor products for Interspeed generics
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    equivalents = service.find_competitor_equivalents(nippon_product, exact_match)

    if not equivalents:
        raise HTTPException(
            status_code=404,
            detail=f"No competitor equivalents found for: {nippon_product}",
        )

    return equivalents


@router.get("/generics/search", response_model=list[GenericResponse])
async def search_generics(
    query: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db),
):
    """
    Search generics by name

    Returns generics matching the search query.

    - **query**: Search term (searches in generic name)
    - **limit**: Maximum number of results
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    generics = service.search_generics(query, limit=limit)

    if not generics:
        raise HTTPException(
            status_code=404, detail=f"No generics found matching: {query}"
        )

    return generics


@router.delete("/generics/{generic_id}", response_model=dict)
async def delete_generic(generic_id: int, db: Session = Depends(get_db)):
    """
    Delete a generic and its equivalences

    Deletes the specified generic and all its product equivalences.

    - **generic_id**: ID of the generic to delete
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    deleted = service.delete_generic(generic_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Generic not found: {generic_id}")

    db.commit()

    return {"status": "success", "message": f"Generic {generic_id} deleted"}


@router.delete("/brands/{brand_id}", response_model=dict)
async def delete_brand(brand_id: int, db: Session = Depends(get_db)):
    """
    Delete a brand and its products

    Deletes the specified brand and all its products and equivalences.

    - **brand_id**: ID of the brand to delete
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    service = CompetitorService(db)
    deleted = service.delete_brand(brand_id)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Brand not found: {brand_id}")

    db.commit()

    return {"status": "success", "message": f"Brand {brand_id} deleted"}
