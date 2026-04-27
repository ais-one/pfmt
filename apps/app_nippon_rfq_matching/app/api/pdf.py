"""
API endpoints for PDF generation
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem, RFQMatch
from apps.app_nippon_rfq_matching.app.models.schemas import (
    PDFFormatInfo,
    PDFFormatsResponse,
    PDFGenerateRequest,
)
from apps.app_nippon_rfq_matching.app.services.pdf_service import (
    PDFConfig,
    PDFFormatType,
    pdf_service,
)

router = APIRouter(prefix="/pdf", tags=["pdf"])


@router.get("/formats", response_model=PDFFormatsResponse)
async def get_pdf_formats():
    """
    Get available PDF format types

    Returns:
        List of available PDF formats with descriptions
    """
    formats = [
        PDFFormatInfo(
            format_type="table",
            name="Table Format",
            description="Compact table view showing RFQ items with matched products and scores",
        ),
        PDFFormatInfo(
            format_type="side_by_side",
            name="Side by Side Comparison",
            description="Detailed side-by-side comparison between RFQ items and matched products",
        ),
        PDFFormatInfo(
            format_type="summary",
            name="Summary Report",
            description="Comprehensive summary with statistics, top matches, and detailed breakdown",
        ),
    ]

    return PDFFormatsResponse(formats=formats, default_format="table")


@router.post("/generate")
async def generate_pdf_report(
    request: PDFGenerateRequest, db: Session = Depends(get_db)
):
    """
    Generate PDF report for RFQ vs Product Master comparison

    Args:
        request: PDF generation request with rfq_id, format_type, min_score, and include_competitor

    Returns:
        PDF file as binary response

    Example:
        POST /api/v1/pdf/generate
        {
            "rfq_id": "RFQ-12345",
            "format_type": "table",
            "min_score": 70.0,
            "include_competitor": false
        }
    """
    # Validate format type
    try:
        format_type = PDFFormatType(request.format_type)
    except ValueError:
        available = pdf_service.list_formats()
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format_type '{request.format_type}'. Available formats: {', '.join(available)}",
        )

    # If competitor matching is requested, use the new service
    if request.include_competitor:
        from apps.app_nippon_rfq_matching.app.services.rfq_competitor_matching import (
            RFQCompetitorMatcher,
        )

        matcher = RFQCompetitorMatcher(db)
        competitor_results = matcher.match_rfq_by_id(
            request.rfq_id, max_results_per_item=request.max_results
        )

        if not competitor_results["matches"]:
            raise HTTPException(
                status_code=404,
                detail=f"No RFQ items found for rfq_id: {request.rfq_id}",
            )

        # Build match data from competitor results
        # Note: Only take the FIRST (top score) match for each RFQ item to avoid noise
        # min_score is ignored since results are already sorted by score
        matches = []
        for item_match in competitor_results["matches"]:
            # Only take the first (top) match
            if item_match["nippon_matches"]:
                nippon_match = item_match["nippon_matches"][0]
                matches.append(
                    {
                        "rfq": {
                            "raw_text": item_match["raw_text"],
                            "clean_text": item_match["clean_text"],
                            "qty": item_match["qty"],
                            "uom": item_match["uom"],
                            "source": "competitor_match",
                            "color": item_match.get("color"),
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
                            "extracted_color": item_match.get("color"),
                            "color_match": item_match.get("color") is not None,
                        },
                        "competitor_info": {
                            "competitor_matches": item_match.get(
                                "competitor_matches", []
                            )[:5],
                            "keywords": item_match.get("keywords", []),
                            "np_marine_product": nippon_match.get(
                                "competitor_source", {}
                            ).get("np_marine_product"),
                            "generic_names": nippon_match.get("generic_names", []),
                        },
                    }
                )

        # Build RFQ items data
        rfq_items_data = [
            {
                "id": item_match["rfq_item_id"],
                "raw_text": item_match["raw_text"],
                "clean_text": item_match["clean_text"],
                "qty": item_match["qty"],
                "uom": item_match["uom"],
                "color": item_match.get("color"),
            }
            for item_match in competitor_results["matches"]
        ]

        if not matches:
            raise HTTPException(
                status_code=404, detail=f"No matches found for rfq_id: {request.rfq_id}"
            )

        # Create PDF config
        config = PDFConfig(
            title="RFQ vs Product Master Comparison (with Competitor Matching)",
            company_name="Nippon Paint Marine",
            page_size="A4",
        )

        # Generate PDF
        try:
            pdf_bytes = pdf_service.generate_pdf_from_dict(
                rfq_id=request.rfq_id,
                rfq_items=rfq_items_data,
                matches=matches,
                format_type=format_type,
                config=config,
            )

            # Return PDF as response
            filename = (
                f"rfq_comparison_{request.rfq_id}_{request.format_type}_competitor.pdf"
            )
            return Response(
                content=pdf_bytes,
                media_type="application/pdf",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error generating PDF: {str(e)}"
            )

    # Original logic without competitor matching
    # Query RFQ items
    rfq_items_query = db.query(RFQItem).filter(RFQItem.rfq_id == request.rfq_id)
    rfq_items = rfq_items_query.all()

    if not rfq_items:
        raise HTTPException(
            status_code=404, detail=f"No RFQ items found for rfq_id: {request.rfq_id}"
        )

    # Query matches with joins
    query = (
        db.query(RFQMatch, RFQItem, ProductMaster)
        .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
        .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
        .filter(RFQItem.rfq_id == request.rfq_id)
    )

    # Apply min_score filter
    if request.min_score is not None:
        query = query.filter(RFQMatch.score >= request.min_score)

    results = query.all()

    # Build match data
    matches = []
    for match, rfq_item, product in results:
        # Extract color from RFQ text (same logic used during matching)
        from apps.app_nippon_rfq_matching.app.services.matching import matching_service

        extracted_color = matching_service.extract_color_from_text(rfq_item.raw_text)

        # Check if colors match
        color_match = False
        if extracted_color:
            # RFQ has color
            product_color = str(product.color).strip() if product.color else ""

            # If product doesn't have color -> NOT MATCH
            if (
                not product_color
                or product_color == "-"
                or product_color.upper() == "NONE"
            ):
                color_match = False
            else:
                # Both have colors, check if they match
                color_match = matching_service._colors_match(
                    extracted_color, product_color
                )

        matches.append(
            {
                "rfq": {
                    "raw_text": rfq_item.raw_text,
                    "clean_text": rfq_item.clean_text,
                    "qty": rfq_item.qty,
                    "uom": rfq_item.uom,
                    "source": rfq_item.source,
                    "color": extracted_color,
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
                    "extracted_color": extracted_color,
                    "color_match": color_match,
                },
            }
        )

    if not matches:
        raise HTTPException(
            status_code=404,
            detail=f"No matches found for rfq_id: {request.rfq_id} with min_score: {request.min_score}",
        )

    # Build RFQ items data
    rfq_items_data = [item.to_dict() for item in rfq_items]

    # Create PDF config with hardcoded values
    config = PDFConfig(
        title="RFQ vs Product Master Comparison",
        company_name="Nippon Paint Marine",
        page_size="A4",
    )

    # Generate PDF
    try:
        pdf_bytes = pdf_service.generate_pdf_from_dict(
            rfq_id=request.rfq_id,
            rfq_items=rfq_items_data,
            matches=matches,
            format_type=format_type,
            config=config,
        )

        # Return PDF as response
        filename = f"rfq_comparison_{request.rfq_id}_{request.format_type}.pdf"
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating PDF: {str(e)}")


@router.get("/generate/{rfq_id}")
async def generate_pdf_report_get(
    rfq_id: str,
    format_type: str = Query(
        "table", description="PDF format: table, side_by_side, summary"
    ),
    min_score: float | None = Query(
        70.0, ge=0, le=100, description="Minimum match score"
    ),
    include_competitor: bool = Query(
        False, description="Include competitor product matching"
    ),
    max_results: int = Query(
        20, ge=1, le=100, description="Max results per item for competitor matching"
    ),
    db: Session = Depends(get_db),
):
    """
    Generate PDF report for RFQ vs Product Master comparison (GET method)

    Args:
        rfq_id: RFQ identifier
        format_type: PDF format type (table, side_by_side, summary)
        min_score: Minimum match score to include (default: 70.0)
        include_competitor: Include competitor product matching (default: false)
        max_results: Maximum results per item for competitor matching (default: 20)

    Returns:
        PDF file as binary response

    Example:
        GET /api/v1/pdf/generate/RFQ-12345?format_type=table&min_score=70&include_competitor=true
    """
    # Create request object and reuse POST logic
    request = PDFGenerateRequest(
        rfq_id=rfq_id,
        format_type=format_type,
        min_score=min_score,
        include_competitor=include_competitor,
        max_results=max_results,
    )

    return await generate_pdf_report(request, db)


@router.get("/preview/{rfq_id}")
async def preview_pdf_data(
    rfq_id: str,
    min_score: float | None = Query(70.0, ge=0, le=100),
    db: Session = Depends(get_db),
):
    """
    Preview match data before generating PDF

    Returns the match data that would be included in the PDF without generating the actual file.
    Useful for previewing what data will be in the report.

    Args:
        rfq_id: RFQ identifier
        min_score: Minimum match score

    Returns:
        JSON with match data and statistics
    """
    # Query RFQ items
    rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

    if not rfq_items:
        raise HTTPException(
            status_code=404, detail=f"No RFQ items found for rfq_id: {rfq_id}"
        )

    # Query matches
    query = (
        db.query(RFQMatch, RFQItem, ProductMaster)
        .join(RFQItem, RFQMatch.rfq_item_id == RFQItem.id)
        .join(ProductMaster, RFQMatch.product_master_id == ProductMaster.id)
        .filter(RFQItem.rfq_id == rfq_id)
    )

    if min_score is not None:
        query = query.filter(RFQMatch.score >= min_score)

    results = query.all()

    # Build match data
    matches = []
    scores = []
    for match, rfq_item, product in results:
        match_data = {
            "rfq": {
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
            },
            "match_info": {
                "score": match.score,
                "method": match.method,
            },
        }
        matches.append(match_data)
        scores.append(match.score)

    # Calculate statistics
    stats = {
        "rfq_id": rfq_id,
        "total_rfq_items": len(rfq_items),
        "total_matches": len(matches),
        "min_score_filter": min_score,
    }

    if scores:
        stats.update(
            {
                "average_score": sum(scores) / len(scores),
                "max_score": max(scores),
                "min_score": min(scores),
                "high_confidence": len([s for s in scores if s >= 85]),
                "medium_confidence": len([s for s in scores if 70 <= s < 85]),
                "low_confidence": len([s for s in scores if s < 70]),
            }
        )
    else:
        stats.update(
            {
                "average_score": 0,
                "max_score": 0,
                "min_score": 0,
                "high_confidence": 0,
                "medium_confidence": 0,
                "low_confidence": 0,
            }
        )

    return {
        "statistics": stats,
        "matches": matches,
    }
