"""
API endpoints for querying tickets/RFQ items
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.core.logging_config import get_logger
from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem, RFQMatch
from apps.app_nippon_rfq_matching.app.models.schemas import (
    APIErrorResponse,
)

router = APIRouter(prefix="/tickets", tags=["tickets"])

logger = get_logger(__name__)


class TicketResponse(BaseModel):
    """Response schema for ticket data"""

    id: int
    rfq_id: str
    raw_text: str
    clean_text: str | None = None
    color: str | None = None
    qty: str | None = None
    uom: str | None = None
    source: str
    created_at: str
    has_match: bool


@router.get("/", response_model=list[TicketResponse])
async def get_tickets(
    has_match: bool | None = Query(
        None, description="Filter by match status: true for matched, false for no match"
    ),
    source: str | None = Query(None, description="Filter by source"),
    rfq_id: str | None = Query(None, description="Filter by RFQ ID"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of tickets to return"
    ),
    offset: int = Query(0, ge=0, description="Number of tickets to skip"),
    db: Session = Depends(get_db),
):
    """
    Get list of tickets/RFQ items with optional filtering

    - **has_match**: Filter by match status (null for all, true for matched, false for no match)
    - **source**: Filter by source (e.g., 'excel', 'pdf')
    - **rfq_id**: Filter by specific RFQ ID
    - **limit**: Maximum number of tickets to return (default: 100, max: 1000)
    - **offset**: Number of tickets to skip for pagination (default: 0)

    Returns list of tickets with match status
    """
    try:
        # Base query
        query = db.query(RFQItem)

        # Apply filters
        if has_match is not None:
            if has_match:
                # Only tickets that have matches
                matched_ids = db.query(RFQMatch.rfq_item_id).distinct()
                query = query.filter(RFQItem.id.in_(matched_ids))
            else:
                # Only tickets that don't have matches
                matched_ids = db.query(RFQMatch.rfq_item_id).distinct()
                query = query.filter(~RFQItem.id.in_(matched_ids))

        if source:
            query = query.filter(RFQItem.source == source)

        if rfq_id:
            query = query.filter(RFQItem.rfq_id == rfq_id)

        # Apply pagination
        tickets = (
            query.order_by(RFQItem.created_at.desc()).offset(offset).limit(limit).all()
        )

        # Convert to response format with match status
        result = []
        for ticket in tickets:
            # Check if ticket has any matches
            has_match_flag = (
                db.query(func.count(RFQMatch.id))
                .filter(RFQMatch.rfq_item_id == ticket.id)
                .scalar()
                > 0
            )

            result.append(
                TicketResponse(
                    id=ticket.id,
                    rfq_id=ticket.rfq_id,
                    raw_text=ticket.raw_text,
                    clean_text=ticket.clean_text,
                    color=ticket.color,
                    qty=ticket.qty,
                    uom=ticket.uom,
                    source=ticket.source,
                    created_at=ticket.created_at.isoformat()
                    if ticket.created_at
                    else None,
                    has_match=has_match_flag,
                )
            )

        return result

    except Exception as e:
        logger.error(f"Error getting tickets: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=APIErrorResponse(
                message="Failed to retrieve tickets",
                error=str(e),
                error_code="TICKET_RETRIEVAL_ERROR",
            ).dict(exclude_none=True),
        )


@router.get("/no-match", response_model=list[TicketResponse])
async def get_no_match_tickets(
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of tickets to return"
    ),
    offset: int = Query(0, ge=0, description="Number of tickets to skip"),
    db: Session = Depends(get_db),
):
    """
    Get list of tickets that have no matches

    - **limit**: Maximum number of tickets to return (default: 100, max: 1000)
    - **offset**: Number of tickets to skip for pagination (default: 0)

    Returns list of tickets without any matches
    """
    try:
        # Get all matched ticket IDs
        matched_ids = db.query(RFQMatch.rfq_item_id).distinct().subquery()

        # Query tickets that are not in the matched IDs
        query = db.query(RFQItem).filter(~RFQItem.id.in_(matched_ids))

        # Apply pagination
        tickets = (
            query.order_by(RFQItem.created_at.desc()).offset(offset).limit(limit).all()
        )

        # Convert to response format
        result = []
        for ticket in tickets:
            result.append(
                TicketResponse(
                    id=ticket.id,
                    rfq_id=ticket.rfq_id,
                    raw_text=ticket.raw_text,
                    clean_text=ticket.clean_text,
                    color=ticket.color,
                    qty=ticket.qty,
                    uom=ticket.uom,
                    source=ticket.source,
                    created_at=ticket.created_at.isoformat()
                    if ticket.created_at
                    else None,
                    has_match=False,
                )
            )

        return result

    except Exception as e:
        logger.error(f"Error getting no match tickets: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=APIErrorResponse(
                message="Failed to retrieve no match tickets",
                error=str(e),
                error_code="NO_MATCH_TICKET_RETRIEVAL_ERROR",
            ).dict(exclude_none=True),
        )


@router.get("/stats")
async def get_ticket_stats(db: Session = Depends(get_db)):
    """
    Get statistics about tickets

    Returns:
    - total_tickets: Total number of tickets
    - matched_tickets: Number of tickets with matches
    - no_match_tickets: Number of tickets without matches
    - by_source: Breakdown by source
    """
    try:
        # Total tickets
        total_tickets = db.query(func.count(RFQItem.id)).scalar()

        # Matched tickets
        matched_ids = db.query(RFQMatch.rfq_item_id).distinct()
        matched_tickets = (
            db.query(func.count(RFQItem.id))
            .filter(RFQItem.id.in_(matched_ids))
            .scalar()
        )

        # No match tickets
        no_match_tickets = total_tickets - matched_tickets

        # By source
        by_source = {}
        source_counts = (
            db.query(RFQItem.source, func.count(RFQItem.id))
            .group_by(RFQItem.source)
            .all()
        )

        for source, count in source_counts:
            by_source[source] = {
                "total": count,
                "matched": db.query(func.count(RFQItem.id))
                .filter(RFQItem.source == source, RFQItem.id.in_(matched_ids))
                .scalar(),
                "no_match": count
                - db.query(func.count(RFQItem.id))
                .filter(RFQItem.source == source, RFQItem.id.in_(matched_ids))
                .scalar(),
            }

        return {
            "total_tickets": total_tickets,
            "matched_tickets": matched_tickets,
            "no_match_tickets": no_match_tickets,
            "by_source": by_source,
        }

    except Exception as e:
        logger.error(f"Error getting ticket stats: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=APIErrorResponse(
                message="Failed to retrieve ticket statistics",
                error=str(e),
                error_code="TICKET_STATS_ERROR",
            ).dict(exclude_none=True),
        )
