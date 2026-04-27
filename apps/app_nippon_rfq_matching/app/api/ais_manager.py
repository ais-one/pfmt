"""
AIS Manager API endpoints
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status

from apps.app_nippon_rfq_matching.app.api.security import get_current_user
from apps.app_nippon_rfq_matching.app.services.ais_manager_service import (
    ais_manager_service,
)

router = APIRouter()

logger = logging.getLogger(__name__)


@router.post("/ais-manager/tickets", response_model=dict[str, Any])
async def create_ais_manager_ticket(
    request: Request,
    title: str,
    description: str,
    category_id: int = 209,
    status: str = "open",
    is_priority: bool = False,
    user: dict = Depends(get_current_user),
):
    """
    Create a ticket in AIS Manager system

    This endpoint creates a ticket in the AIS Manager system for tracking and management.
    """
    # Get user info from middleware
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="User not authenticated")

    # Validate required fields
    if not title or not description:
        raise HTTPException(
            status_code=400, detail="title and description are required"
        )

    # Log authenticated user
    user_data = user.get("data", {}).get("user", {})
    email = user_data.get("email", "Unknown")
    user_id_field = user_data.get("id", "Unknown")
    logger.info(
        f"Ticket creation initiated by user: {email} (ID: {user_id_field}) - Title: {title}"
    )

    # Validate status values
    valid_statuses = ["open", "in_progress", "resolved", "closed"]
    if status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}",
        )

    # Create ticket (get token from cookie)
    auth_cookie = request.cookies.get("Authorization")
    if not auth_cookie:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization token cookie is missing",
        )

    # Extract Bearer token
    if auth_cookie.startswith("Bearer "):
        token = auth_cookie[7:]
    else:
        token = auth_cookie

    result = ais_manager_service.create_ticket(
        user_id=user_id,
        title=title,
        description=description,
        category_id=category_id,
        status=status,
        is_priority=is_priority,
        bearer_token=token,
    )

    if result is None:
        raise HTTPException(
            status_code=500, detail="Failed to create ticket in AIS Manager system"
        )

    return {
        "success": True,
        "message": "Ticket created successfully in AIS Manager",
        "ticket_data": result,
    }


@router.post("/ais-manager/rfq-tickets", response_model=dict[str, Any])
async def create_rfq_ais_manager_ticket(
    request: Request, rfq_id: str, ticket_reason: str, rfq_data: dict[str, Any]
):
    """
    Create a ticket in AIS Manager specifically for RFQ issues

    This endpoint creates tickets for various RFQ-related issues like:
    - no_match: When no suitable product match is found
    - competitor_mapping_needed: When competitor product needs mapping
    - nippon_product_update_needed: When Nippon product needs update
    """
    # Get user info from middleware
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="User not authenticated")

    # Validate required fields
    if not rfq_id or not ticket_reason:
        raise HTTPException(
            status_code=400, detail="rfq_id and ticket_reason are required"
        )

    # Validate ticket reason
    valid_reasons = [
        "no_match",
        "competitor_mapping_needed",
        "nippon_product_update_needed",
    ]
    if ticket_reason not in valid_reasons:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ticket_reason. Must be one of: {', '.join(valid_reasons)}",
        )

    # Create RFQ-specific ticket (get token from cookie)
    auth_cookie = request.cookies.get("Authorization")
    if not auth_cookie:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization token cookie is missing",
        )

    # Extract Bearer token
    if auth_cookie.startswith("Bearer "):
        token = auth_cookie[7:]
    else:
        token = auth_cookie

    result = ais_manager_service.create_rfq_ticket(
        rfq_id=rfq_id,
        user_id=user_id,
        ticket_reason=ticket_reason,
        rfq_data=rfq_data,
        bearer_token=token,
    )

    if result is None:
        raise HTTPException(
            status_code=500, detail="Failed to create RFQ ticket in AIS Manager system"
        )

    return {
        "success": True,
        "message": "RFQ ticket created successfully in AIS Manager",
        "ticket_data": result,
    }


@router.post("/ais-manager/unmatched-items-tickets", response_model=dict[str, Any])
async def create_tickets_for_unmatched_items(
    request: Request, rfq_id: str, unmatched_items: list[dict[str, Any]]
):
    """
    Create tickets in AIS Manager for all unmatched RFQ items

    This endpoint creates individual tickets for each unmatched RFQ item,
    automatically determining the type of mismatch (product name, color, or both).
    """
    # Get user info from middleware
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="User not authenticated")

    # Validate required fields
    if not rfq_id or not unmatched_items:
        raise HTTPException(
            status_code=400, detail="rfq_id and unmatched_items are required"
        )

    # Validate unmatched items structure
    if not isinstance(unmatched_items, list):
        raise HTTPException(status_code=400, detail="unmatched_items must be a list")

    # Validate each item
    for i, item in enumerate(unmatched_items):
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=400, detail=f"Item {i} must be a dictionary"
            )
        # Required fields for each item
        required_fields = ["raw_text", "product_name"]
        for field in required_fields:
            if field not in item:
                raise HTTPException(
                    status_code=400, detail=f"Item {i} missing required field: {field}"
                )

    # Create tickets for all unmatched items (get token from cookie)
    auth_cookie = request.cookies.get("Authorization")
    if not auth_cookie:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization token cookie is missing",
        )

    # Extract Bearer token
    if auth_cookie.startswith("Bearer "):
        token = auth_cookie[7:]
    else:
        token = auth_cookie

    results = ais_manager_service.create_tickets_for_unmatched_items(
        unmatched_items=unmatched_items,
        rfq_id=rfq_id,
        user_id=user_id,
        bearer_token=token,
    )

    # Count successful tickets
    successful_tickets = len([r for r in results if r is not None])
    failed_tickets = len([r for r in results if r is None])

    # Log successful post request to AIS Manager tickets
    logger.info(
        f"POST Request - AIS Manager Tickets: Created {successful_tickets} tickets for {len(unmatched_items)} "
        f"unmatched items in RFQ {rfq_id} (user: {user_id})"
    )

    return {
        "success": True,
        "message": f"Created tickets for {successful_tickets} unmatched items",
        "statistics": {
            "total_items": len(unmatched_items),
            "successful_tickets": successful_tickets,
            "failed_tickets": failed_tickets,
            "success_rate": f"{(successful_tickets / len(unmatched_items) * 100):.1f}%",
        },
        "ticket_results": results,
    }
