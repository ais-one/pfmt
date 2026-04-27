"""
Ticket Generation Service

Service for generating tickets for non-matching RFQ items
"""

import logging
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.rfq import RFQItem
from apps.app_nippon_rfq_matching.app.services.matching_data_service import (
    matching_data_service,
)

logger = logging.getLogger(__name__)


class TicketGenerationService:
    """Service for generating tickets for non-matching RFQ items"""

    def __init__(self):
        """Initialize ticket generation service"""
        self.supported_regions = [
            "Indonesia",
            "Malaysia",
            "Singapore",
            "Thailand",
            "Vietnam",
        ]

    def generate_ticket_for_rfq_item(
        self,
        rfq_item: dict[str, Any],
        rfq_id: str,
        reason: str = "no_match",
        priority: str = "medium",
        assignee: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate a ticket for a single RFQ item.

        Args:
            rfq_item: RFQ item dictionary
            rfq_id: RFQ identifier
            reason: Reason for ticket (no_match, price_needed, color_needed)
            priority: Ticket priority (low, medium, high)
            assignee: Person to assign the ticket to

        Returns:
            Generated ticket dictionary
        """
        ticket_id = f"TICKET-{uuid.uuid4().hex[:8].upper()}"

        ticket = {
            "ticket_id": ticket_id,
            "rfq_id": rfq_id,
            "rfq_item_id": rfq_item.get("id"),
            "raw_text": rfq_item.get("raw_text", ""),
            "clean_text": rfq_item.get("clean_text", ""),
            "extracted_qty": rfq_item.get("qty"),
            "extracted_uom": rfq_item.get("uom"),
            "extracted_color": rfq_item.get("normalized_color")
            or rfq_item.get("color"),
            "priority": priority,
            "reason": reason,
            "status": "open",
            "assignee": assignee or "product_team",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "metadata": {
                "source_file": rfq_id,
                "item_index": rfq_item.get("id"),
                "normalized_name": rfq_item.get("normalized_name"),
                "product_type": rfq_item.get("product_type"),
                "competitor_info": {
                    "source_brand": rfq_item.get("source_brand"),
                    "source_color_code": rfq_item.get("source_color_code"),
                    "npms_color_code": rfq_item.get("npms_color_code"),
                }
                if rfq_item.get("product_type") == "competitor"
                else None,
            },
        }

        logger.info(f"Generated ticket {ticket_id} for RFQ item {rfq_item.get('id')}")
        return ticket

    def generate_tickets_for_unmatched_items(
        self,
        unmatched_items: list[dict[str, Any]],
        rfq_id: str,
        region: str = "Indonesia",
    ) -> list[dict[str, Any]]:
        """
        Generate tickets for all unmatched RFQ items.

        Args:
            unmatched_items: List of unmatched RFQ items
            rfq_id: RFQ identifier
            region: Region name

        Returns:
            List of generated tickets
        """
        tickets = []

        for item in unmatched_items:
            # Determine ticket reason based on item information
            reason = self._determine_ticket_reason(item, region)

            # Set priority based on reason
            priority = self._determine_ticket_priority(reason, item)

            # Generate ticket
            ticket = self.generate_ticket_for_rfq_item(
                item, rfq_id, reason=reason, priority=priority
            )
            tickets.append(ticket)

        logger.info(
            f"Generated {len(tickets)} tickets for unmatched items in RFQ {rfq_id}"
        )
        return tickets

    def _determine_ticket_reason(self, item: dict[str, Any], region: str) -> str:
        """
        Determine the reason for generating a ticket.

        Args:
            item: Unmatched RFQ item
            region: Region name

        Returns:
            Ticket reason
        """
        # Check if it's a competitor product that might need mapping
        if item.get("product_type") == "competitor":
            if item.get("source_brand"):
                return "competitor_mapping_needed"
            else:
                return "competitor_identification_needed"

        # Check if it's a Nippon product that couldn't be matched
        elif item.get("product_type") == "nippon":
            return "nippon_product_update_needed"

        # Default case
        return "no_match"

    def _determine_ticket_priority(self, reason: str, item: dict[str, Any]) -> str:
        """
        Determine ticket priority based on reason and item information.

        Args:
            reason: Ticket reason
            item: RFQ item

        Returns:
            Ticket priority (low, medium, high)
        """
        high_priority_reasons = ["competitor_mapping_needed", "price_needed"]

        medium_priority_reasons = ["nippon_product_update_needed", "color_needed"]

        if reason in high_priority_reasons:
            return "high"
        elif reason in medium_priority_reasons:
            return "medium"
        else:
            return "low"

    def save_tickets_to_database(
        self, tickets: list[dict[str, Any]], db: Session
    ) -> list[Any]:
        """
        Save generated tickets to database.

        Args:
            tickets: List of ticket dictionaries
            db: Database session

        Returns:
            List of saved ticket database records
        """
        from apps.app_nippon_rfq_matching.app.models.ticket import (
            Ticket,
        )  # Assuming you have a Ticket model

        saved_tickets = []

        for ticket_data in tickets:
            # Create ticket record
            ticket_record = Ticket(
                ticket_id=ticket_data["ticket_id"],
                rfq_id=ticket_data["rfq_id"],
                rfq_item_id=ticket_data.get("rfq_item_id"),
                raw_text=ticket_data["raw_text"],
                clean_text=ticket_data["clean_text"],
                extracted_qty=ticket_data.get("extracted_qty"),
                extracted_uom=ticket_data.get("extracted_uom"),
                extracted_color=ticket_data.get("extracted_color"),
                priority=ticket_data["priority"],
                reason=ticket_data["reason"],
                status=ticket_data["status"],
                assignee=ticket_data["assignee"],
                metadata=ticket_data["metadata"],
            )

            db.add(ticket_record)
            saved_tickets.append(ticket_record)

        try:
            db.commit()
            for ticket in saved_tickets:
                db.refresh(ticket)

            logger.info(f"Saved {len(saved_tickets)} tickets to database")

        except Exception as e:
            db.rollback()
            logger.error(f"Error saving tickets to database: {e}")
            raise

        return saved_tickets

    def generate_and_save_tickets(
        self, rfq_id: str, db: Session, region: str = "Indonesia"
    ) -> dict[str, Any]:
        """
        Generate and save tickets for unmatched items in an RFQ.

        Args:
            rfq_id: RFQ identifier
            db: Database session
            region: Region name

        Returns:
            Result with tickets and statistics
        """
        # Get unmatched items

        rfq_items = db.query(RFQItem).filter(RFQItem.rfq_id == rfq_id).all()

        if not rfq_items:
            return {"success": False, "error": f"No RFQ items found for {rfq_id}"}

        # Get matching data
        rfq_items_dict = [item.to_dict() for item in rfq_items]

        # Normalize items
        from apps.app_nippon_rfq_matching.app.services.pdf_comparison.exporter import (
            PDFExporter,
        )

        pdf_exporter = PDFExporter()

        normalized_items = pdf_exporter._get_normalized_items(
            rfq_items_dict, use_normalization=True, db=db
        )

        # Find matches
        matches = pdf_exporter.find_product_matches(normalized_items, db)

        # Enrich with product data
        enriched_matches = matching_data_service.enrich_matches_with_product_data(
            matches, db
        )

        # Separate unmatched items
        unmatched_items = []
        for match in enriched_matches:
            if not (match["product_master"] and match["product_master"].get("id")):
                unmatched_items.append(match)

        # Generate tickets
        tickets = self.generate_tickets_for_unmatched_items(
            unmatched_items, rfq_id, region
        )

        # Save to database
        saved_tickets = []
        if tickets:
            saved_tickets = self.save_tickets_to_database(tickets, db)

        # Calculate statistics
        stats = {
            "total_items": len(rfq_items),
            "unmatched_count": len(unmatched_items),
            "tickets_generated": len(tickets),
            "tickets_saved": len(saved_tickets),
            "ticket_statistics": {
                "high_priority": sum(1 for t in tickets if t["priority"] == "high"),
                "medium_priority": sum(1 for t in tickets if t["priority"] == "medium"),
                "low_priority": sum(1 for t in tickets if t["priority"] == "low"),
                "by_reason": {},
            },
        }

        # Count tickets by reason
        for ticket in tickets:
            reason = ticket["reason"]
            if reason not in stats["ticket_statistics"]["by_reason"]:
                stats["ticket_statistics"]["by_reason"][reason] = 0
            stats["ticket_statistics"]["by_reason"][reason] += 1

        return {
            "success": True,
            "rfq_id": rfq_id,
            "statistics": stats,
            "tickets": saved_tickets,
            "unmatched_items": unmatched_items,
        }


# Singleton instance
ticket_generation_service = TicketGenerationService()
