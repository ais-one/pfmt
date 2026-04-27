"""
AIS Manager Service for integration with ticket system
"""

import logging
from typing import Any

import requests

from apps.app_nippon_rfq_matching.app.core.config import settings

logger = logging.getLogger(__name__)


class AISManagerService:
    """Service for integrating with AIS Manager ticket system"""

    def __init__(self):
        self.base_url = settings.AISMBACKEND_URL
        self.timeout = 30  # seconds

    def create_ticket(
        self,
        user_id: str,
        title: str,
        description: str,
        category_id: int = 209,
        status: str = "open",
        is_priority: bool = False,
        bearer_token: str = None,
    ) -> dict[str, Any] | None:
        """
        Create a ticket in AIS Manager

        Args:
            user_id: User ID from the system
            title: Ticket title
            description: Ticket description
            category_id: Category ID (default: 209)
            status: Ticket status (default: "open")
            is_priority: Whether ticket is priority (default: False)
            bearer_token: Bearer token for authentication

        Returns:
            Response data if successful, None otherwise
        """
        if not bearer_token:
            logger.error("AIS Manager Bearer Token not provided - cannot create ticket")
            return None

        url = f"{self.base_url}/api/ais-manager/tickets"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        }

        payload = {
            "platform": "rfq-matching-service",
            "user_id": user_id,
            "title": title,
            "description": description,
            "category_id": category_id,
            "status": status,
            "is_priority": is_priority,
        }

        # Log the ticket creation attempt
        logger.info(f"Attempting to create ticket in AIS Manager for user {user_id}")
        logger.debug(f"Ticket creation URL: {url}")
        logger.debug(f"Ticket creation payload: {payload}")

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=self.timeout
            )

            # Log response details
            logger.debug(f"Ticket creation response status: {response.status_code}")
            logger.debug(f"Ticket creation response headers: {dict(response.headers)}")

            if response.status_code == 201:
                ticket_data = response.json()
                logger.info(
                    f"POST Request - AIS Manager Ticket: Successfully created ticket {ticket_data.get('id')} for user "
                    f"{user_id}"
                )
                logger.debug(f"Ticket data received: {ticket_data}")
                return ticket_data
            else:
                # Log detailed error information
                error_body = response.text
                logger.error(
                    f"Failed to create ticket: {response.status_code} - {error_body}"
                )
                logger.error(f"Failed request details - URL: {url}, Headers: {headers}")
                logger.error(f"Failed request payload: {payload}")

                # Try to get more error details from response
                try:
                    error_response = response.json()
                    logger.error(f"Error response JSON: {error_response}")
                except Exception:
                    logger.error(f"Error response is not valid JSON: {error_body}")

                return None

        except requests.exceptions.RequestException as e:
            # Log detailed error information for request exceptions
            logger.error(f"Request exception while creating ticket: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")

            # Log specific exception details
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            elif hasattr(e, "request") and e.request is not None:
                logger.error(f"Request URL: {e.request.url}")
                logger.error(f"Request method: {e.request.method}")

            return None

    def create_rfq_ticket(
        self,
        rfq_id: str,
        user_id: str,
        ticket_reason: str,
        rfq_data: dict[str, Any],
        bearer_token: str,
        category_id: int = 209,
    ) -> dict[str, Any] | None:
        """
        Create a ticket specifically for RFQ issues

        Args:
            rfq_id: RFQ identifier
            user_id: User ID from the system
            ticket_reason: Reason for creating ticket (no_match, competitor_mapping_needed, etc.)
            rfq_data: RFQ data for ticket description
            bearer_token: Bearer token for authentication (optional)

        Returns:
            Response data if successful, None otherwise
        """
        # Map RFQ reasons to ticket titles and descriptions
        ticket_configs = {
            "no_match": {
                "title": f"No Match Found for RFQ {rfq_id}",
                "description": f"RFQ ID: {rfq_id}\n"
                f"Product: {rfq_data.get('product_name', 'Unknown')}\n"
                f"Customer: {rfq_data.get('customer_name', 'Unknown')}\n"
                f"Raw Text: {rfq_data.get('raw_text', '')}\n"
                f"Extracted Color: {rfq_data.get('color', 'None')}\n"
                f"No suitable product match found in database. Manual review required.",
            },
            "competitor_mapping_needed": {
                "title": f"Competitor Mapping Needed for RFQ {rfq_id}",
                "description": f"RFQ ID: {rfq_id}\n"
                f"Product: {rfq_data.get('product_name', 'Unknown')}\n"
                f"Customer: {rfq_data.get('customer_name', 'Unknown')}\n"
                f"Competitor Brand: {rfq_data.get('source_brand', 'Unknown')}\n"
                f"Extracted Color: {rfq_data.get('color', 'None')}\n"
                f"Competitor product identified but not mapped. Mapping required for proper matching.",
            },
            "nippon_product_update_needed": {
                "title": f"Nippon Product Update Needed for RFQ {rfq_id}",
                "description": f"RFQ ID: {rfq_id}\n"
                f"Product: {rfq_data.get('product_name', 'Unknown')}\n"
                f"Customer: {rfq_data.get('customer_name', 'Unknown')}\n"
                f"Extracted Color: {rfq_data.get('color', 'None')}\n"
                f"Nippon product identified but needs database update. Product information update required.",
            },
        }

        config = ticket_configs.get(ticket_reason, ticket_configs["no_match"])

        # Set priority based on ticket reason
        is_priority = ticket_reason in [
            "competitor_mapping_needed",
            "nippon_product_update_needed",
        ]

        return self.create_ticket(
            user_id=user_id,
            title=config["title"],
            description=config["description"],
            bearer_token=bearer_token,
            category_id=category_id,  # Use passed category_id
            status="open",
            is_priority=is_priority,
        )

    def create_ticket_for_unmatched_item(
        self,
        rfq_item: dict[str, Any],
        rfq_id: str,
        user_id: str,
        mismatch_type: str = "no_match",
        bearer_token: str = None,
        category_id: int = 209,
    ) -> dict[str, Any] | None:
        """
        Create a ticket for a single unmatched RFQ item with detailed mismatch information

        Args:
            rfq_item: The unmatched RFQ item
            rfq_id: RFQ identifier
            user_id: User ID from the system
            mismatch_type: Type of mismatch (product_name, color, both)
            bearer_token: Bearer token for authentication (optional)

        Returns:
            Response data if successful, None otherwise
        """
        # Log the start of ticket creation process
        logger.info(
            f"Starting ticket creation for unmatched item RFQ {rfq_id}, Item ID: {rfq_item.get('id')}, User: {user_id}"
        )
        logger.debug(f"Unmatched item details: {rfq_item}")

        # Determine ticket reason based on mismatch type
        if mismatch_type == "color":
            title = f"Color Match Needed for RFQ {rfq_id}"
            description = (
                f"RFQ ID: {rfq_id}\n"
                f"Product: {rfq_item.get('product_name', 'Unknown')}\n"
                f"Extracted Color: {rfq_item.get('color', 'None')}\n"
                f"UOM: {rfq_item.get('uom', 'None')}\n"
                f"Quantity: {rfq_item.get('qty', 'None')}\n"
                f"Raw Text: {rfq_item.get('raw_text', '')}\n"
                f"Color information found but no matching product with this color. Color mapping or update required."
            )
            is_priority = True  # High priority - color affects pricing
            logger.info(
                f"Color mismatch detected for RFQ {rfq_id}, creating high priority ticket"
            )
        elif rfq_item.get("product_type") == "competitor":
            title = f"Competitor Product Mapping Needed for RFQ {rfq_id}"
            description = (
                f"RFQ ID: {rfq_id}\n"
                f"Competitor Product: {rfq_item.get('product_name', 'Unknown')}\n"
                f"Competitor Brand: {rfq_item.get('source_brand', 'Unknown')}\n"
                f"Extracted Color: {rfq_item.get('color', 'None')}\n"
                f"UOM: {rfq_item.get('uom', 'None')}\n"
                f"Quantity: {rfq_item.get('qty', 'None')}\n"
                f"Raw Text: {rfq_item.get('raw_text', '')}\n"
                f"Competitor product requires mapping to internal products. Manual review needed."
            )
            is_priority = True  # High priority - competitor mapping affects business
            logger.info(
                f"Competitor product mapping needed for RFQ {rfq_id}, creating high priority ticket"
            )
        elif rfq_item.get("product_type") == "nippon":
            title = f"Nippon Product Update Needed for RFQ {rfq_id}"
            description = (
                f"RFQ ID: {rfq_id}\n"
                f"Nippon Product: {rfq_item.get('product_name', 'Unknown')}\n"
                f"Extracted Color: {rfq_item.get('color', 'None')}\n"
                f"UOM: {rfq_item.get('uom', 'None')}\n"
                f"Quantity: {rfq_item.get('qty', 'None')}\n"
                f"Raw Text: {rfq_item.get('raw_text', '')}\n"
                f"Nippon product not found in database. Product information update required."
            )
            is_priority = False  # Medium priority - internal update
            logger.info(
                f"Nippon product update needed for RFQ {rfq_id}, creating medium priority ticket"
            )
        else:
            title = f"No Match Found for RFQ {rfq_id}"
            description = (
                f"RFQ ID: {rfq_id}\n"
                f"Product: {rfq_item.get('product_name', 'Unknown')}\n"
                f"Extracted Color: {rfq_item.get('color', 'None')}\n"
                f"UOM: {rfq_item.get('uom', 'None')}\n"
                f"Quantity: {rfq_item.get('qty', 'None')}\n"
                f"Raw Text: {rfq_item.get('raw_text', '')}\n"
                f"No suitable product match found in database. Manual review required."
            )
            is_priority = False  # Low priority - general no match
            logger.info(
                f"General no match for RFQ {rfq_id}, creating standard priority ticket"
            )

        logger.info(f"Creating ticket with title: {title}, Priority: {is_priority}")

        result = self.create_ticket(
            user_id=user_id,
            title=title,
            description=description,
            bearer_token=bearer_token,
            category_id=category_id,  # Use passed category_id
            status="open",
            is_priority=is_priority,
        )

        if result:
            ticket_id = result.get("id")
            logger.info(
                f"Successfully created ticket {ticket_id} for RFQ {rfq_id}, Item ID: {rfq_item.get('id')}"
            )
            logger.debug(f"Ticket creation result: {result}")
        else:
            logger.error(
                f"Failed to create ticket for RFQ {rfq_id}, Item ID: {rfq_item.get('id')}"
            )

        return result

    def create_tickets_for_unmatched_items(
        self,
        unmatched_items: list[dict[str, Any]],
        rfq_id: str,
        user_id: str,
        bearer_token: str,
        category_id: int = 209,
    ) -> list[dict[str, Any] | None]:
        """
        Create tickets for all unmatched RFQ items, avoiding duplicates for the same RFQ ID

        Args:
            unmatched_items: List of unmatched RFQ items
            rfq_id: RFQ identifier
            user_id: User ID from the system
            bearer_token: Bearer token for authentication (required)

        Returns:
            List of ticket creation results (some might be None if failed)
        """
        # Log the start of batch ticket creation
        logger.info(
            f"Starting batch ticket creation for RFQ {rfq_id} with {len(unmatched_items)} unmatched items"
        )
        logger.debug(f"Unmatched items count: {len(unmatched_items)}")

        # First, check if there are already tickets for this RFQ ID
        if self._has_existing_tickets_for_rfq(rfq_id, bearer_token):
            logger.info(
                f"Skipping ticket creation for RFQ {rfq_id} - tickets already exist"
            )
            logger.debug(
                f"Found existing tickets for RFQ {rfq_id}, not creating new ones"
            )
            return [None] * len(unmatched_items)

        results = []

        # Log details of each unmatched item before processing
        logger.info(f"Processing unmatched items for RFQ {rfq_id}:")
        for i, item in enumerate(unmatched_items):
            mismatch_type = self._determine_mismatch_type(item)
            logger.info(
                f"Item {i + 1}: ID={item.get('id')}, Product={item.get('product_name')}, "
                f"Raw Text='{item.get('raw_text')[:50]}...', Mismatch Type={mismatch_type}"
            )

        for item in unmatched_items:
            # Determine mismatch type
            mismatch_type = self._determine_mismatch_type(item)

            # Create ticket for this item
            logger.debug(f"Creating ticket for item ID: {item.get('id')}")
            result = self.create_ticket_for_unmatched_item(
                rfq_item=item,
                rfq_id=rfq_id,
                user_id=user_id,
                mismatch_type=mismatch_type,
                bearer_token=bearer_token,
                category_id=category_id,
            )
            results.append(result)

            # Very small delay to avoid rate limiting
            import time

            time.sleep(0.01)

        successful_tickets = len([r for r in results if r is not None])
        failed_tickets = len(unmatched_items) - successful_tickets

        # Log detailed results
        logger.info(f"Batch ticket creation completed for RFQ {rfq_id}")
        logger.info(
            f"Summary: {successful_tickets}/{len(unmatched_items)} tickets created successfully"
        )
        logger.info(
            f"Details: {successful_tickets} successful, {failed_tickets} failed"
        )

        if failed_tickets > 0:
            failed_indices = [i for i, r in enumerate(results) if r is None]
            logger.warning(
                f"Failed to create tickets for item indices: {failed_indices}"
            )

        # Log ticket IDs for successful creations
        successful_ticket_ids = []
        for i, result in enumerate(results):
            if result and "id" in result:
                ticket_id = result["id"]
                successful_ticket_ids.append(ticket_id)
                logger.debug(
                    f"Ticket {ticket_id} created for item {unmatched_items[i].get('id')}"
                )

        if successful_ticket_ids:
            logger.info(f"Successfully created ticket IDs: {successful_ticket_ids}")

        return results

    def _determine_mismatch_type(self, item: dict[str, Any]) -> str:
        """
        Determine the type of mismatch for an RFQ item

        Args:
            item: RFQ item to analyze

        Returns:
            Type of mismatch (product_name, color, both)
        """
        # If it's a competitor or nippon product, it's a product type issue
        if item.get("product_type") in ["competitor", "nippon"]:
            return "product_name"

        # If color is present but no match, it's a color issue
        if item.get("color") and not item.get("product_master_id"):
            return "color"

        # Default is no match (could be either or both)
        return "no_match"

    def _has_existing_tickets_for_rfq(
        self, rfq_id: str, bearer_token: str = None
    ) -> bool:
        """
        Check if there are already existing tickets for the given RFQ ID in AIS Manager

        Args:
            rfq_id: RFQ identifier to check
            bearer_token: Bearer token for authentication (optional)

        Returns:
            True if tickets already exist for this RFQ, False otherwise
        """
        try:
            # Check if bearer token is provided
            if not bearer_token:
                logger.error(
                    "AIS Manager Bearer Token not provided - cannot check for existing tickets"
                )
                return False

            # Query AIS Manager API for existing tickets with this RFQ ID
            url = f"{self.base_url}/api/ais-manager/tickets"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {bearer_token}",
            }

            params = {"platform": "rfq-matching-service", "rfq_id": rfq_id, "limit": 1}

            # Log the duplicate check attempt
            logger.info(f"Checking for existing tickets for RFQ {rfq_id}")
            logger.debug(f"Query URL: {url}")
            logger.debug(f"Query params: {params}")

            response = requests.get(
                url, headers=headers, params=params, timeout=self.timeout
            )

            # Log response details
            logger.debug(
                f"Existing tickets check response status: {response.status_code}"
            )

            if response.status_code == 200:
                tickets_data = response.json()
                # Check if any tickets exist for this RFQ
                if tickets_data and isinstance(tickets_data, list):
                    existing_tickets = len(tickets_data)
                    if existing_tickets > 0:
                        logger.info(
                            f"Found {existing_tickets} existing tickets for RFQ {rfq_id} in AIS Manager"
                        )
                        # Log details of existing tickets
                        for ticket in tickets_data:
                            logger.debug(
                                f"Existing ticket - ID: {ticket.get('id')}, Title: {ticket.get('title')}"
                            )
                        return True
                    else:
                        logger.info(f"No existing tickets found for RFQ {rfq_id}")
                        return False
                else:
                    logger.warning(
                        f"Unexpected response format when checking existing tickets: {tickets_data}"
                    )
                    return False
            else:
                error_body = response.text
                logger.error(
                    f"Failed to check existing tickets for RFQ {rfq_id}: {response.status_code} - {error_body}"
                )
                # If we can't check due to API error, assume no duplicates to avoid missing tickets
                return False

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request exception while checking existing tickets for RFQ {rfq_id}: {str(e)}"
            )
            logger.error(f"Exception type: {type(e).__name__}")
            # If we can't check, assume no duplicates to avoid missing tickets
            return False


# Service instance
ais_manager_service = AISManagerService()
