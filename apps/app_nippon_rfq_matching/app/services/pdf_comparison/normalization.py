"""
PDF Comparison Normalization Module

This module contains the normalization mixin class for RFQ item normalization.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
    openai_normalization_service,
)

logger = logging.getLogger(__name__)


class NormalizationMixin:
    """
    Mixin class for RFQ item normalization functionality.

    Handles OpenAI-based normalization of RFQ items.
    """

    def _prepare_items_for_normalization_disabled(
        self, rfq_items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Prepare RFQ items when normalization is disabled.

        Args:
            rfq_items: List of RFQ item dictionaries

        Returns:
            List of items with disabled normalization fields
        """
        for item in rfq_items:
            item["normalized_name"] = None
            item["normalized_color"] = None
            item["product_type"] = None
            item["normalization_method"] = "disabled"
        return rfq_items

    def _update_item_with_normalization_result(
        self,
        item: dict[str, Any],
        normalized_name: str,
        normalized_color: str,
        prod_type: str,
        available_colors: list[str],
        result: dict[str, Any],
        index: int,
    ) -> dict[str, Any]:
        """
        Update a single RFQ item with normalization results.

        Args:
            item: RFQ item dictionary
            normalized_name: Normalized product name
            normalized_color: Normalized color
            prod_type: Product type (nippon/competitor)
            available_colors: List of available colors
            result: Full normalization result from OpenAI
            index: Index in the batch

        Returns:
            Updated item dictionary
        """
        item["normalized_name"] = normalized_name
        item["normalized_color"] = normalized_color
        item["product_type"] = prod_type
        item["available_colors"] = available_colors
        item["normalization_method"] = "openai"

        # Add competitor mapping fields if available
        if "nippon_equivalent_names" in result:
            item["nippon_equivalent_name"] = result["nippon_equivalent_names"][index]
        if "source_brands" in result:
            item["source_brand"] = result["source_brands"][index]
        if "source_color_codes" in result:
            item["source_color_code"] = result["source_color_codes"][index]
        if "npms_color_codes" in result:
            item["npms_color_code"] = result["npms_color_codes"][index]

        return item

    def _process_normalization_batch(
        self,
        batch: list[dict[str, Any]],
        db: Session,
        batch_num: int,
        total_batches: int,
    ) -> list[dict[str, Any]]:
        """
        Process a single batch of RFQ items for normalization.

        Args:
            batch: List of RFQ item dictionaries
            db: Database session
            batch_num: Current batch number
            total_batches: Total number of batches

        Returns:
            List of normalized items

        Raises:
            Exception: If normalization fails
        """
        descriptions = [item.get("raw_text", "") for item in batch]

        result = openai_normalization_service.normalize_rfq_items(
            rfq_descriptions=descriptions, db=db
        )

        # Update items with normalized names, colors, types, and competitor mapping info
        normalized_items = []
        for j, (
            item,
            normalized_name,
            normalized_color,
            prod_type,
            available_colors,
        ) in enumerate(
            zip(
                batch,
                result["after"],
                result.get("colors", [None] * len(result["after"])),
                result["types"],
                result.get(
                    "available_colors", [[] for _ in range(len(result["after"]))]
                ),
            )
        ):
            updated_item = self._update_item_with_normalization_result(
                item,
                normalized_name,
                normalized_color,
                prod_type,
                available_colors,
                result,
                j,
            )
            normalized_items.append(updated_item)

        logger.info(f"Normalized batch {batch_num}/{total_batches}")
        return normalized_items

    def normalize_rfq_items(
        self, rfq_items: list[dict[str, Any]], db: Session, batch_size: int = 50
    ) -> list[dict[str, Any]]:
        """
        Normalize RFQ items using OpenAI.

        Args:
            rfq_items: List of RFQ item dictionaries
            db: Database session
            batch_size: Batch size for OpenAI API calls

        Returns:
            List of normalized RFQ items with normalized_name and normalized_color fields
        """
        if not openai_normalization_service.enabled:
            logger.warning(
                "OpenAI normalization service is disabled, skipping normalization"
            )
            return self._prepare_items_for_normalization_disabled(rfq_items)

        logger.info(f"Normalizing {len(rfq_items)} RFQ items using OpenAI")

        # Process in batches to avoid token limits
        normalized_items = []
        total_batches = (len(rfq_items) + batch_size - 1) // batch_size

        for i in range(0, len(rfq_items), batch_size):
            batch = rfq_items[i : i + batch_size]
            batch_num = i // batch_size + 1

            try:
                batch_normalized = self._process_normalization_batch(
                    batch, db, batch_num, total_batches
                )
                normalized_items.extend(batch_normalized)

            except Exception as e:
                # Fail fast - re-raise the exception instead of continuing
                logger.error(
                    f"CRITICAL: Error normalizing batch {batch_num}/{total_batches}: {e}"
                )
                logger.error("Fail-fast enabled: Aborting normalization process")
                raise  # Re-raise the exception to fail fast

        return normalized_items

    def _apply_normalization(self, rfq_items: list[dict], db: Session) -> list[dict]:
        """
        Apply normalization to RFQ items.

        Args:
            rfq_items: List of RFQ items
            db: Database session

        Returns:
            Normalized items list
        """
        logger.info("Step 2: Normalizing RFQ items with OpenAI")
        return self.normalize_rfq_items(rfq_items, db)

    def _prepare_items_without_normalization(self, rfq_items: list[dict]) -> list[dict]:
        """
        Prepare items without normalization.

        Args:
            rfq_items: List of RFQ items

        Returns:
            Items with normalization fields set to None/skipped
        """
        logger.info("Step 2: Skipping normalization")
        for item in rfq_items:
            item["normalized_name"] = None
            item["normalized_color"] = None
            item["product_type"] = None
            item["normalization_method"] = "skipped"
        return rfq_items

    def _get_normalized_items(
        self, rfq_items: list[dict], use_normalization: bool, db: Session
    ) -> list[dict]:
        """
        Get normalized items based on normalization flag.

        Args:
            rfq_items: List of RFQ items
            use_normalization: Whether to use normalization
            db: Database session

        Returns:
            Normalized or original items
        """
        if use_normalization:
            return self._apply_normalization(rfq_items, db)
        return self._prepare_items_without_normalization(rfq_items)
