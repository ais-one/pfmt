"""
Service for handling file uploads, parsing, and storage
"""

import asyncio
import concurrent.futures
import logging
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.models.pricing import ProductPrices, Region
from apps.app_nippon_rfq_matching.app.models.rfq import (
    RFQItem,
    RFQMatch,
    UploadedFile,
)
from apps.app_nippon_rfq_matching.app.services.matching import matching_service
from apps.app_nippon_rfq_matching.app.utils.csv_storage import csv_storage
from apps.app_nippon_rfq_matching.app.utils.eml_parser import process_eml_for_products
from apps.app_nippon_rfq_matching.app.utils.extract_and_route_rfq_pdf_parser import (
    extract_and_route,
)
from apps.app_nippon_rfq_matching.app.utils.parsers import (
    clean_raw_text_rfq,
    parse_ceru_excel,
    parse_iatp_excel,
    parse_iatp_excel_with_multi_region,
    parse_iatp_excel_with_pricing,
    process_iatp_excel_with_database_insertion,
)

logger = logging.getLogger(__name__)

# Thread pool for CPU-intensive operations (PDF parsing)
_thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="pdf_parser"
)


class RFQService:
    """Service for RFQ processing operations"""

    def __init__(self):
        """Initialize RFQ service"""
        self.upload_dir = settings.UPLOAD_DIR
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        # Don't initialize converter here - create per-thread to avoid blocking

    def save_uploaded_file(
        self, file_content: bytes, original_filename: str
    ) -> UploadedFile:
        """
        Save uploaded file to disk

        Args:
            file_content: File content as bytes
            original_filename: Original filename

        Returns:
            UploadedFile record
        """
        # Generate unique filename
        file_ext = Path(original_filename).suffix
        stored_filename = f"{uuid.uuid4()}{file_ext}"
        file_path = self.upload_dir / stored_filename

        # Save file
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Determine file type
        if file_ext.lower() in [".xlsx", ".xls"]:
            file_type = "excel"
        elif file_ext.lower() == ".eml":
            file_type = "eml"
        else:
            file_type = "pdf"

        return UploadedFile(
            original_filename=original_filename,
            stored_filename=stored_filename,
            file_type=file_type,
            file_path=str(file_path),
            status="pending",
        )

    def parse_excel_file(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse IATP Excel file

        Args:
            file_path: Path to Excel file

        Returns:
            List of parsed product records
        """
        products = parse_iatp_excel(file_path)

        # Add clean product name
        for product in products:
            if product.get("product_name"):
                product["clean_product_name"] = clean_raw_text_rfq(
                    product["product_name"]
                )

        return products

    def parse_iatp_excel_with_pricing_file(self, file_path: str) -> dict[str, Any]:
        """
        Parse IATP Excel file with pricing support

        Args:
            file_path: Path to Excel file

        Returns:
            Dictionary containing:
            - 'products': List of parsed product records
            - 'pricing': List of parsed pricing records
        """
        result = parse_iatp_excel_with_pricing(file_path)

        # Add clean product name
        for product in result["products"]:
            if product.get("product_name"):
                product["clean_product_name"] = clean_raw_text_rfq(
                    product["product_name"]
                )

        return result

    def parse_iatp_excel_with_multi_region_file(self, file_path: str) -> dict[str, Any]:
        """
        Parse IATP Excel file with multi-region pricing support

        Args:
            file_path: Path to Excel file

        Returns:
            Dictionary containing:
            - 'products': List of product records with pricing data
            - 'regions': List of detected regions
            - 'summary': Processing summary
        """
        result = parse_iatp_excel_with_multi_region(file_path)

        # Add clean product name
        for product in result["products"]:
            if product.get("product_name"):
                product["clean_product_name"] = clean_raw_text_rfq(
                    product["product_name"]
                )

        return result

    async def process_iatp_excel_with_multi_region_and_pricing(
        self, file_path: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded IATP Excel file with multi-region pricing and database insertion

        Args:
            file_path: Path to uploaded file
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result with multi-region pricing and insertion statistics
        """
        # Process Excel with multi-region support and database insertion
        db_results = process_iatp_excel_with_database_insertion(file_path, db)

        # Parse Excel to get product data for response
        parsed_data = self.parse_iatp_excel_with_multi_region_file(file_path)

        # Build response
        response = {
            "status": "success",
            "products_count": len(parsed_data["products"]),
            "regions_count": len(parsed_data["regions"]),
            "products_with_pricing": parsed_data["summary"]["products_with_pricing"],
            "regions_inserted": db_results["regions_inserted"],
            "pricing_records_inserted": db_results["pricing_records_inserted"],
            "parse_timestamp": parsed_data["summary"]["parse_timestamp"],
            "summary": parsed_data["summary"],
            "products": parsed_data["products"],
        }

        return response

    def parse_ceru_excel_file(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse CERU Excel RFQ file format

        Args:
            file_path: Path to the CERU Excel file

        Returns:
            List of parsed RFQ items
        """
        return parse_ceru_excel(file_path)

    def parse_eml_file(self, file_path: str) -> tuple:
        """
        Parse EML email file and extract RFQ product data

        Args:
            file_path: Path to the EML file

        Returns:
            Tuple of (rfq_items, rfq_id, email_metadata)
            - rfq_items: List of parsed RFQ items
            - rfq_id: Extracted RFQ ID (or None)
            - email_metadata: Email metadata dictionary
        """
        result = process_eml_for_products(file_path)

        rfq_items = result.get("items", [])
        rfq_id = result.get("rfq_id")
        email_metadata = result.get("email_metadata", {})

        # Add clean text to each item
        for item in rfq_items:
            if item.get("raw_text"):
                item["clean_text"] = clean_raw_text_rfq(item["raw_text"])

        return rfq_items, rfq_id, email_metadata

    def _parse_pdf_sync(self, file_path: str) -> list[dict[str, Any]]:
        """
        Synchronous PDF parsing - runs in worker thread

        Uses extract_and_route for parsing PDF files.

        Args:
            file_path: Path to PDF file

        Returns:
            List of parsed RFQ items
        """

        handler_result = extract_and_route(file_path)

        if handler_result is not None:
            rfq_items = []
            for _, row in handler_result.iterrows():
                # Get column names from the DataFrame (handle both lowercase and capitalized)
                # Try lowercase first (onesea, columbia, drylog format)
                raw_text = row.get("description") or row.get("Description", "")
                # Get qty - try lowercase first, then capitalized
                qty = row.get("qty") or row.get("Quantity") or row.get("Qty")
                # Get uom - try lowercase first
                uom = row.get("uom") or row.get("UoM") or row.get("UOM")

                item = {
                    "raw_text": str(raw_text).strip()
                    if pd.notna(raw_text) and raw_text
                    else "",
                    "qty": str(qty).strip() if pd.notna(qty) and qty else None,
                    "uom": str(uom).strip() if pd.notna(uom) and uom else None,
                    "source": "pdf_extract_and_route",
                }
                # Add clean text
                if item.get("raw_text"):
                    item["clean_text"] = clean_raw_text_rfq(item["raw_text"])
                rfq_items.append(item)
            return rfq_items

        # Fallback to pdf_plumber if extract_and_route fails
        logger.warning(
            "extract_and_route returned no results, trying pdf_plumber fallback"
        )

        try:
            from apps.app_nippon_rfq_matching.app.utils.pdf_plumber_parsing_rfq_2 import (
                parse_rfq_pdf_structured,
            )

            df = parse_rfq_pdf_structured(file_path)

            # Map fields to match expected format
            rfq_items = []
            for _, row in df.iterrows():
                item = {
                    "raw_text": row.get("description", ""),
                    "qty": str(row.get("qty", ""))
                    if pd.notna(row.get("qty"))
                    else None,
                    "uom": row.get("uom"),
                    "source": "pdf_plumber_rfq_2",
                }
                # Add clean text
                if item.get("raw_text"):
                    item["clean_text"] = clean_raw_text_rfq(item["raw_text"])
                rfq_items.append(item)

            logger.info(
                f"Successfully parsed {len(rfq_items)} items using pdf_plumber_rfq_2"
            )

            # Validate that we have valid items
            valid_items = [item for item in rfq_items if item.get("raw_text")]
            if len(valid_items) == 0:
                logger.warning("pdf_plumber_rfq_2 returned no valid items")
                return []
            else:
                return valid_items

        except Exception as e:
            logger.error(
                f"Error parsing PDF with pdf_plumber_rfq_2: {e}", exc_info=True
            )
            return []

    async def parse_pdf_file(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse RFQ PDF file using pdfplumber (async - runs in thread pool)

        Args:
            file_path: Path to PDF file

        Returns:
            List of parsed RFQ items
        """
        # Run CPU-intensive PDF parsing in thread pool
        loop = asyncio.get_event_loop()
        rfq_items = await loop.run_in_executor(
            _thread_pool, self._parse_pdf_sync, file_path
        )
        return rfq_items

    def save_product_master_with_pricing_to_db(
        self,
        products: list[dict[str, Any]],
        pricing: list[dict[str, Any]],
        uploaded_file_id: int,
        db: Session,
    ) -> dict[str, Any]:
        """
        Save product master and pricing to database with duplicate handling

        Args:
            products: List of product records
            pricing: List of pricing records
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Dictionary with keys:
            - created: List of newly created ProductMaster records
            - duplicates: List of duplicate product info
            - skipped_count: Number of skipped items
            - created_pricing: List of newly created ProductPrices records
        """
        from sqlalchemy.exc import IntegrityError

        db_products = []
        db_pricing = []
        duplicates = []
        skipped_count = 0
        region_map = {}  # Cache for region_id mapping

        # First, save all products
        for product in products:
            sheet_type = product.get("sheet_type")
            pmc = product.get("pmc")
            product_name = product.get("product_name")
            color = product.get("color")

            # Skip if missing required fields
            if not all([sheet_type, pmc, product_name]):
                logger.warning(
                    f"Skipping product with missing required fields: sheet_type={sheet_type}, pmc={pmc}, "
                    f"product_name={product_name}"
                )
                skipped_count += 1
                continue

            # Check if product already exists
            existing_product = (
                db.query(ProductMaster)
                .filter(
                    ProductMaster.sheet_type == sheet_type,
                    ProductMaster.pmc == pmc,
                    ProductMaster.product_name == product_name,
                    ProductMaster.color == color,
                )
                .first()
            )

            if existing_product:
                # Track duplicate
                duplicates.append(
                    {
                        "sheet_type": sheet_type,
                        "pmc": pmc,
                        "product_name": product_name,
                        "color": color,
                        "existing_id": existing_product.id,
                    }
                )
                logger.debug(
                    f"Duplicate product found: sheet_type={sheet_type}, pmc={pmc}, product_name={product_name}, "
                    f"color={color}"
                )
            else:
                # Create new product
                db_product = ProductMaster(
                    uploaded_file_id=uploaded_file_id,
                    sheet_name=product.get("sheet_name"),
                    sheet_type=sheet_type,
                    row_excel=product.get("row_excel"),
                    pmc=pmc,
                    product_name=product_name,
                    color=color,
                    clean_product_name=product.get("clean_product_name"),
                )
                db.add(db_product)
                db_products.append(db_product)
                logger.debug(
                    f"Created new product: sheet_type={sheet_type}, pmc={pmc}, product_name={product_name}, "
                    f"color={color}"
                )

        try:
            db.commit()

            # Refresh to get IDs
            for product in db_products:
                db.refresh(product)

            logger.info(
                f"Saved ProductMaster: created={len(db_products)}, duplicates={len(duplicates)}, "
                f"skipped={skipped_count}"
            )

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error saving ProductMaster: {e}")
            raise

        # Save regions and pricing
        for pricing_record in pricing:
            region_name = pricing_record.get("region")
            product_master_id = pricing_record.get("product_master_id")

            if not region_name or not product_master_id:
                continue

            # Get region ID (create if not exists)
            if region_name not in region_map:
                region = db.query(Region).filter(Region.name == region_name).first()
                if not region:
                    region = Region(name=region_name)
                    db.add(region)
                    db.commit()
                    db.refresh(region)
                region_map[region_name] = region.id

            region_id = region_map[region_name]

            # Check if pricing already exists for this product and region
            existing_pricing = (
                db.query(ProductPrices)
                .filter(
                    ProductPrices.product_master_id == product_master_id,
                    ProductPrices.region_id == region_id,
                    ProductPrices.size == pricing_record.get("size"),
                    ProductPrices.uom == pricing_record.get("uom"),
                )
                .first()
            )

            if not existing_pricing:
                # Create new pricing record
                db_pricing_record = ProductPrices(
                    product_master_id=product_master_id,
                    region_id=region_id,
                    size=pricing_record.get("size"),
                    uom=pricing_record.get("uom"),
                    price=pricing_record.get("price"),
                    price_raw=pricing_record.get("price_raw"),
                )
                db.add(db_pricing_record)
                db_pricing.append(db_pricing_record)

        try:
            db.commit()
            logger.info(f"Saved ProductPrices: created={len(db_pricing)}")

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error saving ProductPrices: {e}")
            raise

        return {
            "created": db_products,
            "duplicates": duplicates,
            "skipped_count": skipped_count,
            "created_pricing": db_pricing,
        }

    def save_product_master_to_db(
        self, products: list[dict[str, Any]], uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Save product master to database with duplicate handling

        Args:
            products: List of product records
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Dictionary with keys:
            - created: List of newly created ProductMaster records
            - duplicates: List of duplicate product info (sheet_type, pmc, product_name, color)
            - skipped_count: Number of skipped items (empty/invalid)
        """
        from sqlalchemy.exc import IntegrityError

        db_products = []
        duplicates = []
        skipped_count = 0

        for product in products:
            sheet_type = product.get("sheet_type")
            pmc = product.get("pmc")
            product_name = product.get("product_name")
            color = product.get("color")

            # Skip if missing required fields
            if not all([sheet_type, pmc, product_name]):
                logger.warning(
                    f"Skipping product with missing required fields: sheet_type={sheet_type}, pmc={pmc}, "
                    f"product_name={product_name}"
                )
                skipped_count += 1
                continue

            # Check if product already exists
            existing_product = (
                db.query(ProductMaster)
                .filter(
                    ProductMaster.sheet_type == sheet_type,
                    ProductMaster.pmc == pmc,
                    ProductMaster.product_name == product_name,
                    ProductMaster.color == color,
                )
                .first()
            )

            if existing_product:
                # Track duplicate
                duplicates.append(
                    {
                        "sheet_type": sheet_type,
                        "pmc": pmc,
                        "product_name": product_name,
                        "color": color,
                        "existing_id": existing_product.id,
                    }
                )
                logger.debug(
                    f"Duplicate product found: sheet_type={sheet_type}, pmc={pmc}, product_name={product_name}, "
                    f"color={color}"
                )
            else:
                # Create new product
                db_product = ProductMaster(
                    uploaded_file_id=uploaded_file_id,
                    sheet_name=product.get("sheet_name"),
                    sheet_type=sheet_type,
                    row_excel=product.get("row_excel"),
                    pmc=pmc,
                    product_name=product_name,
                    color=color,
                    clean_product_name=product.get("clean_product_name"),
                )
                db.add(db_product)
                db_products.append(db_product)
                logger.debug(
                    f"Created new product: sheet_type={sheet_type}, pmc={pmc}, product_name={product_name}, "
                    f"color={color}"
                )

        try:
            db.commit()

            # Refresh to get IDs
            for product in db_products:
                db.refresh(product)

            logger.info(
                f"Saved ProductMaster: created={len(db_products)}, duplicates={len(duplicates)}, "
                f"skipped={skipped_count}"
            )

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error saving ProductMaster: {e}")
            raise

        return {
            "created": db_products,
            "duplicates": duplicates,
            "skipped_count": skipped_count,
        }

    def save_rfq_items_to_db(
        self,
        rfq_items: list[dict[str, Any]],
        rfq_id: str,
        uploaded_file_id: int,
        db: Session,
    ) -> list[RFQItem]:
        """
        Save RFQ items to database using upsert logic.

        For each item:
        - If exists (by rfq_id, clean_text, color), update it
        - If not exists, insert new record

        This preserves existing items for the same RFQ ID that are not in the new batch,
        allowing incremental updates rather than full replacement.

        Args:
            rfq_items: List of RFQ item records
            rfq_id: RFQ identifier
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            List of created/updated RFQItem records
        """
        from sqlalchemy.exc import IntegrityError

        db_items = []
        created_count = 0
        updated_count = 0
        skipped_count = 0

        for item in rfq_items:
            raw_text = item.get("raw_text")
            clean_text = item.get("clean_text")
            color = item.get("color")

            if not raw_text:
                logger.warning(
                    f"Skipping RFQ item with empty raw_text for rfq_id={rfq_id}"
                )
                skipped_count += 1
                continue

            # Try to find existing item by unique constraint (rfq_id, clean_text, color)
            existing_item = (
                db.query(RFQItem)
                .filter(
                    RFQItem.rfq_id == rfq_id,
                    RFQItem.clean_text == clean_text,
                    RFQItem.color == color,
                )
                .first()
            )

            if existing_item:
                # Update existing item
                existing_item.raw_text = raw_text
                existing_item.qty = item.get("qty")
                existing_item.uom = item.get("uom")
                existing_item.uploaded_file_id = uploaded_file_id
                # source is usually kept as-is or can be updated
                # existing_item.source = item.get("source", existing_item.source)
                db_items.append(existing_item)
                updated_count += 1
                logger.debug(
                    f"Updated RFQ item: rfq_id={rfq_id}, "
                    f"clean_text={clean_text[:50] if clean_text else raw_text[:50]}..., color={color}"
                )
            else:
                # Insert new item
                db_item = RFQItem(
                    uploaded_file_id=uploaded_file_id,
                    rfq_id=rfq_id,
                    raw_text=raw_text,
                    clean_text=clean_text,
                    color=color,
                    qty=item.get("qty"),
                    uom=item.get("uom"),
                    source=item.get("source", "unknown"),
                )
                db.add(db_item)
                db_items.append(db_item)
                created_count += 1
                logger.debug(
                    f"Created new RFQ item: rfq_id={rfq_id}, "
                    f"clean_text={clean_text[:50] if clean_text else raw_text[:50]}..., color={color}"
                )

        try:
            db.commit()

            # Refresh to get IDs for new items
            for item in db_items:
                db.refresh(item)

            logger.info(
                f"Saved RFQ items: created={created_count}, updated={updated_count}, "
                f"skipped={skipped_count} for rfq_id={rfq_id}"
            )

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error saving RFQ items for rfq_id={rfq_id}: {e}")
            raise

        return db_items

    def save_matches_to_db(
        self, matches: list[dict[str, Any]], rfq_items: list[RFQItem], db: Session
    ) -> list[RFQMatch]:
        """
        Save match results to database (handles structured format)

        Args:
            matches: List of match results in structured format (rfq, product_master, match_info)
            rfq_items: List of RFQItem database records
            db: Database session

        Returns:
            List of created RFQMatch records
        """
        db_matches = []
        skipped_count = 0

        for i, match in enumerate(matches):
            # Get corresponding RFQ item
            rfq_item = rfq_items[i] if i < len(rfq_items) else None

            if rfq_item:
                # Extract data from structured format
                match_info = match.get("match_info", {})
                product_master = match.get("product_master", {})
                product_master_id = product_master.get("id")

                # Skip if no product master match found (product_master_id is None)
                if product_master_id is None:
                    skipped_count += 1
                    logger.debug(
                        f"Skipping match for RFQ item {rfq_item.id}: no product master match found"
                    )
                    continue

                db_match = RFQMatch(
                    rfq_item_id=rfq_item.id,
                    product_master_id=product_master_id,
                    matched_text=product_master.get("clean_product_name"),
                    score=match_info.get("score"),
                    method=match_info.get("method"),
                )
                db.add(db_match)
                db_matches.append(db_match)

        try:
            db.commit()

            # Refresh to get IDs
            for match in db_matches:
                db.refresh(match)

            logger.info(
                f"Saved {len(db_matches)} matches to database, skipped {skipped_count} (no match found)"
            )

        except Exception as e:
            db.rollback()
            logger.error(f"Error saving matches to database: {e}")
            # Don't fail the upload if match saving fails
            raise

        return db_matches

    def save_to_csv(
        self,
        products: list[dict[str, Any]] | None = None,
        rfq_items: list[dict[str, Any]] | None = None,
        rfq_id: str | None = None,
        matches: list[dict[str, Any]] | None = None,
    ):
        """
        Save data to CSV files

        Args:
            products: Product master records
            rfq_items: RFQ item records
            rfq_id: RFQ identifier
            matches: Match results
        """
        if products:
            csv_storage.save_product_master(products)

        if rfq_items and rfq_id:
            csv_storage.save_rfq_items(rfq_items, rfq_id)

        if matches:
            csv_storage.save_rfq_matches(matches)

    async def process_iatp_excel_with_pricing_upload(
        self, file_path: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded IATP Excel file with pricing support (async - matching service reload runs in background)

        Args:
            file_path: Path to uploaded file
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result with pricing information
        """
        # Parse Excel with pricing (fast, not async)
        result = self.parse_iatp_excel_with_pricing_file(file_path)
        products = result["products"]
        pricing = result["pricing"]

        # Pre-loop map is built only from existing DB records; products not yet
        # persisted are resolved after the save call below.
        product_id_map = {}
        for product in products:
            db_product = None
            if product.get("pmc"):
                db_product = (
                    db.query(ProductMaster)
                    .filter(
                        ProductMaster.pmc == product.get("pmc"),
                        ProductMaster.product_name == product.get("product_name"),
                        ProductMaster.color == product.get("color"),
                    )
                    .first()
                )

            if db_product:
                product_id_map[
                    f"{product.get('pmc')}_{product.get('product_name')}"
                ] = db_product.id

        # Update pricing records with product_master_id
        for pricing_record in pricing:
            pmc = pricing_record.get("pmc")
            product_name = pricing_record.get("product_name")
            if pmc and product_name:
                key = f"{pmc}_{product_name}"
                if key in product_id_map:
                    pricing_record["product_master_id"] = product_id_map[key]

        # Save to database with uploaded_file_id
        save_result = self.save_product_master_with_pricing_to_db(
            products, pricing, uploaded_file_id, db
        )

        db_products = save_result["created"]
        created_pricing = save_result["created_pricing"]
        duplicates = save_result["duplicates"]
        skipped_count = save_result["skipped_count"]

        # Save to CSV (only save newly created products)
        if db_products:
            products_data = [p.to_dict() for p in db_products]
            self.save_to_csv(products=products_data)

        # Reload matching service in background (non-blocking)
        async def reload_background():
            try:
                await self.reload_matching_service_async(db)
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    f"Background matching service reload failed: {e}"
                )

        import asyncio

        asyncio.create_task(reload_background())

        # Build response
        response = {
            "status": "success",
            "products_count": len(products),
            "pricing_count": len(pricing),
            "created_count": len(db_products),
            "created_pricing_count": len(created_pricing),
            "duplicate_count": len(duplicates),
            "skipped_count": skipped_count,
            "products": [p.to_dict() for p in db_products],
            "pricing": [p.to_dict() for p in created_pricing],
            "duplicates": duplicates,
            "parser_type": "iatp_with_pricing",
        }

        # Add warning message if duplicates found
        if duplicates:
            response["message"] = (
                f"IATP upload with pricing completed. {len(duplicates)} duplicate products found (already in product "
                f"master). Only {len(db_products)} new products and {len(created_pricing)} pricing records added."
            )
            response["has_duplicates"] = True
        else:
            response["message"] = (
                f"IATP upload with pricing completed successfully. {len(db_products)} products and "
                f"{len(created_pricing)} pricing records added."
            )
            response["has_duplicates"] = False

        return response

    async def process_excel_upload(
        self, file_path: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded Excel file (async - matching service reload runs in background)

        Args:
            file_path: Path to uploaded file
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result with duplicate information
        """
        # Parse Excel (fast, not async)
        products = self.parse_excel_file(file_path)

        # Save to database with uploaded_file_id
        result = self.save_product_master_to_db(products, uploaded_file_id, db)

        db_products = result["created"]
        duplicates = result["duplicates"]
        skipped_count = result["skipped_count"]

        # Save to CSV (only save newly created products)
        if db_products:
            products_data = [p.to_dict() for p in db_products]
            self.save_to_csv(products=products_data)

        # Reload matching service in background (non-blocking)
        async def reload_background():
            try:
                await self.reload_matching_service_async(db)
            except Exception as e:
                import logging

                logging.getLogger(__name__).warning(
                    f"Background matching service reload failed: {e}"
                )

        import asyncio

        asyncio.create_task(reload_background())

        # Build response
        response = {
            "status": "success",
            "products_count": len(products),
            "created_count": len(db_products),
            "duplicate_count": len(duplicates),
            "skipped_count": skipped_count,
            "products": [p.to_dict() for p in db_products],
            "duplicates": duplicates,
        }

        # Add warning message if duplicates found
        if duplicates:
            response["message"] = (
                f"Upload completed. {len(duplicates)} duplicate products found (already in product master). Only "
                f"{len(db_products)} new products added."
            )
            response["has_duplicates"] = True
        else:
            response["message"] = (
                f"Upload completed successfully. {len(db_products)} products added to product master."
            )
            response["has_duplicates"] = False

        return response

    async def process_ceru_excel_upload(
        self, file_path: str, rfq_id: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded CERU Excel file (RFQ format)

        Args:
            file_path: Path to uploaded file
            rfq_id: RFQ identifier
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result
        """
        # Parse CERU Excel (not async - fast operation)
        rfq_items = self.parse_ceru_excel_file(file_path)

        # Save to database with uploaded_file_id
        db_items = self.save_rfq_items_to_db(rfq_items, rfq_id, uploaded_file_id, db)

        # Save to CSV
        self.save_to_csv(rfq_items=rfq_items, rfq_id=rfq_id)

        # Perform matching
        matches = self.perform_matching(rfq_items, db)

        # Save matches to database
        if matches:
            try:
                self.save_matches_to_db(matches, db_items, db)
                logger.info(
                    f"Saved {len(matches)} matches to database for RFQ {rfq_id}"
                )
            except Exception as e:
                # Rollback to ensure session is in clean state
                db.rollback()
                logger.error(f"Error saving matches to database: {e}")
                # Don't fail the upload if match saving matches - continue with response
                matches = []  # Clear matches so response doesn't have invalid data

        return {
            "status": "success",
            "rfq_id": rfq_id,
            "uploaded_file_id": uploaded_file_id,
            "rfq_items_count": len(rfq_items),
            "rfq_items": [item.to_dict() for item in db_items],
            "matches": matches,
            "parser": "ceru_excel",
            "fallback_used": False,
        }

    async def process_routed_excel_upload(
        self, file_path: str, rfq_id: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded Excel file with auto-detection (CERU, ZO Paint, Indonesia formats)

        This method uses the routing utility to auto-detect the Excel format and parse accordingly.
        Supports CERU, ZO Paint, and Indonesian RFQ formats.

        Args:
            file_path: Path to uploaded file
            rfq_id: RFQ identifier
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result with parser type used
        """
        from apps.app_nippon_rfq_matching.app.utils.route_excel_upload import (
            PARSER_CERU,
            PARSER_INDONESIA,
            PARSER_ZO_PAINT,
            route_and_parse,
        )

        # Use routing utility to auto-detect and parse Excel
        route_result = route_and_parse(file_path)

        if route_result.get("error"):
            raise ValueError(f"Failed to parse Excel file: {route_result.get('error')}")

        parser_type = route_result.get("parser_type")
        data = route_result.get("data")

        rfq_items = []
        parser_name = "unknown"

        # Convert parsed data to RFQ items based on parser type
        if parser_type == PARSER_CERU and data:
            for item in data.get("items", []):
                rfq_items.append(
                    {
                        "raw_text": item.get("item_name", ""),
                        "clean_text": item.get("item_name", ""),
                        "qty": str(item.get("qty_reqd", ""))
                        if item.get("qty_reqd")
                        else None,
                        "uom": item.get("unit"),
                        "source": "ceru_excel",
                    }
                )
            parser_name = "ceru_excel"

        elif parser_type == PARSER_ZO_PAINT and data:
            for item in data:
                rfq_items.append(
                    {
                        "raw_text": item.get("description", ""),
                        "clean_text": item.get("product_name", ""),
                        "qty": str(item.get("req", "")) if item.get("req") else None,
                        "uom": item.get("unit"),
                        "source": "zo_paint_excel",
                    }
                )
            parser_name = "zo_paint_excel"

        elif parser_type == PARSER_INDONESIA and data:
            for item in data:
                description = item.get("Description") or item.get("description", "")
                rfq_items.append(
                    {
                        "raw_text": str(description).strip() if description else "",
                        "clean_text": str(description).strip() if description else "",
                        "qty": str(item.get("QTY", "")) if item.get("QTY") else None,
                        "uom": item.get("Unit"),
                        "source": "indonesia_excel",
                    }
                )
            parser_name = "indonesia_excel"

        else:
            raise ValueError(
                f"Unsupported Excel format or no data detected. Parser: {parser_type}"
            )

        # Save to database with uploaded_file_id
        db_items = self.save_rfq_items_to_db(rfq_items, rfq_id, uploaded_file_id, db)

        # Save to CSV
        self.save_to_csv(rfq_items=rfq_items, rfq_id=rfq_id)

        # Perform matching
        matches = self.perform_matching(rfq_items, db)

        # Save matches to database
        if matches:
            try:
                self.save_matches_to_db(matches, db_items, db)
                logger.info(
                    f"Saved {len(matches)} matches to database for RFQ {rfq_id}"
                )
            except Exception as e:
                # Rollback to ensure session is in clean state
                db.rollback()
                logger.error(f"Error saving matches to database: {e}")
                # Don't fail the upload if match saving matches - continue with response
                matches = []  # Clear matches so response doesn't have invalid data

        return {
            "status": "success",
            "rfq_id": rfq_id,
            "uploaded_file_id": uploaded_file_id,
            "rfq_items_count": len(rfq_items),
            "rfq_items": [item.to_dict() for item in db_items],
            "matches": matches,
            "parser": parser_name,
            "fallback_used": False,
        }

    async def process_pdf_upload(
        self, file_path: str, rfq_id: str, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded PDF file (async - PDF parsing runs in thread pool)

        Args:
            file_path: Path to uploaded file
            rfq_id: RFQ identifier
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result
        """
        # Parse PDF (async - runs in thread pool)
        rfq_items = await self.parse_pdf_file(file_path)

        # Save to database with uploaded_file_id
        db_items = self.save_rfq_items_to_db(rfq_items, rfq_id, uploaded_file_id, db)

        # Save to CSV
        self.save_to_csv(rfq_items=rfq_items, rfq_id=rfq_id)

        # Perform matching
        matches = self.perform_matching(rfq_items, db)

        # Save matches to database
        if matches:
            try:
                saved_matches = self.save_matches_to_db(matches, db_items, db)
                logger.info(
                    f"Saved {len(saved_matches)} matches to database for RFQ {rfq_id}"
                )
            except Exception as e:
                # Rollback to ensure session is in clean state
                db.rollback()
                logger.error(f"Error saving matches to database: {e}")
                # Don't fail the upload if match saving matches - continue with response
                matches = []  # Clear matches so response doesn't have invalid data

        # Get parsing metadata if available
        parse_metadata = getattr(self, "_parse_metadata", {}).get(file_path, {})
        if parse_metadata:
            # Clear metadata after use
            if hasattr(self, "_parse_metadata") and file_path in self._parse_metadata:
                del self._parse_metadata[file_path]

        return {
            "status": "success",
            "rfq_id": rfq_id,
            "uploaded_file_id": uploaded_file_id,
            "rfq_items_count": len(rfq_items),
            "rfq_items": [item.to_dict() for item in db_items],
            "matches": matches,
        }

    async def process_eml_upload(
        self, file_path: str, rfq_id: str | None, uploaded_file_id: int, db: Session
    ) -> dict[str, Any]:
        """
        Process uploaded EML email file

        Args:
            file_path: Path to uploaded file
            rfq_id: RFQ identifier (optional, extracted from email if not provided)
            uploaded_file_id: ID of the uploaded file
            db: Database session

        Returns:
            Processing result
        """
        # Parse EML file (synchronous - fast operation)
        rfq_items, extracted_rfq_id, email_metadata = self.parse_eml_file(file_path)

        # Use extracted RFQ ID if not provided
        if not rfq_id:
            rfq_id = extracted_rfq_id
            # Generate default RFQ ID if still None
            if not rfq_id:
                import os
                import re
                import uuid

                # Extract meaningful parts from filename for uniqueness
                filename = os.path.basename(file_path)
                name_without_ext = os.path.splitext(filename)[0]

                # Clean filename: remove special characters and limit length
                clean_name = re.sub(r"[^A-Za-z0-9\-]", "-", name_without_ext)
                clean_name = re.sub(r"-+", "-", clean_name)  # Remove multiple dashes
                clean_name = clean_name[:30]  # Limit length

                # Generate RFQ ID with filename uniqueness
                unique_id = f"RFQ-EML-{clean_name}-{uuid.uuid4().hex[:4].upper()}"
                rfq_id = unique_id
                logger.info(f"Generated RFQ ID from filename: {rfq_id}")

        logger.info(f"Processing EML file with RFQ ID: {rfq_id}")

        # Save to database with uploaded_file_id
        db_items = self.save_rfq_items_to_db(rfq_items, rfq_id, uploaded_file_id, db)

        # Save to CSV
        self.save_to_csv(rfq_items=rfq_items, rfq_id=rfq_id)

        # Perform matching
        matches = self.perform_matching(rfq_items, db)

        # Save matches to database
        if matches:
            try:
                saved_matches = self.save_matches_to_db(matches, db_items, db)
                logger.info(
                    f"Saved {len(saved_matches)} matches to database for RFQ {rfq_id}"
                )
            except Exception as e:
                # Rollback to ensure session is in clean state
                db.rollback()
                logger.error(f"Error saving matches to database: {e}")
                # Don't fail the upload if match saving matches - continue with response
                matches = []  # Clear matches so response doesn't have invalid data

        return {
            "status": "success",
            "rfq_id": rfq_id,
            "uploaded_file_id": uploaded_file_id,
            "rfq_items_count": len(rfq_items),
            "rfq_items": [item.to_dict() for item in db_items],
            "matches": matches,
        }

    def reload_matching_service(self, db: Session):
        """
        Reload matching service with current product master (synchronous)

        Args:
            db: Database session
        """
        products = db.query(ProductMaster).all()
        products_data = [p.to_dict() for p in products]
        df = pd.DataFrame(products_data)
        matching_service.load_product_master(df)

    async def reload_matching_service_async(self, db: Session):
        """
        Reload matching service with current product master (async - runs in thread pool)

        Args:
            db: Database session
        """

        def _reload_sync():
            products = db.query(ProductMaster).all()
            products_data = [p.to_dict() for p in products]
            df = pd.DataFrame(products_data)
            matching_service.load_product_master(df)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(_thread_pool, _reload_sync)

    def perform_matching(
        self, rfq_items: list[dict[str, Any]], db: Session
    ) -> list[dict[str, Any]]:
        """
        Perform matching for RFQ items with color awareness

        Args:
            rfq_items: List of RFQ items
            db: Database session

        Returns:
            List of match results with structured data (rfq, product_master, match_info)
        """
        # Get match results with color information
        match_results = matching_service.match_rfq_items(rfq_items)

        # Populate product_master data for each match
        for result in match_results:
            matched_text = result["product_master"]["clean_product_name"]
            extracted_color = result["match_info"]["extracted_color"]
            color_match = result["match_info"]["color_match"]

            # Add extracted color to RFQ data
            result["rfq"]["color"] = extracted_color

            # Build query - first filter by product name
            query = db.query(ProductMaster).filter(
                ProductMaster.clean_product_name == matched_text
            )

            # If color match is True, also filter by color
            product = None
            if color_match and extracted_color:
                # Try to find product with matching color
                # First check for exact color match
                product = query.filter(ProductMaster.color == extracted_color).first()

                if not product:
                    # Try partial color match
                    product = query.filter(
                        ProductMaster.color.contains(extracted_color)
                    ).first()

            # No color match or color not found, get first product by name
            if not product:
                product = query.first()

            # Populate product_master data
            if product:
                result["product_master"]["id"] = product.id
                result["product_master"]["pmc"] = product.pmc
                result["product_master"]["product_name"] = product.product_name
                result["product_master"]["color"] = product.color
                result["product_master"]["sheet_type"] = product.sheet_type
            else:
                result["product_master"]["id"] = None
                result["product_master"]["pmc"] = None
                result["product_master"]["product_name"] = None
                result["product_master"]["color"] = None
                result["product_master"]["sheet_type"] = None

        return match_results


# Singleton instance
rfq_service = RFQService()
