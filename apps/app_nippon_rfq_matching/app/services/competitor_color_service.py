"""
Service for parsing competitor color comparison PDF
"""

import asyncio
import concurrent.futures
import logging
import re
from typing import Any

import pandas as pd
import pdfplumber
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models.competitor import CompetitorColorComparison

logger = logging.getLogger(__name__)

# Thread pool for CPU-intensive operations (PDF parsing)
_thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="color_parser"
)


class CompetitorColorService:
    """Service for parsing and storing competitor color comparison data"""

    def __init__(self):
        """Initialize the service"""
        pass

    def _parse_pdf_sync(self, file_path: str) -> dict[str, list[dict[str, Any]]]:
        """
        Synchronous PDF parsing - runs in worker thread using pdfplumber

        Args:
            file_path: Path to the PDF file

        Returns:
            Dictionary with parsed data separated by brand
        """
        import threading

        current_thread = threading.current_thread()
        logger.info(
            f"[{current_thread.name}] Starting PDF parsing in thread: {file_path}"
        )

        try:
            logger.info(
                f"[{current_thread.name}] Parsing PDF with pdfplumber: {file_path}"
            )

            result = {}

            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    logger.info(f"Processing page {page_num + 1}/{len(pdf.pages)}")

                    # Extract tables from page
                    tables = page.extract_tables()

                    for table_idx, table in enumerate(tables):
                        if not table or len(table) < 2:
                            continue

                        # Convert to DataFrame for easier processing
                        headers = table[0]
                        data_rows = table[1:]

                        # Create DataFrame
                        table_df = pd.DataFrame(data_rows, columns=headers)

                        logger.info(
                            f"Processing table {table_idx} on page {page_num + 1}:"
                        )
                        logger.info(f"  Shape: {table_df.shape}")
                        logger.info(f"  Columns: {list(table_df.columns)}")

                        # Convert to list of lists for processing
                        table_data = table_df.values.tolist()

                        # Determine brand from table columns
                        brand = self._determine_brand_from_columns(
                            list(table_df.columns)
                        )

                        if brand:
                            if brand not in result:
                                result[brand] = []

                            rows = self._extract_color_rows_from_table(
                                table_data, brand, list(table_df.columns)
                            )
                            result[brand].extend(rows)
                            logger.info(
                                f"Extracted {len(rows)} rows for {brand} from table {table_idx}"
                            )
                        else:
                            logger.warning(
                                f"Could not determine brand for table {table_idx}, skipping"
                            )

            # Log summary
            for brand, items in result.items():
                logger.info(f"Total parsed for {brand}: {len(items)} items")

            return result

        except Exception as e:
            logger.error(f"Error parsing PDF: {e}", exc_info=True)
            raise

    async def parse_pdf(self, file_path: str) -> dict[str, list[dict[str, Any]]]:
        """
        Parse competitor color comparison PDF using pdfplumber (async - runs in thread pool)

        Args:
            file_path: Path to the PDF file

        Returns:
            Dictionary with parsed data separated by brand:
            {
                "JOTUN": [{"item_no": 1, "source_code": "...", "npms_code": "...", "raw_text": "..."}],
                "INTERNATIONAL": [...]
            }
        """
        import threading

        main_thread = threading.current_thread()
        logger.info(
            f"[MAIN THREAD: {main_thread.name}] Starting async PDF parsing: {file_path}"
        )

        loop = asyncio.get_event_loop()

        logger.info(f"[MAIN THREAD: {main_thread.name}] Submitting to thread pool...")

        parsed_data = await loop.run_in_executor(
            _thread_pool, self._parse_pdf_sync, file_path
        )

        logger.info(
            f"[MAIN THREAD: {main_thread.name}] Completed async PDF parsing: {file_path}"
        )
        return parsed_data

    # Backward compatibility - alias for old method name
    async def parse_pdf_with_docling(
        self, file_path: str
    ) -> dict[str, list[dict[str, Any]]]:
        """Backward compatibility alias for parse_pdf"""
        logger.warning("parse_pdf_with_docling is deprecated, use parse_pdf instead")
        return await self.parse_pdf(file_path)

    def _determine_brand_from_columns(self, columns: list[str]) -> str | None:
        """
        Determine the brand from table column names

        Args:
            columns: List of column names

        Returns:
            Brand name or None
        """
        columns_text = " ".join([str(col).upper() for col in columns])

        logger.info(f"Determining brand from columns: {columns_text}")

        for col in columns:
            col_upper = str(col).upper()
            if (
                "CODE" in col_upper
                and "NPMS" not in col_upper
                and "RECOMMENDED" not in col_upper
            ):
                brand = col_upper.replace("CODE", "").replace("COLUMN", "").strip()
                if brand:
                    logger.info(
                        f"Extracted brand from column: {brand} (from column: '{col}')"
                    )
                    return brand

        brand_patterns = {
            "JOTUN": ["JOTUN", "JOTUN CODE", "JOTUN PAINT"],
            "INTERNATIONAL PAINT": ["INTERNATIONAL PAINT", "INTERNATIONAL PAINT CODE"],
            "INTERNATIONAL": ["INTERNATIONAL", "INT'L", "INTL", "INT."],
            "HEMPEL": ["HEMPEL", "HEMPEL CODE"],
            "RAL": ["RAL", "RAL CODE"],
            "SIGMA": ["SIGMA", "SIGMA COATINGS"],
            "AKZO": ["AKZO", "AKZONOBEL"],
            "PPG": ["PPG"],
            "SHERWIN": ["SHERWIN", "SHERWIN WILLIAMS"],
        }

        for brand, patterns in brand_patterns.items():
            for pattern in patterns:
                if pattern in columns_text:
                    logger.info(f"Detected brand: {brand} (pattern: '{pattern}')")
                    return brand

        logger.warning(f"Could not determine brand from columns: {columns}")
        return None

    def _extract_color_rows_from_table(
        self, table_data: list[list[Any]], brand: str, columns: list[str]
    ) -> list[dict[str, Any]]:
        """
        Extract color comparison rows from table

        Args:
            table_data: Table data as 2D list
            brand: Brand name
            columns: List of column names

        Returns:
            List of color comparison dictionaries
        """
        rows = []

        try:
            item_no_col_idx = None
            source_code_col_idx = None
            npms_code_col_idx = None

            for idx, col in enumerate(columns):
                col_upper = str(col).upper()
                if "ITEM" in col_upper and ("NO" in col_upper or "NUMBER" in col_upper):
                    item_no_col_idx = idx
                elif (
                    "CODE" in col_upper
                    and "NPMS" not in col_upper
                    and "RECOMMENDED" not in col_upper
                ):
                    source_code_col_idx = idx
                elif "NPMS" in col_upper or "RECOMMENDED" in col_upper:
                    npms_code_col_idx = idx

            logger.info(
                f"Column indices for {brand}: item_no={item_no_col_idx}, source_code={source_code_col_idx}, "
                f"npms={npms_code_col_idx}"
            )

            header_row_idx = -1
            for idx, row in enumerate(table_data):
                row_text = " ".join(
                    [str(cell).upper() for cell in row if str(cell).strip()]
                )
                if "ITEM" in row_text and ("NO" in row_text or "NUMBER" in row_text):
                    header_row_idx = idx
                    break

            if header_row_idx == -1:
                header_row_idx = 0

            for row_idx in range(header_row_idx + 1, len(table_data)):
                row = table_data[row_idx]

                if not row or all(not str(cell).strip() for cell in row):
                    continue

                item_no = None
                if item_no_col_idx is not None and item_no_col_idx < len(row):
                    first_cell = str(row[item_no_col_idx]).strip()
                    digits = re.findall(r"\d+", first_cell)
                    if digits:
                        item_no = int(digits[0])

                if item_no is None:
                    for cell in row:
                        digits = re.findall(r"\d+", str(cell))
                        if digits:
                            item_no = int(digits[0])
                            break

                if item_no is None:
                    continue

                source_code = None
                if source_code_col_idx is not None and source_code_col_idx < len(row):
                    source_code = str(row[source_code_col_idx]).strip()
                    if not source_code:
                        source_code = None

                npms_code = None
                if npms_code_col_idx is not None and npms_code_col_idx < len(row):
                    npms_code = str(row[npms_code_col_idx]).strip()
                    if not npms_code:
                        npms_code = None

                if source_code:
                    source_code, npms_code = self._split_npms_code_from_source(
                        source_code, npms_code, brand
                    )
                    logger.debug(
                        f"Split source_code: '{source_code}', npms_code: '{npms_code}'"
                    )

                raw_text = " | ".join(
                    [str(cell).strip() for cell in row if str(cell).strip()]
                )

                if source_code or npms_code:
                    rows.append(
                        {
                            "item_no": item_no,
                            "source_brand": brand,
                            "source_code": source_code or "",
                            "npms_code": npms_code or "",
                            "raw_text": raw_text,
                        }
                    )
                    logger.debug(
                        f"Extracted row {len(rows)}: item_no={item_no}, source_code={source_code}, "
                        f"npms_code={npms_code}"
                    )

        except Exception as e:
            logger.error(f"Error extracting color rows from table: {e}", exc_info=True)

        logger.info(f"Total rows extracted for {brand}: {len(rows)}")
        return rows

    def _split_npms_code_from_source(
        self, source_code: str, existing_npms: str | None, brand: str
    ) -> tuple[str, str | None]:
        """
        Split source_code if it contains NPMS color code at the end
        """
        if not source_code:
            return source_code, existing_npms

        source_upper = source_code.upper()

        npms_patterns = [
            r"\s+RAL\s+\d+",
            r"\s+Y\d+",
            r"\s+537\s+",
            r"\s+\d{3,4}\s*$",
        ]

        import re as re_module

        for pattern in npms_patterns:
            match = re_module.search(pattern, source_upper)
            if match:
                split_pos = match.start()
                npms_part = source_code[split_pos:].strip()
                source_part = source_code[:split_pos].strip()

                logger.info("Edge case detected: source_code contains NPMS code at end")
                logger.info(f"  Original: '{source_code}'")
                logger.info(f"  Split to: source='{source_part}', npms='{npms_part}'")

                if not existing_npms:
                    return source_part, npms_part
                else:
                    return source_part, existing_npms

        return source_code, existing_npms

    def save_to_database(
        self,
        parsed_data: dict[str, list[dict[str, Any]]],
        uploaded_file_id: int,
        db: Session,
    ) -> dict[str, Any]:
        """
        Save parsed competitor color comparison data to database with duplicate handling
        """
        from sqlalchemy.exc import IntegrityError

        db_records = []
        duplicates = []
        skipped_count = 0

        try:
            for brand, items in parsed_data.items():
                for item_data in items:
                    item_no = item_data.get("item_no")
                    source_brand = item_data.get("source_brand")
                    source_code = item_data.get("source_code")
                    npms_code = item_data.get("npms_code")

                    if not all([item_no is not None, source_brand, source_code]):
                        logger.warning(
                            f"Skipping record with missing required fields: item_no={item_no}, "
                            f"source_brand={source_brand}, source_code={source_code}"
                        )
                        skipped_count += 1
                        continue

                    existing = (
                        db.query(CompetitorColorComparison)
                        .filter(
                            CompetitorColorComparison.item_no == item_no,
                            CompetitorColorComparison.source_brand == source_brand,
                            CompetitorColorComparison.source_code == source_code,
                        )
                        .first()
                    )

                    if existing:
                        duplicates.append(
                            {
                                "item_no": item_no,
                                "source_brand": source_brand,
                                "source_code": source_code,
                                "existing_id": existing.id,
                            }
                        )
                        logger.debug(
                            f"Duplicate record found: item_no={item_no}, source_brand={source_brand}, "
                            f"source_code={source_code}"
                        )
                    else:
                        db_record = CompetitorColorComparison(
                            item_no=item_no,
                            source_brand=source_brand,
                            source_code=source_code,
                            npms_code=npms_code,
                            raw_text=item_data.get("raw_text"),
                            uploaded_file_id=uploaded_file_id,
                        )
                        db.add(db_record)
                        db_records.append(db_record)
                        logger.debug(
                            f"Created new record: item_no={item_no}, source_brand={source_brand}, "
                            f"source_code={source_code}"
                        )

            try:
                db.commit()

                for record in db_records:
                    db.refresh(record)

                logger.info(
                    f"Saved CompetitorColorComparison: created={len(db_records)}, duplicates={len(duplicates)}, "
                    f"skipped={skipped_count}"
                )

            except IntegrityError as e:
                db.rollback()
                logger.error(f"Integrity error saving CompetitorColorComparison: {e}")
                raise

            return {
                "created": db_records,
                "duplicates": duplicates,
                "skipped_count": skipped_count,
            }

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Integrity error saving CompetitorColorComparison: {e}")
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Error saving to database: {e}", exc_info=True)
            raise

    def get_comparisons(
        self, source_brand: str | None = None, db: Session = None
    ) -> list[CompetitorColorComparison]:
        """
        Get competitor color comparisons from database
        """
        if db is None:
            raise ValueError("Database session is required")

        query = db.query(CompetitorColorComparison)

        if source_brand:
            query = query.filter(CompetitorColorComparison.source_brand == source_brand)

        return query.order_by(CompetitorColorComparison.item_no).all()


# Singleton instance
competitor_color_service = CompetitorColorService()
