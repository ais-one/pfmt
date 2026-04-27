"""
LlamaParse PDF parser with fallback to pdfplumber

This module provides PDF parsing using LlamaParse API with automatic fallback
to pdfplumber if LlamaParse fails.
"""

import logging
import time
from typing import Any

import pandas as pd
import requests

from apps.app_nippon_rfq_matching.app.utils.parsers import (
    clean_raw_text_rfq,
    parse_all_rfq,
)

logger = logging.getLogger(__name__)


class LlamaParseParser:
    """Parser using LlamaParse API with pdfplumber fallback"""

    def __init__(self, api_key: str, timeout: int = 300):
        """
        Initialize LlamaParse parser

        Args:
            api_key: LlamaParse API key
            timeout: Maximum time to wait for parsing (seconds)
        """
        self.api_key = api_key
        self.base_url = "https://api.cloud.llamaindex.ai/api/v1/parsing"
        self.timeout = timeout
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }

    def upload_file(self, file_path: str) -> dict[str, Any]:
        """
        Upload file to LlamaParse

        Args:
            file_path: Path to file

        Returns:
            Upload response with job ID

        Raises:
            requests.RequestException: If upload fails
        """
        url = f"{self.base_url}/upload"

        files = {"file": open(file_path, "rb")}

        data = {
            "output_tables_as_HTML": "true",
            "preserve_layout_alignment_across_pages": "true",
        }

        response = requests.post(url, headers=self.headers, files=files, data=data)
        response.raise_for_status()

        return response.json()

    def wait_until_done(self, job_id: str) -> bool:
        """
        Wait for parsing job to complete

        Args:
            job_id: Job ID from upload

        Returns:
            True if successful, False if failed
        """
        url = f"{self.base_url}/job/{job_id}"
        start_time = time.time()

        while True:
            # Check timeout
            if time.time() - start_time > self.timeout:
                logger.error(
                    f"LlamaParse timeout after {self.timeout}s for job {job_id}"
                )
                return False

            try:
                res = requests.get(url, headers=self.headers).json()
                status = res.get("status")

                logger.info(f"LlamaParse job {job_id} → {status}")

                if status == "SUCCESS":
                    return True
                elif status == "ERROR":
                    error = res.get("error", "Unknown error")
                    logger.error(f"LlamaParse failed for job {job_id}: {error}")
                    return False

                # Wait before polling again
                time.sleep(2)

            except requests.RequestException as e:
                logger.error(f"Error checking LlamaParse job status: {e}")
                return False

    def get_result(self, job_id: str) -> dict[str, Any] | None:
        """
        Get parsing result

        Args:
            job_id: Job ID

        Returns:
            Parsed result or None if not available
        """
        url = f"{self.base_url}/job/{job_id}/result/json"

        response = requests.get(url, headers=self.headers)

        if response.status_code == 404:
            logger.warning(f"LlamaParse result not ready for job {job_id}")
            return None

        response.raise_for_status()
        return response.json()

    def get_markdown_result(self, job_id: str) -> str | None:
        """
        Get markdown parsing result

        Args:
            job_id: Job ID

        Returns:
            Markdown content or None if not available
        """
        url = f"{self.base_url}/job/{job_id}/result/markdown"

        response = requests.get(url, headers=self.headers)

        if response.status_code == 404:
            logger.warning(f"LlamaParse markdown result not ready for job {job_id}")
            return None

        response.raise_for_status()
        return response.text

    def parse_with_llamaparse(self, file_path: str) -> list[dict[str, Any]] | None:
        """
        Parse PDF using LlamaParse API

        Args:
            file_path: Path to PDF file

        Returns:
            List of parsed RFQ items or None if parsing failed
        """
        logger.info(f"Attempting LlamaParse for: {file_path}")

        try:
            # Upload file
            upload_result = self.upload_file(file_path)
            job_id = upload_result.get("id")

            if not job_id:
                logger.error("No job ID returned from LlamaParse upload")
                return None

            logger.info(f"LlamaParse job ID: {job_id}")

            # Wait for completion
            if not self.wait_until_done(job_id):
                return None

            # Get markdown result (better for table extraction)
            markdown = self.get_markdown_result(job_id)

            if markdown and len(markdown) > 100:  # Basic validation
                logger.info(f"Got markdown from LlamaParse, length: {len(markdown)}")
                logger.debug(f"Markdown preview (first 500 chars):\n{markdown[:500]}")

                # Try to parse tables from markdown using pandas
                rfq_items = self._extract_tables_from_markdown(markdown)
                if rfq_items:
                    return rfq_items
                else:
                    logger.warning("No items extracted from LlamaParse markdown")
            else:
                logger.warning(
                    f"Markdown result too short or empty: {len(markdown) if markdown else 0}"
                )

            # Fallback to JSON result
            result = self.get_result(job_id)

            if not result:
                logger.warning("Empty JSON result from LlamaParse")
                return None

            # Debug: Log result structure
            logger.debug(f"LlamaParse result keys: {list(result.keys())}")
            logger.debug(f"LlamaParse result: {str(result)[:500]}")

            # Parse tables from LlamaParse result
            return self._extract_tables_from_llamaparse(result)

        except requests.RequestException as e:
            logger.error(f"LlamaParse request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"LlamaParse parsing failed: {e}", exc_info=True)
            return None

    def _extract_tables_from_markdown(self, markdown: str) -> list[dict[str, Any]]:
        """
        Extract RFQ items from LlamaParse markdown result

        Args:
            markdown: Markdown content from LlamaParse

        Returns:
            List of parsed RFQ items
        """
        rfq_items = []
        tables_dict = {}

        try:
            from io import StringIO

            html_tables = pd.read_html(StringIO(markdown))

            logger.info(f"Found {len(html_tables)} tables in LlamaParse markdown")

            for table_idx, html_table in enumerate(html_tables):
                logger.info(f"LlamaParse Table {table_idx}:")
                logger.info(f"  Shape: {html_table.shape}")
                logger.info(f"  Columns: {list(html_table.columns)}")
                logger.debug(f"  Head:\n{html_table.head().to_string()}")

                # Convert to dict format expected by parse_all_rfq
                table_dict = html_table.to_dict()
                tables_dict[f"table_{table_idx}"] = table_dict

        except Exception as e:
            logger.warning(f"Could not parse tables from LlamaParse markdown: {e}")

        # If we have tables_dict, parse it using existing parser
        if tables_dict:
            rfq_items = parse_all_rfq(tables_dict)

        # If still no items, try to extract from markdown text directly
        if not rfq_items:
            logger.info(
                "No items from tables, trying to extract from markdown text directly"
            )
            rfq_items = self._extract_items_from_markdown_text(markdown)

        # Add clean text
        for item in rfq_items:
            if item.get("raw_text"):
                item["clean_text"] = clean_raw_text_rfq(item["raw_text"])

        logger.info(f"Extracted {len(rfq_items)} RFQ items from LlamaParse markdown")
        return rfq_items

    def _extract_items_from_markdown_text(self, markdown: str) -> list[dict[str, Any]]:
        """
        Extract RFQ items from markdown text by parsing table-like structures

        Args:
            markdown: Markdown content

        Returns:
            List of parsed RFQ items
        """
        import re

        rfq_items = []
        lines = markdown.split("\n")

        # Look for table-like patterns in markdown
        # Pattern: | # | Supplier Part No. | Description | UoM | Qty |
        table_start_pattern = re.compile(r"^\|.*\|$")

        in_table = False
        headers = []

        for i, line in enumerate(lines):
            line = line.strip()

            # Check if this looks like a table header
            if table_start_pattern.match(line):
                cells = [
                    cell.strip() for cell in line.split("|")[1:-1]
                ]  # Remove empty first/last

                # Check if this looks like an RFQ table header
                if any(
                    keyword in " ".join(cells).lower()
                    for keyword in ["#", "part", "description", "qty", "uom"]
                ):
                    headers = cells
                    in_table = True
                    logger.info(f"Found potential RFQ table at line {i}: {headers}")
                    continue

            # Skip separator line (|---|---|---|)
            if in_table and re.match(r"^\|[\s\-:]+\|$", line):
                continue

            # Extract data rows
            if in_table and table_start_pattern.match(line):
                cells = [cell.strip() for cell in line.split("|")[1:-1]]

                if (
                    len(cells) >= 3 and cells[0]
                ):  # At least 3 columns and first cell not empty
                    # Try to map columns to RFQ fields
                    item = {"source": "llamaparse_markdown_table"}

                    # Map based on header position
                    for j, header in enumerate(headers):
                        if j < len(cells):
                            header_lower = header.lower()
                            if "#" in header or "part" in header_lower:
                                item["part_no"] = cells[j]
                            elif "description" in header_lower:
                                item["raw_text"] = cells[j]
                            elif "qty" in header_lower:
                                item["qty"] = cells[j]
                            elif "uom" in header_lower:
                                item["uom"] = cells[j]

                    # If no description found, use first non-empty cell
                    if "raw_text" not in item or not item["raw_text"]:
                        for cell in cells:
                            if cell and cell not in item.values():
                                item["raw_text"] = cell
                                break

                    if item.get("raw_text"):
                        rfq_items.append(item)

            # End of table (empty line or non-table line)
            if in_table and not table_start_pattern.match(line) and line:
                # Check if we've collected enough items, then stop
                if rfq_items:
                    break

        logger.info(f"Extracted {len(rfq_items)} items from markdown text parsing")
        return rfq_items

    def _extract_tables_from_llamaparse(
        self, result: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Extract RFQ items from LlamaParse result

        Args:
            result: LlamaParse result JSON

        Returns:
            List of parsed RFQ items
        """
        rfq_items = []
        tables_dict = {}

        # LlamaParse returns different formats depending on the document
        # Check for markdown/text content first
        markdown = result.get("markdown") or result.get("text")

        if markdown:
            # Try to parse tables from markdown using pandas
            try:
                from io import StringIO

                html_tables = pd.read_html(StringIO(markdown))

                logger.info(f"Found {len(html_tables)} tables in LlamaParse markdown")

                for table_idx, html_table in enumerate(html_tables):
                    logger.info(f"LlamaParse Table {table_idx}:")
                    logger.info(f"  Shape: {html_table.shape}")
                    logger.info(f"  Columns: {list(html_table.columns)}")

                    # Convert to dict format expected by parse_all_rfq
                    table_dict = html_table.to_dict()
                    tables_dict[f"table_{table_idx}"] = table_dict

            except Exception as e:
                logger.warning(f"Could not parse tables from LlamaParse markdown: {e}")

        # Check for items array (structured data)
        items = result.get("items")
        if items:
            logger.info(f"Processing {len(items)} items from LlamaParse")

            for item in items:
                # Extract item properties based on LlamaParse structure
                raw_text = (
                    item.get("description")
                    or item.get("text")
                    or item.get("item_description")
                    or ""
                )

                qty = item.get("quantity") or item.get("qty")
                uom = item.get("unit") or item.get("uom")

                if raw_text:
                    rfq_items.append(
                        {
                            "raw_text": raw_text,
                            "qty": qty,
                            "uom": uom,
                            "source": "llamaparse_items",
                        }
                    )

        # If we have tables_dict, parse it using existing parser
        if tables_dict and not rfq_items:
            rfq_items = parse_all_rfq(tables_dict)

        # Add clean text
        for item in rfq_items:
            if item.get("raw_text"):
                item["clean_text"] = clean_raw_text_rfq(item["raw_text"])

        logger.info(f"Extracted {len(rfq_items)} RFQ items from LlamaParse")
        return rfq_items

    def parse_with_pdfplumber_fallback(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse PDF using pdfplumber as fallback

        Args:
            file_path: Path to PDF file

        Returns:
            List of parsed RFQ items
        """
        logger.info(f"Falling back to pdfplumber for: {file_path}")

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
                    "source": "pdfplumber_fallback",
                }
                # Add clean text
                if item.get("raw_text"):
                    item["clean_text"] = clean_raw_text_rfq(item["raw_text"])
                rfq_items.append(item)

            logger.info(f"Extracted {len(rfq_items)} RFQ items from pdfplumber")
            return rfq_items

        except Exception as e:
            logger.error(f"Error in pdfplumber fallback: {e}", exc_info=True)
            return []

    def parse_pdf(
        self, file_path: str, use_fallback: bool = True
    ) -> list[dict[str, Any]]:
        """
        Parse PDF using LlamaParse with pdfplumber fallback

        Args:
            file_path: Path to PDF file
            use_fallback: Whether to use pdfplumber fallback if LlamaParse fails

        Returns:
            List of parsed RFQ items
        """
        # Try LlamaParse first
        rfq_items = self.parse_with_llamaparse(file_path)

        if rfq_items and len(rfq_items) > 0:
            logger.info(f"LlamaParse successfully parsed {len(rfq_items)} items")
            return {"items": rfq_items, "parser": "llamaparse", "fallback_used": False}

        # Fallback to pdfplumber if enabled and LlamaParse failed
        if use_fallback:
            logger.warning(
                "LlamaParse failed or returned empty results, using pdfplumber fallback"
            )
            rfq_items = self.parse_with_pdfplumber_fallback(file_path)
            return {"items": rfq_items, "parser": "pdfplumber", "fallback_used": True}

        return {"items": [], "parser": "none", "fallback_used": False}
