"""
EML Parser utility for extracting RFQ product data from email files.

This module provides functions to parse EML email files and extract
product information from HTML tables within the email content.
"""

import logging
from difflib import get_close_matches
from email import policy
from email.parser import BytesParser
from io import StringIO
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def parse_eml(file_path: str) -> dict[str, Any]:
    """
    Parse EML file and extract email metadata and content.

    Args:
        file_path: Path to the EML file

    Returns:
        Dictionary containing email data with keys:
        - subject: Email subject
        - from: Sender email address
        - to: Recipient email address
        - date: Email date
        - text: Plain text content
        - html: HTML content
        - attachments: List of attachment filenames
    """
    with open(file_path, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    data = {
        "subject": msg.get("subject", ""),
        "from": msg.get("from", ""),
        "to": msg.get("to", ""),
        "date": msg.get("date", ""),
        "text": None,
        "html": None,
        "attachments": [],
    }

    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition", ""))

        if content_type == "text/plain" and "attachment" not in disposition:
            try:
                data["text"] = part.get_content()
            except Exception as e:
                logger.warning(f"Could not extract text content: {e}")

        elif content_type == "text/html" and "attachment" not in disposition:
            try:
                data["html"] = part.get_content()
            except Exception as e:
                logger.warning(f"Could not extract HTML content: {e}")

        elif part.get_content_disposition() == "attachment":
            filename = part.get_filename()
            if filename:
                data["attachments"].append(filename)

    return data


def parse_html_table(table) -> list[list[str]]:
    """
    Parse HTML table element into list of rows.

    Args:
        table: BeautifulSoup table element

    Returns:
        List of rows, where each row is a list of cell values
    """
    rows_data = []

    for row in table.find_all("tr"):
        cols = row.find_all(["td", "th"])
        cols = [col.get_text(strip=True) for col in cols]

        if cols:  # skip empty row
            rows_data.append(cols)

    return rows_data


def find_product_table(tables: list) -> pd.DataFrame | None:
    """
    Find the product table from a list of HTML tables.

    Looks for tables containing key columns like 'QTY' to identify
    the product data table.

    Args:
        tables: List of BeautifulSoup table elements

    Returns:
        DataFrame of the product table, or None if not found
    """
    for idx, table in enumerate(tables):
        try:
            df_list = pd.read_html(StringIO(str(table)))
            if df_list:
                current_df = df_list[0]

                # Check for key columns to identify the product table
                # Support both uppercase and lowercase column names
                columns_str = str(current_df.columns).upper()
                if "QTY" in columns_str or "QTY" in current_df.columns:
                    logger.info(f"Found product table at index {idx}")

                    # Check if first row is actually a header (contains "QTY", "Qty", etc.)
                    if not current_df.empty:
                        first_row = current_df.iloc[0].astype(str).str.upper().tolist()
                        if any("QTY" in str(cell) for cell in first_row):
                            logger.info("First row appears to be a header, skipping it")
                            # Use first row as column names and skip it
                            current_df.columns = current_df.iloc[0]
                            current_df = current_df.iloc[1:].reset_index(drop=True)

                    return current_df
        except Exception as e:
            logger.debug(f"Could not parse table {idx}: {e}")
            continue

    return None


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column name with fuzzy matching to handle typos.

    Args:
        col_name: Original column name

    Returns:
        Normalized column name or original if no match found
    """
    if not col_name or not isinstance(col_name, str):
        return col_name

    col_lower = col_name.lower().strip()

    # Known column names and their variations
    column_mappings = {
        "product name": [
            "product",
            "product name",
            "product_name",
            "description",
            "item",
            "material",
        ],
        "color": ["color", "colour", "shade"],
        "pack size": ["pack size", "pack_size", "size", "package"],
        "qty": ["qty", "quantity", "quant", "amount"],
        "uom": ["uom", "unit", "unit of measure"],
    }

    # First try exact match
    for standard_name, variations in column_mappings.items():
        if col_lower in [v.lower() for v in variations]:
            return standard_name

    # Then try fuzzy matching for typos (e.g., "roduct name" -> "product name")
    for standard_name, variations in column_mappings.items():
        all_variations = variations + [standard_name]
        matches = get_close_matches(
            col_lower, [v.lower() for v in all_variations], n=1, cutoff=0.7
        )
        if matches:
            return standard_name

    # Return original if no match
    return col_name


def extract_rfq_id_from_subject(subject: str) -> str | None:
    """
    Extract RFQ ID from email subject.

    Looks for patterns like:
    - NPM250005566
    - NPM260000417
    - RFQ Number_ ECA_S_RFQ_25_0192

    Args:
        subject: Email subject line

    Returns:
        Extracted RFQ ID or None
    """
    import re

    if not subject:
        return None

    # Try to find NPM pattern
    npm_match = re.search(r"NPM\d+", subject)
    if npm_match:
        return f"RFQ-{npm_match.group(0)}"

    # Try to find RFQ pattern
    rfq_match = re.search(r"RFQ\s*Number[_:]\s*(\S+)", subject, re.IGNORECASE)
    if rfq_match:
        return f"RFQ-{rfq_match.group(1)}"

    # Try to find any RFQ-XXX pattern
    general_match = re.search(r"RFQ[-_]?\w+", subject, re.IGNORECASE)
    if general_match:
        return general_match.group(0).replace("_", "-")

    return None


def process_eml_for_products(file_path: str) -> dict[str, Any]:
    """
    Process EML file and extract RFQ product data.

    This function:
    1. Parses the EML file to extract HTML content
    2. Finds the product table in the HTML
    3. Converts the table to a DataFrame
    4. Extracts RFQ items with qty, uom, and description

    Args:
        file_path: Path to the EML file

    Returns:
        Dictionary containing:
        - rfq_id: Extracted RFQ ID (or None)
        - items: List of RFQ item dictionaries with keys:
            - raw_text: Product description
            - qty: Quantity
            - uom: Unit of measure
            - source: Source identifier (eml_parser)
        - email_metadata: Email metadata (subject, from, to, date)
        - attachments: List of attachment filenames
    """
    logger.info(f"Processing EML file: {file_path}")

    # Parse EML file
    email_data = parse_eml(file_path)

    logger.info(f"Email subject: {email_data.get('subject', 'No subject')}")

    html_content = email_data.get("html")
    if not html_content:
        logger.warning(f"No HTML content found in EML file: {file_path}")
        return {
            "rfq_id": None,
            "items": [],
            "email_metadata": email_data,
            "attachments": email_data.get("attachments", []),
        }

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, "lxml")
    tables = soup.find_all("table")

    logger.info(f"Found {len(tables)} tables in HTML content")

    if not tables:
        logger.warning(f"No tables found in EML file: {file_path}")
        return {
            "rfq_id": None,
            "items": [],
            "email_metadata": email_data,
            "attachments": email_data.get("attachments", []),
        }

    # Find the product table
    product_df = find_product_table(tables)

    if product_df is None or product_df.empty:
        logger.warning(f"No product table found in EML file: {file_path}")
        return {
            "rfq_id": None,
            "items": [],
            "email_metadata": email_data,
            "attachments": email_data.get("attachments", []),
        }

    logger.info(f"Product table shape: {product_df.shape}")
    logger.info(f"Product table preview: {product_df.head()}")
    logger.info(f"Product table columns: {list(product_df.columns)}")

    # Extract RFQ items from DataFrame
    rfq_items = []

    # Normalize column names using fuzzy matching to handle typos
    original_columns = list(product_df.columns)
    normalized_columns = [normalize_column_name(str(col)) for col in product_df.columns]
    product_df.columns = normalized_columns

    logger.info(f"Normalized columns: {normalized_columns}")
    logger.info(f"Column mapping: {dict(zip(original_columns, normalized_columns))}")

    for _, row in product_df.iterrows():
        # Try different column name variations (now normalized)
        raw_text = (
            row.get("description")
            or row.get("item")
            or row.get("product name")
            or row.get("material")
            or ""
        )

        # Get color if available
        color = row.get("color") or ""

        qty = row.get("qty") or ""
        uom = row.get("uom") or ""

        # Skip rows without description
        if pd.notna(raw_text) and str(raw_text).strip():
            item = {
                "raw_text": str(raw_text).strip(),
                "color": str(color).strip() if pd.notna(color) and color else None,
                "qty": str(qty).strip() if pd.notna(qty) and qty else None,
                "uom": str(uom).strip() if pd.notna(uom) and uom else None,
                "source": "eml_parser",
            }
            rfq_items.append(item)

    logger.info(f"Extracted {len(rfq_items)} RFQ items from EML file")

    # Extract RFQ ID from subject
    rfq_id = extract_rfq_id_from_subject(email_data.get("subject", ""))

    return {
        "rfq_id": rfq_id,
        "items": rfq_items,
        "email_metadata": {
            "subject": email_data.get("subject", ""),
            "from": email_data.get("from", ""),
            "to": email_data.get("to", ""),
            "date": email_data.get("date", ""),
        },
        "attachments": email_data.get("attachments", []),
    }


def parse_eml_for_rfqs(file_path: str) -> list[dict[str, Any]]:
    """
    Parse EML file and return list of RFQ items in standard format.

    This is a simplified interface that returns only the RFQ items list,
    compatible with other parsers in the system.

    Args:
        file_path: Path to the EML file

    Returns:
        List of RFQ item dictionaries with keys:
        - raw_text: Product description
        - qty: Quantity
        - uom: Unit of measure
        - source: Source identifier (eml_parser)
    """
    result = process_eml_for_products(file_path)
    return result.get("items", [])
