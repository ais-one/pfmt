"""
RFQ Columbia PDF Parser

Utility functions for parsing Columbia RFQ PDF files using pdfplumber.
This parser handles PDFs with structured tables and extracts item information
including descriptions, units of measure, and quantities.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Default noise keywords to filter out from descriptions
DEFAULT_NOISE_KEYWORDS = ["buyer", "comment", "note", "office"]


def extract_tables_from_pdf(path: str) -> list[pd.DataFrame]:
    """
    Extract all tables from a PDF file using pdfplumber.

    Args:
        path: Path to the PDF file

    Returns:
        List of DataFrames, one for each table found

    Example:
        >>> tables = extract_tables_from_pdf("rfq_columbia.pdf")
        >>> print(f"Found {len(tables)} tables")
    """
    import pdfplumber

    dfs = []

    try:
        with pdfplumber.open(path) as pdf:
            for page_idx, page in enumerate(pdf.pages):
                tables = page.extract_tables()

                for table_idx, table in enumerate(tables):
                    df = pd.DataFrame(table)
                    dfs.append(df)
                    logger.debug(f"Extracted table {table_idx} from page {page_idx}")

        logger.info(f"Found {len(dfs)} tables in PDF: {path}")

    except Exception as e:
        logger.error(f"Error extracting tables from PDF {path}: {e}", exc_info=True)
        raise

    return dfs


def normalize_tables(
    dfs: list[pd.DataFrame],
    column_map: dict[str, int],
    noise_keywords: list[str] | None = None,
) -> pd.DataFrame:
    """
    Normalize extracted tables to a standard format.

    Args:
        dfs: List of DataFrames from extract_tables_from_pdf
        column_map: Dictionary mapping column names to their positions.
                   Example: {"description": 4, "uom": 7, "qty": 8}
        noise_keywords: List of keywords to filter out (lowercase).
                       Defaults to ["buyer", "comment", "note", "office"]

    Returns:
        Normalized DataFrame with description, uom, and qty columns

    Example:
        >>> column_map = {"description": 4, "uom": 7, "qty": 8}
        >>> df = normalize_tables(tables, column_map)
        >>> print(df.head())
    """
    if noise_keywords is None:
        noise_keywords = DEFAULT_NOISE_KEYWORDS

    all_rows = []

    for df_idx, df in enumerate(dfs):
        df = df.replace(["", " "], np.nan)

        for row_idx, row in df.iterrows():
            try:
                desc = row.iloc[column_map["description"]]
                uom = row.iloc[column_map["uom"]]
                qty = row.iloc[column_map["qty"]]
            except IndexError:
                logger.debug(
                    f"Table {df_idx}, row {row_idx}: Incomplete columns, skipping"
                )
                continue

            # Skip rows without description
            if pd.isna(desc):
                continue

            desc_str = str(desc).strip()

            # Filter out noise rows
            if any(keyword in desc_str.lower() for keyword in noise_keywords):
                logger.debug(f"Filtered noise row: {desc_str[:50]}...")
                continue

            all_rows.append(
                {
                    "description": desc_str,
                    "uom": str(uom).strip() if pd.notna(uom) else None,
                    "qty": qty,
                }
            )

    result_df = pd.DataFrame(all_rows)
    logger.info(f"Normalized {len(all_rows)} rows from {len(dfs)} tables")

    return result_df


def clean_dataframe(
    df: pd.DataFrame, remove_buyer_comments: bool = True, remove_uom_suffix: bool = True
) -> pd.DataFrame:
    """
    Clean the DataFrame by removing unwanted text and formatting.

    Args:
        df: DataFrame to clean
        remove_buyer_comments: Whether to remove "Buyer Comments" text from descriptions
        remove_uom_suffix: Whether to remove "-UOM:" suffixes from descriptions

    Returns:
        Cleaned DataFrame

    Example:
        >>> df = clean_dataframe(df)
        >>> print(df[['description', 'uom', 'qty']].head())
    """
    df = df.dropna(subset=["description"]).copy()

    # Clean descriptions
    if remove_buyer_comments:
        df["description"] = (
            df["description"]
            .astype(str)
            .str.replace(r"Buyer Comments.*", "", regex=True, case=False)
            .str.strip()
        )

    if remove_uom_suffix:
        df["description"] = (
            df["description"]
            .astype(str)
            .str.replace(r"-UOM:.*", "", regex=True, case=False)
            .str.strip()
        )

    # Clean UOM column
    df["uom"] = df["uom"].astype(str).str.strip()
    df["uom"] = df["uom"].replace("None", "")

    # Convert qty to numeric
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    df = df.reset_index(drop=True)

    logger.debug(f"Cleaned DataFrame: {len(df)} rows")

    return df


def parse_rfq_columbia_pdf(
    path: str,
    column_map: dict[str, int] = {"description": 4, "uom": 5, "qty": 6},
    noise_keywords: list[str] | None = None,
    remove_buyer_comments: bool = True,
    remove_uom_suffix: bool = True,
) -> pd.DataFrame:
    """
    Parse Columbia RFQ PDF file and extract structured data.

    This function:
    1. Extracts all tables from the PDF
    2. Normalizes tables using the column map
    3. Cleans descriptions and data
    4. Returns a cleaned DataFrame

    Args:
        path: Path to the PDF file
        column_map: Dictionary mapping column names to their positions.
                   Example: {"description": 4, "uom": 7, "qty": 8}
        noise_keywords: List of keywords to filter out from descriptions
        remove_buyer_comments: Whether to remove "Buyer Comments" from descriptions
        remove_uom_suffix: Whether to remove "-UOM:" suffixes from descriptions

    Returns:
        Cleaned DataFrame with description, uom, and qty columns

    Example:
        >>> column_map = {"description": 1, "uom": 5, "qty": 6}
        >>> df = parse_rfq_columbia_pdf("rfq_columbia.pdf", column_map)
        >>> print(df.head())
    """
    # Extract tables
    dfs = extract_tables_from_pdf(path)

    logger.info("Tables found in PDF columbia ")
    logger.info(dfs)

    if not dfs:
        logger.warning(f"No tables found in PDF: {path}")
        return pd.DataFrame()

    # Normalize tables
    df = normalize_tables(dfs, column_map, noise_keywords)

    if df.empty:
        logger.warning(f"No valid data extracted from PDF: {path}")
        return pd.DataFrame()

    # Clean DataFrame
    df = clean_dataframe(df, remove_buyer_comments, remove_uom_suffix)

    logger.info(f"Parsed Columbia PDF: {len(df)} items from {path}")

    return df


def parse_rfq_columbia_pdf_to_dict(
    path: str, column_map: dict[str, int], noise_keywords: list[str] | None = None
) -> list[dict[str, Any]]:
    """
    Parse Columbia RFQ PDF and return as list of dictionaries.

    This is a convenience function that converts the DataFrame result
    to a list of dicts for easier API integration.

    Args:
        path: Path to the PDF file
        column_map: Dictionary mapping column names to their positions
        noise_keywords: List of keywords to filter out from descriptions

    Returns:
        List of dictionaries, one per row in the parsed table

    Example:
        >>> column_map = {"description": 1, "uom": 5, "qty": 6}
        >>> items = parse_rfq_columbia_pdf_to_dict("rfq_columbia.pdf", column_map)
        >>> for item in items:
        ...     print(f"{item['description']}: {item['qty']} {item['uom']}")
    """
    df = parse_rfq_columbia_pdf(
        path=path, column_map=column_map, noise_keywords=noise_keywords
    )

    # Convert NaN to None for JSON serialization
    result = df.where(pd.notna(df), None).to_dict("records")

    return result


def batch_parse_columbia_pdfs(
    file_paths: list[str],
    column_map: dict[str, int],
    noise_keywords: list[str] | None = None,
) -> pd.DataFrame:
    """
    Parse multiple Columbia RFQ PDF files and combine into a single DataFrame.

    Args:
        file_paths: List of paths to PDF files
        column_map: Dictionary mapping column names to their positions
        noise_keywords: List of keywords to filter out from descriptions

    Returns:
        Combined DataFrame with data from all files

    Example:
        >>> files = ["rfq1.pdf", "rfq2.pdf", "rfq3.pdf"]
        >>> column_map = {"description": 1, "uom": 5, "qty": 6}
        >>> df = batch_parse_columbia_pdfs(files, column_map)
        >>> print(f"Total items: {len(df)}")
    """
    dataframes = []

    for file_path in file_paths:
        try:
            df = parse_rfq_columbia_pdf(
                path=file_path, column_map=column_map, noise_keywords=noise_keywords
            )
            # Add source file column for tracking
            df["source_file"] = Path(file_path).name
            dataframes.append(df)

        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}", exc_info=True)
            continue

    if not dataframes:
        logger.warning("No files were successfully parsed")
        return pd.DataFrame()

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Combined {len(dataframes)} files into {len(combined_df)} total items")

    return combined_df


def detect_column_map(path: str) -> dict[str, int] | None:
    """
    Attempt to detect the column map by analyzing the PDF table structure.

    This is a heuristic function that tries to identify which columns
    contain description, UOM, and quantity data.

    Args:
        path: Path to the PDF file

    Returns:
        Dictionary with detected column positions, or None if detection fails

    Example:
        >>> column_map = detect_column_map("rfq_columbia.pdf")
        >>> if column_map:
        ...     df = parse_rfq_columbia_pdf("rfq_columbia.pdf", column_map)
    """
    import pdfplumber

    try:
        with pdfplumber.open(path) as pdf:
            # Check first few pages for tables
            for page in pdf.pages[:3]:
                tables = page.extract_tables()
                if not tables:
                    continue

                # Analyze first table
                df = pd.DataFrame(tables[0])

                # Look for common patterns
                # Description: usually the longest text column
                # UOM: often short (EA, PCS, etc.)
                # Qty: numeric

                for idx in range(len(df.columns)):
                    # Sample first non-header row
                    if len(df) > 1:
                        df.iloc[1]
                    else:
                        df.iloc[0]

                    # This is a simplified heuristic
                    # In production, you might want more sophisticated detection
                    pass

                logger.warning("Column map detection not fully implemented")
                return None

    except Exception as e:
        logger.error(f"Error detecting column map for {path}: {e}", exc_info=True)

    return None
