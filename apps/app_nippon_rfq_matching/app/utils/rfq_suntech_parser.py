"""
RFQ SunTech PDF Parser

Utility functions for parsing SunTech RFQ PDF files using pdfplumber.
This parser handles multi-page PDFs with tables that need to be merged.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_columns(cols: list) -> list[str]:
    """
    Normalize column names by removing newlines and stripping whitespace.

    Args:
        cols: List of column names

    Returns:
        List of normalized column names
    """
    return [str(c).replace("\n", " ").strip() for c in cols]


def is_valid_column_table(df: pd.DataFrame, keywords: list[str] | None = None) -> bool:
    """
    Heuristic check to determine if a table has valid RFQ column headers.

    Args:
        df: DataFrame to check (first row should contain headers)
        keywords: List of keywords to search for in column names.
                  Defaults to ["description", "item", "qty", "quantity"]

    Returns:
        True if the table appears to have valid RFQ columns
    """
    if keywords is None:
        keywords = ["description", "item", "qty", "quantity"]

    cols = normalize_columns(df.iloc[0])

    return any(any(k in str(col).lower() for k in keywords) for col in cols)


def extract_tables_from_pdf(path: str) -> list[pd.DataFrame]:
    """
    Extract all tables from a PDF file using pdfplumber.

    Args:
        path: Path to the PDF file

    Returns:
        List of DataFrames, one for each table found
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

        logger.info(f"Extracted tables: {dfs}")

    except Exception as e:
        logger.error(f"Error extracting tables from PDF {path}: {e}", exc_info=True)
        raise

    return dfs


def parse_rfq_pdf(
    path: str,
    column_keywords: list[str] | None = None,
    remove_office_notes: bool = True,
    description_column: str = "Description",
) -> pd.DataFrame:
    """
    Parse SunTech RFQ PDF file and extract merged table data.

    This function:
    1. Extracts all tables from the PDF
    2. Validates tables have proper column headers
    3. Merges tables with matching column structures
    4. Cleans the data (removes empty rows, Office Notes, etc.)

    Args:
        path: Path to the PDF file
        column_keywords: Keywords for validating column headers.
                        Defaults to ["description", "item", "qty", "quantity"]
        remove_office_notes: Whether to remove rows containing "Office Notes"
        description_column: Name of the description column for validation

    Returns:
        Cleaned DataFrame with merged RFQ data

    Example:
        >>> df = parse_rfq_pdf("rfq_suntech.pdf")
        >>> print(df.columns)
        >>> print(df.head())
    """

    # =========================
    # 1. Extract all tables
    # =========================
    dfs = extract_tables_from_pdf(path)

    if not dfs:
        logger.warning(f"No tables found in PDF: {path}")
        return pd.DataFrame()

    cleaned_tables = []
    base_columns = None

    # =========================
    # 2. Process and merge tables
    # =========================
    for idx, raw_df in enumerate(dfs):
        if raw_df.empty:
            logger.debug(f"Skipping empty table {idx}")
            continue

        # Validate first table has proper columns
        if idx == 0 and not is_valid_column_table(raw_df, column_keywords):
            logger.warning(f"Table {idx} does not have valid RFQ columns, skipping")
            continue

        df = raw_df.copy()

        # Set first row as header
        df.columns = normalize_columns(df.iloc[0])
        df = df[1:].reset_index(drop=True)

        # Save base columns from first valid table
        if base_columns is None:
            base_columns = df.columns
            logger.debug(f"Base columns set to: {list(base_columns)}")

        # Skip tables with different column structure
        if len(df.columns) != len(base_columns):
            logger.debug(
                f"Table {idx} has {len(df.columns)} columns, expected {len(base_columns)}, skipping"
            )
            continue

        # Force columns to match base structure
        df.columns = base_columns

        cleaned_tables.append(df)
        logger.debug(f"Added table {idx} to merge list")

    if not cleaned_tables:
        logger.warning("No valid tables to merge")
        return pd.DataFrame()

    # =========================
    # 3. Merge all tables
    # =========================
    df_join = pd.concat(cleaned_tables, ignore_index=True)
    logger.info(f"Merged {len(cleaned_tables)} tables into {len(df_join)} rows")

    # =========================
    # 4. Clean the data
    # =========================
    df_join = df_join.replace(["", " "], np.nan)

    # Remove Office Notes rows if enabled
    if remove_office_notes and description_column in df_join.columns:
        before_count = len(df_join)
        df_join = df_join[
            ~df_join[description_column]
            .astype(str)
            .str.contains("Office Notes", na=False)
        ]
        removed_count = before_count - len(df_join)
        if removed_count > 0:
            logger.debug(f"Removed {removed_count} 'Office Notes' rows")

    # Drop rows with missing description
    before_count = len(df_join)
    df_join = df_join.dropna(subset=[description_column])
    dropped_count = before_count - len(df_join)
    if dropped_count > 0:
        logger.debug(f"Dropped {dropped_count} rows with missing description")

    df_join = df_join.reset_index(drop=True)

    logger.info(
        f"Final DataFrame has {len(df_join)} rows and {len(df_join.columns)} columns"
    )

    return df_join


def parse_rfq_pdf_to_dict(
    path: str,
    column_keywords: list[str] | None = None,
) -> list[dict]:
    """
    Parse SunTech RFQ PDF and return as list of dictionaries.

    This is a convenience function that converts the DataFrame result
    to a list of dicts for easier API integration.

    Args:
        path: Path to the PDF file
        column_keywords: Keywords for validating column headers

    Returns:
        List of dictionaries, one per row in the parsed table

    Example:
        >>> items = parse_rfq_pdf_to_dict("rfq_suntech.pdf")
        >>> for item in items:
        ...     print(item.get("Description"), item.get("Quantity"))
    """
    df = parse_rfq_pdf(path, column_keywords)

    # Convert NaN to None for JSON serialization
    result = df.where(pd.notna(df), None).to_dict("records")

    return result
