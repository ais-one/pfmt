"""
RFQ DryLog Service PDF Parser

Utility functions for parsing DryLog Service RFQ PDF files using pdfplumber.
This parser handles PDFs with dynamic table structures and extracts item information
by detecting "-UOM:" patterns in descriptions.
"""

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Default pattern to identify item rows
DEFAULT_UOM_PATTERN = r"-UOM:"

# Default noise patterns to remove from descriptions
DEFAULT_NOISE_PATTERNS = [
    (r"Buyer Comments.*", re.IGNORECASE),
    (r"-UOM:.*", re.IGNORECASE),
]


def extract_tables_from_pdf(path: str) -> list[pd.DataFrame]:
    """
    Extract all tables from a PDF file using pdfplumber.

    Args:
        path: Path to the PDF file

    Returns:
        List of DataFrames, one for each table found

    Example:
        >>> tables = extract_tables_from_pdf("rfq_drylog.pdf")
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
    dfs: list[pd.DataFrame], uom_pattern: str = DEFAULT_UOM_PATTERN
) -> pd.DataFrame:
    """
    Normalize extracted tables by detecting description columns dynamically.

    This function scans each row for cells containing the UOM pattern
    (default: "-UOM:") and extracts description, UOM, and quantity from
    the detected positions.

    Args:
        dfs: List of DataFrames from extract_tables_from_pdf
        uom_pattern: Regex pattern to identify description columns.
                    Defaults to r"-UOM:"

    Returns:
        Normalized DataFrame with description, uom, and qty columns

    Example:
        >>> df = normalize_tables(tables)
        >>> print(df.head())
    """
    all_extracted_rows = []

    for df_idx, df in enumerate(dfs):
        for r_idx in range(len(df)):
            row = df.iloc[r_idx]

            description = None
            uom = None
            qty = None
            desc_idx = None

            # Find description column by searching for UOM pattern
            for col_idx, cell_value in row.items():
                if isinstance(cell_value, str) and re.search(uom_pattern, cell_value):
                    description = cell_value
                    desc_idx = col_idx
                    break

            # Skip non-item rows
            if not description:
                logger.debug(
                    f"Table {df_idx}, row {r_idx}: No UOM pattern found, skipping"
                )
                continue

            # Extract UOM and quantity from columns after description
            found = []
            if desc_idx is not None:
                for i in range(desc_idx + 1, len(row)):
                    val = row.iloc[i]

                    if pd.notna(val) and str(val).strip():
                        found.append(val)

            if len(found) >= 1:
                uom = found[0]
            if len(found) >= 2:
                qty = found[1]

            all_extracted_rows.append(
                {"description": description, "uom": uom, "qty": qty}
            )

    result_df = pd.DataFrame(all_extracted_rows)
    logger.info(f"Normalized {len(all_extracted_rows)} rows from {len(dfs)} tables")

    return result_df


def clean_dataframe(
    df: pd.DataFrame, noise_patterns: list[tuple] | None = None
) -> pd.DataFrame:
    """
    Clean the DataFrame by removing unwanted text and formatting.

    Args:
        df: DataFrame to clean
        noise_patterns: List of (pattern, flags) tuples to remove from descriptions.
                       Defaults to removing "Buyer Comments" and "-UOM:" suffixes.

    Returns:
        Cleaned DataFrame

    Example:
        >>> df = clean_dataframe(df)
        >>> print(df[['description', 'uom', 'qty']].head())
    """
    if noise_patterns is None:
        noise_patterns = DEFAULT_NOISE_PATTERNS

    # Drop rows with missing description
    df = df.dropna(subset=["description"]).copy()

    # Convert description to string
    df["description"] = df["description"].astype(str)

    # Apply noise pattern removal
    for pattern, flags in noise_patterns:
        df["description"] = df["description"].apply(
            lambda x: re.sub(pattern, "", x, flags=flags)
        )

    # Strip whitespace
    df["description"] = df["description"].str.strip()

    # Clean UOM column
    df["uom"] = df["uom"].astype(str).str.strip()
    df["uom"] = df["uom"].replace("None", "")

    # Convert qty to numeric
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    df = df.reset_index(drop=True)

    logger.debug(f"Cleaned DataFrame: {len(df)} rows")

    return df


def parse_rfq_drylog_pdf(
    path: str,
    uom_pattern: str = DEFAULT_UOM_PATTERN,
    noise_patterns: list[tuple] | None = None,
) -> pd.DataFrame:
    """
    Parse DryLog Service RFQ PDF file and extract structured data.

    This function:
    1. Extracts all tables from the PDF
    2. Normalizes tables by detecting UOM patterns dynamically
    3. Cleans descriptions and data
    4. Returns a cleaned DataFrame

    Args:
        path: Path to the PDF file
        uom_pattern: Regex pattern to identify description columns.
                    Defaults to r"-UOM:"
        noise_patterns: List of (pattern, flags) tuples to remove from descriptions

    Returns:
        Cleaned DataFrame with description, uom, and qty columns

    Example:
        >>> df = parse_rfq_drylog_pdf("rfq_drylog.pdf")
        >>> print(df.head())
    """
    # Extract tables
    dfs = extract_tables_from_pdf(path)

    if not dfs:
        logger.warning(f"No tables found in PDF: {path}")
        return pd.DataFrame()

    # Normalize tables
    df = normalize_tables(dfs, uom_pattern)

    if df.empty:
        logger.warning(f"No valid data extracted from PDF: {path}")
        return pd.DataFrame()

    # Clean DataFrame
    df = clean_dataframe(df, noise_patterns)

    logger.info(f"Parsed DryLog PDF: {len(df)} items from {path}")

    return df


def parse_rfq_drylog_pdf_to_dict(
    path: str,
    uom_pattern: str = DEFAULT_UOM_PATTERN,
    noise_patterns: list[tuple] | None = None,
) -> list[dict[str, Any]]:
    """
    Parse DryLog Service RFQ PDF and return as list of dictionaries.

    This is a convenience function that converts the DataFrame result
    to a list of dicts for easier API integration.

    Args:
        path: Path to the PDF file
        uom_pattern: Regex pattern to identify description columns
        noise_patterns: List of (pattern, flags) tuples to remove from descriptions

    Returns:
        List of dictionaries, one per row in the parsed table

    Example:
        >>> items = parse_rfq_drylog_pdf_to_dict("rfq_drylog.pdf")
        >>> for item in items:
        ...     print(f"{item['description']}: {item['qty']} {item['uom']}")
    """
    df = parse_rfq_drylog_pdf(
        path=path, uom_pattern=uom_pattern, noise_patterns=noise_patterns
    )

    # Convert NaN to None for JSON serialization
    result = df.where(pd.notna(df), None).to_dict("records")

    return result


def batch_parse_drylog_pdfs(
    file_paths: list[str],
    uom_pattern: str = DEFAULT_UOM_PATTERN,
    noise_patterns: list[tuple] | None = None,
) -> pd.DataFrame:
    """
    Parse multiple DryLog Service RFQ PDF files and combine into a single DataFrame.

    Args:
        file_paths: List of paths to PDF files
        uom_pattern: Regex pattern to identify description columns
        noise_patterns: List of (pattern, flags) tuples to remove from descriptions

    Returns:
        Combined DataFrame with data from all files

    Example:
        >>> files = ["rfq1.pdf", "rfq2.pdf", "rfq3.pdf"]
        >>> df = batch_parse_drylog_pdfs(files)
        >>> print(f"Total items: {len(df)}")
    """
    dataframes = []

    for file_path in file_paths:
        try:
            df = parse_rfq_drylog_pdf(
                path=file_path, uom_pattern=uom_pattern, noise_patterns=noise_patterns
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


def detect_uom_pattern(path: str, sample_rows: int = 5) -> str | None:
    """
    Attempt to detect the UOM pattern used in the PDF.

    This function scans the first few tables and rows to find common
    UOM indicator patterns like "-UOM:", "UOM:", "Unit:", etc.

    Args:
        path: Path to the PDF file
        sample_rows: Number of rows to sample for pattern detection

    Returns:
        Detected regex pattern, or None if detection fails

    Example:
        >>> pattern = detect_uom_pattern("rfq_drylog.pdf")
        >>> if pattern:
        ...     df = parse_rfq_drylog_pdf("rfq_drylog.pdf", uom_pattern=pattern)
    """
    import pdfplumber

    common_patterns = [
        r"-UOM:",
        r"UOM:",
        r"Unit:",
        r"-UNIT:",
        r"\[UOM\]",
    ]

    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages[:3]:
                tables = page.extract_tables()
                if not tables:
                    continue

                df = pd.DataFrame(tables[0])

                for r_idx in range(min(sample_rows, len(df))):
                    row = df.iloc[r_idx]

                    for cell_value in row:
                        if isinstance(cell_value, str):
                            for pattern in common_patterns:
                                if re.search(pattern, cell_value):
                                    logger.info(f"Detected UOM pattern: {pattern}")
                                    return pattern

    except Exception as e:
        logger.error(f"Error detecting UOM pattern for {path}: {e}", exc_info=True)

    logger.warning("Could not detect UOM pattern, using default")
    return None
