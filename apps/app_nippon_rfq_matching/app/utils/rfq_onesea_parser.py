"""
RFQ OneSea PDF Parser

Utility functions for parsing OneSea RFQ PDF files using pdfplumber.
This parser handles PDFs with table structures and includes robust column
alignment detection using shift mechanisms for misaligned data.
"""

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Default column positions for OneSea format
DEFAULT_DESCRIPTION_IDX = 4
DEFAULT_UOM_IDX = 5
DEFAULT_QTY_IDX = 6
DEFAULT_MIN_LEN = 7

# Default validation patterns
DEFAULT_QTY_PATTERN = r"^\d+(\.\d+)?$"
DEFAULT_UOM_PATTERN = r"^[A-Z]{2,5}$"

# Default filter keywords (lowercase)
DEFAULT_FILTER_KEYWORDS = [
    "office notes",
    "description",  # Header row indicator
    "qty",  # Header row indicator
]


def extract_tables_from_pdf(path: str) -> list[pd.DataFrame]:
    """
    Extract all tables from a PDF file using pdfplumber.

    Args:
        path: Path to the PDF file

    Returns:
        List of DataFrames, one for each table found

    Example:
        >>> tables = extract_tables_from_pdf("rfq_onesea.pdf")
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


def normalize_row_length(row: list, min_len: int = DEFAULT_MIN_LEN) -> list[str]:
    """
    Normalize row length by padding with empty strings or truncating.

    Args:
        row: List of cell values
        min_len: Minimum length for the row

    Returns:
        Normalized row as list of strings

    Example:
        >>> normalize_row_length(['A', 'B'], min_len=5)
        ['A', 'B', '', '', '']
    """
    row = [str(c).strip() if c is not None else "" for c in row]

    if len(row) < min_len:
        row += [""] * (min_len - len(row))

    return row


def is_valid_row(row: list, filter_keywords: list[str] | None = None) -> bool:
    """
    Check if a row is valid for parsing (not a header, noise, or empty).

    Args:
        row: List of cell values
        filter_keywords: List of keywords to filter out (lowercase).
                        Defaults to ["office notes", "description", "qty"]

    Returns:
        True if the row should be parsed, False otherwise

    Example:
        >>> is_valid_row(['1', 'Item', 'EA', '10'])
        True
        >>> is_valid_row(['Office Notes', 'Some text'])
        False
    """
    if filter_keywords is None:
        filter_keywords = DEFAULT_FILTER_KEYWORDS

    text = " ".join([str(c) for c in row]).lower()

    # Check if empty
    if not text.strip():
        return False

    # Check for filter keywords
    for keyword in filter_keywords:
        if keyword in text:
            logger.debug(f"Filtered row containing: {keyword}")
            return False

    return True


def parse_by_index(
    row: list,
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    qty_pattern: str = DEFAULT_QTY_PATTERN,
    uom_pattern: str = DEFAULT_UOM_PATTERN,
    validate_uom: bool = False,
) -> dict[str, Any] | None:
    """
    Parse a row using fixed column indices.

    Args:
        row: List of cell values (should be normalized first)
        description_idx: Index of description column
        uom_idx: Index of UOM column
        qty_idx: Index of quantity column
        qty_pattern: Regex pattern to validate quantity
        uom_pattern: Regex pattern to validate UOM
        validate_uom: Whether to strictly validate UOM format

    Returns:
        Dictionary with description, uom, and qty, or None if validation fails

    Example:
        >>> row = ['', '', '', '', 'Wrench', 'EA', '10']
        >>> parse_by_index(row)
        {'description': 'Wrench', 'uom': 'EA', 'qty': 10.0}
    """
    # Ensure row is long enough
    if len(row) <= max(description_idx, uom_idx, qty_idx):
        return None

    description = row[description_idx]
    uom = row[uom_idx]
    qty = row[qty_idx]

    # Validate quantity (must be numeric)
    if not qty or not re.match(qty_pattern, str(qty).strip()):
        return None

    # Validate UOM if enabled
    if validate_uom and uom:
        if not re.match(uom_pattern, str(uom).strip()):
            return None

    # Clean description
    description = str(description).strip() if description else ""

    if not description:
        return None

    return {
        "description": description,
        "uom": uom.strip() if uom else None,
        "qty": float(qty),
    }


def parse_with_shift(
    row: list,
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    max_shifts: int = 1,
) -> dict[str, Any] | None:
    """
    Parse a row with column shift detection for misaligned data.

    Tries multiple shift positions:
    1. No shift (original position)
    2. Shift left (columns may be offset to the right)
    3. Shift right (columns may be offset to the left)

    Args:
        row: List of cell values
        description_idx: Base index of description column
        uom_idx: Base index of UOM column
        qty_idx: Base index of quantity column
        max_shifts: Maximum number of positions to shift

    Returns:
        Dictionary with description, uom, and qty, or None if all attempts fail

    Example:
        >>> row = ['', '', '', 'Wrench', 'EA', '10', '']
        >>> parse_with_shift(row)  # Detects and handles misalignment
        {'description': 'Wrench', 'uom': 'EA', 'qty': 10.0}
    """
    row = normalize_row_length(row)

    # Try no shift first
    parsed = parse_by_index(row, description_idx, uom_idx, qty_idx)
    if parsed:
        logger.debug("Parsed row without shift")
        return parsed

    # Try shifting left (data might be offset to the right)
    for shift in range(1, max_shifts + 1):
        shifted_left = row[shift:] + [""] * shift
        parsed = parse_by_index(shifted_left, description_idx, uom_idx, qty_idx)
        if parsed:
            logger.debug(f"Parsed row with left shift of {shift}")
            return parsed

    # Try shifting right (data might be offset to the left)
    for shift in range(1, max_shifts + 1):
        shifted_right = [""] * shift + row[:-shift]
        parsed = parse_by_index(shifted_right, description_idx, uom_idx, qty_idx)
        if parsed:
            logger.debug(f"Parsed row with right shift of {shift}")
            return parsed

    return None


def parse_multiple_tables(
    dfs: list[pd.DataFrame],
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    filter_keywords: list[str] | None = None,
    max_shifts: int = 1,
) -> pd.DataFrame:
    """
    Parse multiple tables and combine results.

    Args:
        dfs: List of DataFrames from extract_tables_from_pdf
        description_idx: Index of description column
        uom_idx: Index of UOM column
        qty_idx: Index of quantity column
        filter_keywords: List of keywords to filter out
        max_shifts: Maximum number of positions to shift for alignment

    Returns:
        Combined DataFrame with all parsed items

    Example:
        >>> df = parse_multiple_tables(tables)
        >>> print(df.head())
    """
    all_results = []

    for df_idx, df in enumerate(dfs):
        for row_idx, row in df.iterrows():
            row_list = row.tolist()

            if not is_valid_row(row_list, filter_keywords):
                logger.debug(f"Table {df_idx}, row {row_idx}: Invalid row, skipping")
                continue

            parsed = parse_with_shift(
                row_list, description_idx, uom_idx, qty_idx, max_shifts
            )

            if parsed:
                all_results.append(parsed)

    result_df = pd.DataFrame(all_results)
    logger.info(f"Parsed {len(all_results)} items from {len(dfs)} tables")

    return result_df


def parse_rfq_onesea_pdf(
    path: str,
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    filter_keywords: list[str] | None = None,
    max_shifts: int = 1,
) -> pd.DataFrame:
    """
    Parse OneSea RFQ PDF file and extract structured data.

    This function:
    1. Extracts all tables from the PDF
    2. Parses each table with column shift detection
    3. Validates and filters rows
    4. Returns a cleaned DataFrame

    Args:
        path: Path to the PDF file
        description_idx: Index of description column
        uom_idx: Index of UOM column
        qty_idx: Index of quantity column
        filter_keywords: List of keywords to filter out
        max_shifts: Maximum number of positions to shift for alignment

    Returns:
        Cleaned DataFrame with description, uom, and qty columns

    Example:
        >>> df = parse_rfq_onesea_pdf("rfq_onesea.pdf")
        >>> print(df.head())
    """
    # Extract tables
    dfs = extract_tables_from_pdf(path)

    if not dfs:
        logger.warning(f"No tables found in PDF: {path}")
        return pd.DataFrame()

    # Parse multiple tables
    df = parse_multiple_tables(
        dfs, description_idx, uom_idx, qty_idx, filter_keywords, max_shifts
    )

    if df.empty:
        logger.warning(f"No valid data extracted from PDF: {path}")
        return pd.DataFrame()

    logger.info(f"Parsed OneSea PDF: {len(df)} items from {path}")

    return df


def parse_rfq_onesea_pdf_to_dict(
    path: str,
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    filter_keywords: list[str] | None = None,
    max_shifts: int = 1,
) -> list[dict[str, Any]]:
    """
    Parse OneSea RFQ PDF and return as list of dictionaries.

    This is a convenience function that converts the DataFrame result
    to a list of dicts for easier API integration.

    Args:
        path: Path to the PDF file
        description_idx: Index of description column
        uom_idx: Index of UOM column
        qty_idx: Index of quantity column
        filter_keywords: List of keywords to filter out
        max_shifts: Maximum number of positions to shift for alignment

    Returns:
        List of dictionaries, one per row in the parsed table

    Example:
        >>> items = parse_rfq_onesea_pdf_to_dict("rfq_onesea.pdf")
        >>> for item in items:
        ...     print(f"{item['description']}: {item['qty']} {item['uom']}")
    """
    df = parse_rfq_onesea_pdf(
        path=path,
        description_idx=description_idx,
        uom_idx=uom_idx,
        qty_idx=qty_idx,
        filter_keywords=filter_keywords,
        max_shifts=max_shifts,
    )

    # Convert NaN to None for JSON serialization
    result = df.where(pd.notna(df), None).to_dict("records")

    return result


def batch_parse_onesea_pdfs(
    file_paths: list[str],
    description_idx: int = DEFAULT_DESCRIPTION_IDX,
    uom_idx: int = DEFAULT_UOM_IDX,
    qty_idx: int = DEFAULT_QTY_IDX,
    filter_keywords: list[str] | None = None,
    max_shifts: int = 1,
) -> pd.DataFrame:
    """
    Parse multiple OneSea RFQ PDF files and combine into a single DataFrame.

    Args:
        file_paths: List of paths to PDF files
        description_idx: Index of description column
        uom_idx: Index of UOM column
        qty_idx: Index of quantity column
        filter_keywords: List of keywords to filter out
        max_shifts: Maximum number of positions to shift for alignment

    Returns:
        Combined DataFrame with data from all files

    Example:
        >>> files = ["rfq1.pdf", "rfq2.pdf", "rfq3.pdf"]
        >>> df = batch_parse_onesea_pdfs(files)
        >>> print(f"Total items: {len(df)}")
    """
    dataframes = []

    for file_path in file_paths:
        try:
            df = parse_rfq_onesea_pdf(
                path=file_path,
                description_idx=description_idx,
                uom_idx=uom_idx,
                qty_idx=qty_idx,
                filter_keywords=filter_keywords,
                max_shifts=max_shifts,
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


def auto_detect_columns(
    path: str, sample_rows: int = 10
) -> tuple[int, int, int] | None:
    """
    Attempt to auto-detect column positions for description, UOM, and quantity.

    This function samples rows from the PDF and tries to identify patterns
    that indicate which columns contain the relevant data.

    Args:
        path: Path to the PDF file
        sample_rows: Number of rows to sample for detection

    Returns:
        Tuple of (description_idx, uom_idx, qty_idx) or None if detection fails

    Example:
        >>> columns = auto_detect_columns("rfq_onesea.pdf")
        >>> if columns:
        ...     desc_idx, uom_idx, qty_idx = columns
        ...     df = parse_rfq_onesea_pdf("rfq_onesea.pdf", desc_idx, uom_idx, qty_idx)
    """
    dfs = extract_tables_from_pdf(path)

    if not dfs:
        return None

    # Sample rows from first few tables
    sampled_rows = []
    for df in dfs[:3]:
        for _, row in df.iterrows():
            if len(sampled_rows) >= sample_rows:
                break
            row_list = row.tolist()
            if is_valid_row(row_list):
                sampled_rows.append(normalize_row_length(row_list))

    if not sampled_rows:
        logger.warning("No valid rows found for column detection")
        return None

    # Analyze patterns to find likely column positions
    # This is a simplified heuristic - could be enhanced with ML

    for desc_idx in range(3, 7):
        for uom_idx in range(desc_idx + 1, desc_idx + 3):
            qty_idx = uom_idx + 1

            valid_count = 0
            for row in sampled_rows:
                if len(row) > qty_idx:
                    parsed = parse_by_index(row, desc_idx, uom_idx, qty_idx)
                    if parsed:
                        valid_count += 1

            # If more than 70% of rows parse successfully, use these positions
            if valid_count / len(sampled_rows) > 0.7:
                logger.info(
                    f"Auto-detected columns: desc={desc_idx}, uom={uom_idx}, qty={qty_idx}"
                )
                return (desc_idx, uom_idx, qty_idx)

    logger.warning("Could not auto-detect columns, using defaults")
    return None
