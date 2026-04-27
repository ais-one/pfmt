"""CERU Excel parser utilities.

Parses RFQ Excel files in CERU format, extracting header information
and item details from cell data dictionaries.
"""

from typing import Any

# Footer/signature keywords that indicate end of item list
FOOTER_KEYWORDS = {
    "PREPARED BY",
    "APPROVED BY",
    "CHECKED BY",
    "AUTHORISED BY",
    "RECEIVED BY",
    "REVIEWED BY",
    "SIGNATURE",
    "DATE",
    "RANK",
    "MASTER",
    "CHIEF OFFICER",
    "CAPTAIN",
}


def is_ceru_excel(cell_dict: dict[str, Any]) -> bool:
    """Check if the cell data represents a CERU format Excel file.

    Args:
        cell_dict: Dictionary mapping cell coordinates to values.

    Returns:
        True if the cell data contains CERU-specific markers.
    """
    values = [str(v).upper() for v in cell_dict.values() if v]

    has_vessel = any("CERULEAN" in v for v in values)
    has_reqn = any("CERU-" in v for v in values)
    has_section = any("REQUISITION DETAILS" in v for v in values)

    return has_vessel and has_reqn and has_section


def parse_ceru_header(cell_dict: dict[str, Any]) -> dict[str, Any]:
    """Extract header information from CERU Excel cell data.

    Args:
        cell_dict: Dictionary mapping cell coordinates to values.

    Returns:
        Dictionary containing vessel, date, requisition details, type, and priority.
    """
    return {
        "vessel": cell_dict.get("B2"),
        "date": cell_dict.get("M2"),
        "requisition_name": cell_dict.get("D11"),
        "requisition_number": cell_dict.get("D12"),
        "type": cell_dict.get("D13"),
        "priority": cell_dict.get("D14"),
    }


def get_table_header(cell_dict: dict[str, Any], header_row: int = 20) -> dict[str, str]:
    """Extract table headers from a specific row.

    Args:
        cell_dict: Dictionary mapping cell coordinates to values.
        header_row: Row number containing the headers (default: 20).

    Returns:
        Dictionary mapping column letters to header names.
    """
    header_map = {}

    for cell, value in cell_dict.items():
        col = "".join(filter(str.isalpha, str(cell)))
        row = int("".join(filter(str.isdigit, str(cell))))

        if row == header_row:
            header_map[col] = str(value).strip().upper()

    return header_map


def parse_ceru_items(
    cell_dict: dict[str, Any],
    start_row: int = 23,
    max_rows: int = 100,
) -> list[dict[str, Any]]:
    """Extract item details from CERU Excel cell data.

    Args:
        cell_dict: Dictionary mapping cell coordinates to values.
        start_row: Row number where item data starts (default: 23).
        max_rows: Maximum number of rows to parse (safety limit).

    Returns:
        List of item dictionaries containing no, item_name, present_rob,
        qty_reqd, and unit.
    """
    items = []
    row = start_row
    rows_parsed = 0

    while rows_parsed < max_rows:
        item_name = cell_dict.get(f"C{row}")
        item_no = cell_dict.get(f"B{row}")

        # Stop condition: no item name
        if not item_name:
            break

        # Stop condition: footer keywords detected
        item_name_upper = str(item_name).upper().strip()
        if any(keyword in item_name_upper for keyword in FOOTER_KEYWORDS):
            break

        # Stop condition: item number (column B) is not a number
        # Valid items have numeric sequence numbers in column B
        if item_no is not None and not isinstance(item_no, int | float):
            # Check if it looks like a number string
            if not str(item_no).isdigit():
                break

        item = {
            "no": item_no,
            "item_name": item_name,
            "present_rob": cell_dict.get(f"K{row}"),
            "qty_reqd": cell_dict.get(f"L{row}"),
            "unit": cell_dict.get(f"M{row}"),
        }

        items.append(item)
        row += 1
        rows_parsed += 1

    return items


def parse_ceru_excel(cell_dict: dict[str, Any]) -> dict[str, Any] | None:
    """Parse CERU Excel cell data into structured format.

    Args:
        cell_dict: Dictionary mapping cell coordinates to values.

    Returns:
        Dictionary with 'header' and 'items' keys, or None if not a valid
        CERU Excel file.
    """
    if not is_ceru_excel(cell_dict):
        return None

    return {
        "header": parse_ceru_header(cell_dict),
        "items": parse_ceru_items(cell_dict),
    }
