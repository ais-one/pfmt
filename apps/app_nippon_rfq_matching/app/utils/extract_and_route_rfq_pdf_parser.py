from apps.app_nippon_rfq_matching.app.utils.rfq_columbia_parser import (
    parse_rfq_columbia_pdf,
)
from apps.app_nippon_rfq_matching.app.utils.rfq_drylog_parser import (
    parse_rfq_drylog_pdf,
)
from apps.app_nippon_rfq_matching.app.utils.rfq_onesea_parser import (
    parse_rfq_onesea_pdf,
)
from apps.app_nippon_rfq_matching.app.utils.rfq_suntech_parser import (
    parse_rfq_pdf as parse_rfq_suntech_pdf,
)
from apps.app_nippon_rfq_matching.app.utils.vendor_parsing_from_pdf import (
    extract_vendor_name,
)


def handle_default(path: str):
    # No vendor-specific parser matched; caller treats None as unrouted.
    return None


def get_handler_from_buyer(buyer_name: str):
    buyer_lower = buyer_name.lower()

    for vendor, config in VENDOR_HANDLERS.items():
        for keyword in config["keywords"]:
            if keyword in buyer_lower:
                return config["handler"], vendor

    return handle_default, "unknown"


def extract_and_route(path):
    result = extract_vendor_name(path)

    if not result or "buyer" not in result:
        return None

    buyer = result["buyer"]

    handler, vendor = get_handler_from_buyer(buyer)

    print(f"Detected vendor: {vendor}")

    # Pass the file path to the handler (not the result dict)
    return handler(path)


VENDOR_HANDLERS = {
    "onesea": {"keywords": ["onesea"], "handler": parse_rfq_onesea_pdf},
    "drylog": {"keywords": ["drylog"], "handler": parse_rfq_drylog_pdf},
    "suntech": {"keywords": ["suntech"], "handler": parse_rfq_suntech_pdf},
    "columbia": {"keywords": ["columbia"], "handler": parse_rfq_columbia_pdf},
}
