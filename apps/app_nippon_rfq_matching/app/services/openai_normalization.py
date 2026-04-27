"""
OpenAI Normalization Service

Service for normalizing RFQ item descriptions using OpenAI's chat completion API.
This service uses GPT models to intelligently match RFQ descriptions to standardized product names.
"""

import json
import logging
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.database import ProductMaster
from apps.app_nippon_rfq_matching.app.utils.resilience import (
    CircuitBreakerOpenError,
    MaxRetriesExceededError,
    ResilientCallers,
)

logger = logging.getLogger(__name__)


def _strip_brand_prefix(text: str) -> str:
    """
    Strip common Nippon brand prefixes from product names.

    Args:
        text: Product name text

    Returns:
        Text with brand prefixes removed

    Examples:
        "NIPPON NEO GUARD" → "NEO GUARD"
        "NP U-MARINE FINISH" → "U-MARINE FINISH"
        "NIPPON PAINT MARINE A-MARINE" → "A-MARINE"
        "TETZSOL 500 ECO" → "TETZSOL 500 ECO" (unchanged)
    """
    if not text:
        return text

    import re

    # List of brand prefixes to strip (case-insensitive)
    # Order matters - longer prefixes first
    prefixes = [
        r"^NIPPON\s+PAINT\s+MARINE\s+",
        r"^NIPPON\s+PAINT\s+",
        r"^NIPPON\s+MARINE\s+",
        r"^NIPPON\s+",
        r"^NP\s+PAINT\s+MARINE\s+",
        r"^NP\s+MARINE\s+",
        r"^NP\s+PAINT\s+",
        r"^NP\s+",
        r"^NIPPAINT\s+",
    ]

    original = text

    # Special handling for NIPPON MARINE cases
    if text.startswith(("NIPPON MARINE ", "nippon marine ", "Nippon marine ")):
        # Only strip "NIPPON " and keep "MARINE"
        text = re.sub(r"^NIPPON\s+", "", text, flags=re.IGNORECASE)
        # Ensure "MARINE" is preserved
        if not text.startswith("MARINE "):
            text = f"MARINE {text}".strip()
    else:
        # Strip each prefix
        for prefix in prefixes:
            text = re.sub(prefix, "", text, flags=re.IGNORECASE)

    # Clean up extra whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Convert to uppercase for consistency with product master
    text = text.upper()

    logger.debug(f"Stripped brand prefix: '{original}' → '{text}'")
    return text


# Prompt template for checking if RFQ items are Nippon products (STEP 1)
CHECK_NIPPON_PRODUCT_PROMPT = """You are a product classification engine for marine paint products.

TASK: Classify RFQ descriptions as Nippon products or NOT Nippon products.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||description", "1||description")
- The "||" delimiter separates the index from the description
- In your response, include ONLY the description text (without the index and "||")
- The "before" array should contain the raw description text only

MATCHING STRATEGY:
1. Case-insensitive matching: "tetzsol" matches "TETZSOL" matches "Tetzsol"
2. Check ONLY against Nippon products list
3. Return the NORMALIZED product name WITHOUT brand prefix (preserve case from normalized version)
4. If no match found in Nippon list, return null

BRAND PREFIX STRIPPING CRITICAL RULES:
- ALWAYS remove brand prefixes from the RETURNED product name
- Common prefixes to strip: NIPPON, NP, NIPPAINT, NIPPON PAINT, NIPPON PAINT MARINE, NP MARINE, NP PAINT, NIPPON MARINE
- Examples:
  * "NIPPON NEO GUARD" → "NEO GUARD" (return normalized version)
  * "NP U-MARINE FINISH" → "U-MARINE FINISH" (return normalized version)
  * "NIPPON PAINT MARINE A-MARINE" → "A-MARINE" (return normalized version)

MATCHING RULES:
- Ignore spaces, hyphens, and underscores
- Ignore volumes (20L, 5L, 1L, 16L, 17L, 15L)
- Ignore packaging (PAIL, LTR, CAN)
- Ignore color words (WHITE, GREEN, GRAY, YELLOW, BLACK, SILVER, RED, BLUE, ORANGE, etc.)
- Ignore color codes (000, 060, 442, 355, 632, 2244, 257, 258, 4218, 8130, 7132, RAL3000, etc.)
- Ignore product type codes (STD, FC, BASE, HS, ECO, etc.) AFTER the main product name
- Ignore prefix tags like "[LI]"
- Ignore Chinese characters (环氧, 稀释剂, 固化剂, 醇酸, 高温, etc.)

IMPORTANT FOR MULTILINGUAL INPUT:
- RFQ descriptions may contain Chinese characters mixed with English product names
- Focus on the ENGLISH product name part
- Match based on the core product name, ignoring variant numbers (80, 200, 700, etc.)

CRITICAL - NORMALIZED MATCH REQUIRED:
- The reference list shows both ORIGINAL and NORMALIZED product names (format: "ORIGINAL|NORMALIZED")
- YOU MUST return ONLY the NORMALIZED name (the part after "|", without brand prefix)
- DO NOT return the original name from RFQ if it contains brand prefixes
- DO NOT invent or modify product names
- If input is "TETZSOL 200 SILVER" and reference has "TETZSOL 200 ECO|TETZSOL 200 ECO", return "TETZSOL 200 ECO"
- If input is "NIPPON NEO GUARD" and reference has "NIPPON NEO GUARD|NEO GUARD", return "NEO GUARD"

OUTPUT FORMAT (strict JSON):
{{
  "before": ["desc1", "desc2"],
  "after": ["Product Name", null],  # Use NORMALIZED name from reference list (without brand prefix)
  "is_nippon": [true, false]
}}

is_nippon: true if matched, false if null

EXAMPLES:
- "Tetzsol 500 Eco Silver" → "TETZSOL 500 ECO", true (returns normalized name)
- "Nippon U-Marine Finish 000 White" → "U-MARINE FINISH", true (strips NIPPON prefix, returns normalized)
- "Nippon Marine Thinner 700" → "MARINE THINNER 700", true (strips NIPPON prefix, returns normalized)
- "NP U-MARINE FINISH" → "U-MARINE FINISH", true (strips NP prefix, returns normalized)
- "TETZSOL 200 SILVER" → "TETZSOL 200 ECO", true (returns normalized name from reference)
- "JOTAMASTIC 80 RED A 16L" → null, false (not in Nippon list)
- "PILOT II STD 4218 GREY 20L" → null, false (not in Nippon list)

IMPORTANT:
- The NIPPO list contains original and normalized names in "original|normalized" format
- You MUST return ONLY the normalized part (after "|") without brand prefixes
- Match using case-insensitive comparison
- NEVER return a product name with brand prefixes like "NIPPON" or "NP"
- ALWAYS strip brand prefixes before returning the product name
"""


# Prompt template for normalizing Nippon product colors (STEP 2)
NIPPON_COLOR_NORMALIZATION_PROMPT = """You are a color extraction engine for Nippon marine paint products.

TASK: Extract color names from Nippon product descriptions EXACTLY as they appear in the reference list.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||description", "1||description")
- The "||" delimiter separates the index from the description
- In your response, include ONLY the description text (without the index and "||")
- The "before" array should contain the raw description text only

COLOR MATCHING RULES:
- ALL valid colors are listed in the COLORS section below (from product master table)
- Extract EXACT color from that list (case-insensitive)
- If no color match found in list, use null
- Do NOT create colors not in the list

EXTRACTION STRATEGY - EXTRACT "AS-IS" (NO PREFERENCE):
1. If the input contains a color CODE (with numbers), extract WITH the code:
   - Numbered colors: "000 WHITE", "060 GRAY", "355 SIGNAL YELLOW"

2. If the input contains ONLY a color name (no code), extract the color name:
   - "NIPPON U-MARINE FINISH WHITE" → "WHITE"
   - "TETZSOL 200 SILVER" → "SILVER"

3. DO NOT convert between numbered and base versions:
   - If input has "000 WHITE", extract "000 WHITE" (NOT "WHITE")
   - If input has "WHITE", extract "WHITE" (NOT "000 WHITE")
   - Extract EXACTLY what is in the input

OUTPUT FORMAT (strict JSON):
{{
  "before": ["desc1", "desc2"],
  "colors": ["WHITE", "SILVER", "000 WHITE", null]
}}

EXAMPLES - EXTRACT AS-IS:
- "NIPPON U-MARINE FINISH WHITE 20L" → colors:"WHITE" (no code, extract name only)
- "TETZSOL 200 SILVER" → colors:"SILVER" (no code, extract name only)
- "NIPPON A-MARINE 000 WHITE" → colors:"000 WHITE" (has code, extract with code)
- "NIPPAINT E-MARINE 400 060 GRAY" → colors:"060 GRAY" (has code, extract with code)

IMPORTANT:
- The COLORS list below contains ALL valid colors from product master table
- Match using case-insensitive comparison
- Return the color EXACTLY as it appears in the COLORS list
- Extract AS-IS from input - do NOT convert between numbered and base versions
"""


# Prompt template for competitor product normalization (STEP 3)
COMPETITOR_PRODUCT_NORMALIZATION_PROMPT = """You are a product name normalization engine for competitor marine paint products.

TASK: Match RFQ descriptions to competitor product names from the reference list.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||description", "1||description")
- The "||" delimiter separates the index from the description
- In your response, include ONLY the description text (without the index and "||")
- The "before" array should contain the raw description text only

MATCHING STRATEGY:
1. Case-insensitive matching: "pilot" matches "PILOT" matches "Pilot"
2. Check ONLY against Competitor products list
3. Return the EXACT product name from the reference list (preserve the case from the list)
4. If no match found, return null

MATCHING RULES:
- Ignore spaces, hyphens, and underscores
- Ignore volumes (20L, 5L, 1L, 16L, 17L, 15L)
- Ignore packaging (PAIL, LTR, CAN)
- Ignore color words (WHITE, GREEN, GRAY, YELLOW, BLACK, SILVER, RED, BLUE, ORANGE, etc.)
- Ignore color codes (000, 060, 442, 355, 632, 2244, 257, 258, 4218, 8130, 7132, RAL3000, etc.)
- Ignore product type codes (STD, FC, BASE, HS, ECO, etc.) AFTER the main product name
- Ignore prefix tags like "[LI]"
- Ignore Chinese characters (环氧, 稀释剂, 固化剂, 醇酸, 高温, etc.)

IMPORTANT FOR MULTILINGUAL INPUT:
- RFQ descriptions may contain Chinese characters mixed with English product names
- Focus on the ENGLISH product name part (e.g., from "环氧红底漆 JOTAMASTIC 80 RED A 16L", extract "JOTAMASTIC")
- Match based on the BASE product name (ignore variant numbers like 80, 200, 700, etc.)

CRITICAL - EXACT MATCH REQUIRED:
- You MUST return a product name that EXISTS in the competitor reference list below
- DO NOT invent or modify product names
- For competitor products: match the BASE product name (ignore variants like "80", "STD", "FC", etc.)
- If input is "JOTAMASTIC 80 RED A 16L" and reference has "JOTAMASTIC", return "JOTAMASTIC"

OUTPUT FORMAT (strict JSON):
{{
  "before": ["desc1", "desc2"],
  "after": ["Product Name", "Product Name"],
  "types": ["competitor", null]
}}

types: "competitor" | null (no match)

EXAMPLES:
- "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L" → matches "PENGUARD" from COMP list → "PENGUARD", "competitor"
- "环氧红底漆 JOTAMASTIC 80 RED A 16L" → matches "JOTAMASTIC" from COMP list → "JOTAMASTIC", "competitor"
- "信号红 PILOT II RAL3000 5L" → matches "PILOT II" from COMP list → "PILOT II", "competitor"
- "醇酸灰面漆 PILOT II STD 4218 GREY 20L" → matches "PILOT II" from COMP list → "PILOT II", "competitor"
- "TETZSOL 200 SILVER" → null, null (not in competitor list - this is a Nippon product)

IMPORTANT:
- The COMP list below contains ALL valid competitor products
- Match using case-insensitive comparison
- Return the product name EXACTLY as it appears in the reference list
- NEVER return a product name that doesn't exist in the reference list
- For competitor products: match the BASE product name (ignore variants like "80", "STD", "FC", etc.)
"""  # noqa: E501


# Prompt template for competitor color normalization (STEP 4)
COMPETITOR_COLOR_NORMALIZATION_PROMPT = """You are a color extraction engine for competitor marine paint products.

TASK: Extract competitor color codes and map them to NPMS color codes from the reference data.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||description", "1||description")
- The "||" delimiter separates the index from the description
- In your response, include ONLY the description text (without the index and "||")
- The "before" array should contain the raw description text only

COLOR MATCHING RULES:
- The COMPETITOR_COLORS section below contains competitor color mapping data
- Each entry has format: "number | competitor_color_code | npms_code" or "competitor_color_code | npms_code"
- Extract the competitor color code from input and match it to get the NPMS code
- If no match found, extract any color information as-is

EXTRACTION STRATEGY:
1. Try to match competitor color code from the reference data
2. If found, return the NPMS code
3. If not found, extract color as-is from input

OUTPUT FORMAT (strict JSON):
{{
  "before": ["PILOT II 4218 GREY 20L", "Jotun- Pilot II- Green 137"],
  "colors": ["4218 GREY", "GREEN 137"],
  "source_codes": ["4218 GREY", "GREEN 137"],
}}

EXAMPLES:
- "PILOT II 4218 GREY 20L" → colors:"4218 GREY"
- "JOTAMASTIC 80 RED A 16L" → colors:"80 RED"
- "PILOT II RAL3000 5L" → colors:"RAL3000"
- "Jotun- Pilot II- Green 137" → colors:"GREEN 137"
- "Jotun- Pilot II- Grey 38" → colors:"GREY 38"
- "Jotun- Penguard FC STD 038- Grey" → colors:"GREY 038"
- "Jotun- Jotafix FU TC STD 137- Green 137" → colors:"GREEN 137"

IMPORTANT:
- The COMPETITOR_COLORS section contains all valid color mappings
- Match using case-insensitive comparison
- Return the color code EXACTLY as it appears in the reference or input
"""


# Legacy prompt for backward compatibility (combined product + color)
NORMALIZATION_SYSTEM_PROMPT = (
    CHECK_NIPPON_PRODUCT_PROMPT
    + """

"""
    + NIPPON_COLOR_NORMALIZATION_PROMPT
    + """

"""
    + COMPETITOR_PRODUCT_NORMALIZATION_PROMPT
    + """

"""
    + COMPETITOR_COLOR_NORMALIZATION_PROMPT
)


# Prompt template for competitor color PDF row normalization (special case)
COMPETITOR_COLOR_PDF_ROW_NORMALIZATION_PROMPT = """You are a color data extraction engine for competitor color comparison tables.

TASK: Extract and normalize color codes from raw PDF table rows.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||raw_text", "1||raw_text")
- The "||" delimiter separates the index from the raw text
- In your response, include ONLY the raw text (without the index and "||")
- The "before" array should contain the raw text only

EXTRACTION STRATEGY:
1. Parse the raw text which may contain: "item_no | competitor_color_code | npms_code" or similar formats
2. Extract the competitor color code (middle column)
3. Extract the NPMS color code (right column)
4. Handle various separators: "|", "   ", "\t", etc.

COMMON PATTERNS:
- "28 | Grey 403 | RAL7038" → source_code: "Grey 403", npms_code: "RAL7038"
- "45 | Red 123 | Y538" → source_code: "Red 123", npms_code: "Y538"
- "12 | CS-625 SIGNAL RED | 537 SIGNAL RED" → source_code: "CS-625 SIGNAL RED", npms_code: "537 SIGNAL RED"
- "100 | White | WHITE" → source_code: "White", npms_code: "WHITE"

COLOR CODE FORMATS:
- Jotun codes: "Grey 403", "Red 123", "CS-625 SIGNAL RED"
- RAL codes: "RAL7038", "RAL3000", "RAL 5022"
- NCS codes: "NCS S 2080-R", "NCS 1580-B"
- NPMS codes: "Y538", "537 SIGNAL RED", "060 GRAY"

OUTPUT FORMAT (strict JSON):
{{
  "before": ["28 | Grey 403 | RAL7038", "45 | Red 123 | Y538"],
  "source_codes": ["Grey 403", "Red 123"],
  "npms_codes": ["RAL7038", "Y538"],
  "item_numbers": [28, 45]
}}

EXAMPLES:
- "28 | Grey 403 | RAL7038" → source_codes: "Grey 403", npms_codes: "RAL7038", item_numbers: 28
- "45 | Red 123 | Y538" → source_codes: "Red 123", npms_codes: "Y538", item_numbers: 45
- "12 | CS-625 SIGNAL RED | 537 SIGNAL RED" → source_codes: "CS-625 SIGNAL RED", npms_codes: "537 SIGNAL RED", item_numbers: 12
- "100 | White | WHITE" → source_codes: "White", npms_codes: "WHITE", item_numbers: 100

IMPORTANT:
- Extract the competitor color code EXACTLY as it appears (preserve case and spacing)
- Extract the NPMS color code EXACTLY as it appears
- Extract item numbers as integers
- Handle various separator formats (|, tabs, spaces)
- If a field is missing or unclear, use null
"""  # noqa: E501


# Prompt template for color extraction (separate from product name)
COLOR_EXTRACTION_PROMPT = """You are a color extraction engine for marine paint products.

TASK: Extract color names and codes from product descriptions EXACTLY as they appear.

INPUT FORMAT: Items are numbered with "||" delimiter (e.g., "0||description", "1||description")
- The "||" delimiter separates the index from the description
- In your response, include ONLY the description text (without the index and "||")
- The "before" array should contain the raw description text only

COLOR MATCHING RULES:
- ALL valid colors are listed in the COLORS section below
- Extract EXACT color from that list (case-insensitive)
- If no color match found in list, use null
- Do NOT create colors not in the list

EXTRACTION STRATEGY - EXTRACT "AS-IS" (NO PREFERENCE):
1. If the input contains a color CODE (with numbers), extract WITH the code:
   - Competitor color codes: "80 RED", "4218 GREY", "8130 YELLOW", "CS-625 SIGNAL RED"
   - RAL codes: "RAL3000", "RAL9010"
   - Numbered colors: "000 WHITE", "060 GRAY", "355 SIGNAL YELLOW"

2. If the input contains ONLY a color name (no code), extract the color name:
   - "PILOT II WHITE 20L" → "WHITE"
   - "TETZSOL 200 SILVER" → "SILVER"
   - "NOA 60 HS BUFF" → "BUFF"

3. DO NOT convert between numbered and base versions:
   - If input has "000 WHITE", extract "000 WHITE" (NOT "WHITE")
   - If input has "WHITE", extract "WHITE" (NOT "000 WHITE")
   - If input has "060 GRAY", extract "060 GRAY" (NOT "GRAY")
   - Extract EXACTLY what is in the input

OUTPUT FORMAT (strict JSON):
{{
  "before": ["desc1", "desc2"],
  "colors": ["WHITE", "SILVER", "80 RED", "RAL3000", "000 WHITE", null]
}}

EXAMPLES - EXTRACT AS-IS:
- "PILOT II WHITE 20L" → colors:"WHITE" (no code, extract name only)
- "TETZSOL 200 SILVER" → colors:"SILVER" (no code, extract name only)
- "A-MARINE 000 WHITE" → colors:"000 WHITE" (has code, extract with code)
- "JOTAMASTIC 80 RED A 16L" → colors:"80 RED" (competitor code, extract with code)
- "PILOT II RAL3000 5L" → colors:"RAL3000" (RAL code, extract with code)
- "PILOT II STD 4218 GREY 20L" → colors:"4218 GREY" (competitor code, extract with code)
- "NIPPAINT E-MARINE 400 060 GRAY" → colors:"060 GRAY" (has code, extract with code)
- "Nippon U-Marine Finish White" → colors:"WHITE" (no code, extract name only)
- "Jotun- Pilot II- Green 137" => Green 137
- "Jotun- Pilot II- White" => White
- "Jotun- Pilot II- Grey 38" => Grey 038 or Grey 38
- "Jotun- Jotafix FU TC STD 137- Green" => Green 137 or 137 Green
- "Jotun- Penguard FC STD 038- Grey" => Grey 038 or 038 Grey
- "Jotun- Penguard FC 137" => null
- "Jotun- Penguard FC Green 574" => Green 574
- "Jotun- Pioneer TC Black" => Black
- "Jotun- Pioneer TC Yellow", => Yellow
- "Jotun- Pioneer TC Red Ral 3000" => Red or  Ral 3000
- "Jotun- Thinner no. 17" => null
- "Jotun- Thinner no. 10" => null
- "Jotun- Thinner no. 7" => null
- "Jotun- Thinner no. 2 => null

IMPORTANT:
- The COLORS list below contains ALL valid colors
- Match using case-insensitive comparison
- Return the color EXACTLY as it appears in the COLORS list
- Extract AS-IS from input - do NOT convert between numbered and base versions
"""


# Legacy prompt for backward compatibility (combined product + color for old methods)
# This combines Nippon and Competitor product name normalization
PRODUCT_NAME_NORMALIZATION_PROMPT = (
    CHECK_NIPPON_PRODUCT_PROMPT
    + """

"""
    + COMPETITOR_PRODUCT_NORMALIZATION_PROMPT
)


# Legacy prompt for backward compatibility (combined product + color)
NORMALIZATION_SYSTEM_PROMPT = (
    PRODUCT_NAME_NORMALIZATION_PROMPT
    + """

"""
    + COLOR_EXTRACTION_PROMPT
)


class OpenAINormalizationService:
    """
    OpenAI-based normalization service using chat completion API.

    This service uses GPT models to normalize RFQ descriptions to standardized
    product names from the product master and competitor products.
    """

    def __init__(self):
        """Initialize OpenAI normalization service."""
        self.enabled = OPENAI_AVAILABLE and bool(settings.OPENAI_API_KEY)
        self.model = getattr(settings, "OPENAI_CHAT_MODEL", "gpt-4o-mini")
        self.temperature = getattr(settings, "OPENAI_TEMPERATURE", 0.0)
        self.max_tokens = getattr(settings, "OPENAI_MAX_TOKENS", 4000)

        # In-memory cache for normalization results
        self._memory_cache: dict[str, dict[str, Any]] = {}
        self._memory_cache_max_size = 1000
        self._cache_stats = {"hits": 0, "misses": 0, "api_calls": 0}

        if self.enabled:
            # Initialize OpenAI client (retry handled by resilient caller)
            self.client = OpenAI(
                api_key=settings.OPENAI_API_KEY,
                timeout=30.0,  # 30 second timeout
            )
            # Get resilient caller for OpenAI API calls
            self.resilient_caller = ResilientCallers.get_openai_normalization()
            logger.info(
                f"OpenAI normalization service initialized with model: {self.model}"
            )
            logger.info("Resilient caller enabled: circuit breaker + retry mechanism")
        else:
            self.client = None
            self.resilient_caller = None
            logger.warning(
                "OpenAI normalization service disabled (OPENAI_API_KEY not set or openai not installed)"
            )

    def _get_reference_products(self, db: Session) -> dict[str, list[str]]:
        """
        Get reference products and colors from database for the prompt.

        Args:
            db: Database session

        Returns:
            Dictionary with 'nippon', 'competitor', 'colors', and 'competitor_colors' lists
        """
        # Get Nippon products
        nippon_products = db.query(ProductMaster.product_name).distinct().all()
        nippon_list = [p[0] for p in nippon_products if p[0]]

        # Create normalized versions of Nippon products (strip brand prefixes)
        nippon_normalized_list = []
        nippon_reference_list = []

        for product_name in nippon_list:
            # Strip brand prefix from normalized version
            normalized_name = _strip_brand_prefix(product_name)
            nippon_normalized_list.append(normalized_name)

            # Add both original and normalized to reference list for AI
            nippon_reference_list.append(f"{product_name}|{normalized_name}")

        # Get competitor products
        from apps.app_nippon_rfq_matching.app.models.competitor import CompetitorProduct

        competitor_products = db.query(CompetitorProduct.name).distinct().all()
        competitor_list = [p[0] for p in competitor_products if p[0]]

        # Get distinct colors from product_master
        colors = (
            db.query(ProductMaster.color)
            .filter(ProductMaster.color.isnot(None))
            .distinct()
            .all()
        )
        color_list = sorted([c[0] for c in colors if c[0] and c[0].strip()])

        # Get competitor color mappings from competitor_color_comparison table
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorColorComparison,
        )

        competitor_colors = (
            db.query(
                CompetitorColorComparison.raw_text,
                CompetitorColorComparison.source_code,
                CompetitorColorComparison.npms_code,
            )
            .filter(CompetitorColorComparison.raw_text.isnot(None))
            .all()
        )

        # Format competitor colors as "raw_text" or "source_code | npms_code"
        competitor_color_list = []
        for cc in competitor_colors:
            if cc.source_code and cc.npms_code:
                competitor_color_list.append(cc.source_code)

        return {
            "nippon": nippon_reference_list,  # Now contains "original|normalized" format
            "nippon_normalized": nippon_normalized_list,  # Just normalized names for AI reference
            "nippon_original": nippon_list,  # Original names from DB
            "competitor": competitor_list,
            "colors": color_list,
            "competitor_colors": competitor_color_list,
        }

    def _get_nippon_equivalent(
        self, competitor_product_name: str, db: Session
    ) -> str | None:
        """
        Get Nippon equivalent name for a competitor product.

        Args:
            competitor_product_name: Competitor product name
            db: Database session

        Returns:
            Nippon equivalent product name or None if not found
        """
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorProduct,
            ProductEquivalent,
        )

        competitor_product = (
            db.query(CompetitorProduct)
            .filter(CompetitorProduct.name == competitor_product_name)
            .first()
        )

        if competitor_product:
            equivalent = (
                db.query(ProductEquivalent)
                .filter(
                    ProductEquivalent.competitor_product_id == competitor_product.id
                )
                .first()
            )

            if equivalent:
                return equivalent.nippon_product_name

        return None

    def _get_competitor_brand(
        self, competitor_product_name: str, db: Session
    ) -> str | None:
        """
        Get brand name for a competitor product (JOTUN, INTERNATIONAL, etc.).

        Args:
            competitor_product_name: Competitor product name
            db: Database session

        Returns:
            Brand name in uppercase or None if not found
        """
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorProduct,
        )

        competitor_product = (
            db.query(CompetitorProduct)
            .filter(CompetitorProduct.name == competitor_product_name)
            .first()
        )

        if competitor_product and competitor_product.brand:
            return competitor_product.brand.name.upper()

        return None

    def _get_npms_color_code(
        self, source_brand: str, source_color_code: str, db: Session
    ) -> str | None:
        """
        Map competitor color code to NPMS color code using CompetitorColorComparison table.

        Args:
            source_brand: Competitor brand name (e.g., "JOTUN", "INTERNATIONAL")
            source_color_code: Competitor color code (e.g., "12345", "NCS-001")
            db: Database session

        Returns:
            NPMS color code or None if not found
        """
        from apps.app_nippon_rfq_matching.app.models.competitor import (
            CompetitorColorComparison,
        )

        logger.info(f"source_brand[DEBUG_get_npms_color_code]: {source_brand}")
        logger.info(
            f"source_color_code[DEBUG_get_npms_color_code]: {source_color_code}"
        )

        # Try exact match first
        color_mapping = (
            db.query(CompetitorColorComparison)
            .filter(
                CompetitorColorComparison.source_brand == source_brand,
                CompetitorColorComparison.source_code == source_color_code,
            )
            .first()
        )

        if color_mapping and color_mapping.npms_code:
            logger.info(f"color_mapping[DEBUG_get_npms_color_code]: {color_mapping}")
            logger.info(
                f"npms_code[DEBUG_get_npms_color_code]: {color_mapping.npms_code}"
            )
            return color_mapping.npms_code

        logger.info("Try case-insensitive match _get_npms_color_code")

        # Try case-insensitive match
        color_mapping = (
            db.query(CompetitorColorComparison)
            .filter(
                CompetitorColorComparison.source_brand.ilike(f"%{source_brand}%"),
                CompetitorColorComparison.raw_text.ilike(f"%{source_color_code}%"),
            )
            .first()
        )
        logger.info(
            f"color_mapping case-insensitive match[DEBUG_get_npms_color_code]: {color_mapping}"
        )
        if color_mapping and color_mapping.npms_code:
            logger.info("if color_mapping and color_mapping.npms_code")
            logger.info(f"color_mapping[DEBUG_get_npms_color_code]: {color_mapping}")
            logger.info(
                f"npms_code[DEBUG_get_npms_color_code]: {color_mapping.npms_code}"
            )
            return color_mapping.npms_code

        return None

    def _extract_color_code_from_text(self, text: str) -> str | None:
        """
        Extract competitor color code from text.

        Examples:
        - "JOTUN GALVOSIL 157 JOTUN COLOR 12345" → "12345"
        - "INTERNATIONAL PAINT NCS-001" → "NCS-001"
        - "JOTUN 12345 RED" → "12345"
        - "PENGUARD FC STD 2244 REDBROEN 17L" → "2244 REDBROEN"
        - "PILOT II RAL3000 5L" → "RAL3000"

        Args:
            text: Input text to extract color code from

        Returns:
            Color code or None
        """
        import re

        if not text:
            return None

        text_upper = text.upper()

        # Pattern 1: Brand + COLOR + code (e.g., "JOTUN COLOR 12345")
        pattern1 = r"(?:JOTUN|INTERNATIONAL|SIGMA|HEMPEL|PPG)\s+COLOR\s+([A-Z0-9\-]+)"
        match = re.search(pattern1, text_upper)
        if match:
            return match.group(1)

        # Pattern 2: RAL color codes (e.g., "RAL3000", "RAL 3000")
        pattern2 = r"\bRAL\s*(\d{4})\b"
        match = re.search(pattern2, text_upper)
        if match:
            return f"RAL{match.group(1)}"

        # Pattern 3: Number + color name combination (e.g., "2244 REDBROEN", "4218 GREY")
        # Match pattern like XXXX COLORNAME where XXXX is 4 digits
        pattern3 = r"\b(\d{4})\s+([A-Z\s]+?)(?:\s+\d+[Ll]|\s*$|,)"
        match = re.search(pattern3, text_upper)
        if match:
            return f"{match.group(1)} {match.group(2).strip()}"

        # Pattern 4: Alphanumeric code at end (e.g., "NCS-001", "12345")
        pattern4 = r"\b([A-Z]{0,3}[\-0-9]{3,})\b$"
        match = re.search(pattern4, text_upper)
        if match:
            return match.group(1)

        # Pattern 5: Standalone number code (e.g., "12345")
        pattern5 = r"\b(\d{4,})\b"
        matches = re.findall(pattern5, text_upper)
        if matches:
            # Return the last number (likely the color code)
            return matches[-1]

        return None

    def _get_colors_for_product(self, product_name: str, db: Session) -> list[str]:
        """
        Get available colors for a specific product from database.

        Args:
            product_name: Product name to get colors for
            db: Database session

        Returns:
            List of available colors for this product
        """
        colors = (
            db.query(ProductMaster.color)
            .filter(
                ProductMaster.product_name == product_name,
                ProductMaster.color.isnot(None),
                ProductMaster.color != "",
            )
            .distinct()
            .all()
        )

        return sorted([c[0] for c in colors if c[0] and c[0].strip()])

    def _build_prompt(
        self, rfq_descriptions: list[str], reference_products: dict[str, list[str]]
    ) -> str:
        """
        Build compact user prompt with RFQ descriptions and reference data.

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            reference_products: Dictionary with nippon, competitor, and colors lists

        Returns:
            Compact formatted prompt string
        """
        # Build compact reference section (comma-separated to save tokens)
        ref_parts = []

        # Nippon products - send ALL products for exact matching
        nippon_count = len(reference_products["nippon"])
        ref_parts.append(
            f"NIPPO({nippon_count}): {', '.join(reference_products['nippon'])}"
        )

        # Competitor products - send ALL products for exact matching
        comp_count = len(reference_products["competitor"])
        ref_parts.append(
            f"COMP({comp_count}): {', '.join(reference_products['competitor'])}"
        )

        # Colors - show ALL distinct colors (not just samples)
        colors = reference_products.get("colors", [])
        colors_count = len(colors)
        if colors_count > 0:
            # Show ALL colors - they are short and important for exact matching
            ref_parts.append(f"COLORS: {', '.join(colors)}")

        # RFQ items to normalize
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Combine into compact prompt
        prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Normalize to JSON."""
        return prompt

    def _get_cache_key(self, text: str) -> str:
        """Generate cache key from text (normalized)"""
        return text.strip().lower()

    def _get_from_memory_cache(self, cache_key: str) -> dict[str, Any] | None:
        """Get from in-memory cache"""
        result = self._memory_cache.get(cache_key)
        if result:
            self._cache_stats["hits"] += 1
        return result

    def _store_in_memory_cache(self, cache_key: str, value: dict[str, Any]):
        """Store in in-memory cache with LRU eviction"""
        if len(self._memory_cache) >= self._memory_cache_max_size:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self._memory_cache))
            del self._memory_cache[oldest_key]
        self._memory_cache[cache_key] = value

    def _get_from_db_cache(self, raw_text: str, db: Session) -> dict[str, Any] | None:
        """Get normalization result from database cache"""
        from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache

        cache_entry = (
            db.query(NormalizationCache)
            .filter(NormalizationCache.raw_text == raw_text.strip())
            .first()
        )

        if cache_entry:
            # Update usage stats
            cache_entry.times_used += 1
            db.commit()

            return {
                "normalized": cache_entry.normalized_text,
                "color": cache_entry.normalized_color,
                "type": cache_entry.product_type,
                "confidence": cache_entry.match_confidence,
                "source": "db_cache",
            }
        return None

    def _store_in_db_cache(
        self,
        raw_text: str,
        normalized_text: str | None,
        product_type: str | None,
        db: Session,
        normalized_color: str | None = None,
    ):
        """Store normalization result in database cache with embeddings and color"""
        from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache

        # Generate embeddings in background (non-blocking)
        embedding_model_name = "all-MiniLM-L6-v2"
        embeddings = None

        # Try to generate embeddings (non-fatal if fails)
        try:
            from apps.app_nippon_rfq_matching.app.services.embeddings import (
                ensure_embeddings_for_cache,
            )

            embeddings = ensure_embeddings_for_cache(
                raw_text.strip(), normalized_text, embedding_model_name
            )
            if embeddings.get("raw_text_embedding"):
                logger.debug(f"Generated embeddings for: '{raw_text[:50]}...'")
        except Exception as e:
            logger.warning(f"Failed to generate embeddings (non-critical): {e}")
            embeddings = None

        cache_entry = NormalizationCache(
            raw_text=raw_text.strip(),
            normalized_text=normalized_text,
            normalized_color=normalized_color,
            product_type=product_type,
        )

        # Add embeddings if available
        if embeddings:
            cache_entry.raw_text_embedding = embeddings.get("raw_text_embedding")
            cache_entry.normalized_text_embedding = embeddings.get(
                "normalized_text_embedding"
            )
            cache_entry.embedding_model = embedding_model_name

        try:
            db.add(cache_entry)
            db.commit()
            logger.debug(
                f"Cached normalization: '{raw_text}' -> '{normalized_text}' (color: {normalized_color})"
            )
        except IntegrityError:
            # Already exists, update it
            db.rollback()
            existing = (
                db.query(NormalizationCache)
                .filter(NormalizationCache.raw_text == raw_text.strip())
                .first()
            )
            if existing:
                existing.normalized_text = normalized_text
                existing.normalized_color = normalized_color
                existing.product_type = product_type
                existing.times_used += 1
                # Update embeddings if they weren't set before
                if embeddings and not existing.raw_text_embedding:
                    existing.raw_text_embedding = embeddings.get("raw_text_embedding")
                    existing.normalized_text_embedding = embeddings.get(
                        "normalized_text_embedding"
                    )
                    existing.embedding_model = embedding_model_name
                db.commit()

    def _call_openai_api(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        Call OpenAI API for normalization (internal method).

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with normalized results
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(
            f"Loaded {len(reference_products['nippon'])} Nippon products, "
            f"{len(reference_products['competitor'])} competitor products, "
            f"{len(reference_products.get('colors', []))} colors"
        )

        # Log colors available for matching
        colors_list = reference_products.get("colors", [])
        if colors_list:
            logger.info("=" * 80)
            logger.info(f"COLORS AVAILABLE FOR MATCHING ({len(colors_list)} total):")
            # Show colors in groups of 20 for readability
            for i in range(0, min(len(colors_list), 100), 20):
                color_group = colors_list[i : i + 20]
                logger.info(
                    f"  {i + 1}-{min(i + 20, len(colors_list))}: {', '.join(color_group)}"
                )
            if len(colors_list) > 100:
                logger.info(f"  ... and {len(colors_list) - 100} more colors")
            logger.info("=" * 80)

        # Log competitor products for debugging
        logger.info("=" * 80)
        logger.info("COMPETITOR PRODUCTS AVAILABLE FOR MATCHING:")
        for product in reference_products["competitor"][:20]:  # Show first 20
            logger.info(f"  - {product}")
        if len(reference_products["competitor"]) > 20:
            logger.info(f"  ... and {len(reference_products['competitor']) - 20} more")
        logger.info("=" * 80)

        # Log Nippon products for debugging
        logger.info("=" * 80)
        logger.info("NIPPON PRODUCTS AVAILABLE FOR MATCHING (showing first 30):")
        for product in reference_products["nippon"][:30]:  # Show first 30
            logger.info(f"  - {product}")
        if len(reference_products["nippon"]) > 30:
            logger.info(f"  ... and {len(reference_products['nippon']) - 30} more")
        logger.info("=" * 80)

        # Build prompt
        user_prompt = self._build_prompt(rfq_descriptions, reference_products)

        # Log the full prompt being sent to OpenAI
        logger.info("=" * 80)
        logger.info("OPENAI API REQUEST - FULL PROMPT")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(NORMALIZATION_SYSTEM_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": NORMALIZATION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller (circuit breaker + retry)
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except CircuitBreakerOpenError as e:
            logger.error(f"Circuit breaker is OPEN: {e}")
            raise ValueError(
                f"OpenAI service is temporarily unavailable due to repeated failures. Please try again later. Details: "
                f"{e}"
            )
        except MaxRetriesExceededError as e:
            logger.error(f"Max retries exceeded: {e}")
            raise ConnectionError(
                f"Failed to connect to OpenAI after multiple attempts: {e}"
            )
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("OPENAI API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info(
            f"Estimated cost: ${usage.total_tokens / 1000000 * 0.15:.4f} (gpt-4o-mini)"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result or "after" not in result:
            raise ValueError(
                "Invalid response format: missing 'before' or 'after' keys"
            )

        if "types" not in result:
            # If "types" is missing, initialize with None values
            result["types"] = [None] * len(result["after"])

        if "colors" not in result:
            # If "colors" is missing, initialize with None values
            result["colors"] = [None] * len(result["after"])

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        if len(result["after"]) != len(rfq_descriptions):
            raise ValueError(
                f"After array length mismatch: expected {len(rfq_descriptions)}, got {len(result['after'])}"
            )

        if len(result["types"]) != len(rfq_descriptions):
            raise ValueError(
                f"Types array length mismatch: expected {len(rfq_descriptions)}, got {len(result['types'])}"
            )

        if len(result["colors"]) != len(rfq_descriptions):
            raise ValueError(
                f"Colors array length mismatch: expected {len(rfq_descriptions)}, got {len(result['colors'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("PARSED RESULTS FROM OPENAI:")
        logger.info("=" * 80)
        for i, (before, after, color, prod_type) in enumerate(
            zip(
                result["before"],
                result["after"],
                result.get("colors", [None] * len(result["after"])),
                result["types"],
            ),
            1,
        ):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   AFTER:  {after}")
            logger.info(f"   COLOR:  {color}")
            logger.info(f"   TYPE:   {prod_type}")
        logger.info("=" * 80)

        return result

    def _normalize_product_names_only(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        Normalize product names only (preserves model numbers like 700, 100, 500).

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with normalized product names:
            {
                "before": [...],
                "after": [...],
                "types": [...]
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(
            f"Normalizing product names for {len(rfq_descriptions)} items (model numbers preserved)"
        )

        # Build prompt for product names only
        ref_parts = []

        # Nippon products - send ALL products for exact matching
        nippon_count = len(reference_products["nippon"])
        ref_parts.append(
            f"NIPPO({nippon_count}): {', '.join(reference_products['nippon'])}"
        )

        # Competitor products - send ALL products for exact matching
        comp_count = len(reference_products["competitor"])
        ref_parts.append(
            f"COMP({comp_count}): {', '.join(reference_products['competitor'])}"
        )

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Normalize to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("PRODUCT NAME NORMALIZATION - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(PRODUCT_NAME_NORMALIZATION_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": PRODUCT_NAME_NORMALIZATION_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller (circuit breaker + retry)
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except CircuitBreakerOpenError as e:
            logger.error(f"Circuit breaker is OPEN: {e}")
            raise ValueError(
                f"OpenAI service is temporarily unavailable due to repeated failures. Please try again later. Details: "
                f"{e}"
            )
        except MaxRetriesExceededError as e:
            logger.error(f"Max retries exceeded: {e}")
            raise ConnectionError(
                f"Failed to connect to OpenAI after multiple attempts: {e}"
            )
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("PRODUCT NAME NORMALIZATION - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result or "after" not in result:
            raise ValueError(
                "Invalid response format: missing 'before' or 'after' keys"
            )

        if "types" not in result:
            result["types"] = [None] * len(result["after"])

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("PRODUCT NAME NORMALIZATION - RESULTS")
        logger.info("=" * 80)
        for i, (before, after, prod_type) in enumerate(
            zip(result["before"], result["after"], result["types"]), 1
        ):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   AFTER:  {after}")
            logger.info(f"   TYPE:   {prod_type}")
        logger.info("=" * 80)

        return result

    def _extract_colors_only(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        Extract colors only (separate from product name normalization).

        Args:
            rfq_descriptions: List of RFQ descriptions to extract colors from
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with extracted colors:
            {
                "before": [...],
                "colors": [...]
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(f"Extracting colors for {len(rfq_descriptions)} items")

        # Build prompt for color extraction only
        # Get colors - show ALL colors for exact matching
        colors = reference_products.get("colors", [])
        colors_count = len(colors)

        ref_parts = []
        if colors_count > 0:
            # Show ALL colors - they are short and important for exact matching
            ref_parts.append(f"COLORS({colors_count}): {', '.join(colors)}")

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Extract colors to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("COLOR EXTRACTION - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(COLOR_EXTRACTION_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": COLOR_EXTRACTION_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller (circuit breaker + retry)
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except CircuitBreakerOpenError as e:
            logger.error(f"Circuit breaker is OPEN: {e}")
            raise ValueError(
                f"OpenAI service is temporarily unavailable due to repeated failures. Please try again later. Details: "
                f"{e}"
            )
        except MaxRetriesExceededError as e:
            logger.error(f"Max retries exceeded: {e}")
            raise ConnectionError(
                f"Failed to connect to OpenAI after multiple attempts: {e}"
            )
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("COLOR EXTRACTION - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result:
            raise ValueError("Invalid response format: missing 'before' key")

        if "colors" not in result:
            result["colors"] = [None] * len(result.get("before", []))

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # NO POST-PROCESSING - extract colors as-is from OpenAI
        # (do NOT convert between numbered and base versions)

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Apply fallback color extraction for any None results
        logger.info("Applying fallback color extraction for None results...")
        for i, (before, color) in enumerate(zip(result["before"], result["colors"])):
            if color is None and before:
                fallback_color = self._fallback_color_extraction(before)
                if fallback_color:
                    result["colors"][i] = fallback_color
                    logger.info(f"Fallback extracted: '{before}' → '{fallback_color}'")

        # Log parsed results
        logger.info("=" * 80)
        logger.info("COLOR EXTRACTION - RESULTS (WITH FALLBACK)")
        logger.info("=" * 80)
        for i, (before, color) in enumerate(zip(result["before"], result["colors"]), 1):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   COLOR:  {color}")
            if color is None:
                logger.info("   STATUS: No color extracted")
        logger.info("=" * 80)

        return result

    def _fallback_color_extraction(self, text: str) -> str | None:
        """
        Fallback method to extract color codes from text when OpenAI fails.

        This handles cases like "Pilot 2 Green 574" -> "Green 574"

        Args:
            text: Input text to extract color from

        Returns:
            Color code or None
        """
        if not text:
            return None

        import re

        # Convert to uppercase for consistency
        text_upper = text.upper()

        # Pattern for "COLOR NUMBER" format (e.g., "GREEN 574", "BLUE 123")
        # This matches patterns like "PILOT 2 GREEN 574" → "GREEN 574"
        color_pattern = r"\b([A-Z]+\s+\d{3})\b"

        # Look for color patterns in the text
        matches = re.findall(color_pattern, text_upper)

        if matches:
            # Return the first match found
            return matches[0]

        # Additional patterns for other color formats
        # Pattern for "NUMBER COLOR" (e.g., "574 GREEN")
        num_color_pattern = r"\b(\d{3}\s+[A-Z]+)\b"
        num_matches = re.findall(num_color_pattern, text_upper)

        if num_matches:
            return num_matches[0]

        # Try existing regex patterns from _extract_color_code_from_text
        # Pattern 1: Number + color name combination (3 or 4 digits)
        pattern3 = r"\b(\d{3,4})\s+([A-Z\s]+?)(?:\s+\d+[Ll]|\s*$|,)"
        match = re.search(pattern3, text_upper)
        if match:
            return f"{match.group(1)} {match.group(2).strip()}"

        # Pattern 2: Standalone number code (e.g., "80", "123")
        pattern5 = r"\b(\d{3,})\b"
        matches = re.findall(pattern5, text_upper)
        if matches:
            # Return the last number (likely the color code)
            return matches[-1]

        return None

    def _prefer_numbered_color(
        self, color: str | None, available_colors: list[str]
    ) -> str | None:
        """
        Return color as-is (no conversion between numbered and base versions).

        Color extraction is now "as-is":
        - If input has "80 RED", extract "80 RED" (NOT convert to "RED")
        - If input has "WHITE", extract "WHITE" (NOT convert to "000 WHITE")
        - If input has "000 WHITE", extract "000 WHITE" (NOT convert to "WHITE")

        Args:
            color: Color from OpenAI extraction
            available_colors: List of available colors from database (unused)

        Returns:
            The color as-is (no conversion)
        """
        # Return color as-is - no conversion
        return color

    def _match_colors_for_products(
        self, items: list[dict[str, Any]], db: Session
    ) -> list[dict[str, Any]]:
        """
        Match colors for each item based on the product's available colors.

        For each item with a matched product name:
        1. For competitor products: get Nippon equivalent name and brand
        2. For competitor products: try to map competitor color code to NPMS code
        3. Get available colors for that product from database (using Nippon name for competitors)
        4. Match the input color against available colors
        5. Return matched color OR list of available colors if no match

        Args:
            items: List of items with 'raw_text', 'normalized_name', and 'product_type' fields
            db: Database session

        Returns:
            List of items with 'matched_color', 'available_colors', and optionally
            'nippon_equivalent_name', 'source_brand', 'source_color_code', 'npms_color_code' fields
        """
        logger.info("=" * 80)
        logger.info("PRODUCT-SPECIFIC COLOR MATCHING (WITH COMPETITOR COLOR MAPPING)")
        logger.info("=" * 80)

        results = []

        for i, item in enumerate(items, 1):
            raw_text = item.get("raw_text", "")
            product_name = item.get("normalized_name")
            product_type = item.get("product_type")

            if not product_name:
                # No product matched, skip color matching
                results.append({**item, "matched_color": None, "available_colors": []})
                logger.info(f"{i}. Product: (no match) → Color: N/A")
                continue

            # For competitor products, get Nippon equivalent for color matching
            nippon_name = None
            source_brand = None

            if product_type == "competitor":
                nippon_name = self._get_nippon_equivalent(product_name, db)
                source_brand = self._get_competitor_brand(product_name, db)

                if not nippon_name:
                    # No Nippon equivalent found, skip color matching
                    results.append(
                        {
                            **item,
                            "nippon_equivalent_name": None,
                            "matched_color": None,
                            "available_colors": [],
                        }
                    )
                    logger.info(
                        f"{i}. Product: {product_name} (Competitor) → No Nippon equivalent → Color: Skipped"
                    )
                    continue

                item["nippon_equivalent_name"] = nippon_name

            # Use Nippon name for color lookup
            lookup_name = nippon_name if product_type == "competitor" else product_name

            # Get available colors for this product
            available_colors = self._get_colors_for_product(lookup_name, db)

            if not available_colors:
                # No colors available for this product
                results.append({**item, "matched_color": None, "available_colors": []})
                logger.info(f"{i}. Product: {lookup_name} → No colors available")
                continue

            # For competitor products, try to map color using CompetitorColorComparison
            matched_color = None

            if product_type == "competitor" and source_brand:
                # Extract color code from raw text
                source_color_code = self._extract_color_code_from_text(raw_text)

                logger.info(
                    f"product_type == 'competitor' and source_brand:  source_color_code[DEBUG_get_npms_color_code]: "
                    f"{source_color_code}"
                )

                if source_color_code:
                    # Map to NPMS code
                    npms_code = self._get_npms_color_code(
                        source_brand, source_color_code, db
                    )

                    logger.info(f"npms_code[DEBUG_get_npms_color_code]: {npms_code}")

                    if npms_code:
                        # Match NPMS code against available colors
                        matched_color = self._match_color_from_text(
                            npms_code, available_colors
                        )

                        if matched_color:
                            item["source_brand"] = source_brand
                            item["source_color_code"] = source_color_code
                            item["npms_color_code"] = npms_code

                            logger.info(
                                f"{i}. Product: {product_name} (Competitor) → Nippon: {nippon_name}"
                            )
                            logger.info(
                                f"    Brand: {source_brand}, Source Color: {source_color_code} → NPMS: {npms_code}"
                            )
                            logger.info(f"    Matched Color: {matched_color} ✓")

            # If no match via competitor color mapping, try regular matching
            if not matched_color and product_type == "nippon":
                matched_color = self._match_color_from_text(raw_text, available_colors)

                if matched_color:
                    logger.info(
                        f"{i}. Product: {lookup_name} → Color: {matched_color} ✓"
                    )
                else:
                    colors_str = ", ".join(available_colors[:5])
                    if len(available_colors) > 5:
                        colors_str += f" ...+{len(available_colors) - 5}more"
                    logger.info(
                        f"{i}. Product: {lookup_name} → Color: (no match) → Available: [{colors_str}]"
                    )

            results.append(
                {
                    **item,
                    "matched_color": matched_color,
                    "available_colors": available_colors,
                }
            )

        logger.info("=" * 80)
        return results

    def _apply_competitor_color_mapping(
        self, items: list[dict[str, Any]], db: Session
    ) -> list[dict[str, Any]]:
        """
        Apply competitor color mapping for competitor products.

        This method only handles competitor color mapping (from competitor color codes to NPMS codes).
        Color extraction is done separately by OpenAI.

        Args:
            items: List of items with 'raw_text', 'normalized_name', 'product_type', and 'extracted_color' fields
            db: Database session

        Returns:
            List of items with 'final_color', 'available_colors', and optionally
            'nippon_equivalent_name', 'source_brand', 'source_color_code', 'npms_color_code' fields
        """
        logger.info("=" * 80)
        logger.info("COMPETITOR COLOR MAPPING (colors extracted by OpenAI)")
        logger.info("=" * 80)

        results = []

        for i, item in enumerate(items, 1):
            raw_text = item.get("raw_text", "")
            product_name = item.get("normalized_name")
            product_type = item.get("product_type")
            extracted_color = item.get("extracted_color")

            # Start with the color extracted by OpenAI
            final_color = extracted_color

            # For competitor products, try to map competitor color code to NPMS code
            if product_type == "competitor":
                nippon_name = self._get_nippon_equivalent(product_name, db)
                source_brand = self._get_competitor_brand(product_name, db)

                if nippon_name:
                    item["nippon_equivalent_name"] = nippon_name

                if source_brand:
                    # Extract color code from raw text
                    source_color_code = self._extract_color_code_from_text(raw_text)

                    if source_color_code:
                        # Map to NPMS code
                        npms_code = self._get_npms_color_code(
                            source_brand, source_color_code, db
                        )

                        if npms_code:
                            item["source_brand"] = source_brand
                            item["source_color_code"] = source_color_code
                            item["npms_color_code"] = npms_code

                            # Get available colors for the Nippon equivalent product
                            available_colors = self._get_colors_for_product(
                                nippon_name, db
                            )

                            # Try to match NPMS code against available colors
                            matched_color = self._match_color_from_text(
                                npms_code, available_colors
                            )

                            if matched_color:
                                final_color = matched_color
                                logger.info(
                                    f"{i}. Product: {product_name} (Competitor) → Nippon: {nippon_name}"
                                )
                                logger.info(
                                    f"    Brand: {source_brand}, Source Color: {source_color_code} → NPMS: {npms_code}"
                                )
                                logger.info(
                                    f"    OpenAI Color: {extracted_color} → NPMS Matched: {final_color} ✓"
                                )

            # Get available colors for the product
            lookup_name = product_name
            if product_type == "competitor":
                nippon_name = item.get("nippon_equivalent_name")
                if nippon_name:
                    lookup_name = nippon_name

            available_colors = []

            logger.info(f"{i}. Product: {lookup_name} → Color: {final_color} ✓")

            results.append(
                {
                    **item,
                    "final_color": final_color,
                    "available_colors": available_colors,
                }
            )

        logger.info("=" * 80)
        return results

    def _match_color_from_text(
        self, text: str, available_colors: list[str]
    ) -> str | None:
        """
        Match color from text against available colors.

        Args:
            text: Input text to extract color from
            available_colors: List of available colors to match against

        Returns:
            Matched color or None
        """
        from difflib import SequenceMatcher

        text_lower = text.lower()

        # First try exact match (case-insensitive)
        for color in available_colors:
            if color.lower() in text_lower:
                return color

        # Try fuzzy match for colors that might have variations
        best_match = None
        best_score = 0.0

        for color in available_colors:
            # Check if any word in the color name appears in the text
            color_words = color.lower().split()
            for word in color_words:
                if len(word) < 3:  # Skip very short words
                    continue
                if word in text_lower:
                    # Found a matching word, calculate similarity
                    score = SequenceMatcher(None, word, text_lower).ratio()
                    if score > best_score and score > 0.6:  # 60% similarity threshold
                        best_score = score
                        best_match = color

        return best_match

    def _check_nippon_products(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        STEP 1: Check if RFQ items are Nippon products.

        Args:
            rfq_descriptions: List of RFQ descriptions to check
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with check results:
            {
                "before": [...],
                "after": [...],  # normalized product names (only Nippon matches)
                "is_nippon": [...]  # boolean array
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(
            f"STEP 1: Checking {len(rfq_descriptions)} items for Nippon products"
        )

        # Build prompt for Nippon product check
        ref_parts = []
        nippon_count = len(reference_products["nippon"])
        ref_parts.append(
            f"NIPPO({nippon_count}): {', '.join(reference_products['nippon'])}"
        )

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Check and classify to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("STEP 1: CHECK NIPPON PRODUCTS - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(CHECK_NIPPON_PRODUCT_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": CHECK_NIPPON_PRODUCT_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except (CircuitBreakerOpenError, MaxRetriesExceededError) as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("STEP 1: CHECK NIPPON PRODUCTS - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result:
            raise ValueError("Invalid response format: missing 'before' key")

        if "after" not in result:
            result["after"] = [None] * len(result.get("before", []))

        if "is_nippon" not in result:
            result["is_nippon"] = [False] * len(result.get("before", []))

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("STEP 1: CHECK NIPPON PRODUCTS - RESULTS")
        logger.info("=" * 80)
        for i, (before, after, is_nippon) in enumerate(
            zip(result["before"], result["after"], result["is_nippon"]), 1
        ):
            status = "NIPPON" if is_nippon else "NOT NIPPON"
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   AFTER:  {after}")
            logger.info(f"   STATUS: {status}")
        logger.info("=" * 80)

        return result

    def _normalize_nippon_colors(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        STEP 2: Normalize colors for Nippon products from product master table.

        Args:
            rfq_descriptions: List of RFQ descriptions to extract colors from
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with extracted colors:
            {
                "before": [...],
                "colors": [...]
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(
            f"STEP 2: Extracting colors for {len(rfq_descriptions)} Nippon products"
        )

        # Build prompt for color extraction
        colors = reference_products.get("colors", [])
        colors_count = len(colors)

        ref_parts = []
        if colors_count > 0:
            ref_parts.append(f"COLORS({colors_count}): {', '.join(colors)}")

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Extract colors to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("STEP 2: NIPPON COLOR NORMALIZATION - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(NIPPON_COLOR_NORMALIZATION_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": NIPPON_COLOR_NORMALIZATION_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except (CircuitBreakerOpenError, MaxRetriesExceededError) as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("STEP 2: NIPPON COLOR NORMALIZATION - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result:
            raise ValueError("Invalid response format: missing 'before' key")

        if "colors" not in result:
            result["colors"] = [None] * len(result.get("before", []))

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("STEP 2: NIPPON COLOR NORMALIZATION - RESULTS")
        logger.info("=" * 80)
        for i, (before, color) in enumerate(zip(result["before"], result["colors"]), 1):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   COLOR:  {color}")
        logger.info("=" * 80)

        return result

    def _normalize_competitor_products(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        STEP 3: Normalize competitor product names.

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with normalized product names:
            {
                "before": [...],
                "after": [...],
                "types": [...]  # "competitor" or null
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(f"STEP 3: Normalizing {len(rfq_descriptions)} competitor products")

        # Build prompt for competitor product normalization
        ref_parts = []
        comp_count = len(reference_products["competitor"])
        ref_parts.append(
            f"COMP({comp_count}): {', '.join(reference_products['competitor'])}"
        )

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Normalize to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("STEP 3: COMPETITOR PRODUCT NORMALIZATION - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(COMPETITOR_PRODUCT_NORMALIZATION_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": COMPETITOR_PRODUCT_NORMALIZATION_PROMPT,
                    },
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except (CircuitBreakerOpenError, MaxRetriesExceededError) as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("STEP 3: COMPETITOR PRODUCT NORMALIZATION - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result or "after" not in result:
            raise ValueError(
                "Invalid response format: missing 'before' or 'after' keys"
            )

        if "types" not in result:
            result["types"] = [None] * len(result["after"])

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("STEP 3: COMPETITOR PRODUCT NORMALIZATION - RESULTS")
        logger.info("=" * 80)
        for i, (before, after, prod_type) in enumerate(
            zip(result["before"], result["after"], result["types"]), 1
        ):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   AFTER:  {after}")
            logger.info(f"   TYPE:   {prod_type}")
        logger.info("=" * 80)

        return result

    def _normalize_competitor_colors(
        self,
        rfq_descriptions: list[str],
        db: Session,
        reference_products: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """
        STEP 4: Normalize competitor colors from competitor color table.

        Args:
            rfq_descriptions: List of RFQ descriptions to extract colors from
            db: Database session
            reference_products: Optional pre-loaded reference products

        Returns:
            Dictionary with extracted colors:
            {
                "before": [...],
                "colors": [...],
                "source_codes": [...]
            }
        """
        # Get reference products from database if not provided
        if reference_products is None:
            reference_products = self._get_reference_products(db)

        logger.info(
            f"STEP 4: Extracting colors for {len(rfq_descriptions)} competitor products"
        )

        # Build prompt for color extraction
        competitor_colors = reference_products.get("competitor_colors", [])
        colors_count = len(competitor_colors)

        ref_parts = []
        if colors_count > 0:
            # Show competitor colors (source_code from competitor_color_comparison table)
            ref_parts.append(f"COMPETITOR_COLORS({colors_count}):")
            # Show first 100 colors to avoid token limit
            for cc in competitor_colors[:200]:
                ref_parts.append(f"  - {cc}")
            if len(competitor_colors) > 200:
                ref_parts.append(f"  ... and {len(competitor_colors) - 100} more")

        # RFQ items
        items = []
        for i, desc in enumerate(rfq_descriptions):
            items.append(f"{i}||{desc}")

        # Build prompt
        user_prompt = f"""REFERENCE:
{chr(10).join(ref_parts)}

INPUT:
{chr(10).join(items)}

Extract colors to JSON."""

        # Log the prompt
        logger.info("=" * 80)
        logger.info("STEP 4: COMPETITOR COLOR NORMALIZATION - OPENAI API REQUEST")
        logger.info("=" * 80)
        logger.info("SYSTEM PROMPT:")
        logger.info(COMPETITOR_COLOR_NORMALIZATION_PROMPT)
        logger.info("")
        logger.info("USER PROMPT:")
        logger.info(user_prompt)
        logger.info("=" * 80)

        # Define the OpenAI API call function
        def _make_openai_call():
            """Internal function to make OpenAI API call."""
            return self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": COMPETITOR_COLOR_NORMALIZATION_PROMPT,
                    },
                    {"role": "user", "content": user_prompt},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},
            )

        # Call OpenAI API with resilient caller
        try:
            response = self.resilient_caller.call(_make_openai_call)
        except (CircuitBreakerOpenError, MaxRetriesExceededError) as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise

        # Extract response
        content = response.choices[0].message.content
        usage = response.usage

        logger.info("=" * 80)
        logger.info("STEP 4: COMPETITOR COLOR NORMALIZATION - API RESPONSE")
        logger.info("=" * 80)
        logger.info(f"Model: {self.model}")
        logger.info(
            f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
            f"{usage.total_tokens} total"
        )
        logger.info("")
        logger.info("RAW RESPONSE:")
        logger.info(content)
        logger.info("=" * 80)

        # Parse JSON response
        result = json.loads(content)

        # Validate response
        if "before" not in result:
            raise ValueError("Invalid response format: missing 'before' key")

        if "colors" not in result:
            result["colors"] = [None] * len(result.get("before", []))

        if "source_codes" not in result:
            result["source_codes"] = [None] * len(result.get("before", []))

        if len(result["before"]) != len(rfq_descriptions):
            raise ValueError(
                f"Response length mismatch: expected {len(rfq_descriptions)}, got {len(result['before'])}"
            )

        # Add metadata
        result["model"] = self.model
        result["usage"] = {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        }

        # Log parsed results
        logger.info("=" * 80)
        logger.info("STEP 4: COMPETITOR COLOR NORMALIZATION - RESULTS")
        logger.info("=" * 80)
        for i, (before, color, source_code) in enumerate(
            zip(result["before"], result["colors"], result["source_codes"]), 1
        ):
            logger.info(f"{i}. BEFORE: {before}")
            logger.info(f"   COLOR:  {color}")
            logger.info(f"   SOURCE CODE:  {source_code}")
        logger.info("=" * 80)

        return result

    def normalize_competitor_color_pdf_rows(
        self, raw_rows: list[str], batch_size: int = 50
    ) -> list[dict[str, Any]]:
        """
        Normalize competitor color PDF rows using OpenAI.

        This method is specifically designed to parse raw PDF table rows like
        "28 | Grey 403 | RAL7038" and extract structured data.

        Args:
            raw_rows: List of raw text rows from PDF table
            batch_size: Number of rows to process per batch (default: 50)

        Returns:
            List of dictionaries with extracted data:
            [
                {
                    "raw_text": "28 | Grey 403 | RAL7038",
                    "source_code": "Grey 403",
                    "npms_code": "RAL7038",
                    "item_number": 28
                },
                ...
            ]
        """
        if not self.enabled:
            raise ValueError(
                "OpenAI normalization service is disabled. Set OPENAI_API_KEY to enable."
            )

        if not raw_rows:
            return []

        logger.info(
            f"Normalizing {len(raw_rows)} competitor color PDF rows using OpenAI {self.model}"
        )

        # Process in batches to avoid token limits
        all_results = []

        for i in range(0, len(raw_rows), batch_size):
            batch = raw_rows[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(raw_rows) + batch_size - 1) // batch_size

            logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} rows)"
            )

            # Prepare items for OpenAI
            items = []
            for j, row in enumerate(batch):
                items.append(f"{j}||{row}")

            # Build prompt
            user_prompt = f"""INPUT:
{chr(10).join(items)}

Extract color codes to JSON."""

            # Log the prompt
            logger.info("=" * 80)
            logger.info(
                f"COMPETITOR COLOR PDF ROW NORMALIZATION - BATCH {batch_num}/{total_batches}"
            )
            logger.info("=" * 80)
            logger.info("SYSTEM PROMPT:")
            logger.info(COMPETITOR_COLOR_PDF_ROW_NORMALIZATION_PROMPT)
            logger.info("")
            logger.info("USER PROMPT:")
            logger.info(user_prompt)
            logger.info("=" * 80)

            # Define the OpenAI API call function
            def _make_openai_call():
                """Internal function to make OpenAI API call."""
                return self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": COMPETITOR_COLOR_PDF_ROW_NORMALIZATION_PROMPT,
                        },
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    response_format={"type": "json_object"},
                )

            # Call OpenAI API with resilient caller
            try:
                response = self.resilient_caller.call(_make_openai_call)
            except (CircuitBreakerOpenError, MaxRetriesExceededError) as e:
                logger.error(f"OpenAI API call failed for batch {batch_num}: {e}")
                # Add placeholder results for failed batch
                for row in batch:
                    all_results.append(
                        {
                            "raw_text": row,
                            "source_code": None,
                            "npms_code": None,
                            "item_number": None,
                            "error": str(e),
                        }
                    )
                continue

            # Extract response
            content = response.choices[0].message.content
            usage = response.usage

            logger.info("=" * 80)
            logger.info(f"BATCH {batch_num}/{total_batches} - API RESPONSE")
            logger.info("=" * 80)
            logger.info(f"Model: {self.model}")
            logger.info(
                f"Token usage: {usage.prompt_tokens} prompt + {usage.completion_tokens} completion = "
                f"{usage.total_tokens} total"
            )
            logger.info("")
            logger.info("RAW RESPONSE:")
            logger.info(content)
            logger.info("=" * 80)

            # Parse JSON response
            result = json.loads(content)

            # Validate response
            if "before" not in result:
                logger.warning(
                    f"Batch {batch_num}: Invalid response format - missing 'before' key"
                )
                for row in batch:
                    all_results.append(
                        {
                            "raw_text": row,
                            "source_code": None,
                            "npms_code": None,
                            "item_number": None,
                            "error": "Invalid response format",
                        }
                    )
                continue

            # Build results
            before_list = result.get("before", [])
            source_codes = result.get("source_codes", [])
            npms_codes = result.get("npms_codes", [])
            item_numbers = result.get("item_numbers", [])

            # Ensure all arrays have the same length
            max_len = max(
                len(before_list), len(source_codes), len(npms_codes), len(item_numbers)
            )
            source_codes.extend([None] * (max_len - len(source_codes)))
            npms_codes.extend([None] * (max_len - len(npms_codes)))
            item_numbers.extend([None] * (max_len - len(item_numbers)))

            for j, (raw_text, source_code, npms_code, item_number) in enumerate(
                zip(before_list, source_codes, npms_codes, item_numbers)
            ):
                all_results.append(
                    {
                        "raw_text": raw_text,
                        "source_code": source_code,
                        "npms_code": npms_code,
                        "item_number": item_number,
                    }
                )

            # Log parsed results
            logger.info("=" * 80)
            logger.info(f"BATCH {batch_num}/{total_batches} - RESULTS")
            logger.info("=" * 80)
            for j, (raw_text, source_code, npms_code, item_number) in enumerate(
                zip(
                    before_list[:10],
                    source_codes[:10],
                    npms_codes[:10],
                    item_numbers[:10],
                ),
                1,
            ):  # Show first 10
                logger.info(f"{j}. RAW: {raw_text}")
                logger.info(f"   SOURCE: {source_code}")
                logger.info(f"   NPMS:   {npms_code}")
                logger.info(f"   ITEM:   {item_number}")
            if len(before_list) > 10:
                logger.info(f"   ... and {len(before_list) - 10} more")
            logger.info("=" * 80)

        return all_results

    def _separate_cached_from_uncached_items(
        self, rfq_descriptions: list[str], db: Session, use_cache: bool
    ) -> tuple[dict, list[str], list[int]]:
        """
        Separate RFQ descriptions into cached and uncached items.

        Returns:
            Tuple of (cached_results, uncached_descriptions, uncached_indices)
        """
        cached_results = {}
        uncached_descriptions = []
        uncached_indices = []

        if not use_cache:
            uncached_descriptions = rfq_descriptions
            uncached_indices = list(range(len(rfq_descriptions)))
            return cached_results, uncached_descriptions, uncached_indices

        for i, desc in enumerate(rfq_descriptions):
            cache_key = self._get_cache_key(desc)

            # Check memory cache first
            mem_result = self._get_from_memory_cache(cache_key)
            if mem_result:
                cached_results[i] = mem_result
                logger.debug(f"Memory cache hit for: {desc}")
                continue

            # Check database cache
            db_result = self._get_from_db_cache(desc, db)
            if db_result:
                cached_results[i] = db_result
                self._store_in_memory_cache(cache_key, db_result)
                logger.debug(f"DB cache hit for: {desc}")
                continue

            # Not found in any cache
            uncached_descriptions.append(desc)
            uncached_indices.append(i)
            self._cache_stats["misses"] += 1

        return cached_results, uncached_descriptions, uncached_indices

    def _separate_nippon_from_competitor_items(
        self, descriptions: list[str], is_nippon_flags: list[bool]
    ) -> tuple[list[int], list[str], list[int], list[str]]:
        """
        Separate descriptions into Nippon and Competitor groups based on flags.

        Returns:
            Tuple of (nippon_indices, nippon_descriptions, competitor_indices, competitor_descriptions)
        """
        nippon_indices = []
        nippon_descriptions = []
        competitor_indices = []
        competitor_descriptions = []

        for i, (desc, is_nippon) in enumerate(zip(descriptions, is_nippon_flags)):
            if is_nippon:
                nippon_indices.append(i)
                nippon_descriptions.append(desc)
            else:
                competitor_indices.append(i)
                competitor_descriptions.append(desc)

        return (
            nippon_indices,
            nippon_descriptions,
            competitor_indices,
            competitor_descriptions,
        )

    def _fill_nippon_results(
        self,
        nippon_check_result: dict[str, Any],
        nippon_colors_result: dict[str, Any],
        nippon_indices: list[int],
        final_after: list,
        final_types: list,
        final_colors: list,
    ) -> dict[str, int]:
        """
        Fill in Nippon product results into the final arrays.

        Returns:
            Token usage dict with added tokens
        """
        usage = {
            "prompt_tokens": nippon_colors_result["usage"]["prompt_tokens"],
            "completion_tokens": nippon_colors_result["usage"]["completion_tokens"],
            "total_tokens": nippon_colors_result["usage"]["total_tokens"],
        }

        for local_idx, global_idx in enumerate(nippon_indices):
            # Add bounds checking for all fields to prevent IndexError
            after_list = nippon_check_result.get("after", [])
            colors_list = nippon_colors_result.get("colors", [])

            if global_idx < len(after_list):
                final_after[global_idx] = after_list[global_idx]
            else:
                final_after[global_idx] = None

            final_types[global_idx] = "nippon"

            if local_idx < len(colors_list):
                final_colors[global_idx] = colors_list[local_idx]
            else:
                # Log missing color and set to None
                logger.warning(
                    f"Nippon color not found for index {local_idx}, setting to None"
                )
                final_colors[global_idx] = None

        return usage

    def _fill_competitor_results(
        self,
        competitor_products_result: dict[str, Any],
        competitor_colors_result: dict[str, Any],
        competitor_indices: list[int],
        final_after: list,
        final_types: list,
        final_colors: list,
    ) -> dict[str, int]:
        """
        Fill in competitor product results into the final arrays.

        Returns:
            Token usage dict with added tokens
        """
        usage = {
            "prompt_tokens": (
                competitor_products_result["usage"]["prompt_tokens"]
                + competitor_colors_result["usage"]["prompt_tokens"]
            ),
            "completion_tokens": (
                competitor_products_result["usage"]["completion_tokens"]
                + competitor_colors_result["usage"]["completion_tokens"]
            ),
            "total_tokens": (
                competitor_products_result["usage"]["total_tokens"]
                + competitor_colors_result["usage"]["total_tokens"]
            ),
        }

        for local_idx, global_idx in enumerate(competitor_indices):
            # Add bounds checking for all fields to prevent IndexError
            after_list = competitor_products_result.get("after", [])
            types_list = competitor_products_result.get("types", [])
            colors_list = competitor_colors_result.get("colors", [])

            if local_idx < len(after_list):
                final_after[global_idx] = after_list[local_idx]
            else:
                final_after[global_idx] = None

            if local_idx < len(types_list):
                final_types[global_idx] = types_list[local_idx]
            else:
                final_types[global_idx] = None

            if local_idx < len(colors_list):
                final_colors[global_idx] = colors_list[local_idx]
            else:
                # Log missing color and set to None
                logger.warning(
                    f"Competitor color not found for index {local_idx}, setting to None"
                )
                final_colors[global_idx] = None

        return usage

    def _merge_cached_and_api_results(
        self,
        rfq_descriptions: list[str],
        cached_results: dict,
        api_result: dict[str, Any],
        uncached_indices: list[int],
        model: str,
    ) -> dict[str, Any]:
        """
        Merge cached results and API results into final result.

        Returns:
            Merged final result dictionary
        """
        final_result = {
            "before": rfq_descriptions,
            "after": [None] * len(rfq_descriptions),
            "colors": [None] * len(rfq_descriptions),
            "available_colors": [[] for _ in range(len(rfq_descriptions))],
            "types": [None] * len(rfq_descriptions),
            "nippon_equivalent_names": [None] * len(rfq_descriptions),
            "source_brands": [None] * len(rfq_descriptions),
            "source_color_codes": [None] * len(rfq_descriptions),
            "npms_color_codes": [None] * len(rfq_descriptions),
            "model": model,
            "usage": api_result.get("usage", {}),
            "cache_stats": {
                "cached": len(cached_results),
                "api_calls": len(uncached_indices),
                "hit_rate": f"{len(cached_results)}/{len(rfq_descriptions)}",
            },
        }

        # Fill in cached results
        for i, cached_data in cached_results.items():
            final_result["after"][i] = cached_data["normalized"]
            final_result["colors"][i] = cached_data.get("color")
            final_result["types"][i] = cached_data.get("type")

        # Fill in API results
        for idx, api_idx in enumerate(uncached_indices):
            if api_idx < len(api_result.get("after", [])):
                final_result["after"][api_idx] = api_result["after"][idx]
            if api_idx < len(api_result.get("types", [])):
                final_result["types"][api_idx] = api_result["types"][idx]
            if api_idx < len(api_result.get("colors", [])):
                final_result["colors"][api_idx] = api_result["colors"][idx]
            if api_idx < len(api_result.get("available_colors", [])):
                final_result["available_colors"][api_idx] = api_result[
                    "available_colors"
                ][idx]
            if api_idx < len(api_result.get("nippon_equivalent_names", [])):
                final_result["nippon_equivalent_names"][api_idx] = api_result[
                    "nippon_equivalent_names"
                ][idx]
            if api_idx < len(api_result.get("source_brands", [])):
                final_result["source_brands"][api_idx] = api_result["source_brands"][
                    idx
                ]
            if api_idx < len(api_result.get("source_color_codes", [])):
                final_result["source_color_codes"][api_idx] = api_result[
                    "source_color_codes"
                ][idx]
            if api_idx < len(api_result.get("npms_color_codes", [])):
                final_result["npms_color_codes"][api_idx] = api_result[
                    "npms_color_codes"
                ][idx]

        return final_result

    def normalize_rfq_items(
        self, rfq_descriptions: list[str], db: Session, use_cache: bool = True
    ) -> dict[str, Any]:
        """
        Normalize RFQ items with multi-layer caching.

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            use_cache: Whether to use cached results

        Returns:
            Dictionary with normalized results:
            {
                "before": [...],
                "after": [...],
                "types": [...],
                "model": str,
                "usage": {...},
                "cache_stats": {...}
            }

        Raises:
            ValueError: If service is disabled or API call fails
        """
        if not self.enabled:
            raise ValueError(
                "OpenAI normalization service is disabled. Set OPENAI_API_KEY to enable."
            )

        if not rfq_descriptions:
            return {
                "before": [],
                "after": [],
                "colors": [],
                "types": [],
                "model": self.model,
                "usage": {},
                "cache_stats": {"cached": 0, "api_calls": 0, "hit_rate": "0/0"},
            }

        logger.info(
            f"Normalizing {len(rfq_descriptions)} RFQ items using OpenAI {self.model}"
        )

        # Log RFQ descriptions to normalize
        logger.info("=" * 80)
        logger.info("RFQ DESCRIPTIONS TO NORMALIZE:")
        for i, desc in enumerate(rfq_descriptions, 1):
            logger.info(f"  {i}. {desc}")
        logger.info("=" * 80)

        # Phase 1: Separate cached from uncached items using helper function
        cached_results, uncached_descriptions, uncached_indices = (
            self._separate_cached_from_uncached_items(rfq_descriptions, db, use_cache)
        )

        # Phase 2: Call OpenAI API for uncached items only
        api_result = {
            "before": [],
            "after": [],
            "colors": [],
            "types": [],
            "model": self.model,
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }

        if uncached_descriptions:
            logger.info(
                f"Calling OpenAI API for {len(uncached_descriptions)} uncached items (using SEPARATED flow for "
                f"Nippon/Competitor)"
            )
            self._cache_stats["api_calls"] += 1

            # Get reference products once for all uncached items
            reference_products = self._get_reference_products(db)

            # SEPARATED FLOW - Step 1: Check if RFQ items are Nippon products
            nippon_check_result = self._check_nippon_products(
                uncached_descriptions, db, reference_products
            )

            # Separate items into Nippon and Competitor groups using helper function
            (
                nippon_indices,
                nippon_descriptions,
                competitor_indices,
                competitor_descriptions,
            ) = self._separate_nippon_from_competitor_items(
                uncached_descriptions, nippon_check_result["is_nippon"]
            )

            logger.info(
                f"Separated: {len(nippon_descriptions)} Nippon items, {len(competitor_descriptions)} competitor items"
            )

            # Initialize result arrays with None values
            final_after = [None] * len(uncached_descriptions)
            final_types = [None] * len(uncached_descriptions)
            final_colors = [None] * len(uncached_descriptions)
            total_usage = {
                "prompt_tokens": nippon_check_result["usage"]["prompt_tokens"],
                "completion_tokens": nippon_check_result["usage"]["completion_tokens"],
                "total_tokens": nippon_check_result["usage"]["total_tokens"],
            }

            # SEPARATED FLOW - Step 2: If Nippon, normalize colors from product master table
            if nippon_descriptions:
                logger.info(f"Processing {len(nippon_descriptions)} Nippon items")
                nippon_colors_result = self._normalize_nippon_colors(
                    nippon_descriptions, db, reference_products
                )

                # Fill in Nippon results using helper function
                nippon_usage = self._fill_nippon_results(
                    nippon_check_result,
                    nippon_colors_result,
                    nippon_indices,
                    final_after,
                    final_types,
                    final_colors,
                )

                total_usage["prompt_tokens"] += nippon_usage["prompt_tokens"]
                total_usage["completion_tokens"] += nippon_usage["completion_tokens"]
                total_usage["total_tokens"] += nippon_usage["total_tokens"]

            # SEPARATED FLOW - Step 3: If competitor, normalize competitor products
            if competitor_descriptions:
                logger.info(
                    f"Processing {len(competitor_descriptions)} competitor items"
                )
                competitor_products_result = self._normalize_competitor_products(
                    competitor_descriptions, db, reference_products
                )

                # SEPARATED FLOW - Step 4: If competitor, normalize colors from competitor color table
                competitor_colors_result = self._normalize_competitor_colors(
                    competitor_descriptions, db, reference_products
                )

                # Fill in competitor results using helper function
                competitor_usage = self._fill_competitor_results(
                    competitor_products_result,
                    competitor_colors_result,
                    competitor_indices,
                    final_after,
                    final_types,
                    final_colors,
                )

                total_usage["prompt_tokens"] += competitor_usage["prompt_tokens"]
                total_usage["completion_tokens"] += competitor_usage[
                    "completion_tokens"
                ]
                total_usage["total_tokens"] += competitor_usage["total_tokens"]

            # Combine results
            api_result = {
                "before": uncached_descriptions,
                "after": final_after,
                "colors": final_colors,
                "available_colors": [[] for _ in range(len(uncached_descriptions))],
                "types": final_types,
                "nippon_equivalent_names": [None] * len(uncached_descriptions),
                "source_brands": [None] * len(uncached_descriptions),
                "source_color_codes": [None] * len(uncached_descriptions),
                "npms_color_codes": [None] * len(uncached_descriptions),
                "model": self.model,
                "usage": total_usage,
            }

            # Store new results in caches
            for desc, normalized, color, prod_type in zip(
                api_result["before"],
                api_result["after"],
                api_result.get("colors", [None] * len(api_result["after"])),
                api_result["types"],
            ):
                cache_key = self._get_cache_key(desc)
                cache_value = {
                    "normalized": normalized,
                    "color": color,
                    "type": prod_type,
                    "source": "openai",
                }
                self._store_in_memory_cache(cache_key, cache_value)
                self._store_in_db_cache(
                    desc, normalized, prod_type, db, normalized_color=color
                )

        # Phase 3: Merge cached and API results using helper function
        final_result = self._merge_cached_and_api_results(
            rfq_descriptions, cached_results, api_result, uncached_indices, self.model
        )

        # Log detailed before/after results with types and colors
        logger.info("=" * 80)
        logger.info("NORMALIZATION RESULTS - BEFORE/AFTER/COLOR/TYPE")
        logger.info("=" * 80)
        for i, (
            before,
            after,
            color,
            available_colors,
            prod_type,
            nippon_equiv,
            source_brand,
            source_color,
            npms_color,
        ) in enumerate(
            zip(
                final_result["before"],
                final_result["after"],
                final_result.get("colors", [None] * len(final_result["after"])),
                final_result.get(
                    "available_colors", [[] for _ in range(len(final_result["after"]))]
                ),
                final_result["types"],
                final_result.get(
                    "nippon_equivalent_names", [None] * len(final_result["after"])
                ),
                final_result.get("source_brands", [None] * len(final_result["after"])),
                final_result.get(
                    "source_color_codes", [None] * len(final_result["after"])
                ),
                final_result.get(
                    "npms_color_codes", [None] * len(final_result["after"])
                ),
            ),
            1,
        ):
            if after and prod_type:
                type_display = prod_type.upper()
                if prod_type == "nippon":
                    type_display = "🔵 NIPPON"
                elif prod_type == "competitor":
                    type_display = "🔴 COMPETITOR"
                status = f"✓ MATCHED [{type_display}]"
            elif after:
                status = "⚠ MATCHED [UNKNOWN TYPE]"
            else:
                status = "✗ NO MATCH"
            logger.info(f"{i:2d}. [{status}]")
            logger.info(f"    BEFORE: {before}")
            if after:
                logger.info(f"    AFTER:  {after}")
                if prod_type == "competitor" and nippon_equiv:
                    logger.info(f"    NIPPON EQUIVALENT: {nippon_equiv}")
                if color:
                    logger.info(f"    COLOR: {color} ✓")
                elif available_colors:
                    colors_str = ", ".join(available_colors[:5])
                    if len(available_colors) > 5:
                        colors_str += f" ...+{len(available_colors) - 5}more"
                    logger.info(f"    COLOR: (no match) → Available: [{colors_str}]")
                if source_brand and source_color and npms_color:
                    logger.info(
                        f"    COLOR MAPPING: {source_brand} {source_color} → NPMS {npms_color}"
                    )
                if prod_type:
                    logger.info(f"    TYPE:  {prod_type}")
            else:
                logger.info("    AFTER:  (null)")
            logger.info("")  # Empty line for readability
        logger.info("=" * 80)

        # Log cache performance
        logger.info(f"Cache performance: {final_result['cache_stats']}")
        logger.info(
            f"Total cache stats: hits={self._cache_stats['hits']}, "
            f"misses={self._cache_stats['misses']}, "
            f"api_calls={self._cache_stats['api_calls']}"
        )

        # Log statistics by type
        matched_count = sum(1 for item in final_result["after"] if item is not None)
        nippon_count = sum(1 for t in final_result["types"] if t == "nippon")
        competitor_count = sum(1 for t in final_result["types"] if t == "competitor")
        logger.info(
            f"Normalization Summary: {matched_count}/{len(rfq_descriptions)} items matched"
        )
        logger.info(f"  - Nippon products: {nippon_count}")
        logger.info(f"  - Competitor products: {competitor_count}")
        logger.info(f"  - No match: {len(rfq_descriptions) - matched_count}")
        logger.info("=" * 80)

        return final_result

    def normalize_single_item(self, rfq_description: str, db: Session) -> str | None:
        """
        Normalize a single RFQ item description.

        Args:
            rfq_description: Single RFQ description to normalize
            db: Database session

        Returns:
            Normalized product name or None if no match found
        """
        if not rfq_description or not rfq_description.strip():
            return None

        result = self.normalize_rfq_items([rfq_description], db)
        return result["after"][0] if result["after"] else None

    def normalize_with_confidence(
        self, rfq_descriptions: list[str], db: Session, include_raw: bool = False
    ) -> list[dict[str, Any]]:
        """
        Normalize RFQ items and return detailed results with metadata.

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            include_raw: Whether to include raw descriptions in output

        Returns:
            List of dictionaries with normalization results:
            [
                {
                    "raw": "original description",
                    "normalized": "normalized name" or None,
                    "matched": bool,
                    "model": "gpt-4o-mini"
                },
                ...
            ]
        """
        if not rfq_descriptions:
            return []

        result = self.normalize_rfq_items(rfq_descriptions, db)

        output = []
        for i, (raw, normalized) in enumerate(zip(result["before"], result["after"])):
            item = {
                "normalized": normalized,
                "matched": normalized is not None,
                "model": result.get("model", self.model),
            }
            if include_raw:
                item["raw"] = raw
            # Add competitor mapping fields if available
            if "nippon_equivalent_names" in result and i < len(
                result["nippon_equivalent_names"]
            ):
                item["nippon_equivalent_name"] = result["nippon_equivalent_names"][i]
            if "source_brands" in result and i < len(result["source_brands"]):
                item["source_brand"] = result["source_brands"][i]
            if "source_color_codes" in result and i < len(result["source_color_codes"]):
                item["source_color_code"] = result["source_color_codes"][i]
            if "npms_color_codes" in result and i < len(result["npms_color_codes"]):
                item["npms_color_code"] = result["npms_color_codes"][i]
            output.append(item)

        return output

    def normalize_product_names_only(
        self, rfq_descriptions: list[str], db: Session, use_cache: bool = False
    ) -> dict[str, Any]:
        """
        Normalize product names only (preserves model numbers like 700, 100, 500).

        This is a separate public method for product name normalization only.
        Use this when you want to normalize product names without extracting colors.

        Args:
            rfq_descriptions: List of RFQ descriptions to normalize
            db: Database session
            use_cache: Whether to use cached results (default: False for this method)

        Returns:
            Dictionary with normalized product names:
            {
                "before": [...],
                "after": [...],
                "types": [...],
                "model": str,
                "usage": {...}
            }

        Raises:
            ValueError: If service is disabled or API call fails
        """
        if not self.enabled:
            raise ValueError(
                "OpenAI normalization service is disabled. Set OPENAI_API_KEY to enable."
            )

        if not rfq_descriptions:
            return {
                "before": [],
                "after": [],
                "types": [],
                "model": self.model,
                "usage": {},
            }

        logger.info(
            f"Normalizing product names for {len(rfq_descriptions)} items (model numbers preserved)"
        )

        # Call the product name normalization method
        result = self._normalize_product_names_only(rfq_descriptions, db)

        return result

    def extract_colors_only(
        self, rfq_descriptions: list[str], db: Session, use_cache: bool = False
    ) -> dict[str, Any]:
        """
        Extract colors only (separate from product name normalization).

        This is a separate public method for color extraction only.
        Use this when you want to extract colors without normalizing product names.

        Args:
            rfq_descriptions: List of RFQ descriptions to extract colors from
            db: Database session
            use_cache: Whether to use cached results (default: False for this method)

        Returns:
            Dictionary with extracted colors:
            {
                "before": [...],
                "colors": [...],
                "model": str,
                "usage": {...}
            }

        Raises:
            ValueError: If service is disabled or API call fails
        """
        if not self.enabled:
            raise ValueError(
                "OpenAI normalization service is disabled. Set OPENAI_API_KEY to enable."
            )

        if not rfq_descriptions:
            return {"before": [], "colors": [], "model": self.model, "usage": {}}

        logger.info(f"Extracting colors for {len(rfq_descriptions)} items")

        # Call the color extraction method
        result = self._extract_colors_only(rfq_descriptions, db)

        return result


# Singleton instance
openai_normalization_service = OpenAINormalizationService()
