"""
Text Normalization Utilities

Utility functions for normalizing and cleaning text data
for better matching and comparison.
"""

import logging
import re

logger = logging.getLogger(__name__)


# Common noise patterns to filter out
NOISE_PATTERNS = [
    r"office notes?",
    r"buyer comments?",
    r"remarks?",
    r"note\s*:?",
    r"comment\s*:",
]

# Common product synonyms/abbreviations mapping
SYNONYMS = {
    "thinner": "thinner 024",
    "paint": "coating",
    "coating": "paint",
    "af": "anti fouling",
    "a/f": "anti fouling",
}

# Common abbreviations to expand
ABBREVIATIONS = {
    "np": "nippon",
    "npe": "nippon paint epoxy",
    "npa": "nippon paint anti fouling",
    "lt": "liter",
    "ltr": "liter",
    "ea": "each",
    "pcs": "pieces",
    "kg": "kilogram",
}


def normalize_text(text: str | None) -> str:
    """
    Normalize text for better matching.

    Steps:
    1. Convert to lowercase
    2. Remove special characters (except hyphens, spaces)
    3. Remove extra whitespace
    4. Remove noise patterns

    Args:
        text: Input text to normalize

    Returns:
        Normalized text string

    Example:
        >>> normalize_text("Office Notes: THINNER 024 (Nippon)")
        'thinner 024 nippon'
    """
    if not text:
        return ""

    # Convert to lowercase
    text = str(text).lower()

    # Remove noise patterns
    for pattern in NOISE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    # Remove special characters (except hyphens, spaces, alphanumeric)
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)

    # Replace multiple spaces with single space
    text = re.sub(r"\s+", " ", text)

    # Strip whitespace
    text = text.strip()

    return text


def expand_synonyms(text: str, synonym_map: dict | None = None) -> str:
    """
    Expand synonyms in text.

    Args:
        text: Input text
        synonym_map: Dictionary of synonyms mapping

    Returns:
        Text with expanded synonyms

    Example:
        >>> expand_synonyms("thinner for cleaning")
        'thinner 024 for cleaning'
    """
    if not text:
        return ""

    if synonym_map is None:
        synonym_map = SYNONYMS

    text_lower = text.lower()

    for synonym, expansion in synonym_map.items():
        # Word boundary matching
        pattern = r"\b" + re.escape(synonym) + r"\b"
        text_lower = re.sub(pattern, expansion, text_lower)

    return text_lower


def expand_abbreviations(text: str, abbr_map: dict | None = None) -> str:
    """
    Expand common abbreviations in text.

    Args:
        text: Input text
        abbr_map: Dictionary of abbreviations to expand

    Returns:
        Text with expanded abbreviations

    Example:
        >>> expand_abbreviations("NP Paint 5 LTR")
        'nippon paint 5 liter'
    """
    if not text:
        return ""

    if abbr_map is None:
        abbr_map = ABBREVIATIONS

    text_lower = text.lower()

    for abbr, expansion in abbr_map.items():
        # Word boundary matching
        pattern = r"\b" + re.escape(abbr) + r"\b"
        text_lower = re.sub(pattern, expansion, text_lower)

    return text_lower


def extract_product_keywords(text: str, min_length: int = 3) -> list[str]:
    """
    Extract meaningful product keywords from text.

    Args:
        text: Input text
        min_length: Minimum keyword length

    Returns:
        List of keywords

    Example:
        >>> extract_product_keywords("Nippon Marine Paint Epoxy 123")
        ['nippon', 'marine', 'paint', 'epoxy', '123']
    """
    if not text:
        return []

    # Normalize first
    normalized = normalize_text(text)

    # Remove common filler words
    filler_words = {"for", "with", "and", "or", "the", "a", "an"}

    # Split into words and filter
    keywords = [
        word
        for word in normalized.split()
        if len(word) >= min_length and word not in filler_words
    ]

    return keywords


def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    Calculate simple similarity between two texts using word overlap.

    Args:
        text1: First text
        text2: Second text

    Returns:
        Similarity score between 0 and 1

    Example:
        >>> calculate_text_similarity("nippon paint epoxy", "nippon epoxy paint")
        1.0
    """
    if not text1 or not text2:
        return 0.0

    # Normalize both texts
    norm1 = normalize_text(text1)
    norm2 = normalize_text(text2)

    # Get word sets
    words1 = set(norm1.split())
    words2 = set(norm2.split())

    if not words1 or not words2:
        return 0.0

    # Calculate Jaccard similarity
    intersection = words1 & words2
    union = words1 | words2

    return len(intersection) / len(union) if union else 0.0


def clean_rfq_description(text: str | None) -> str:
    """
    Clean RFQ description for matching.

    This is a comprehensive cleaning function that:
    1. Normalizes text
    2. Expands abbreviations
    3. Removes noise
    4. Extracts core product name

    Args:
        text: Raw RFQ description

    Returns:
        Cleaned description

    Example:
        >>> clean_rfq_description("Office Notes: NP A/F Paint 5 LTR")
        'nippon anti fouling paint 5 liter'
    """
    if not text:
        return ""

    # Step 1: Expand abbreviations
    cleaned = expand_abbreviations(text)

    # Step 2: Expand synonyms
    cleaned = expand_synonyms(cleaned)

    # Step 3: Normalize
    cleaned = normalize_text(cleaned)

    return cleaned


def detect_product_type(text: str, nippon_keywords: set[str] | None = None) -> str:
    """
    Detect if text refers to Nippon product or competitor product.

    Args:
        text: Input text to analyze
        nippon_keywords: Set of Nippon-specific keywords

    Returns:
        "nippon" or "competitor"

    Example:
        >>> detect_product_type("Nippon Paint Marine")
        'nippon'
        >>> detect_product_type("International Paint")
        'competitor'
    """
    from apps.app_nippon_rfq_matching.app.core.config import settings

    if nippon_keywords is None:
        nippon_keywords = set(settings.NIPPON_KEYWORDS)

    normalized = normalize_text(text)

    # Check if any Nippon keyword is present
    for keyword in nippon_keywords:
        if keyword in normalized:
            return "nippon"

    return "competitor"
