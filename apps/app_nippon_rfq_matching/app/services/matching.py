"""
Matching service using fuzzy matching and TF-IDF cosine similarity
"""

import asyncio
import concurrent.futures
import pickle
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from apps.app_nippon_rfq_matching.app.core.config import settings

# Thread pool for CPU-intensive matching operations
_matching_thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="matching_service"
)


class MatchingService:
    """Service for matching RFQ items to product master"""

    def __init__(self):
        """Initialize matching service"""
        self._vectorizer: TfidfVectorizer | None = None
        self._tfidf_matrix = None
        self._master_list: list[str] = []
        self._product_master_df: pd.DataFrame | None = None
        self._is_loaded = False
        self._is_loading = False
        self._load_lock = asyncio.Lock()

        # Store product master with color for color-aware matching
        self._product_master_with_color: pd.DataFrame = pd.DataFrame()

        # Path to save/load vectorizer
        self.model_dir = settings.STORAGE_DIR / "models"
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.vectorizer_path = self.model_dir / "vectorizer.pkl"
        self.tfidf_matrix_path = self.model_dir / "tfidf_matrix.pkl"
        self.master_list_path = self.model_dir / "master_list.pkl"
        self.product_master_path = self.model_dir / "product_master.pkl"

    @property
    def is_loaded(self) -> bool:
        """Check if models are loaded"""
        return self._is_loaded

    @property
    def vectorizer(self) -> TfidfVectorizer | None:
        """Get vectorizer (lazy load)"""
        return self._vectorizer

    @property
    def tfidf_matrix(self):
        """Get TF-IDF matrix (lazy load)"""
        return self._tfidf_matrix

    @property
    def master_list(self) -> list[str]:
        """Get master list (lazy load)"""
        return self._master_list

    @property
    def product_master_df(self) -> pd.DataFrame:
        """Get product master DataFrame with color information"""
        return self._product_master_with_color

    async def ensure_loaded(self, db_session=None):
        """
        Ensure models are loaded before matching
        Runs in background thread to avoid blocking

        Args:
            db_session: Optional database session for auto-reload
        """
        if self._is_loaded:
            return

        async with self._load_lock:
            # Double-check after acquiring lock
            if self._is_loaded:
                return

            # If another coroutine is loading, wait for it
            while self._is_loading:
                await asyncio.sleep(0.1)

            # Start loading
            self._is_loading = True

            try:
                # Try to load from disk first (fast)
                if self.vectorizer_path.exists():
                    await self._load_models_async()
                elif db_session is not None:
                    # No cached models, load from database

                    await self._load_from_database_async(db_session)
                else:
                    # No data available yet
                    pass

                self._is_loaded = True
            finally:
                self._is_loading = False

    async def _load_models_async(self):
        """Load models from disk in background thread"""

        def _load_sync():
            data = {}
            if self.vectorizer_path.exists():
                with open(self.vectorizer_path, "rb") as f:
                    data["vectorizer"] = pickle.load(f)

            if self.tfidf_matrix_path.exists():
                with open(self.tfidf_matrix_path, "rb") as f:
                    data["matrix"] = pickle.load(f)

            if self.master_list_path.exists():
                with open(self.master_list_path, "rb") as f:
                    data["master_list"] = pickle.load(f)

            if self.product_master_path.exists():
                with open(self.product_master_path, "rb") as f:
                    data["product_master"] = pickle.load(f)

            return data

        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(_matching_thread_pool, _load_sync)

        self._vectorizer = data.get("vectorizer")
        self._tfidf_matrix = data.get("matrix")
        self._master_list = data.get("master_list", [])
        self._product_master_with_color = data.get("product_master", pd.DataFrame())

    async def _load_from_database_async(self, db_session):
        """Load product master from database in background"""
        from apps.app_nippon_rfq_matching.app.models.database import ProductMaster

        def _load_sync():
            products = db_session.query(ProductMaster).all()
            products_data = [p.to_dict() for p in products]
            df = pd.DataFrame(products_data)

            if not df.empty and "clean_product_name" in df.columns:
                self._master_list = df["clean_product_name"].tolist()
                self._build_tfidf_matrix_sync()

                # Auto-save for next time
                self._save_models_sync()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(_matching_thread_pool, _load_sync)

    def load_product_master(self, product_master_df: pd.DataFrame):
        """
        Load product master data and build TF-IDF matrix (synchronous)
        This is called after uploading new product data

        Args:
            product_master_df: DataFrame with product master data
        """
        self._product_master_df = product_master_df

        # Store product master with color for color-aware matching
        self._product_master_with_color = product_master_df.copy()

        # Extract clean product names
        if "clean_product_name" in product_master_df.columns:
            self._master_list = product_master_df["clean_product_name"].tolist()
        elif "clean" in product_master_df.columns:
            self._master_list = product_master_df["clean"].tolist()
        else:
            raise ValueError(
                "Product master must have 'clean_product_name' or 'clean' column"
            )

        # Build TF-IDF matrix
        self._build_tfidf_matrix_sync()

        # Mark as loaded
        self._is_loaded = True

        # Save models for next startup
        self.save_models()

    def _build_tfidf_matrix_sync(self):
        """Build TF-IDF matrix from master list (synchronous)"""
        self._vectorizer = TfidfVectorizer()
        self._tfidf_matrix = self._vectorizer.fit_transform(self._master_list)

    def fuzzy_match(self, text: str) -> tuple[str, float, int]:
        """
        Perform fuzzy matching using token set ratio

        Args:
            text: Text to match

        Returns:
            Tuple of (matched_text, score, index)
        """
        match, score, idx = process.extractOne(
            text, self.master_list, scorer=fuzz.token_set_ratio
        )
        return match, score, idx

    def cosine_match(self, text: str) -> tuple[str, float, int]:
        """
        Perform cosine similarity matching using TF-IDF

        Args:
            text: Text to match

        Returns:
            Tuple of (matched_text, score, index)
        """
        vec = self.vectorizer.transform([text])
        scores = cosine_similarity(vec, self.tfidf_matrix)[0]

        idx = int(scores.argmax())
        score = float(scores[idx])

        return self.master_list[idx], score, idx

    def hybrid_match(self, text: str) -> tuple[str, float, str]:
        """
        Perform hybrid matching (fuzzy + cosine) and return best result

        Args:
            text: Text to match

        Returns:
            Tuple of (matched_text, score, method)
        """
        f_match, f_score, f_idx = self.fuzzy_match(text)
        c_match, c_score, c_idx = self.cosine_match(text)

        # Normalize cosine score to 0-100
        c_score_normalized = c_score * 100

        # Choose best match
        if f_score >= c_score_normalized:
            return f_match, f_score, "fuzzy"
        else:
            return c_match, c_score_normalized, "cosine"

    def extract_color_from_text(self, text: str) -> str | None:
        """
        Extract color code/name from RFQ text

        Args:
            text: RFQ text

        Returns:
            Extracted color or None
        """
        import re

        from rapidfuzz import fuzz

        text_upper = text.upper()

        # Common color patterns
        color_patterns = [
            r"\b(\d{3}\s+(?:WHITE|BLACK|GRAY|GREY|RED|BLUE|GREEN|YELLOW|BROWN|ORANGE|PURPLE|PINK|SILVER|GOLD|BEIGE|CREAM|IVORY|NAVY|OLIVE|TEAL|MAROON|LAVENDER|TAN|CYAN|MAGENTA|LIME|CORAL|INDIGO|VIOLET|AQUA|CHARCOAL|BRONZE|COPPER|PLATINUM|PEARL|CHAMPAGNE|CHOCOLATE|COFFEE|CARBON|GRAPHITE|SLATE|SKY|MIDNIGHT|FOREST|ARMY|MOSS|JADE|TURQUOISE|AZURE|CRIMSON|SCARLET|BURGUNDY|PLUM|RASPBERRY|STRAWBERRY|CHERRY|ROSE|LILAC|MAUVE|HEATHER|DOVE|EGGSHELL|ALMOND|BISQUE|VANILLA|BUTTERSCOTCH|CARAMEL|TOFFEE|HONEY|AMBER|GINGER|RUST|TERRACOTTA|SIENNA|MUSTARD|CANARY|LEMON|LIME|MINT|SAGE|PEAR|APRICOT|PEACH|MELON|CORAL|SALMON|TANGERINE|POMEGRENATE|FUCHSIA|MAGENTA|ORCHID|VIOLET|INDIGO|BLUEBERRY|GRAPE|RAISIN|PRUNE|DATE|FIG|CHESTNUT|WALNUT|HAZELNUT|PECAN|MACADAMIA|PISTACHIO|ALMOND|CASHEW|PEANUT|BUTTER|CREAM|YOGURT))\b",
            r"\b(0\d{2})\b",  # Color codes like 060, 442, 355
            r"\b(N\d+\.?\d*)\b",  # NCS color codes
            r"\b(RAL\s*\d+)\b",  # RAL color codes
            r"\b(#[0-9A-Fa-f]{6})\b",  # Hex color codes
        ]

        # Try to extract color code
        for pattern in color_patterns:
            match = re.search(pattern, text_upper)
            if match:
                return match.group(1)

        # Try to match against known colors from product master
        if (
            not self._product_master_with_color.empty
            and "color" in self._product_master_with_color.columns
        ):
            known_colors = (
                self._product_master_with_color["color"].dropna().unique().tolist()
            )

            for color in known_colors:
                color_upper = str(color).upper()
                # Check if color name appears in text
                if color_upper in text_upper:
                    return color
                # Check for fuzzy match
                if fuzz.partial_ratio(color_upper, text_upper) > 85:
                    return color

        return None

    def match_with_color(self, text: str, top_n: int = 10) -> dict[str, Any]:
        """
        Match RFQ item to product master with color awareness

        Args:
            text: RFQ text to match
            top_n: Number of top matches to consider for color filtering

        Returns:
            Dictionary with match results including color information
        """
        # Extract color from RFQ text
        extracted_color = self.extract_color_from_text(text)

        # Get top N matches by product name
        top_matches = self.get_top_matches(text, top_n=top_n)

        if not top_matches:
            return {
                "matched": None,
                "score": 0,
                "method": "no_match",
                "extracted_color": extracted_color,
                "matched_color": None,
                "color_match": False,
            }

        # If RFQ has color, check if product has matching color
        if extracted_color and not self._product_master_with_color.empty:
            # Find best match with matching color
            for match in top_matches:
                matched_text = match["matched"]

                # Find this product in the product master with its color
                product_rows = self._product_master_with_color[
                    self._product_master_with_color["clean_product_name"]
                    == matched_text
                ]

                if not product_rows.empty:
                    for _, product in product_rows.iterrows():
                        product_color = str(product.get("color", "")).strip()

                        # If RFQ has color but product doesn't have color -> NOT MATCH
                        if (
                            not product_color
                            or product_color.upper() == "NONE"
                            or product_color == "-"
                        ):
                            return {
                                "matched": matched_text,
                                "score": match["score"],
                                "method": match["method"],
                                "extracted_color": extracted_color,
                                "matched_color": product.get("color"),
                                "color_match": False,  # Not match because product has no color
                            }

                        # Check color match
                        if self._colors_match(extracted_color, product_color):
                            return {
                                "matched": matched_text,
                                "score": match["score"],
                                "method": match["method"],
                                "extracted_color": extracted_color,
                                "matched_color": product.get("color"),
                                "color_match": True,
                            }

            # No exact color match found, return best product name match
            best_match = top_matches[0]
            return {
                "matched": best_match["matched"],
                "score": best_match["score"],
                "method": best_match["method"],
                "extracted_color": extracted_color,
                "matched_color": None,
                "color_match": False,  # Not match because colors don't match
            }

        # No color extraction, return best match
        best_match = top_matches[0]
        return {
            "matched": best_match["matched"],
            "score": best_match["score"],
            "method": best_match["method"],
            "extracted_color": extracted_color,
            "matched_color": None,
            "color_match": False,
        }

    def _colors_match(self, color1: str, color2: str) -> bool:
        """
        Check if two colors match

        Args:
            color1: First color (extracted from RFQ)
            color2: Second color (from product master)

        Returns:
            True if colors match
        """
        if not color1 or not color2:
            return False

        c1 = str(color1).upper().strip()
        c2 = str(color2).upper().strip()

        # Exact match
        if c1 == c2:
            return True

        # Color code match (e.g., "060" == "060 WHITE")
        if c1 in c2 or c2 in c1:
            return True

        # Common color name variations
        color_aliases = {
            "GREY": "GRAY",
            "GRAY": "GREY",
            "SILVER": ["GRAY", "GREY"],
            "BEIGE": ["CREAM", "IVORY"],
            "WHITE": ["SNOW", "IVORY", "CREAM"],
            "BLACK": ["CHARCOAL", "CARBON", "EBONY"],
            "RED": ["CRIMSON", "SCARLET", "MAROON", "BURGUNDY"],
            "BLUE": ["NAVY", "AZURE", "SKY", "MIDNIGHT"],
            "GREEN": ["FOREST", "OLIVE", "JADE", "MINT", "SAGE"],
            "YELLOW": ["GOLD", "CANARY", "LEMON", "MUSTARD"],
            "BROWN": ["TAN", "BRONZE", "COPPER", "CHOCOLATE", "COFFEE"],
        }

        # Check color aliases
        for base, aliases in color_aliases.items():
            if c1 == base and any(
                a in c2 or c2 == a
                for a in (aliases if isinstance(aliases, list) else [aliases])
            ):
                return True
            if c2 == base and any(
                a in c1 or c1 == a
                for a in (aliases if isinstance(aliases, list) else [aliases])
            ):
                return True

        return False

    def match_rfq_items(self, rfq_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Match multiple RFQ items to product master with color awareness

        Args:
            rfq_items: List of RFQ item dictionaries

        Returns:
            List of match results with structured data (rfq, product_master, match_info)
        """
        results = []

        for item in rfq_items:
            clean_text = item.get("clean_text", "")

            if not clean_text:
                # Clean the text if not already cleaned
                from apps.app_nippon_rfq_matching.app.utils.parsers import (
                    clean_raw_text_rfq,
                )

                clean_text = clean_raw_text_rfq(item.get("raw_text", ""))

            # Use color-aware matching
            match_result = self.match_with_color(clean_text)

            # Structure the result with separated data
            results.append(
                {
                    "rfq": {
                        "raw_text": item.get("raw_text", ""),
                        "clean_text": clean_text,
                        "qty": item.get("qty"),
                        "uom": item.get("uom"),
                        "source": item.get("source", ""),
                    },
                    "product_master": {
                        "clean_product_name": match_result["matched"],
                    },
                    "match_info": {
                        "score": match_result["score"],
                        "method": match_result["method"],
                        "extracted_color": match_result.get("extracted_color"),
                        "color_match": match_result.get("color_match", False),
                    },
                }
            )

        return results

    def get_top_matches(self, text: str, top_n: int = 5) -> list[dict[str, Any]]:
        """
        Get top N matches for a given text

        Args:
            text: Text to match
            top_n: Number of top matches to return

        Returns:
            List of top matches with scores
        """
        # Get fuzzy matches
        fuzzy_matches = process.extract(
            text, self.master_list, scorer=fuzz.token_set_ratio, limit=top_n
        )

        # Get cosine matches
        vec = self.vectorizer.transform([text])
        cosine_scores = cosine_similarity(vec, self.tfidf_matrix)[0]
        top_indices = cosine_scores.argsort()[-top_n:][::-1]

        cosine_matches = [
            (self.master_list[idx], float(cosine_scores[idx]) * 100, idx)
            for idx in top_indices
        ]

        # Combine and return best
        results = []
        seen = set()

        for match, score, idx in fuzzy_matches:
            if match not in seen:
                results.append(
                    {"matched": match, "score": score, "method": "fuzzy", "index": idx}
                )
                seen.add(match)

        for match, score, idx in cosine_matches:
            if match not in seen:
                results.append(
                    {"matched": match, "score": score, "method": "cosine", "index": idx}
                )
                seen.add(match)

        # Sort by score descending
        results.sort(key=lambda x: x["score"], reverse=True)

        return results[:top_n]

    def save_models(self):
        """Save vectorizer and matrix to disk"""
        self._save_models_sync()

    def _save_models_sync(self):
        """Save vectorizer and matrix to disk (synchronous)"""
        with open(self.vectorizer_path, "wb") as f:
            pickle.dump(self._vectorizer, f)

        with open(self.tfidf_matrix_path, "wb") as f:
            pickle.dump(self._tfidf_matrix, f)

        with open(self.master_list_path, "wb") as f:
            pickle.dump(self._master_list, f)

        # Save product master with color for color-aware matching
        with open(self.product_master_path, "wb") as f:
            pickle.dump(self._product_master_with_color, f)

    def load_models(self):
        """Load vectorizer and matrix from disk (synchronous - deprecated, use ensure_loaded)"""
        # This is kept for backward compatibility but should use ensure_loaded instead
        if self.vectorizer_path.exists():
            with open(self.vectorizer_path, "rb") as f:
                self._vectorizer = pickle.load(f)

        if self.tfidf_matrix_path.exists():
            with open(self.tfidf_matrix_path, "rb") as f:
                self._tfidf_matrix = pickle.load(f)

        if self.master_list_path.exists():
            with open(self.master_list_path, "rb") as f:
                self._master_list = pickle.load(f)

        if self.product_master_path.exists():
            with open(self.product_master_path, "rb") as f:
                self._product_master_with_color = pickle.load(f)

        self._is_loaded = True


# Singleton instance
matching_service = MatchingService()
