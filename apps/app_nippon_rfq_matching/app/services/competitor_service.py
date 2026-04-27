"""
Competitor Matrix Service

Service for handling competitor matrix Excel file uploads,
parsing, and database operations.
"""

import logging
from typing import Any

import pandas as pd
from sqlalchemy import and_
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.models import (
    Brand,
    CompetitorProduct,
    Generic,
    ProductEquivalent,
)
from apps.app_nippon_rfq_matching.app.models.schemas import (
    BrandResponse,
    CompetitorMatrixUploadResponse,
    GenericResponse,
)
from apps.app_nippon_rfq_matching.app.utils.competitor_matrix_reader import (
    CompetitorMatrixData,
    CompetitorMatrixReader,
)

logger = logging.getLogger(__name__)


class CompetitorService:
    """
    Service for managing competitor matrix data.

    Handles:
    - Parsing competitor matrix Excel files
    - Storing generics, brands, and products
    - Managing product equivalences
    """

    def __init__(self, db: Session):
        """
        Initialize service with database session.

        Args:
            db: SQLAlchemy database session
        """
        self.db = db

    def parse_excel_file(
        self, file_path: str, sheet_name: str = None, header_row: int = None
    ) -> CompetitorMatrixData:
        """
        Parse competitor matrix Excel file.

        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name to parse (default: first sheet)
            header_row: Header row number (default: 5)

        Returns:
            CompetitorMatrixData with parsed data
        """
        reader = CompetitorMatrixReader(header_row=header_row)
        return reader.parse_competitor_matrix(file_path, sheet_name)

    def import_competitor_matrix(
        self, file_path: str, sheet_name: str = None, header_row: int = None
    ) -> CompetitorMatrixUploadResponse:
        """
        Import competitor matrix from Excel file to database.

        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name to parse (default: first sheet)
            header_row: Header row number (default: 5)

        Returns:
            CompetitorMatrixUploadResponse with import results
        """
        logger.info(f"Importing competitor matrix from {file_path}")

        # Parse Excel file
        data = self.parse_excel_file(file_path, sheet_name, header_row)

        logger.info(
            f"Parsed {len(data.generics)} generics, {len(data.brands)} brands, {len(data.products)} products"
        )

        # Import generics
        generics_map = self._import_generics(data.generics)

        # Import brands
        brands_map = self._import_brands(data.brands)

        # Import products and equivalences
        products_count, equivalents_count = self._import_products_and_equivalents(
            data.products, generics_map, brands_map, data.default_df
        )

        # Create response
        response = CompetitorMatrixUploadResponse(
            status="success",
            generics_count=len(data.generics),
            brands_count=len(data.brands),
            products_count=products_count,
            equivalents_count=equivalents_count,
            generics=[
                GenericResponse(id=gid, name=name) for name, gid in generics_map.items()
            ],
            brands=[
                BrandResponse(id=bid, name=name) for name, bid in brands_map.items()
            ],
        )

        logger.info(
            f"Import completed: {response.generics_count} generics, "
            f"{response.brands_count} brands, {response.products_count} products, "
            f"{response.equivalents_count} equivalents"
        )

        return response

    def _import_generics(self, generic_names: list[str]) -> dict[str, int]:
        """
        Import generic names to database.

        Args:
            generic_names: List of generic names

        Returns:
            Dictionary mapping generic name to ID
        """
        generics_map = {}

        for name in generic_names:
            if not name:
                continue

            # Try to get existing or create new
            generic = self.db.query(Generic).filter(Generic.name == name).first()

            if generic is None:
                generic = Generic(name=name)
                self.db.add(generic)
                self.db.flush()
                logger.debug(f"Created new generic: {name}")
            else:
                logger.debug(f"Using existing generic: {name}")

            generics_map[name] = generic.id

        return generics_map

    def _import_brands(self, brand_names: list[str]) -> dict[str, int]:
        """
        Import brand names to database.

        Args:
            brand_names: List of brand names

        Returns:
            Dictionary mapping brand name to ID
        """
        brands_map = {}

        for name in brand_names:
            if not name:
                continue

            # Try to get existing or create new
            brand = self.db.query(Brand).filter(Brand.name == name).first()

            if brand is None:
                brand = Brand(name=name)
                self.db.add(brand)
                self.db.flush()
                logger.debug(f"Created new brand: {name}")
            else:
                logger.debug(f"Using existing brand: {name}")

            brands_map[name] = brand.id

        return brands_map

    def _import_products_and_equivalents(
        self,
        products: list[dict[str, str]],
        generics_map: dict[str, int],
        brands_map: dict[str, int],
        default_df: pd.DataFrame,
    ) -> tuple[int, int]:
        """
        Import products and their equivalences to database.

        Creates direct 1-to-1 mappings between competitor products and Nippon products
        from the same row in the matrix.

        Processes the DataFrame row by row to maintain correct 1-to-1 relationships.

        Args:
            products: List of product dicts with keys: generic, brand, product (not used, we use df)
            generics_map: Mapping of generic name to ID (not used in new schema)
            brands_map: Mapping of brand name to ID
            default_df: DataFrame with the original matrix structure

        Returns:
            Tuple of (products_count, equivalents_count)
        """
        products_count = 0
        equivalents_count = 0

        # Brand names that indicate Nippon products
        nippon_brands = {"NP", "NP MARINE", "NIPPON"}

        # Get competitor brand columns (all columns except GENERIC)
        competitor_brands = [col for col in default_df.columns if col != "GENERIC"]

        # Process DataFrame row by row
        for _, row in default_df.iterrows():
            generic = row.get("GENERIC")

            # Skip rows without generic
            if pd.isna(generic) or str(generic).strip() == "":
                continue

            generic = str(generic).strip()

            # Find Nippon product in this row
            nippon_product_name = None
            for brand in nippon_brands:
                if brand in default_df.columns:
                    product = row.get(brand)
                    if (
                        pd.notna(product)
                        and str(product).strip() != ""
                        and str(product).strip() != "-"
                    ):
                        nippon_product_name = str(product).strip()
                        break

            # Skip if no Nippon product in this row
            if not nippon_product_name:
                logger.debug(f"Skipping row '{generic}': No Nippon product found")
                continue

            # Process each competitor brand in this row
            for brand in competitor_brands:
                # Skip Nippon brands
                if brand.upper() in nippon_brands:
                    continue

                # Skip if brand not in brands_map
                if brand not in brands_map:
                    continue

                product_name = row.get(brand)

                # Skip empty cells
                if (
                    pd.isna(product_name)
                    or str(product_name).strip() == ""
                    or str(product_name).strip() == "-"
                ):
                    continue

                product_name = str(product_name).strip()
                brand_id = brands_map[brand]

                # Create or get competitor product
                product = (
                    self.db.query(CompetitorProduct)
                    .filter(
                        and_(
                            CompetitorProduct.brand_id == brand_id,
                            CompetitorProduct.name == product_name,
                        )
                    )
                    .first()
                )

                if product is None:
                    product = CompetitorProduct(brand_id=brand_id, name=product_name)
                    self.db.add(product)
                    self.db.flush()
                    products_count += 1
                    logger.debug(f"Created new product: {product_name} ({brand})")

                # Create direct equivalence: competitor_product -> nippon_product_name
                existing = (
                    self.db.query(ProductEquivalent)
                    .filter(
                        and_(
                            ProductEquivalent.competitor_product_id == product.id,
                            ProductEquivalent.nippon_product_name
                            == nippon_product_name,
                        )
                    )
                    .first()
                )

                if existing is None:
                    equivalence = ProductEquivalent(
                        competitor_product_id=product.id,
                        nippon_product_name=nippon_product_name,
                    )
                    self.db.add(equivalence)
                    equivalents_count += 1
                    logger.debug(
                        f"Created equivalence: {product_name} ({brand}) -> {nippon_product_name}"
                    )

        return products_count, equivalents_count

    def get_all_generics(self, skip: int = 0, limit: int = 100) -> list[Generic]:
        """
        Get all generics.

        Args:
            skip: Number of records to skip
            limit: Maximum records to return

        Returns:
            List of Generic objects
        """
        return self.db.query(Generic).offset(skip).limit(limit).all()

    def get_all_brands(self, skip: int = 0, limit: int = 100) -> list[Brand]:
        """
        Get all brands.

        Args:
            skip: Number of records to skip
            limit: Maximum records to return

        Returns:
            List of Brand objects
        """
        return self.db.query(Brand).offset(skip).limit(limit).all()

    def get_brand_by_name(self, name: str) -> Brand | None:
        """
        Get brand by name.

        Args:
            name: Brand name

        Returns:
            Brand object or None
        """
        return self.db.query(Brand).filter(Brand.name == name).first()

    def get_generic_by_name(self, name: str) -> Generic | None:
        """
        Get generic by name.

        Args:
            name: Generic name

        Returns:
            Generic object or None
        """
        return self.db.query(Generic).filter(Generic.name == name).first()

    def get_products_by_brand(self, brand_id: int) -> list[CompetitorProduct]:
        """
        Get all products for a brand.

        Args:
            brand_id: Brand ID

        Returns:
            List of CompetitorProduct objects
        """
        return (
            self.db.query(CompetitorProduct)
            .filter(CompetitorProduct.brand_id == brand_id)
            .all()
        )

    def get_products_by_generic(self, generic_id: int) -> list[CompetitorProduct]:
        """
        Get all equivalent products for a generic.

        Args:
            generic_id: Generic ID

        Returns:
            List of CompetitorProduct objects
        """
        return (
            self.db.query(CompetitorProduct)
            .join(
                ProductEquivalent, ProductEquivalent.product_id == CompetitorProduct.id
            )
            .filter(ProductEquivalent.generic_id == generic_id)
            .all()
        )

    def get_equivalent_products(self, generic_name: str) -> list[dict[str, Any]]:
        """
        Get all equivalent products for a generic name.

        Args:
            generic_name: Generic product name

        Returns:
            List of dicts with product and brand info
        """
        generic = self.get_generic_by_name(generic_name)
        if not generic:
            return []

        products = self.get_products_by_generic(generic.id)

        result = []
        for product in products:
            result.append(
                {
                    "id": product.id,
                    "name": product.name,
                    "brand": {"id": product.brand.id, "name": product.brand.name}
                    if product.brand
                    else None,
                }
            )

        return result

    def search_products(self, query: str, limit: int = 20) -> list[CompetitorProduct]:
        """
        Search products by name.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of CompetitorProduct objects
        """
        return (
            self.db.query(CompetitorProduct)
            .filter(CompetitorProduct.name.ilike(f"%{query}%"))
            .limit(limit)
            .all()
        )

    def find_nippon_equivalents_by_competitor(
        self, competitor_product: str, exact_match: bool = False
    ) -> list[dict[str, Any]]:
        """
        Find Nippon products equivalent to a competitor product.

        This searches for competitor products matching the given name
        and returns directly mapped Nippon products.

        Args:
            competitor_product: Competitor product name to find Nippon equivalents for
            exact_match: If True, only exact product matches; if False, uses partial matching

        Returns:
            List of dicts containing:
            - competitor_product: Competitor product name
            - competitor_product_id: ID of the competitor product
            - brand: Competitor brand information
            - nippon_products: List of Nippon product names (direct mapping)
        """
        # Search for matching competitor products
        if exact_match:
            products = (
                self.db.query(CompetitorProduct)
                .filter(CompetitorProduct.name == competitor_product)
                .all()
            )
        else:
            # Partial match - search for products containing the name
            products = (
                self.db.query(CompetitorProduct)
                .filter(CompetitorProduct.name.ilike(f"%{competitor_product}%"))
                .all()
            )

        result = []
        for product in products:
            # Get directly mapped Nippon products for this competitor product
            equivalents = (
                self.db.query(ProductEquivalent)
                .filter(ProductEquivalent.competitor_product_id == product.id)
                .all()
            )

            # Collect unique Nippon product names
            nippon_products = []
            for eq in equivalents:
                if eq.nippon_product_name:
                    # Check if already added (avoid duplicates)
                    if not any(
                        n["product_name"] == eq.nippon_product_name
                        for n in nippon_products
                    ):
                        nippon_products.append(
                            {
                                "product_name": eq.nippon_product_name,
                            }
                        )

            if nippon_products:  # Only add if there are Nippon products found
                result.append(
                    {
                        "competitor_product": product.name,
                        "competitor_product_id": product.id,
                        "brand": {"id": product.brand.id, "name": product.brand.name}
                        if product.brand
                        else None,
                        "nippon_products": nippon_products,
                        "nippon_products_count": len(nippon_products),
                    }
                )

        return result

    def find_competitor_equivalents(
        self, nippon_product: str, exact_match: bool = False
    ) -> list[dict[str, Any]]:
        """
        Find competitor products equivalent to a Nippon product.

        This searches for generics that match the Nippon product name
        and returns all competitor products equivalent to those generics.

        Args:
            nippon_product: Nippon product name or category to find equivalents for
            exact_match: If True, only exact generic matches; if False, uses partial matching

        Returns:
            List of dicts containing:
            - generic: Generic name that matched
            - generic_id: ID of the generic
            - products: List of equivalent competitor products with brand info
        """

        # Search for matching generics
        if exact_match:
            generics = (
                self.db.query(Generic).filter(Generic.name == nippon_product).all()
            )
        else:
            # Partial match - search for generics containing the product name
            generics = (
                self.db.query(Generic)
                .filter(Generic.name.ilike(f"%{nippon_product}%"))
                .all()
            )

            if not generics:
                # Try reverse - check if product name contains generic name
                # Using LIKE with reversed pattern
                generics = (
                    self.db.query(Generic)
                    .filter(Generic.name.ilike(f"%{nippon_product}%"))
                    .all()
                )

        result = []
        for generic in generics:
            # Get all competitor products for this generic
            products = self.get_products_by_generic(generic.id)

            product_list = []
            for product in products:
                product_list.append(
                    {
                        "id": product.id,
                        "name": product.name,
                        "brand": {"id": product.brand.id, "name": product.brand.name}
                        if product.brand
                        else None,
                    }
                )

            if product_list:  # Only add if there are equivalent products
                result.append(
                    {
                        "generic": generic.name,
                        "generic_id": generic.id,
                        "products": product_list,
                        "products_count": len(product_list),
                    }
                )

        return result

    def search_generics(self, query: str, limit: int = 20) -> list[Generic]:
        """
        Search generics by name.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of Generic objects
        """
        return (
            self.db.query(Generic)
            .filter(Generic.name.ilike(f"%{query}%"))
            .limit(limit)
            .all()
        )

    def delete_generic(self, generic_id: int) -> bool:
        """
        Delete a generic and its equivalences.

        Args:
            generic_id: Generic ID

        Returns:
            True if deleted, False if not found
        """
        generic = self.db.query(Generic).filter(Generic.id == generic_id).first()
        if generic:
            # Delete equivalences first (foreign key constraint)
            self.db.query(ProductEquivalent).filter(
                ProductEquivalent.generic_id == generic_id
            ).delete()
            # Delete generic
            self.db.delete(generic)
            return True
        return False

    def delete_brand(self, brand_id: int) -> bool:
        """
        Delete a brand and its products.

        Args:
            brand_id: Brand ID

        Returns:
            True if deleted, False if not found
        """
        brand = self.db.query(Brand).filter(Brand.id == brand_id).first()
        if brand:
            # Delete equivalences for products of this brand
            product_ids = [
                p.id
                for p in self.db.query(CompetitorProduct.id)
                .filter(CompetitorProduct.brand_id == brand_id)
                .all()
            ]
            if product_ids:
                self.db.query(ProductEquivalent).filter(
                    ProductEquivalent.competitor_product_id.in_(product_ids)
                ).delete(synchronize_session=False)
            # Delete products
            self.db.query(CompetitorProduct).filter(
                CompetitorProduct.brand_id == brand_id
            ).delete()
            # Delete brand
            self.db.delete(brand)
            return True
        return False
