"""
Vector Store Service using FAISS

Service for storing and searching embeddings using FAISS (Facebook AI Similarity Search).
Provides efficient similarity search for large datasets.
"""

import logging
import pickle
from pathlib import Path
from typing import Any

import faiss
import numpy as np

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.services.embedding_service import (
    embedding_service,
)

logger = logging.getLogger(__name__)


class VectorStore:
    """
    Vector store using FAISS for efficient similarity search.

    Features:
    - FAISS index for fast similarity search
    - Persistent storage on disk
    - Support for product master and RFQ item embeddings
    - Batch add and search operations
    """

    def __init__(self, index_type: str = "flat"):
        """
        Initialize vector store.

        Args:
            index_type: Type of FAISS index ("flat" for exact search, "ivf" for approximate)
        """
        self.index_type = index_type
        self.dimensions = settings.OPENAI_EMBEDDING_DIMENSIONS

        # Storage directory
        self.storage_dir = Path(settings.VECTOR_DB_PATH)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # Indexes
        self.product_master_index: faiss.Index | None = None
        self.rfq_items_index: faiss.Index | None = None

        # Mappings: index -> product_id
        self.product_master_ids: list[int] = []
        self.rfq_item_ids: list[int] = []

        # Load existing indexes if available
        self._load_indexes()

    def _get_index_path(self, index_name: str) -> Path:
        """Get path for storing index."""
        return self.storage_dir / f"{index_name}.faiss"

    def _get_mapping_path(self, index_name: str) -> Path:
        """Get path for storing ID mappings."""
        return self.storage_dir / f"{index_name}_mapping.pkl"

    def _create_index(self, size: int = 0) -> faiss.Index:
        """
        Create a new FAISS index.

        Args:
            size: Initial size for IVF index (ignored for Flat index)

        Returns:
            FAISS index
        """
        if self.index_type == "ivf":
            # IVF (Inverted File Index) - faster for large datasets
            quantizer = faiss.IndexFlatL2(self.dimensions)
            index = faiss.IndexIVFFlat(quantizer, self.dimensions, min(size, 100))
        else:
            # Flat index - exact search, slower but more accurate
            index = faiss.IndexFlatL2(self.dimensions)

        return index

    def _load_indexes(self):
        """Load existing indexes from disk."""
        # Load product master index
        pm_index_path = self._get_index_path("product_master")
        if pm_index_path.exists():
            try:
                self.product_master_index = faiss.read_index(str(pm_index_path))

                # Load ID mapping
                mapping_path = self._get_mapping_path("product_master")
                if mapping_path.exists():
                    with open(mapping_path, "rb") as f:
                        self.product_master_ids = pickle.load(f)

                logger.info(
                    f"Loaded product master index with {len(self.product_master_ids)} items"
                )
            except Exception as e:
                logger.warning(f"Failed to load product master index: {e}")
                self.product_master_index = None
                self.product_master_ids = []

        # Load RFQ items index
        rfq_index_path = self._get_index_path("rfq_items")
        if rfq_index_path.exists():
            try:
                self.rfq_items_index = faiss.read_index(str(rfq_index_path))

                # Load ID mapping
                mapping_path = self._get_mapping_path("rfq_items")
                if mapping_path.exists():
                    with open(mapping_path, "rb") as f:
                        self.rfq_item_ids = pickle.load(f)

                logger.info(
                    f"Loaded RFQ items index with {len(self.rfq_item_ids)} items"
                )
            except Exception as e:
                logger.warning(f"Failed to load RFQ items index: {e}")
                self.rfq_items_index = None
                self.rfq_item_ids = []

    def _save_indexes(self):
        """Save indexes to disk."""
        # Save product master index
        if self.product_master_index is not None:
            try:
                pm_index_path = self._get_index_path("product_master")
                faiss.write_index(self.product_master_index, str(pm_index_path))

                # Save ID mapping
                mapping_path = self._get_mapping_path("product_master")
                with open(mapping_path, "wb") as f:
                    pickle.dump(self.product_master_ids, f)

            except Exception as e:
                logger.warning(f"Failed to save product master index: {e}")

        # Save RFQ items index
        if self.rfq_items_index is not None:
            try:
                rfq_index_path = self._get_index_path("rfq_items")
                faiss.write_index(self.rfq_items_index, str(rfq_index_path))

                # Save ID mapping
                mapping_path = self._get_mapping_path("rfq_items")
                with open(mapping_path, "wb") as f:
                    pickle.dump(self.rfq_item_ids, f)

            except Exception as e:
                logger.warning(f"Failed to save RFQ items index: {e}")

    def add_product_master_embeddings(
        self, embeddings: dict[int, np.ndarray], save: bool = True
    ):
        """
        Add product master embeddings to the index.

        Args:
            embeddings: Dictionary mapping product_id to embedding vector
            save: Whether to save index to disk after adding
        """
        if not embeddings:
            return

        # Convert to numpy array
        ids = list(embeddings.keys())
        vectors = np.array([embeddings[id] for id in ids], dtype=np.float32)

        # Create index if not exists
        if self.product_master_index is None:
            self.product_master_index = self._create_index(len(vectors))

            # Train if using IVF index
            if self.index_type == "ivf" and not self.product_master_index.is_trained:
                self.product_master_index.train(vectors)

        # Add to index
        len(self.product_master_ids)
        self.product_master_index.add(vectors)

        # Update ID mapping
        self.product_master_ids.extend(ids)

        logger.info(
            f"Added {len(ids)} product master embeddings to index (total: {len(self.product_master_ids)})"
        )

        if save:
            self._save_indexes()

    def add_rfq_item_embeddings(
        self, embeddings: dict[int, np.ndarray], save: bool = True
    ):
        """
        Add RFQ item embeddings to the index.

        Args:
            embeddings: Dictionary mapping rfq_item_id to embedding vector
            save: Whether to save index to disk after adding
        """
        if not embeddings:
            return

        # Convert to numpy array
        ids = list(embeddings.keys())
        vectors = np.array([embeddings[id] for id in ids], dtype=np.float32)

        # Create index if not exists
        if self.rfq_items_index is None:
            self.rfq_items_index = self._create_index(len(vectors))

            # Train if using IVF index
            if self.index_type == "ivf" and not self.rfq_items_index.is_trained:
                self.rfq_items_index.train(vectors)

        # Add to index
        len(self.rfq_item_ids)
        self.rfq_items_index.add(vectors)

        # Update ID mapping
        self.rfq_item_ids.extend(ids)

        logger.info(
            f"Added {len(ids)} RFQ item embeddings to index (total: {len(self.rfq_item_ids)})"
        )

        if save:
            self._save_indexes()

    def search_product_master(
        self, query_embedding: np.ndarray, top_k: int = 5, threshold: float = 0.0
    ) -> list[dict[str, Any]]:
        """
        Search product master by embedding similarity.

        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return
            threshold: Minimum similarity score (0-1, where 1 is exact match)

        Returns:
            List of dictionaries with product_id and similarity score

        Example:
            >>> query = embedding_service.embed_text("marine paint")
            >>> results = vector_store.search_product_master(query, top_k=3)
            >>> results[0]
            {'product_id': 123, 'similarity': 0.92}
        """
        if query_embedding is None:
            return []

        if self.product_master_index is None or len(self.product_master_ids) == 0:
            logger.warning("Product master index is empty")
            return []

        # Reshape query for FAISS
        query_vector = query_embedding.reshape(1, -1).astype(np.float32)

        # Search
        k = min(top_k, len(self.product_master_ids))
        distances, indices = self.product_master_index.search(query_vector, k)

        # Convert distances to similarities (FAISS returns L2 distance)
        # L2 distance ranges from 0 to infinity, where 0 is exact match
        # Convert to similarity: 1 / (1 + distance)
        results = []
        for idx, distance in zip(indices[0], distances[0]):
            if idx == -1:  # FAISS returns -1 for not found
                continue

            # Convert L2 distance to similarity score
            # Using formula: 1 / (1 + distance)
            # This gives us a score between 0 and 1
            similarity = 1.0 / (1.0 + float(distance))

            if similarity >= threshold:
                results.append(
                    {
                        "product_id": self.product_master_ids[int(idx)],
                        "similarity": similarity,
                    }
                )

        logger.debug(
            f"Found {len(results)} product master matches (threshold: {threshold})"
        )

        return results

    def search_rfq_items(
        self, query_embedding: np.ndarray, top_k: int = 5, threshold: float = 0.0
    ) -> list[dict[str, Any]]:
        """
        Search RFQ items by embedding similarity.

        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return
            threshold: Minimum similarity score

        Returns:
            List of dictionaries with rfq_item_id and similarity score
        """
        if query_embedding is None:
            return []

        if self.rfq_items_index is None or len(self.rfq_item_ids) == 0:
            logger.warning("RFQ items index is empty")
            return []

        # Reshape query for FAISS
        query_vector = query_embedding.reshape(1, -1).astype(np.float32)

        # Search
        k = min(top_k, len(self.rfq_item_ids))
        distances, indices = self.rfq_items_index.search(query_vector, k)

        # Convert to similarity scores
        results = []
        for idx, distance in zip(indices[0], distances[0]):
            if idx == -1:
                continue

            similarity = 1.0 / (1.0 + float(distance))

            if similarity >= threshold:
                results.append(
                    {
                        "rfq_item_id": self.rfq_item_ids[int(idx)],
                        "similarity": similarity,
                    }
                )

        return results

    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about the vector store.

        Returns:
            Dictionary with statistics
        """
        stats = {
            "product_master_count": len(self.product_master_ids)
            if self.product_master_index
            else 0,
            "rfq_items_count": len(self.rfq_item_ids) if self.rfq_items_index else 0,
            "dimensions": self.dimensions,
            "index_type": self.index_type,
            "enabled": embedding_service.enabled,
        }

        return stats

    def clear_product_master_index(self):
        """Clear the product master index."""
        self.product_master_index = None
        self.product_master_ids = []
        logger.info("Cleared product master index")

    def clear_rfq_items_index(self):
        """Clear the RFQ items index."""
        self.rfq_items_index = None
        self.rfq_item_ids = []
        logger.info("Cleared RFQ items index")

    def rebuild_product_master_index(self, products: list[dict[str, Any]]):
        """
        Rebuild the product master index from scratch.

        Args:
            products: List of product dictionaries with 'id' and 'clean_product_name'
        """
        logger.info(f"Rebuilding product master index with {len(products)} products")

        # Clear existing index
        self.clear_product_master_index()

        # Generate embeddings
        embeddings = embedding_service.embed_products(products)

        # Add to index
        self.add_product_master_embeddings(embeddings, save=True)

        logger.info(f"Rebuilt product master index with {len(embeddings)} embeddings")

    def rebuild_rfq_items_index(self, rfq_items: list[dict[str, Any]]):
        """
        Rebuild the RFQ items index from scratch.

        Args:
            rfq_items: List of RFQ item dictionaries with 'id' and 'clean_text'
        """
        logger.info(f"Rebuilding RFQ items index with {len(rfq_items)} items")

        # Clear existing index
        self.clear_rfq_items_index()

        # Generate embeddings
        texts = {item["id"]: item.get("clean_text", "") for item in rfq_items}
        embeddings = {}

        for item_id, text in texts.items():
            if text:
                embedding = embedding_service.embed_text(text)
                if embedding is not None:
                    embeddings[item_id] = embedding

        # Add to index
        self.add_rfq_item_embeddings(embeddings, save=True)

        logger.info(f"Rebuilt RFQ items index with {len(embeddings)} embeddings")


# Singleton instance
vector_store = VectorStore()
