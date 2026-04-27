"""
Embedding Service for Semantic Search

Service for generating and managing text embeddings using OpenAI API.
Provides caching and batch processing for efficiency.
"""

import hashlib
import logging
import pickle
from pathlib import Path
from typing import Any

import numpy as np
from openai import OpenAI

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.utils.resilience import (
    CircuitBreakerOpenError,
    MaxRetriesExceededError,
    ResilientCallers,
)

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    Service for generating text embeddings using OpenAI API.

    Features:
    - Batch processing for efficiency
    - Caching to reduce API calls
    - Automatic retry on failure
    """

    def __init__(self):
        """Initialize embedding service with OpenAI client."""
        if not settings.OPENAI_API_KEY:
            logger.warning("OPENAI_API_KEY not set, semantic search will be disabled")
            self.client = None
            self.enabled = False
            self.resilient_caller = None
        else:
            # Initialize OpenAI client (retry handled by resilient caller)
            self.client = OpenAI(
                api_key=settings.OPENAI_API_KEY,
                timeout=30.0,  # 30 second timeout
            )
            # Get resilient caller for OpenAI API calls
            self.resilient_caller = ResilientCallers.get_openai_embedding()
            self.enabled = True
            logger.info(
                "Embedding service initialized with resilient caller (circuit breaker + retry)"
            )

        self.model = settings.OPENAI_EMBEDDING_MODEL
        self.dimensions = settings.OPENAI_EMBEDDING_DIMENSIONS

        # Cache for embeddings
        self.cache_dir = Path(settings.VECTOR_DB_PATH) / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, np.ndarray] = {}
        self._load_cache()

    def _load_cache(self):
        """Load embedding cache from disk."""
        cache_file = self.cache_dir / "embeddings_cache.pkl"

        if cache_file.exists():
            try:
                with open(cache_file, "rb") as f:
                    self._cache = pickle.load(f)
                logger.info(f"Loaded {len(self._cache)} cached embeddings")
            except Exception as e:
                logger.warning(f"Failed to load embedding cache: {e}")
                self._cache = {}

    def _save_cache(self):
        """Save embedding cache to disk."""
        cache_file = self.cache_dir / "embeddings_cache.pkl"

        try:
            with open(cache_file, "wb") as f:
                pickle.dump(self._cache, f)
        except Exception as e:
            logger.warning(f"Failed to save embedding cache: {e}")

    def _get_cache_key(self, text: str) -> str:
        """Generate cache key for text."""
        return hashlib.md5(text.encode()).hexdigest()

    def embed_text(self, text: str, use_cache: bool = True) -> np.ndarray | None:
        """
        Generate embedding for a single text.

        Args:
            text: Text to embed
            use_cache: Whether to use cached embeddings

        Returns:
            Embedding vector as numpy array, or None if failed

        Example:
            >>> service = EmbeddingService()
            >>> embedding = service.embed_text("Nippon Paint Marine")
            >>> embedding.shape
            (512,)
        """
        if not self.enabled:
            logger.warning("Embedding service is disabled (no API key)")
            return None

        if not text or not text.strip():
            return None

        # Check cache
        cache_key = self._get_cache_key(text)
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        try:
            # Define the OpenAI API call function
            def _make_openai_call():
                """Internal function to make OpenAI API call."""
                return self.client.embeddings.create(
                    model=self.model, input=text, dimensions=self.dimensions
                )

            # Call OpenAI API with resilient caller (circuit breaker + retry)
            try:
                response = self.resilient_caller.call(_make_openai_call)
            except CircuitBreakerOpenError as e:
                logger.error(f"Circuit breaker is OPEN for embedding service: {e}")
                return None
            except MaxRetriesExceededError as e:
                logger.error(f"Max retries exceeded for embedding service: {e}")
                return None
            except Exception as e:
                logger.error(f"Failed to generate embedding: {e}")
                return None

            embedding = np.array(response.data[0].embedding, dtype=np.float32)

            # Cache the result
            if use_cache:
                self._cache[cache_key] = embedding
                # Periodically save cache (every 100 embeddings)
                if len(self._cache) % 100 == 0:
                    self._save_cache()

            return embedding

        except Exception as e:
            logger.error(f"Unexpected error in embed_text: {e}")
            return None

    def embed_batch(
        self, texts: list[str], use_cache: bool = True, batch_size: int = 100
    ) -> list[np.ndarray | None]:
        """
        Generate embeddings for multiple texts in batches.

        Args:
            texts: List of texts to embed
            use_cache: Whether to use cached embeddings
            batch_size: Number of texts per batch

        Returns:
            List of embedding vectors (same order as input)

        Example:
            >>> service = EmbeddingService()
            >>> embeddings = service.embed_batch(["Text 1", "Text 2", "Text 3"])
            >>> len(embeddings)
            3
        """
        if not self.enabled:
            logger.warning("Embedding service is disabled (no API key)")
            return [None] * len(texts)

        if not texts:
            return []

        results = [None] * len(texts)
        remaining_indices = []
        remaining_texts = []

        # Check cache first
        for i, text in enumerate(texts):
            if not text or not text.strip():
                results[i] = None
                continue

            cache_key = self._get_cache_key(text)
            if use_cache and cache_key in self._cache:
                results[i] = self._cache[cache_key]
            else:
                remaining_indices.append(i)
                remaining_texts.append(text)

        # Process remaining texts in batches
        if remaining_texts:
            for start_idx in range(0, len(remaining_texts), batch_size):
                batch_end = min(start_idx + batch_size, len(remaining_texts))
                batch_texts = remaining_texts[start_idx:batch_end]

                try:
                    response = self.client.embeddings.create(
                        model=self.model, input=batch_texts, dimensions=self.dimensions
                    )

                    for j, item in enumerate(response.data):
                        original_idx = remaining_indices[start_idx + j]
                        embedding = np.array(item.embedding, dtype=np.float32)
                        results[original_idx] = embedding

                        # Cache the result
                        if use_cache:
                            cache_key = self._get_cache_key(batch_texts[j])
                            self._cache[cache_key] = embedding

                except Exception as e:
                    logger.error(f"Failed to embed batch {start_idx}-{batch_end}: {e}")
                    # Set failed embeddings to None
                    for j in range(batch_end - start_idx):
                        original_idx = remaining_indices[start_idx + j]
                        results[original_idx] = None

        # Save cache after batch processing
        if use_cache and remaining_texts:
            self._save_cache()

        return results

    def embed_products(
        self, products: list[dict[str, Any]], text_field: str = "clean_product_name"
    ) -> dict[int, np.ndarray]:
        """
        Generate embeddings for a list of products.

        Args:
            products: List of product dictionaries
            text_field: Field name containing the text to embed

        Returns:
            Dictionary mapping product_id to embedding vector

        Example:
            >>> products = [{"id": 1, "clean_product_name": "Nippon Paint"}, ...]
            >>> service = EmbeddingService()
            >>> embeddings = service.embed_products(products)
            >>> embeddings[1].shape
            (512,)
        """
        if not self.enabled:
            logger.warning("Embedding service is disabled")
            return {}

        # Extract texts and product IDs
        product_map = {}
        texts = []

        for product in products:
            product_id = product.get("id")
            text = product.get(text_field, "")

            if product_id is not None and text:
                product_map[len(texts)] = product_id
                texts.append(text)

        if not texts:
            return {}

        # Generate embeddings
        embeddings = self.embed_batch(texts)

        # Map back to product IDs
        result = {}
        for i, embedding in enumerate(embeddings):
            if embedding is not None:
                product_id = product_map.get(i)
                if product_id is not None:
                    result[product_id] = embedding

        logger.info(f"Generated {len(result)} embeddings for {len(products)} products")

        return result

    def calculate_similarity(
        self, embedding1: np.ndarray, embedding2: np.ndarray
    ) -> float:
        """
        Calculate cosine similarity between two embeddings.

        Args:
            embedding1: First embedding vector
            embedding2: Second embedding vector

        Returns:
            Similarity score between 0 and 1

        Example:
            >>> emb1 = service.embed_text("Nippon Paint")
            >>> emb2 = service.embed_text("Nippon Marine Paint")
            >>> similarity = service.calculate_similarity(emb1, emb2)
            >>> 0.8 < similarity < 1.0
            True
        """
        if embedding1 is None or embedding2 is None:
            return 0.0

        # Normalize vectors
        norm1 = np.linalg.norm(embedding1)
        norm2 = np.linalg.norm(embedding2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        # Cosine similarity
        similarity = np.dot(embedding1, embedding2) / (norm1 * norm2)

        return float(similarity)

    def find_similar(
        self,
        query_embedding: np.ndarray,
        product_embeddings: dict[int, np.ndarray],
        top_k: int = 5,
        threshold: float = 0.0,
    ) -> list[dict[str, Any]]:
        """
        Find most similar products to a query embedding.

        Args:
            query_embedding: Query embedding vector
            product_embeddings: Dictionary mapping product_id to embedding
            top_k: Number of top results to return
            threshold: Minimum similarity score

        Returns:
            List of dictionaries with product_id and similarity score

        Example:
            >>> query = service.embed_text("marine paint")
            >>> results = service.find_similar(query, embeddings, top_k=3)
            >>> results[0]
            {'product_id': 123, 'similarity': 0.92}
        """
        if query_embedding is None or not product_embeddings:
            return []

        similarities = []

        for product_id, embedding in product_embeddings.items():
            similarity = self.calculate_similarity(query_embedding, embedding)

            if similarity >= threshold:
                similarities.append(
                    {"product_id": product_id, "similarity": similarity}
                )

        # Sort by similarity (descending)
        similarities.sort(key=lambda x: x["similarity"], reverse=True)

        # Return top k
        return similarities[:top_k]


# Singleton instance
embedding_service = EmbeddingService()
