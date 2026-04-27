"""
Embedding Service for Normalization Cache

This service handles generating and managing embeddings for semantic search.
It runs automatically in the background during caching operations.
"""

import logging
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Global embedding model cache
_embedding_model = None
_embedding_model_name = None


def get_embedding_model(model_name: str = "all-MiniLM-L6-v2"):
    """
    Get or load sentence transformer model (cached singleton).

    Args:
        model_name: Name of the sentence-transformer model

    Returns:
        SentenceTransformer model instance
    """
    global _embedding_model, _embedding_model_name

    if _embedding_model is None or _embedding_model_name != model_name:
        try:
            from sentence_transformers import SentenceTransformer

            logger.info(f"Loading embedding model: {model_name}")
            _embedding_model = SentenceTransformer(model_name)
            _embedding_model_name = model_name
            logger.info("Embedding model loaded successfully")
        except ImportError:
            logger.warning("sentence_transformers not installed, embeddings disabled")
            _embedding_model = None
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            _embedding_model = None

    return _embedding_model


def generate_embedding(
    text: str, model_name: str = "all-MiniLM-L6-v2"
) -> list[float] | None:
    """
    Generate embedding for a single text.

    Args:
        text: Text to embed
        model_name: Name of the embedding model

    Returns:
        List of floats representing the embedding, or None if failed
    """
    if not text or not text.strip():
        return None

    model = get_embedding_model(model_name)
    if model is None:
        return None

    try:
        embedding = model.encode(text.strip(), convert_to_numpy=True)
        # Convert to list for JSON serialization
        return embedding.tolist()
    except Exception as e:
        logger.error(f"Failed to generate embedding for '{text[:50]}...': {e}")
        return None


def generate_embeddings_batch(
    texts: list[str], model_name: str = "all-MiniLM-L6-v2"
) -> list[list[float] | None]:
    """
    Generate embeddings for multiple texts (batch processing for efficiency).

    Args:
        texts: List of texts to embed
        model_name: Name of the embedding model

    Returns:
        List of embedding lists (or None for failed items)
    """
    if not texts:
        return []

    model = get_embedding_model(model_name)
    if model is None:
        return [None] * len(texts)

    try:
        # Filter out empty texts but keep track of indices
        non_empty_texts = [
            (i, t.strip()) for i, t in enumerate(texts) if t and t.strip()
        ]
        if not non_empty_texts:
            return [None] * len(texts)

        indices, clean_texts = zip(*non_empty_texts)

        # Batch encode
        embeddings = model.encode(clean_texts, convert_to_numpy=True)

        # Map back to original indices
        result = [None] * len(texts)
        for idx, embedding in zip(indices, embeddings):
            result[idx] = embedding.tolist()

        return result
    except Exception as e:
        logger.error(f"Failed to generate batch embeddings: {e}")
        return [None] * len(texts)


def compute_cosine_similarity(
    embedding1: list[float], embedding2: list[float]
) -> float:
    """
    Compute cosine similarity between two embeddings.

    Args:
        embedding1: First embedding vector
        embedding2: Second embedding vector

    Returns:
        Similarity score between -1 and 1 (1 = identical)
    """
    try:
        vec1 = np.array(embedding1)
        vec2 = np.array(embedding2)

        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(dot_product / (norm1 * norm2))
    except Exception as e:
        logger.error(f"Failed to compute cosine similarity: {e}")
        return 0.0


def find_similar_cache_entries(
    query_text: str,
    db: Session,
    top_k: int = 10,
    product_type: str | None = None,
    min_similarity: float = 0.5,
    embedding_model: str = "all-MiniLM-L6-v2",
) -> list[dict[str, Any]]:
    """
    Find similar cache entries using semantic search.

    Args:
        query_text: Text to search for
        db: Database session
        top_k: Maximum number of results to return
        product_type: Filter by product type ('nippon', 'competitor', or None)
        min_similarity: Minimum similarity threshold (0-1)
        embedding_model: Name of embedding model to use

    Returns:
        List of similar entries with similarity scores:
        [
            {
                "id": 1,
                "raw_text": "...",
                "normalized_text": "...",
                "product_type": "competitor",
                "similarity": 0.95
            },
            ...
        ]
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache

    # Generate query embedding
    query_embedding = generate_embedding(query_text, embedding_model)
    if query_embedding is None:
        logger.warning(
            "Failed to generate query embedding, falling back to text search"
        )
        return []

    # Build query
    query = db.query(NormalizationCache).filter(
        NormalizationCache.raw_text_embedding.isnot(None)
    )

    if product_type:
        query = query.filter(NormalizationCache.product_type == product_type)

    # Get all candidates (for small datasets) or use vector DB for large datasets
    candidates = query.all()

    # Compute similarities
    results = []
    for candidate in candidates:
        if candidate.raw_text_embedding:
            similarity = compute_cosine_similarity(
                query_embedding, candidate.raw_text_embedding
            )
            if similarity >= min_similarity:
                results.append(
                    {
                        "id": candidate.id,
                        "raw_text": candidate.raw_text,
                        "normalized_text": candidate.normalized_text,
                        "product_type": candidate.product_type,
                        "similarity": similarity,
                        "times_used": candidate.times_used,
                    }
                )

    # Sort by similarity and return top_k
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results[:top_k]


def get_training_data(
    db: Session, product_type: str | None = None
) -> list[dict[str, Any]]:
    """
    Export cache data as training-ready format.

    Args:
        db: Database session
        product_type: Filter by product type (optional)

    Returns:
        List of training examples:
        [
            {
                "input": "SIGMA VIKOTE 56 - GREEN",
                "output": "PPG VIKOTE 56",
                "label": "competitor",
                "input_embedding": [...],
                "output_embedding": [...]
            },
            ...
        ]
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache

    query = db.query(NormalizationCache)

    if product_type:
        query = query.filter(NormalizationCache.product_type == product_type)

    entries = query.all()

    training_data = []
    for entry in entries:
        if entry.normalized_text:  # Only include successful matches
            training_data.append(
                {
                    "input": entry.raw_text,
                    "output": entry.normalized_text,
                    "label": entry.product_type,
                    "input_embedding": entry.raw_text_embedding,
                    "output_embedding": entry.normalized_text_embedding,
                    "metadata": {
                        "times_used": entry.times_used,
                        "confidence": entry.match_confidence,
                        "embedding_model": entry.embedding_model,
                    },
                }
            )

    return training_data


def ensure_embeddings_for_cache(
    raw_text: str,
    normalized_text: str | None,
    embedding_model: str = "all-MiniLM-L6-v2",
) -> dict[str, list[float] | None]:
    """
    Generate embeddings for cache entry (runs in background).

    This function is non-blocking and returns embeddings for immediate use,
    but can also be run asynchronously.

    Args:
        raw_text: Original raw text
        normalized_text: Normalized text (can be None)
        embedding_model: Name of embedding model

    Returns:
        Dictionary with embeddings:
        {
            "raw_text_embedding": [...],
            "normalized_text_embedding": [...] or None
        }
    """
    # Generate embeddings (non-blocking)
    raw_emb = generate_embedding(raw_text, embedding_model)
    norm_emb = (
        generate_embedding(normalized_text, embedding_model)
        if normalized_text
        else None
    )

    return {"raw_text_embedding": raw_emb, "normalized_text_embedding": norm_emb}


def get_embedding_stats(db: Session) -> dict[str, Any]:
    """
    Get statistics about embeddings in the cache.

    Args:
        db: Database session

    Returns:
        Dictionary with embedding statistics
    """
    from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache

    total_entries = db.query(NormalizationCache).count()
    entries_with_embeddings = (
        db.query(NormalizationCache)
        .filter(NormalizationCache.raw_text_embedding.isnot(None))
        .count()
    )

    # Get distribution by model
    from sqlalchemy import func

    model_distribution = (
        db.query(NormalizationCache.embedding_model, func.count(NormalizationCache.id))
        .filter(NormalizationCache.embedding_model.isnot(None))
        .group_by(NormalizationCache.embedding_model)
        .all()
    )

    return {
        "total_entries": total_entries,
        "entries_with_embeddings": entries_with_embeddings,
        "coverage_percentage": round(entries_with_embeddings / total_entries * 100, 2)
        if total_entries > 0
        else 0,
        "model_distribution": {model: count for model, count in model_distribution},
    }
