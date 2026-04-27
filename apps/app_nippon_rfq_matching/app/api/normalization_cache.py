"""
Normalization Cache API

Endpoints for managing OpenAI normalization cache.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.models.rfq import NormalizationCache, RFQItem
from apps.app_nippon_rfq_matching.app.models.schemas import (
    CacheStatsResponse,
    NormalizationCacheResponse,
)
from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
    openai_normalization_service,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/normalization-cache", tags=["normalization-cache"])


@router.get("/stats", response_model=CacheStatsResponse)
async def get_cache_stats(db: Session = Depends(get_db)):
    """
    Get cache statistics.

    Returns:
        - total_entries: Total number of cached entries
        - nippon_entries: Entries matched to Nippon products
        - competitor_entries: Entries matched to competitor products
        - no_match_entries: Entries with no match
        - most_used: Top 10 most used cache entries
    """
    total_entries = db.query(NormalizationCache).count()
    nippon_entries = (
        db.query(NormalizationCache)
        .filter(NormalizationCache.product_type == "nippon")
        .count()
    )
    competitor_entries = (
        db.query(NormalizationCache)
        .filter(NormalizationCache.product_type == "competitor")
        .count()
    )

    # Get most used entries
    most_used = (
        db.query(NormalizationCache)
        .order_by(NormalizationCache.times_used.desc())
        .limit(10)
        .all()
    )

    return {
        "total_entries": total_entries,
        "nippon_entries": nippon_entries,
        "competitor_entries": competitor_entries,
        "no_match_entries": total_entries - nippon_entries - competitor_entries,
        "most_used": [NormalizationCacheResponse.model_validate(e) for e in most_used],
    }


@router.post("/clear")
async def clear_cache(confirm: bool = False, db: Session = Depends(get_db)):
    """
    Clear all cache entries.

    Args:
        confirm: Must be set to true to clear cache (safety measure)

    Returns:
        Success message after clearing cache
    """
    if not confirm:
        raise HTTPException(
            status_code=400, detail="Set confirm=true to clear cache (safety measure)"
        )

    deleted_count = db.query(NormalizationCache).delete()
    db.commit()

    # Also clear in-memory cache
    openai_normalization_service._memory_cache.clear()
    openai_normalization_service._cache_stats = {"hits": 0, "misses": 0, "api_calls": 0}

    logger.info(f"Cleared {deleted_count} cache entries")

    return {"message": "Cache cleared successfully", "deleted_count": deleted_count}


@router.post("/prewarm")
async def prewarm_cache(limit: int = 100, db: Session = Depends(get_db)):
    """
    Prewarm cache from existing RFQ items.

    This endpoint processes existing RFQ items that are not yet cached,
    normalizing them and storing the results in the cache.

    Args:
        limit: Maximum number of uncached RFQ items to process (default: 100)

    Returns:
        Summary of prewarming results
    """
    if not openai_normalization_service.enabled:
        raise HTTPException(
            status_code=503, detail="OpenAI normalization service is disabled"
        )

    # Get uncached RFQ items
    cached_raw_texts = {c.raw_text for c in db.query(NormalizationCache.raw_text).all()}
    uncached_items = (
        db.query(RFQItem)
        .filter(~RFQItem.raw_text.in_(cached_raw_texts))
        .limit(limit)
        .all()
    )

    descriptions = [item.raw_text for item in uncached_items]

    if not descriptions:
        return {"message": "No uncached RFQ items found", "processed": 0}

    logger.info(f"Prewarming cache with {len(descriptions)} RFQ items")

    # This will cache the results
    try:
        result = openai_normalization_service.normalize_rfq_items(
            descriptions, db, use_cache=True
        )

        return {
            "message": f"Prewarmed cache with {len(descriptions)} items",
            "processed": len(descriptions),
            "cache_stats": result.get("cache_stats", {}),
        }
    except Exception as e:
        logger.error(f"Error prewarming cache: {e}")
        raise HTTPException(status_code=500, detail=f"Error prewarming cache: {str(e)}")


@router.get("/entries", response_model=list[NormalizationCacheResponse])
async def list_cache_entries(
    skip: int = 0,
    limit: int = 100,
    product_type: str = None,
    db: Session = Depends(get_db),
):
    """
    List cached normalization entries.

    Args:
        skip: Number of entries to skip (pagination)
        limit: Maximum number of entries to return
        product_type: Filter by product type ('nippon', 'competitor', or None)

    Returns:
        List of cache entries
    """
    query = db.query(NormalizationCache)

    if product_type:
        query = query.filter(NormalizationCache.product_type == product_type)

    entries = (
        query.order_by(NormalizationCache.times_used.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return [NormalizationCacheResponse.model_validate(e) for e in entries]


@router.delete("/entries/{entry_id}")
async def delete_cache_entry(entry_id: int, db: Session = Depends(get_db)):
    """
    Delete a specific cache entry by ID.

    Args:
        entry_id: ID of the cache entry to delete

    Returns:
        Success message
    """
    entry = (
        db.query(NormalizationCache).filter(NormalizationCache.id == entry_id).first()
    )

    if not entry:
        raise HTTPException(status_code=404, detail=f"Cache entry {entry_id} not found")

    db.delete(entry)
    db.commit()

    logger.info(f"Deleted cache entry {entry_id}: {entry.raw_text}")

    return {
        "message": f"Deleted cache entry {entry_id}",
        "deleted_raw_text": entry.raw_text,
    }


@router.get("/memory-stats")
async def get_memory_cache_stats():
    """
    Get in-memory cache statistics.

    Returns:
        Current memory cache statistics
    """
    return {
        "memory_cache_size": len(openai_normalization_service._memory_cache),
        "memory_cache_max_size": openai_normalization_service._memory_cache_max_size,
        "cache_stats": openai_normalization_service._cache_stats,
    }


@router.get("/search")
async def semantic_search(
    query: str,
    top_k: int = 10,
    product_type: str = None,
    min_similarity: float = 0.5,
    db: Session = Depends(get_db),
):
    """
    Semantic search using vector embeddings.

    Find similar cache entries based on semantic meaning rather than exact text match.
    This is useful for finding patterns and similar RFQ descriptions.

    Args:
        query: Text to search for
        top_k: Maximum number of results (default: 10)
        product_type: Filter by product type ('nippon', 'competitor', or None)
        min_similarity: Minimum similarity threshold 0-1 (default: 0.5)

    Returns:
        List of similar entries with similarity scores
    """
    from apps.app_nippon_rfq_matching.app.services.embeddings import (
        find_similar_cache_entries,
    )

    results = find_similar_cache_entries(
        query_text=query,
        db=db,
        top_k=top_k,
        product_type=product_type,
        min_similarity=min_similarity,
    )

    return {"query": query, "count": len(results), "results": results}


@router.get("/export/training-data")
async def export_training_data(product_type: str = None, db: Session = Depends(get_db)):
    """
    Export cache data as training-ready format.

    This endpoint exports the normalization cache in a format suitable for:
    - Training custom ML models
    - Fine-tuning language models
    - Creating datasets for semantic search

    Args:
        product_type: Filter by product type ('nippon', 'competitor', or None)

    Returns:
        Training data with inputs, outputs, labels, and embeddings
    """
    from apps.app_nippon_rfq_matching.app.services.embeddings import get_training_data

    training_data = get_training_data(db, product_type)

    return {
        "count": len(training_data),
        "product_type": product_type or "all",
        "data": training_data,
    }


@router.get("/stats/embeddings")
async def get_embedding_statistics(db: Session = Depends(get_db)):
    """
    Get statistics about embeddings in the cache.

    Returns:
        - total_entries: Total cache entries
        - entries_with_embeddings: Entries with vector embeddings
        - coverage_percentage: Percentage of entries with embeddings
        - model_distribution: Distribution of embedding models used
    """
    from apps.app_nippon_rfq_matching.app.services.embeddings import get_embedding_stats

    return get_embedding_stats(db)
