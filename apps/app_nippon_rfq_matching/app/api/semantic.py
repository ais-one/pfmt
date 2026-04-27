"""
Semantic Matching API Endpoints

API endpoints for semantic matching using OpenAI embeddings.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.services.embedding_service import (
    embedding_service,
)
from apps.app_nippon_rfq_matching.app.services.hybrid_matching import (
    enhanced_matching_service,
)
from apps.app_nippon_rfq_matching.app.services.semantic_matching import (
    semantic_matching_service,
)
from apps.app_nippon_rfq_matching.app.services.vector_store import vector_store

router = APIRouter(prefix="/semantic", tags=["Semantic Matching"])


# Request Schemas
class SemanticMatchRequest(BaseModel):
    """Request schema for semantic matching"""

    text: str = Field(..., description="Text to match")
    use_semantic: bool = Field(True, description="Whether to use semantic matching")
    use_color: bool = Field(True, description="Whether to use color-aware matching")
    top_k: int = Field(5, description="Number of top results to return")


class SemanticMatchBatchRequest(BaseModel):
    """Request schema for batch semantic matching"""

    items: list[dict[str, Any]] = Field(..., description="List of RFQ items to match")
    use_semantic: bool = Field(True, description="Whether to use semantic matching")


class VectorStoreInitRequest(BaseModel):
    """Request schema for initializing vector store"""

    force_rebuild: bool = Field(False, description="Force rebuild of vector store")


# Response Schemas
class SemanticMatchResponse(BaseModel):
    """Response schema for semantic matching"""

    matched: bool
    product_id: int | None
    product_name: str | None
    similarity: float
    method: str
    color: str | None
    confidence: float


class VectorStoreStatsResponse(BaseModel):
    """Response schema for vector store statistics"""

    product_master_count: int
    rfq_items_count: int
    dimensions: int
    index_type: str
    enabled: bool


class EmbeddingServiceStatusResponse(BaseModel):
    """Response schema for embedding service status"""

    enabled: bool
    model: str
    dimensions: int
    cache_size: int


# Endpoints
@router.post("/match", response_model=dict[str, Any])
async def match_semantic(request: SemanticMatchRequest, db: Session = Depends(get_db)):
    """
    Match a single text using semantic search.

    Args:
        request: Semantic match request
        db: Database session

    Returns:
        Match result with product information
    """
    try:
        # Use enhanced matching service
        rfq_item = {"raw_text": request.text, "clean_text": request.text}

        result = enhanced_matching_service.match_rfq_item(
            rfq_item=rfq_item,
            db=db,
            use_semantic=request.use_semantic,
            use_color=request.use_color,
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Matching failed: {str(e)}")


@router.post("/match/batch", response_model=list[dict[str, Any]])
async def match_semantic_batch(
    request: SemanticMatchBatchRequest, db: Session = Depends(get_db)
):
    """
    Match multiple texts using semantic search.

    Args:
        request: Batch semantic match request
        db: Database session

    Returns:
        List of match results
    """
    try:
        results = enhanced_matching_service.match_rfq_items(
            rfq_items=request.items, db=db, use_semantic=request.use_semantic
        )

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch matching failed: {str(e)}")


@router.post("/vector-store/init", response_model=dict[str, Any])
async def initialize_vector_store(
    request: VectorStoreInitRequest, db: Session = Depends(get_db)
):
    """
    Initialize or rebuild the vector store with product master data.

    Args:
        request: Vector store initialization request
        db: Database session

    Returns:
        Status message
    """
    try:
        if request.force_rebuild:
            semantic_matching_service.initialize_vector_store(db)
        else:
            # Only initialize if not already initialized
            stats = vector_store.get_stats()
            if stats["product_master_count"] == 0:
                semantic_matching_service.initialize_vector_store(db)

        stats = vector_store.get_stats()
        return {
            "status": "success",
            "message": "Vector store initialized",
            "stats": stats,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Initialization failed: {str(e)}")


@router.get("/vector-store/stats", response_model=VectorStoreStatsResponse)
async def get_vector_store_stats():
    """
    Get vector store statistics.

    Returns:
        Vector store statistics
    """
    try:
        stats = vector_store.get_stats()
        return VectorStoreStatsResponse(**stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@router.get("/embedding/status", response_model=EmbeddingServiceStatusResponse)
async def get_embedding_service_status():
    """
    Get embedding service status.

    Returns:
        Embedding service status
    """
    try:
        return EmbeddingServiceStatusResponse(
            enabled=embedding_service.enabled,
            model=embedding_service.model,
            dimensions=embedding_service.dimensions,
            cache_size=len(embedding_service._cache),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.post("/embedding/generate")
async def generate_embedding(text: str):
    """
    Generate embedding for a single text.

    Args:
        text: Text to embed

    Returns:
        Embedding vector (as list)
    """
    try:
        if not embedding_service.enabled:
            raise HTTPException(
                status_code=400,
                detail="Embedding service is disabled. Please set OPENAI_API_KEY.",
            )

        embedding = embedding_service.embed_text(text)

        if embedding is None:
            raise HTTPException(status_code=500, detail="Failed to generate embedding")

        return {
            "text": text,
            "embedding": embedding.tolist(),
            "dimensions": len(embedding),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to generate embedding: {str(e)}"
        )


@router.get("/stats")
async def get_semantic_stats():
    """
    Get comprehensive semantic matching statistics.

    Returns:
        Semantic matching statistics
    """
    try:
        return {
            "embedding_service": {
                "enabled": embedding_service.enabled,
                "model": embedding_service.model,
                "dimensions": embedding_service.dimensions,
                "cache_size": len(embedding_service._cache),
            },
            "vector_store": vector_store.get_stats(),
            "semantic_matching": {
                "enabled": semantic_matching_service.enabled,
                "semantic_threshold": semantic_matching_service.semantic_threshold,
                "hybrid_threshold": semantic_matching_service.hybrid_threshold,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")
