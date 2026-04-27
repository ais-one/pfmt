"""
Normalization API Endpoints

API endpoints for RFQ item normalization using OpenAI chat completion API.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from apps.app_nippon_rfq_matching.app.core.database import get_db
from apps.app_nippon_rfq_matching.app.services.openai_normalization import (
    openai_normalization_service,
)
from apps.app_nippon_rfq_matching.app.utils.resilience import ResilientCallers

router = APIRouter(prefix="/normalization", tags=["Normalization"])


# Request Schemas
class NormalizationRequest(BaseModel):
    """Request schema for single item normalization"""

    description: str = Field(..., description="RFQ item description to normalize")


class NormalizationBatchRequest(BaseModel):
    """Request schema for batch normalization"""

    descriptions: list[str] = Field(
        ..., description="List of RFQ descriptions to normalize"
    )
    include_raw: bool = Field(
        False, description="Whether to include raw descriptions in response"
    )


class NormalizationValidateRequest(BaseModel):
    """Request schema for normalization validation"""

    before: list[str] = Field(..., description="Original descriptions")
    after: list[str] = Field(..., description="Normalized descriptions")


# Response Schemas
class NormalizationResponse(BaseModel):
    """Response schema for single item normalization"""

    raw: str
    normalized: str | None
    matched: bool
    model: str


class NormalizationBatchResponse(BaseModel):
    """Response schema for batch normalization"""

    before: list[str]
    after: list[str | None]
    model: str
    usage: dict[str, int]


class NormalizationDetailedResponse(BaseModel):
    """Response schema for detailed normalization"""

    results: list[dict[str, Any]]
    model: str
    usage: dict[str, int]


class ServiceStatusResponse(BaseModel):
    """Response schema for service status"""

    enabled: bool
    model: str
    temperature: float
    max_tokens: int


# Endpoints
@router.post("/normalize", response_model=dict[str, Any])
async def normalize_single(
    request: NormalizationRequest, db: Session = Depends(get_db)
):
    """
    Normalize a single RFQ item description.

    Args:
        request: Normalization request with description
        db: Database session

    Returns:
        Normalized result with product name or null if no match

    Example:
        POST /normalization/normalize
        {
            "description": "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L"
        }

        Response:
        {
            "raw": "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L",
            "normalized": "PHENGUARD TC/FC",
            "matched": true,
            "model": "gpt-4o-mini"
        }
    """
    try:
        if not openai_normalization_service.enabled:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        normalized = openai_normalization_service.normalize_single_item(
            rfq_description=request.description, db=db
        )

        return {
            "raw": request.description,
            "normalized": normalized,
            "matched": normalized is not None,
            "model": openai_normalization_service.model,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Normalization failed: {str(e)}")


@router.post("/normalize/batch", response_model=dict[str, Any])
async def normalize_batch(
    request: NormalizationBatchRequest, db: Session = Depends(get_db)
):
    """
    Normalize multiple RFQ item descriptions in batch.

    Args:
        request: Batch normalization request
        db: Database session

    Returns:
        Batch normalization results with before/after arrays

    Example:
        POST /normalization/normalize/batch
        {
            "descriptions": [
                "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L",
                "信号红 PILOT II RAL3000 5L"
            ]
        }

        Response:
        {
            "before": [...],
            "after": ["PHENGUARD TC/FC", "PILOT II"],
            "model": "gpt-4o-mini",
            "usage": {...}
        }
    """
    try:
        if not openai_normalization_service.enabled:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        if not request.descriptions:
            return {
                "before": [],
                "after": [],
                "model": openai_normalization_service.model,
                "usage": {},
            }

        result = openai_normalization_service.normalize_rfq_items(
            rfq_descriptions=request.descriptions, db=db
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Batch normalization failed: {str(e)}"
        )


@router.post("/normalize/detailed", response_model=dict[str, Any])
async def normalize_detailed(
    request: NormalizationBatchRequest, db: Session = Depends(get_db)
):
    """
    Normalize RFQ items and return detailed results with metadata.

    Args:
        request: Batch normalization request
        db: Database session

    Returns:
        Detailed normalization results

    Example:
        POST /normalization/normalize/detailed
        {
            "descriptions": [
                "环氧红棕面漆 PENGUARD FC STD 2244 REDBROEN 17L",
                "稀释剂THINNER 17号"
            ],
            "include_raw": true
        }

        Response:
        {
            "results": [
                {"raw": "...", "normalized": "PHENGUARD TC/FC", "matched": true, "model": "gpt-4o-mini"},
                {"raw": "...", "normalized": null, "matched": false, "model": "gpt-4o-mini"}
            ],
            "model": "gpt-4o-mini",
            "usage": {...}
        }
    """
    try:
        if not openai_normalization_service.enabled:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        results = openai_normalization_service.normalize_with_confidence(
            rfq_descriptions=request.descriptions,
            db=db,
            include_raw=request.include_raw,
        )

        return {
            "results": results,
            "model": openai_normalization_service.model,
            "count": len(results),
            "matched_count": sum(1 for r in results if r["matched"]),
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Detailed normalization failed: {str(e)}"
        )


@router.post("/validate", response_model=dict[str, Any])
async def validate_normalization(request: NormalizationValidateRequest):
    """
    Validate normalization results without calling OpenAI.

    This endpoint is useful for testing or validating existing normalization results.

    Args:
        request: Validation request with before/after arrays

    Returns:
        Validation statistics

    Example:
        POST /normalization/validate
        {
            "before": ["Item 1", "Item 2"],
            "after": ["Product A", null]
        }

        Response:
        {
            "total": 2,
            "matched": 1,
            "unmatched": 1,
            "match_rate": 0.5
        }
    """
    try:
        if len(request.before) != len(request.after):
            raise HTTPException(
                status_code=400,
                detail=f"Array length mismatch: before has {len(request.before)} items, "
                f"after has {len(request.after)} items",
            )

        total = len(request.before)
        matched = sum(1 for item in request.after if item is not None)
        unmatched = total - matched
        match_rate = matched / total if total > 0 else 0.0

        return {
            "total": total,
            "matched": matched,
            "unmatched": unmatched,
            "match_rate": round(match_rate, 3),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@router.get("/status", response_model=ServiceStatusResponse)
async def get_service_status():
    """
    Get normalization service status.

    Returns:
        Service status information

    Example:
        GET /normalization/status

        Response:
        {
            "enabled": true,
            "model": "gpt-4o-mini",
            "temperature": 0.0,
            "max_tokens": 4000
        }
    """
    try:
        return ServiceStatusResponse(
            enabled=openai_normalization_service.enabled,
            model=openai_normalization_service.model,
            temperature=openai_normalization_service.temperature,
            max_tokens=openai_normalization_service.max_tokens,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.get("/health")
async def health_check():
    """
    Health check endpoint for the normalization service.

    Returns:
        Health status

    Example:
        GET /normalization/health

        Response:
        {
            "status": "healthy",
            "enabled": true
        }
    """
    return {
        "status": "healthy" if openai_normalization_service.enabled else "disabled",
        "enabled": openai_normalization_service.enabled,
    }


@router.post("/normalize/product-names", response_model=dict[str, Any])
async def normalize_product_names(
    request: NormalizationBatchRequest, db: Session = Depends(get_db)
):
    """
    Normalize product names only (preserves model numbers like 700, 100, 500).

    This endpoint uses a separate process for product name normalization only,
    which preserves important model numbers that distinguish between products.

    Args:
        request: Batch normalization request with descriptions
        db: Database session

    Returns:
        Product name normalization results

    Example:
        POST /normalization/normalize/product-names
        {
            "descriptions": [
                "Nippon Marine Thinner 700 POLYURE MIGHTYLAC",
                "Nippon Marine Thinner 100",
                "Tetzsol 500 Eco Silver"
            ]
        }

        Response:
        {
            "before": [...],
            "after": [
                "NIPPON MARINE THINNER 700",
                "NIPPON MARINE THINNER 100",
                "NIPPON TETZSOL 500 ECO"
            ],
            "types": ["nippon", "nippon", "nippon"],
            "model": "gpt-4o-mini",
            "usage": {...}
        }
    """
    try:
        if not openai_normalization_service.enabled:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        if not request.descriptions:
            return {
                "before": [],
                "after": [],
                "types": [],
                "model": openai_normalization_service.model,
                "usage": {},
            }

        result = openai_normalization_service.normalize_product_names_only(
            rfq_descriptions=request.descriptions, db=db
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Product name normalization failed: {str(e)}"
        )


@router.post("/normalize/colors", response_model=dict[str, Any])
async def normalize_colors(
    request: NormalizationBatchRequest, db: Session = Depends(get_db)
):
    """
    Extract colors only (separate from product name normalization).

    This endpoint uses a separate process for color extraction only,
    which focuses on identifying exact colors from the database.

    Args:
        request: Batch normalization request with descriptions
        db: Database session

    Returns:
        Color extraction results

    Example:
        POST /normalization/normalize/colors
        {
            "descriptions": [
                "Nippon U-Marine Finish 000 White Base",
                "Nippon O-Marine Finish 355 Signal Yellow",
                "Nippon Marine Thinner 700"
            ]
        }

        Response:
        {
            "before": [...],
            "colors": ["WHITE", "355 SIGNAL YELLOW", null],
            "model": "gpt-4o-mini",
            "usage": {...}
        }
    """
    try:
        if not openai_normalization_service.enabled:
            raise HTTPException(
                status_code=503,
                detail="OpenAI normalization service is disabled. Please set OPENAI_API_KEY.",
            )

        if not request.descriptions:
            return {
                "before": [],
                "colors": [],
                "model": openai_normalization_service.model,
                "usage": {},
            }

        result = openai_normalization_service.extract_colors_only(
            rfq_descriptions=request.descriptions, db=db
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Color extraction failed: {str(e)}"
        )


@router.get("/circuit-breaker/state", response_model=dict[str, Any])
async def get_circuit_breaker_state():
    """
    Get the current state of all circuit breakers.

    Returns:
        Circuit breaker states including failure counts, remaining time, etc.

    Example:
        GET /normalization/circuit-breaker/state

        Response:
        {
            "openai_normalization": {
                "service_name": "openai_normalization",
                "circuit_breaker": {
                    "state": "closed",
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_time": null,
                    "remaining_time": 0.0
                },
                "retry_config": {
                    "max_attempts": 2,
                    "base_delay": 1.0,
                    "max_delay": 5.0
                }
            },
            "openai_embedding": {
                ...
            }
        }
    """
    try:
        return ResilientCallers.get_all_states()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get circuit breaker state: {str(e)}"
        )


@router.post("/circuit-breaker/reset", response_model=dict[str, Any])
async def reset_circuit_breakers():
    """
    Reset all circuit breakers to closed state.

    This is useful for manually recovering from a circuit breaker open state
    after the external service has recovered.

    Returns:
        Success message

    Example:
        POST /normalization/circuit-breaker/reset

        Response:
        {
            "success": true,
            "message": "All circuit breakers reset"
        }
    """
    try:
        ResilientCallers.reset_all()
        return {"success": True, "message": "All circuit breakers reset"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to reset circuit breakers: {str(e)}"
        )
