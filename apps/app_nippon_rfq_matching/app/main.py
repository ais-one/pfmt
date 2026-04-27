"""
RFQ Product Matching API - Main Application
"""

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from apps.app_nippon_rfq_matching.app.api import (
    ais_manager,
    competitor,
    competitor_color,
    files,
    jobs,
    normalization,
    normalization_cache,
    pdf,
    pdf_export,
    query,
    quotation_jobs,
    quotation_pdf,
    quotation_stats,
    semantic,
    table_results,
    tickets,
    tokenization,
    upload,
)
from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.core.database import check_db_health
from apps.app_nippon_rfq_matching.app.core.logging_config import (
    get_logger,
    set_correlation_id,
    setup_logging,
)
from apps.app_nippon_rfq_matching.app.middleware.auth_middleware import (
    AuthenticationMiddleware,
)

# Setup structured logging
setup_logging(log_level="DEBUG" if settings.DEBUG else "INFO", log_to_file=True)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    logger.info("Starting RFQ Product Matching API")
    logger.info(f"Version: {settings.APP_VERSION}")

    # Create storage directories
    settings.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    settings.STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    settings.CSV_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Storage directories created")

    # Database will be initialized lazily on first use (non-blocking)
    logger.info("Database will be initialized on first use")

    # Load matching models in background (non-blocking)
    async def load_models_background():
        """Load matching models in background after API starts"""
        try:
            from apps.app_nippon_rfq_matching.app.services.matching import (
                matching_service,
            )

            logger.info("Loading matching models in background...")
            await matching_service.ensure_loaded()
            logger.info("Matching models loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load matching models in background: {e}")

    # Start background loading but don't wait for it
    import asyncio

    asyncio.create_task(load_models_background())

    logger.info("API started - models loading in background")

    yield

    # Shutdown - cleanup thread pools
    logger.info("Shutting down RFQ Product Matching API")
    from apps.app_nippon_rfq_matching.app.services.matching import _matching_thread_pool
    from apps.app_nippon_rfq_matching.app.services.rfq_service import _thread_pool

    _thread_pool.shutdown(wait=True)
    _matching_thread_pool.shutdown(wait=True)
    logger.info("Thread pools shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="API for RFQ product matching using fuzzy matching and TF-IDF cosine similarity",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Configure CORS
origins = [
    "https://aimanager-stg.visiongroup.co",
    "https://aimanager.visiongroup.co",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://localhost:8081",
    "http://103.103.22.118:8081",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add authentication middleware
app.add_middleware(AuthenticationMiddleware)


# OpenAPI security configuration
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="API for RFQ product matching using fuzzy matching and TF-IDF cosine similarity",
        routes=app.routes,
    )

    # Add security schema for cookie-based Bearer token authentication
    openapi_schema["components"]["securitySchemes"] = {
        "cookie_auth": {
            "type": "apiKey",
            "in": "cookie",
            "name": "Authorization",
            "description": "Authorization cookie with Bearer token (Format: Bearer <token>)",
        }
    }

    # Apply security to all endpoints that require authentication
    # Exclude documentation and health check endpoints

    # Apply security to all paths
    for path, path_item in openapi_schema["paths"].items():
        # Skip authentication for documentation and health endpoints
        if path in ["/", "/docs", "/openapi.json", "/redoc", "/health"]:
            continue

        # Apply security to all methods in this path
        for method, operation in path_item.items():
            # Skip GET operations for health check and root
            if method == "get" and path in ["/", "/health"]:
                continue

            # Add security requirement
            operation["security"] = [{"cookie_auth": []}]

            # Add description for better documentation
            if not operation.get("description"):
                operation["description"] = (
                    "Requires authentication via Authorization cookie"
                )

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


# Correlation ID middleware
@app.middleware("http")
async def add_correlation_id_middleware(request: Request, call_next):
    """Add correlation ID to each request"""
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    set_correlation_id(correlation_id)

    # Log request
    logger.info(
        "Request started",
        extra={
            "request_method": request.method,
            "request_url": str(request.url),
            "request_headers": dict(request.headers),
            "correlation_id": correlation_id,
        },
    )

    try:
        response = await call_next(request)

        # Log response
        logger.info(
            "Request completed",
            extra={
                "response_status": response.status_code,
                "correlation_id": correlation_id,
            },
        )

        return response
    except Exception as e:
        logger.error(
            "Request failed",
            extra={"error": str(e), "correlation_id": correlation_id},
            exc_info=True,
        )
        raise


# Exception handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "message": str(exc)},
    )


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "endpoints": {
            "upload": "/api/v1/upload",
            "jobs": "/api/v1/jobs",
            "files": "/api/v1/files",
            "query": "/api/v1/query",
            "pdf": "/api/v1/pdf",
            "competitor": "/api/v1/competitor",
            "competitor-color": "/api/v1/competitor-color",
            "semantic": "/api/v1/semantic",
            "normalization": "/api/v1/normalization",
            "pdf-export": "/api/v1/pdf-export",
            "table-results": "/api/v1/table-results",
            "quotation-pdf": "/api/v1/quotation-pdf",
            "quotation-jobs": "/api/v1/quotation-jobs",
            "quotation-stats": "/api/v1/quotation-stats",
            "tokenize": "/api/v1/tokenize",
            "normalization-cache": "/api/v1/normalization-cache",
            "ais-manager": "/api/v1/ais-manager",
            "docs": "/docs",
            "health": "/health",
        },
    }


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_healthy = check_db_health()
    return {
        "status": "healthy",
        "database": "connected" if db_healthy else "disconnected",
    }


# Include routers
app.include_router(upload.router, prefix=settings.API_V1_PREFIX)
app.include_router(query.router, prefix=settings.API_V1_PREFIX)
app.include_router(jobs.router, prefix=settings.API_V1_PREFIX)
app.include_router(files.router, prefix=settings.API_V1_PREFIX)
app.include_router(pdf.router, prefix=settings.API_V1_PREFIX)
app.include_router(competitor.router, prefix=settings.API_V1_PREFIX)
app.include_router(competitor_color.router, prefix=settings.API_V1_PREFIX)
app.include_router(semantic.router, prefix=settings.API_V1_PREFIX)
app.include_router(normalization.router, prefix=settings.API_V1_PREFIX)
app.include_router(pdf_export.router, prefix=settings.API_V1_PREFIX)
app.include_router(table_results.router, prefix=settings.API_V1_PREFIX)
app.include_router(tokenization.router, prefix=settings.API_V1_PREFIX)
app.include_router(normalization_cache.router, prefix=settings.API_V1_PREFIX)
app.include_router(quotation_pdf.router, prefix=settings.API_V1_PREFIX)
app.include_router(quotation_jobs.router, prefix=settings.API_V1_PREFIX)
app.include_router(quotation_stats.router, prefix=settings.API_V1_PREFIX)
app.include_router(tickets.router, prefix=settings.API_V1_PREFIX)
app.include_router(ais_manager.router, prefix=settings.API_V1_PREFIX)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)
