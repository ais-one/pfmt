from fastapi import FastAPI

from apps.app_sample.app.api.routes import router
from apps.app_sample.app.settings import settings



def create_app() -> FastAPI:
    app = FastAPI(
        title="PFA App Sample",
        version="0.1.0",
        description="Sample FastAPI service for the Python backend monorepo.",
        debug=settings.debug,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
    )
    app.include_router(router, prefix=settings.api_base_path)
    return app


app = create_app()
