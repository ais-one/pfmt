from fastapi import FastAPI

from apps.app_sample.app.api.routes import router
from apps.app_sample.app.settings import settings
from apps.app_sample.app.version import __version__


def create_app() -> FastAPI:
    app = FastAPI(
        title="PFMT App Sample",
        version=__version__,
        description="Sample FastAPI service for the Python backend monorepo.",
        debug=settings.debug,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
    )
    app.include_router(router, prefix=settings.api_base_path)
    return app


app = create_app()
