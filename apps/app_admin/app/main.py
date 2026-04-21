from fastapi import FastAPI

from apps.app_admin.app.api.routes import router
from apps.app_admin.app.settings import settings
from apps.app_admin.app.version import __version__


def create_app() -> FastAPI:
    app = FastAPI(
        title="PFMT App Admin",
        version=__version__,
        description="Second FastAPI service demonstrating monorepo service separation.",
        debug=settings.debug,
    )
    app.include_router(router, prefix=settings.api_base_path)
    return app


app = create_app()
