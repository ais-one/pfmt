from fastapi import FastAPI

from apps.app_admin.app.api.routes import router
from apps.app_admin.app.settings import settings



def create_app() -> FastAPI:
    app = FastAPI(
        title="PFA App Admin",
        version="0.1.0",
        description="Second FastAPI service demonstrating monorepo service separation.",
        debug=settings.debug,
    )
    app.include_router(router, prefix=settings.api_base_path)
    return app


app = create_app()
