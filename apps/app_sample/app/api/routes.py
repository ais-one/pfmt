from fastapi import APIRouter

from apps.app_sample.app.settings import settings
from common.python import EchoRequest, EchoResponse, HealthResponse, get_logger

logger = get_logger(settings.service_name)
router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["system"])
def healthcheck() -> HealthResponse:
    logger.info("healthcheck requested")
    return HealthResponse(
        status="ok",
        service=settings.service_name,
        environment=settings.environment,
    )


@router.post("/echo", response_model=EchoResponse, tags=["system"])
def echo_message(payload: EchoRequest) -> EchoResponse:
    logger.info("echo requested")
    return EchoResponse(
        service=settings.service_name,
        environment=settings.environment,
        message=payload.message,
    )
