from fastapi import APIRouter

from apps.app_admin.app.settings import settings
from common.python import EchoRequest, EchoResponse, HealthResponse, ServiceInfoResponse, get_logger

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


@router.get("/info", response_model=ServiceInfoResponse, tags=["system"])
def service_info() -> ServiceInfoResponse:
    logger.info("service metadata requested")
    return ServiceInfoResponse(
        service=settings.service_name,
        environment=settings.environment,
        debug=settings.debug,
        host=settings.host,
        port=settings.port,
        api_base_path=settings.api_base_path,
    )


@router.post("/echo", response_model=EchoResponse, tags=["system"])
def echo_message(payload: EchoRequest) -> EchoResponse:
    logger.info("echo requested")
    return EchoResponse(
        service=settings.service_name,
        environment=settings.environment,
        message=payload.message,
    )
