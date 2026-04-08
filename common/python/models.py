from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str


class ServiceInfoResponse(BaseModel):
    service: str
    environment: str
    debug: bool
    host: str
    port: int
    api_base_path: str


class EchoRequest(BaseModel):
    message: str


class EchoResponse(BaseModel):
    service: str
    environment: str
    message: str
