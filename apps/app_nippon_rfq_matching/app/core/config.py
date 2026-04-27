from dataclasses import dataclass, field
from pathlib import Path

from common.python.config import (
    env_bool,
    env_float,
    env_int,
    env_str,
    get_environment,
)

# Anchor default data paths to the app's own folder (apps/app_nippon_rfq_matching/)
# so behavior is stable regardless of where the process is launched from.
# __file__ = .../apps/app_nippon_rfq_matching/app/core/config.py → parents[2] = app root.
_APP_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DATA_DIR = _APP_ROOT / "data"


def _env_path(name: str, default: str) -> Path:
    return Path(env_str(name, default))


@dataclass(frozen=True)
class AppNipponRfqMatchingSettings:
    SERVICE_NAME: str
    ENVIRONMENT: str

    APP_NAME: str
    APP_VERSION: str
    DEBUG: bool

    DATABASE_URL: str

    UPLOAD_DIR: Path
    STORAGE_DIR: Path
    CSV_DIR: Path

    API_PREFIX: str
    API_VERSION: str
    DOCS_ENABLED: bool

    MIN_MATCH_SCORE: float

    LLAMAPARSE_API_KEY: str
    LLAMAPARSE_TIMEOUT: int

    OPENAI_API_KEY: str
    OPENAI_EMBEDDING_MODEL: str
    OPENAI_EMBEDDING_DIMENSIONS: int
    OPENAI_CHAT_MODEL: str
    OPENAI_TEMPERATURE: float
    OPENAI_MAX_TOKENS: int

    SEMANTIC_MATCH_THRESHOLD: float
    HYBRID_MATCH_THRESHOLD: float
    ENABLE_SEMANTIC_SEARCH: bool

    VECTOR_DB_PATH: Path

    AISMBACKEND_URL: str

    NIPPON_KEYWORDS: list[str] = field(
        default_factory=lambda: [
            "nippon",
            "np",
            "nippont",
            "marine",
            "o-marine",
            "a-marine",
            "h-marine",
            "npe",
            "npa",
        ]
    )

    @property
    def API_V1_PREFIX(self) -> str:
        return f"{self.API_PREFIX}/{self.API_VERSION}"


def get_app_nippon_rfq_matching_settings() -> AppNipponRfqMatchingSettings:
    return AppNipponRfqMatchingSettings(
        SERVICE_NAME="app-nippon-rfq-matching",
        ENVIRONMENT=get_environment("APP_NIPPON_RFQ_ENV"),
        APP_NAME=env_str("APP_NIPPON_RFQ_APP_NAME", "RFQ Product Matching API"),
        APP_VERSION=env_str("APP_NIPPON_RFQ_APP_VERSION", "0.1.0"),
        DEBUG=env_bool("APP_NIPPON_RFQ_DEBUG", env_bool("APP_DEBUG", False)),
        DATABASE_URL=env_str(
            "APP_NIPPON_RFQ_DATABASE_URL",
            f"sqlite:///{_DEFAULT_DATA_DIR / 'rfq_matching.db'}",
        ),
        UPLOAD_DIR=_env_path(
            "APP_NIPPON_RFQ_UPLOAD_DIR", str(_DEFAULT_DATA_DIR / "uploads")
        ),
        STORAGE_DIR=_env_path(
            "APP_NIPPON_RFQ_STORAGE_DIR", str(_DEFAULT_DATA_DIR / "storage")
        ),
        CSV_DIR=_env_path(
            "APP_NIPPON_RFQ_CSV_DIR", str(_DEFAULT_DATA_DIR / "storage" / "csv")
        ),
        API_PREFIX=env_str("APP_NIPPON_RFQ_API_PREFIX", "/api/nippon-rfq"),
        API_VERSION=env_str("APP_NIPPON_RFQ_API_VERSION", "v1"),
        DOCS_ENABLED=env_bool("APP_NIPPON_RFQ_DOCS_ENABLED", True),
        MIN_MATCH_SCORE=env_float("APP_NIPPON_RFQ_MIN_MATCH_SCORE", 70.0),
        LLAMAPARSE_API_KEY=env_str("LLAMAPARSE_API_KEY", ""),
        LLAMAPARSE_TIMEOUT=env_int("LLAMAPARSE_TIMEOUT", 300),
        OPENAI_API_KEY=env_str("OPENAI_API_KEY", ""),
        OPENAI_EMBEDDING_MODEL=env_str(
            "OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"
        ),
        OPENAI_EMBEDDING_DIMENSIONS=env_int("OPENAI_EMBEDDING_DIMENSIONS", 512),
        OPENAI_CHAT_MODEL=env_str("OPENAI_CHAT_MODEL", "gpt-4o-mini"),
        OPENAI_TEMPERATURE=env_float("OPENAI_TEMPERATURE", 0.0),
        OPENAI_MAX_TOKENS=env_int("OPENAI_MAX_TOKENS", 4000),
        SEMANTIC_MATCH_THRESHOLD=env_float(
            "APP_NIPPON_RFQ_SEMANTIC_MATCH_THRESHOLD", 0.85
        ),
        HYBRID_MATCH_THRESHOLD=env_float("APP_NIPPON_RFQ_HYBRID_MATCH_THRESHOLD", 0.90),
        ENABLE_SEMANTIC_SEARCH=env_bool("APP_NIPPON_RFQ_ENABLE_SEMANTIC_SEARCH", True),
        VECTOR_DB_PATH=_env_path(
            "APP_NIPPON_RFQ_VECTOR_DB_PATH", str(_DEFAULT_DATA_DIR / "vectors")
        ),
        AISMBACKEND_URL=env_str(
            "APP_NIPPON_RFQ_AISMBACKEND_URL",
            "https://aismanager-stg.visiongroup.co",
        ),
    )


settings = get_app_nippon_rfq_matching_settings()
