"""
Database session management
"""

from collections.abc import Generator
from pathlib import Path
from threading import Lock

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.core.logging_config import get_logger
from apps.app_nippon_rfq_matching.app.models.base import Base

logger = get_logger(__name__)

# Create engine
engine = None
db_available = False
_db_initialized = False
_init_lock = Lock()

# Create session factory
SessionLocal = None


def _ensure_db_initialized():
    """Ensure database is initialized (thread-safe lazy initialization)"""
    global engine, SessionLocal, db_available, _db_initialized

    if _db_initialized:
        return

    with _init_lock:
        # Double-check after acquiring lock
        if _db_initialized:
            return

        try:
            logger.info("Initializing database...")
            # For file-backed SQLite, ensure the parent directory exists
            # so a fresh clone can boot without a pre-created data/ folder.
            if settings.DATABASE_URL.startswith("sqlite:///"):
                db_path = settings.DATABASE_URL.removeprefix("sqlite:///")
                if db_path and db_path != ":memory:":
                    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            # Create engine
            engine = create_engine(
                settings.DATABASE_URL,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 30,  # 30 seconds timeout for SQLite
                }
                if "sqlite" in settings.DATABASE_URL
                else {},
                echo=settings.DEBUG,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,  # Recycle connections after 1 hour
            )

            # Create session factory
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

            # Create tables in specific order to avoid foreign key issues
            # First create tables without foreign keys
            Base.metadata.tables["uploaded_files"].create(bind=engine, checkfirst=True)
            Base.metadata.tables["normalization_cache"].create(
                bind=engine, checkfirst=True
            )
            Base.metadata.tables["product_master_mv"].create(
                bind=engine, checkfirst=True
            )

            # Then create tables with foreign keys
            Base.metadata.tables["product_master"].create(bind=engine, checkfirst=True)
            Base.metadata.tables["rfq_items"].create(bind=engine, checkfirst=True)
            Base.metadata.tables["rfq_matches"].create(bind=engine, checkfirst=True)
            Base.metadata.tables["jobs"].create(bind=engine, checkfirst=True)

            # Create any remaining tables
            Base.metadata.create_all(bind=engine, checkfirst=True)

            # Enable WAL mode for better concurrency on SQLite
            if "sqlite" in settings.DATABASE_URL:
                try:
                    with engine.connect() as conn:
                        conn.execute(text("PRAGMA journal_mode=WAL"))
                        conn.execute(text("PRAGMA synchronous=NORMAL"))
                        conn.commit()
                    logger.info("SQLite WAL mode enabled for better concurrency")
                except Exception as e:
                    logger.warning(f"Could not enable WAL mode: {e}")

            db_available = True
            _db_initialized = True
            logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            db_available = False
            _db_initialized = True  # Mark as attempted to avoid retry loops


def init_db():
    """Initialize database tables (can be called during startup but won't block)"""
    # Don't initialize during startup - will be lazy loaded
    logger.info("Database will be initialized on first use")


def check_db_health() -> bool:
    """Check if database is accessible"""
    _ensure_db_initialized()

    if not db_available or engine is None or SessionLocal is None:
        return False

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.warning(f"Database health check failed: {e}")
        return False


def get_db() -> Generator[Session | None, None, None]:
    """
    Dependency for getting database session
    Returns None if database is not available
    """
    _ensure_db_initialized()

    if not db_available or SessionLocal is None:
        yield None
        return

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
