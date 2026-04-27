"""
Add normalization_cache table migration

This migration creates the normalization_cache table for caching
OpenAI normalization results.
"""

from sqlalchemy import create_engine

from apps.app_nippon_rfq_matching.app.core.config import settings
from apps.app_nippon_rfq_matching.app.models.database import NormalizationCache


def upgrade():
    """Create the normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)
    NormalizationCache.__table__.create(engine, checkfirst=True)
    print("Created normalization_cache table")


def downgrade():
    """Drop the normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)
    NormalizationCache.__table__.drop(engine, checkfirst=True)
    print("Dropped normalization_cache table")


if __name__ == "__main__":
    print("Running migration: add_normalization_cache")
    print("=" * 50)
    upgrade()
    print("=" * 50)
    print("Migration completed successfully!")
