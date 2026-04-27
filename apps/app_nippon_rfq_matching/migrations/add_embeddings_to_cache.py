"""
Add embedding columns to normalization_cache table migration

This migration adds embedding-related columns to support semantic search.
"""

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def upgrade():
    """Add embedding columns to normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Add new columns for embeddings
        conn.execute(
            text("""
            ALTER TABLE normalization_cache
            ADD COLUMN raw_text_embedding JSON
        """)
        )
        print("Added column: raw_text_embedding")

        conn.execute(
            text("""
            ALTER TABLE normalization_cache
            ADD COLUMN normalized_text_embedding JSON
        """)
        )
        print("Added column: normalized_text_embedding")

        conn.execute(
            text("""
            ALTER TABLE normalization_cache
            ADD COLUMN embedding_model VARCHAR(100)
        """)
        )
        print("Added column: embedding_model")

        conn.commit()

    print("Successfully added embedding columns to normalization_cache table")


def downgrade():
    """Remove embedding columns from normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Drop columns (SQLite syntax - different for PostgreSQL)
        # SQLite doesn't support DROP COLUMN directly, need to recreate table
        conn.execute(
            text("""
            CREATE TABLE normalization_cache_new AS
            SELECT id, raw_text, normalized_text, product_type,
                   match_confidence, times_used, last_used_at, created_at
            FROM normalization_cache
        """)
        )

        conn.execute(text("DROP TABLE normalization_cache"))
        conn.execute(
            text("ALTER TABLE normalization_cache_new RENAME TO normalization_cache")
        )

        conn.commit()

    print("Successfully removed embedding columns from normalization_cache table")


if __name__ == "__main__":
    print("Running migration: add_embeddings_to_cache")
    print("=" * 50)
    upgrade()
    print("=" * 50)
    print("Migration completed successfully!")
