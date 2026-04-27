"""
Add normalized_color column to normalization_cache table migration

This migration adds the normalized_color column for storing extracted colors.
"""

from sqlalchemy import create_engine, text

from apps.app_nippon_rfq_matching.app.core.config import settings


def upgrade():
    """Add normalized_color column to normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # Add normalized_color column
        conn.execute(
            text("""
            ALTER TABLE normalization_cache
            ADD COLUMN normalized_color VARCHAR(100)
        """)
        )
        print("Added column: normalized_color")

        conn.commit()

    print("Successfully added normalized_color column to normalization_cache table")


def downgrade():
    """Remove normalized_color column from normalization_cache table"""
    engine = create_engine(settings.DATABASE_URL)

    with engine.connect() as conn:
        # SQLite doesn't support DROP COLUMN directly, need to recreate table
        conn.execute(
            text("""
            CREATE TABLE normalization_cache_new AS
            SELECT id, raw_text, normalized_text, product_type, match_confidence,
                   times_used, last_used_at, created_at, raw_text_embedding,
                   normalized_text_embedding, embedding_model
            FROM normalization_cache
        """)
        )

        conn.execute(text("DROP TABLE normalization_cache"))
        conn.execute(
            text("ALTER TABLE normalization_cache_new RENAME TO normalization_cache")
        )

        conn.commit()

    print("Successfully removed normalized_color column from normalization_cache table")


if __name__ == "__main__":
    print("Running migration: add_color_to_cache")
    print("=" * 50)
    upgrade()
    print("=" * 50)
    print("Migration completed successfully!")
