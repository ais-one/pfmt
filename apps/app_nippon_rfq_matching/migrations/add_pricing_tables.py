"""
Migration: Add pricing tables for IATP Excel parser

This migration adds the following tables:
- regions: Store region names for consistent region management
- product_prices: Store pricing information for products across regions
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import text

# revision identifiers
revision = "20240110_add_pricing_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Create regions table
    op.create_table(
        "regions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(50), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(op.f("ix_regions_name"), "regions", ["name"], unique=True)

    # Create product_prices table
    op.create_table(
        "product_prices",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("product_master_id", sa.Integer(), nullable=False),
        sa.Column("region_id", sa.Integer(), nullable=False),
        sa.Column("size", sa.Float(), nullable=True),
        sa.Column("uom", sa.String(20), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("price_raw", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["product_master_id"],
            ["product_master.id"],
        ),
        sa.ForeignKeyConstraint(
            ["region_id"],
            ["regions.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("product_master_id", "region_id", "size", "uom"),
    )

    # Create index for product_prices
    op.create_index(
        op.f("ix_product_prices_product_master_id"),
        "product_prices",
        ["product_master_id"],
    )
    op.create_index(
        op.f("ix_product_prices_region_id"), "product_prices", ["region_id"]
    )

    # Add sample regions
    connection = op.get_bind()
    sample_regions = [("NPMC",), ("NPMS",), ("NPMK",), ("NPMEU",)]

    stmt = text("""
        INSERT INTO regions (name, created_at)
        VALUES (:name, CURRENT_TIMESTAMP)
        ON CONFLICT (name) DO NOTHING
    """)

    for (region_name,) in sample_regions:
        connection.execute(stmt, {"name": region_name})


def downgrade():
    # Drop product_prices table
    op.drop_table("product_prices")

    # Drop regions table
    op.drop_table("regions")
