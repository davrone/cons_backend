"""add queue_closing table

Revision ID: c9d5e7f8a0b1
Revises: b8c4d5e6f7a9
Create Date: 2025-01-28 10:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "c9d5e7f8a0b1"
down_revision = "b8c4d5e6f7a9"
branch_labels = None
depends_on = None


def _table_exists(conn, table_name: str, schema: str = "cons") -> bool:
    """Проверяет существование таблицы"""
    inspector = inspect(conn)
    tables = inspector.get_table_names(schema=schema)
    return table_name in tables


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _table_exists(conn, "queue_closing"):
        op.create_table(
            "queue_closing",
            sa.Column("period", sa.DateTime(timezone=True), nullable=False),
            sa.Column("manager_key", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("period", "manager_key"),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_table("queue_closing", schema="cons")

