"""add chatwoot_source_id to cons

Revision ID: 34f78fc716d7
Revises: 8b2c380f1e5a
Create Date: 2025-01-27 14:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "34f78fc716d7"
down_revision = "8b2c380f1e5a"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "cons", "chatwoot_source_id"):
        op.add_column(
            "cons",
            sa.Column("chatwoot_source_id", sa.Text(), nullable=True),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("cons", "chatwoot_source_id", schema="cons")

