"""add consultation_enabled to users

Revision ID: k7a8b9c0d1e2
Revises: j6a7b8c9d0e1
Create Date: 2025-01-27 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "k7a8b9c0d1e2"
down_revision = "j6a7b8c9d0e1"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "users", "consultation_enabled"):
        op.add_column(
            "users",
            sa.Column("consultation_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("users", "consultation_enabled", schema="cons")

