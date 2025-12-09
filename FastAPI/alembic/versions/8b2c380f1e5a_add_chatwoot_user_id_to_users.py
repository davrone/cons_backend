"""add chatwoot_user_id to users

Revision ID: 8b2c380f1e5a
Revises: 7a1f269d9a4a
Create Date: 2025-11-27 13:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "8b2c380f1e5a"
down_revision = "7a1f269d9a4a"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "users", "chatwoot_user_id"):
        op.add_column(
            "users",
            sa.Column("chatwoot_user_id", sa.Integer(), nullable=True),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("users", "chatwoot_user_id", schema="cons")

