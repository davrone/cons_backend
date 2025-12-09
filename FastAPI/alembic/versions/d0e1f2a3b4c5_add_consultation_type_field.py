"""add consultation_type field to cons table

Revision ID: d0e1f2a3b4c5
Revises: c9d5e7f8a0b1
Create Date: 2025-01-28 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "d0e1f2a3b4c5"
down_revision = "c9d5e7f8a0b1"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "cons", "consultation_type"):
        op.add_column(
            "cons",
            sa.Column("consultation_type", sa.Text(), nullable=True),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("cons", "consultation_type", schema="cons")

