"""add rating_date to cons_rating_answers

Revision ID: a703616e9a3a
Revises: 34f78fc716d7
Create Date: 2025-01-27 15:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "a703616e9a3a"
down_revision = "34f78fc716d7"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "cons_rating_answers", "rating_date"):
        op.add_column(
            "cons_rating_answers",
            sa.Column("rating_date", sa.DateTime(timezone=True), nullable=True),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("cons_rating_answers", "rating_date", schema="cons")

