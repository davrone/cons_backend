"""add partner to clients

Revision ID: k8a9b0c1d2e3
Revises: k7a8b9c0d1e2
Create Date: 2025-01-27 21:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "k8a9b0c1d2e3"
down_revision = "k7a8b9c0d1e2"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки"""
    inspector = inspect(conn)
    columns = inspector.get_columns(table_name, schema=schema)
    return any(col["name"] == column_name for col in columns)


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "clients", "partner", schema="cons"):
        op.add_column(
            "clients",
            sa.Column("partner", sa.Text(), nullable=True, comment="Обслуживающая организация (partner) для передачи в Chatwoot"),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("clients", "partner", schema="cons")

