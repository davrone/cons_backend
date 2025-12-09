"""add client naming fields

Revision ID: 7a1f269d9a4a
Revises: 
Create Date: 2025-11-27 12:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "7a1f269d9a4a"
down_revision = None
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "clients", "name"):
        op.add_column(
            "clients",
            sa.Column("name", sa.Text(), nullable=True),
            schema="cons",
        )
    
    if not _column_exists(conn, "clients", "contact_name"):
        op.add_column(
            "clients",
            sa.Column("contact_name", sa.Text(), nullable=True),
            schema="cons",
        )
    
    if not _column_exists(conn, "clients", "code_abonent"):
        op.add_column(
            "clients",
            sa.Column("code_abonent", sa.Text(), nullable=True),
            schema="cons",
        )
    
    if not _column_exists(conn, "clients", "subscriber_id"):
        op.add_column(
            "clients",
            sa.Column("subscriber_id", sa.Text(), nullable=True),
            schema="cons",
        )


def downgrade() -> None:
    op.drop_column("clients", "code_abonent", schema="cons")
    op.drop_column("clients", "contact_name", schema="cons")
    op.drop_column("clients", "name", schema="cons")

