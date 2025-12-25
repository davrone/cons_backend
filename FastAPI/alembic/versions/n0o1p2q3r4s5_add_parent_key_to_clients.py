"""add parent_key to clients

Revision ID: n0o1p2q3r4s5
Revises: m9n0o1p2q3r4
Create Date: 2025-12-25 19:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "n0o1p2q3r4s5"
down_revision = "m9n0o1p2q3r4"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    # Добавляем parent_key в таблицу clients
    if not _column_exists(conn, "clients", "parent_key", schema="cons"):
        op.add_column(
            "clients",
            sa.Column("parent_key", sa.Text(), nullable=True),
            schema="cons",
        )
        
        # Создаем индекс для быстрого поиска клиентов по parent_key
        op.create_index(
            "idx_clients_parent_key",
            "clients",
            ["parent_key"],
            schema="cons",
        )
        
        # Создаем составной индекс для поиска по code_abonent + org_inn + parent_key
        op.create_index(
            "idx_clients_code_inn_parent",
            "clients",
            ["code_abonent", "org_inn", "parent_key"],
            schema="cons",
        )


def downgrade() -> None:
    op.drop_index("idx_clients_code_inn_parent", table_name="clients", schema="cons")
    op.drop_index("idx_clients_parent_key", table_name="clients", schema="cons")
    op.drop_column("clients", "parent_key", schema="cons")
