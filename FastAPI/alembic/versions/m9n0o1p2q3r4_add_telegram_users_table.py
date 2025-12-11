"""add telegram_users table

Revision ID: m9n0o1p2q3r4
Revises: k8a9b0c1d2e3
Create Date: 2025-01-28 10:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import UUID


revision = "m9n0o1p2q3r4"
down_revision = "k8a9b0c1d2e3"
branch_labels = None
depends_on = None


def _table_exists(conn, table_name: str, schema: str = "cons") -> bool:
    """Проверяет существование таблицы"""
    inspector = inspect(conn)
    return inspector.has_table(table_name, schema=schema)


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _table_exists(conn, "telegram_users", schema="cons"):
        op.create_table(
            "telegram_users",
            sa.Column("telegram_user_id", sa.BigInteger(), nullable=False, primary_key=True),
            sa.Column("client_id", UUID(as_uuid=True), sa.ForeignKey("cons.clients.client_id"), nullable=True),
            sa.Column("phone_number", sa.Text(), nullable=True),
            sa.Column("username", sa.Text(), nullable=True),
            sa.Column("first_name", sa.Text(), nullable=True),
            sa.Column("last_name", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
            schema="cons",
        )
        
        # Создаем индекс на client_id для быстрого поиска
        op.create_index(
            "idx_telegram_users_client_id",
            "telegram_users",
            ["client_id"],
            schema="cons",
        )


def downgrade() -> None:
    op.drop_index("idx_telegram_users_client_id", table_name="telegram_users", schema="cons")
    op.drop_table("telegram_users", schema="cons")

