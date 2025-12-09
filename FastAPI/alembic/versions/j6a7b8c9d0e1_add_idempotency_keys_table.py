"""add idempotency_keys table

Revision ID: j6a7b8c9d0e1
Revises: i5a6b7c8d9e0
Create Date: 2025-01-27 20:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from datetime import datetime, timezone, timedelta


revision = "j6a7b8c9d0e1"
down_revision = "i5a6b7c8d9e0"
branch_labels = None
depends_on = None


def _table_exists(conn, table_name: str, schema: str = "sys") -> bool:
    """Проверяет существование таблицы"""
    inspector = inspect(conn)
    return inspector.has_table(table_name, schema=schema)


def upgrade() -> None:
    conn = op.get_bind()
    
    # Создаем схему sys если её нет
    op.execute("CREATE SCHEMA IF NOT EXISTS sys")
    
    if not _table_exists(conn, "idempotency_keys", schema="sys"):
        op.create_table(
            "idempotency_keys",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("key", sa.Text(), nullable=False),
            sa.Column("operation_type", sa.Text(), nullable=False),
            sa.Column("resource_id", sa.Text(), nullable=True),
            sa.Column("request_hash", sa.Text(), nullable=True),
            sa.Column("response_data", sa.dialects.postgresql.JSONB(), nullable=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("key", "operation_type", name="uq_idempotency_key"),
            schema="sys",
        )
        
        # Создаем индексы
        op.create_index(
            "ix_idempotency_keys_key",
            "idempotency_keys",
            ["key"],
            schema="sys",
        )
        op.create_index(
            "ix_idempotency_keys_expires_at",
            "idempotency_keys",
            ["expires_at"],
            schema="sys",
        )
        
        # Создаем задачу для очистки истекших ключей (можно запускать периодически)
        # Это можно сделать через APScheduler или cron


def downgrade() -> None:
    op.drop_index("ix_idempotency_keys_expires_at", table_name="idempotency_keys", schema="sys")
    op.drop_index("ix_idempotency_keys_key", table_name="idempotency_keys", schema="sys")
    op.drop_table("idempotency_keys", schema="sys")

