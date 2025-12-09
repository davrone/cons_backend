"""add notification_log table

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2025-01-27 15:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "e1f2a3b4c5d6"
down_revision = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


def _table_exists(conn, table_name: str, schema: str = "log") -> bool:
    """Проверяет существование таблицы"""
    inspector = inspect(conn)
    return inspector.has_table(table_name, schema=schema)


def upgrade() -> None:
    conn = op.get_bind()
    
    # Создаем схему log если её нет
    op.execute("CREATE SCHEMA IF NOT EXISTS log")
    
    if not _table_exists(conn, "notification_log", schema="log"):
        op.create_table(
            "notification_log",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("notification_type", sa.Text(), nullable=False),
            sa.Column("entity_id", sa.Text(), nullable=False),
            sa.Column("unique_hash", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("unique_hash", name="uq_notification_log_hash"),
            schema="log",
        )
        
        # Создаем индексы для быстрого поиска
        op.create_index(
            "ix_notification_log_type_entity",
            "notification_log",
            ["notification_type", "entity_id"],
            schema="log",
        )
        op.create_index(
            "ix_notification_log_hash",
            "notification_log",
            ["unique_hash"],
            schema="log",
        )
        op.create_index(
            "ix_notification_log_created_at",
            "notification_log",
            ["created_at"],
            schema="log",
        )


def downgrade() -> None:
    op.drop_index("ix_notification_log_created_at", table_name="notification_log", schema="log")
    op.drop_index("ix_notification_log_hash", table_name="notification_log", schema="log")
    op.drop_index("ix_notification_log_type_entity", table_name="notification_log", schema="log")
    op.drop_table("notification_log", schema="log")

