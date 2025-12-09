"""add consultation_change_log table

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2025-01-27 16:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "f2a3b4c5d6e7"
down_revision = "e1f2a3b4c5d6"
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
    
    if not _table_exists(conn, "consultation_change_log", schema="log"):
        op.create_table(
            "consultation_change_log",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("cons_id", sa.Text(), nullable=False),
            sa.Column("field_name", sa.Text(), nullable=False),
            sa.Column("old_value", sa.Text(), nullable=True),
            sa.Column("new_value", sa.Text(), nullable=True),
            sa.Column("source", sa.Text(), nullable=False),
            sa.Column("synced_to_chatwoot", sa.Boolean(), nullable=False, server_default="false"),
            sa.Column("synced_to_1c", sa.Boolean(), nullable=False, server_default="false"),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            schema="log",
        )
        
        # Создаем индексы для быстрого поиска
        op.create_index(
            "ix_consultation_change_log_cons_id",
            "consultation_change_log",
            ["cons_id"],
            schema="log",
        )
        op.create_index(
            "ix_consultation_change_log_source",
            "consultation_change_log",
            ["source"],
            schema="log",
        )
        op.create_index(
            "ix_consultation_change_log_sync_status",
            "consultation_change_log",
            ["synced_to_chatwoot", "synced_to_1c"],
            schema="log",
        )


def downgrade() -> None:
    op.drop_index("ix_consultation_change_log_sync_status", table_name="consultation_change_log", schema="log")
    op.drop_index("ix_consultation_change_log_source", table_name="consultation_change_log", schema="log")
    op.drop_index("ix_consultation_change_log_cons_id", table_name="consultation_change_log", schema="log")
    op.drop_table("consultation_change_log", schema="log")

