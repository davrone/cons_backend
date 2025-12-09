"""add user_mapping table

Revision ID: b8c4d5e6f7a9
Revises: a703616e9a3a
Create Date: 2025-01-27 16:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "b8c4d5e6f7a9"
down_revision = "a703616e9a3a"
branch_labels = None
depends_on = None


def _table_exists(conn, table_name: str, schema: str = "sys") -> bool:
    """Проверяет существование таблицы"""
    inspector = inspect(conn)
    tables = inspector.get_table_names(schema=schema)
    return table_name in tables


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _table_exists(conn, "user_mapping"):
        op.create_table(
            "user_mapping",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("chatwoot_user_id", sa.Integer(), nullable=False),
            sa.Column("cl_manager_key", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("chatwoot_user_id", name="uq_user_mapping_chatwoot_user_id"),
            sa.UniqueConstraint("cl_manager_key", name="uq_user_mapping_cl_manager_key"),
            schema="sys",
        )


def downgrade() -> None:
    op.drop_table("user_mapping", schema="sys")

