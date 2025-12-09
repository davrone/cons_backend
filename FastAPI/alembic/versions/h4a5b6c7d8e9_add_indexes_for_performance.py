"""add indexes for performance

Revision ID: h4a5b6c7d8e9
Revises: g3a4b5c6d7e8
Create Date: 2025-01-27 18:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "h4a5b6c7d8e9"
down_revision = "g3a4b5c6d7e8"
branch_labels = None
depends_on = None


def _index_exists(conn, table_name: str, index_name: str, schema: str = "cons") -> bool:
    """Проверяет существование индекса"""
    inspector = inspect(conn)
    indexes = inspector.get_indexes(table_name, schema=schema)
    return any(idx["name"] == index_name for idx in indexes)


def upgrade() -> None:
    conn = op.get_bind()
    
    # Индексы для таблицы cons.cons
    if not _index_exists(conn, "cons", "ix_cons_cl_ref_key", schema="cons"):
        op.create_index("ix_cons_cl_ref_key", "cons", ["cl_ref_key"], schema="cons", postgresql_where=sa.text("cl_ref_key IS NOT NULL"))
    
    if not _index_exists(conn, "cons", "ix_cons_manager", schema="cons"):
        op.create_index("ix_cons_manager", "cons", ["manager"], schema="cons", postgresql_where=sa.text("manager IS NOT NULL"))
    
    if not _index_exists(conn, "cons", "ix_cons_start_date", schema="cons"):
        op.create_index("ix_cons_start_date", "cons", ["start_date"], schema="cons", postgresql_where=sa.text("start_date IS NOT NULL"))
    
    if not _index_exists(conn, "cons", "ix_cons_status", schema="cons"):
        op.create_index("ix_cons_status", "cons", ["status"], schema="cons", postgresql_where=sa.text("status IS NOT NULL"))
    
    if not _index_exists(conn, "cons", "ix_cons_manager_status", schema="cons"):
        op.create_index("ix_cons_manager_status", "cons", ["manager", "status"], schema="cons")
    
    # Индексы для таблицы cons.clients
    if not _index_exists(conn, "clients", "ix_clients_cl_ref_key", schema="cons"):
        op.create_index("ix_clients_cl_ref_key", "clients", ["cl_ref_key"], schema="cons", postgresql_where=sa.text("cl_ref_key IS NOT NULL"))
    
    if not _index_exists(conn, "clients", "ix_clients_source_id", schema="cons"):
        op.create_index("ix_clients_source_id", "clients", ["source_id"], schema="cons", postgresql_where=sa.text("source_id IS NOT NULL"))
    
    # Индексы для таблицы cons.users (для менеджеров)
    if not _index_exists(conn, "users", "ix_users_cl_ref_key", schema="cons"):
        op.create_index("ix_users_cl_ref_key", "users", ["cl_ref_key"], schema="cons", postgresql_where=sa.text("cl_ref_key IS NOT NULL"))


def downgrade() -> None:
    op.drop_index("ix_users_cl_ref_key", table_name="users", schema="cons")
    op.drop_index("ix_clients_source_id", table_name="clients", schema="cons")
    op.drop_index("ix_clients_cl_ref_key", table_name="clients", schema="cons")
    op.drop_index("ix_cons_manager_status", table_name="cons", schema="cons")
    op.drop_index("ix_cons_status", table_name="cons", schema="cons")
    op.drop_index("ix_cons_start_date", table_name="cons", schema="cons")
    op.drop_index("ix_cons_manager", table_name="cons", schema="cons")
    op.drop_index("ix_cons_cl_ref_key", table_name="cons", schema="cons")

