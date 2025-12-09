"""add source field to consultation

Revision ID: g3a4b5c6d7e8
Revises: f2a3b4c5d6e7
Create Date: 2025-01-27 17:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "g3a4b5c6d7e8"
down_revision = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def _column_exists(conn, table_name: str, column_name: str, schema: str = "cons") -> bool:
    """Проверяет существование колонки в таблице"""
    inspector = inspect(conn)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
    return column_name in columns


def upgrade() -> None:
    conn = op.get_bind()
    
    if not _column_exists(conn, "cons", "source"):
        op.add_column(
            "cons",
            sa.Column("source", sa.Text(), nullable=True, server_default="BACKEND"),
            schema="cons",
        )
        
        # Обновляем существующие записи: если есть cl_ref_key и нет source, значит из 1C
        op.execute("""
            UPDATE cons.cons 
            SET source = CASE 
                WHEN cl_ref_key IS NOT NULL AND cons_id NOT LIKE 'temp_%' THEN '1C_CL'
                WHEN cons_id LIKE 'temp_%' THEN 'BACKEND'
                ELSE 'BACKEND'
            END
            WHERE source IS NULL
        """)


def downgrade() -> None:
    op.drop_column("cons", "source", schema="cons")

