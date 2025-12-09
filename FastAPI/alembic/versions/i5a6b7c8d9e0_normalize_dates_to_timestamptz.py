"""normalize dates to timestamptz

Revision ID: i5a6b7c8d9e0
Revises: h4a5b6c7d8e9
Create Date: 2025-01-27 19:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision = "i5a6b7c8d9e0"
down_revision = "h4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Нормализует все даты в БД к единому формату TIMESTAMPTZ.
    
    ВАЖНО: Перед выполнением миграции рекомендуется сделать backup БД.
    """
    conn = op.get_bind()
    
    # Нормализуем period в cons.cons_redate
    # Преобразуем все значения к UTC, если timezone отсутствует
    op.execute("""
        UPDATE cons.cons_redate
        SET period = CASE
            WHEN period::text LIKE '%+%' OR period::text LIKE '%-%' THEN period AT TIME ZONE 'UTC'
            ELSE (period AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE period IS NOT NULL
    """)
    
    # Нормализуем old_date и new_date
    op.execute("""
        UPDATE cons.cons_redate
        SET old_date = CASE
            WHEN old_date::text LIKE '%+%' OR old_date::text LIKE '%-%' THEN old_date AT TIME ZONE 'UTC'
            ELSE (old_date AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE old_date IS NOT NULL
    """)
    
    op.execute("""
        UPDATE cons.cons_redate
        SET new_date = CASE
            WHEN new_date::text LIKE '%+%' OR new_date::text LIKE '%-%' THEN new_date AT TIME ZONE 'UTC'
            ELSE (new_date AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE new_date IS NOT NULL
    """)
    
    # Нормализуем period в cons.calls
    op.execute("""
        UPDATE cons.calls
        SET period = CASE
            WHEN period::text LIKE '%+%' OR period::text LIKE '%-%' THEN period AT TIME ZONE 'UTC'
            ELSE (period AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE period IS NOT NULL
    """)
    
    # Нормализуем period в cons.queue_closing
    op.execute("""
        UPDATE cons.queue_closing
        SET period = CASE
            WHEN period::text LIKE '%+%' OR period::text LIKE '%-%' THEN period AT TIME ZONE 'UTC'
            ELSE (period AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE period IS NOT NULL
    """)
    
    # Нормализуем rating_date в cons.cons_rating_answers
    op.execute("""
        UPDATE cons.cons_rating_answers
        SET rating_date = CASE
            WHEN rating_date::text LIKE '%+%' OR rating_date::text LIKE '%-%' THEN rating_date AT TIME ZONE 'UTC'
            ELSE (rating_date AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        END
        WHERE rating_date IS NOT NULL
    """)


def downgrade() -> None:
    """
    Откат миграции не требуется - мы только нормализуем данные,
    не меняя структуру таблиц.
    """
    pass

