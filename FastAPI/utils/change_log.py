"""
Утилиты для логирования изменений консультаций и отслеживания синхронизации.
"""
import json
from typing import Any, Optional
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ConsultationChangeLog


async def log_consultation_change(
    db: AsyncSession,
    cons_id: str,
    field_name: str,
    old_value: Any,
    new_value: Any,
    source: str,
) -> None:
    """
    Логирует изменение поля консультации.
    
    Args:
        db: Сессия БД
        cons_id: ID консультации
        field_name: Название измененного поля
        old_value: Старое значение
        new_value: Новое значение
        source: Источник изменения (CHATWOOT, 1C_CL, API, ETL)
    """
    # Сериализуем значения в JSON строки
    old_value_str = json.dumps(old_value, ensure_ascii=False, default=str) if old_value is not None else None
    new_value_str = json.dumps(new_value, ensure_ascii=False, default=str) if new_value is not None else None
    
    change_log = ConsultationChangeLog(
        cons_id=cons_id,
        field_name=field_name,
        old_value=old_value_str,
        new_value=new_value_str,
        source=source,
        synced_to_chatwoot=False,
        synced_to_1c=False,
        created_at=datetime.now(timezone.utc)
    )
    db.add(change_log)
    await db.flush()


async def mark_change_synced(
    db: AsyncSession,
    cons_id: str,
    field_name: str,
    synced_to_chatwoot: bool = False,
    synced_to_1c: bool = False,
) -> None:
    """
    Отмечает изменение как синхронизированное.
    
    Args:
        db: Сессия БД
        cons_id: ID консультации
        field_name: Название поля
        synced_to_chatwoot: Синхронизировано в Chatwoot
        synced_to_1c: Синхронизировано в 1C:ЦЛ
    """
    from sqlalchemy import select, update
    
    # Обновляем последнюю запись для этого поля
    result = await db.execute(
        select(ConsultationChangeLog)
        .where(
            ConsultationChangeLog.cons_id == cons_id,
            ConsultationChangeLog.field_name == field_name
        )
        .order_by(ConsultationChangeLog.created_at.desc())
        .limit(1)
    )
    change_log = result.scalar_one_or_none()
    
    if change_log:
        if synced_to_chatwoot:
            change_log.synced_to_chatwoot = True
        if synced_to_1c:
            change_log.synced_to_1c = True
        await db.flush()

