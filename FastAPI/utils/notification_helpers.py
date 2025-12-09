"""
Утилиты для работы с уведомлениями и предотвращения дублирования.
"""
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models import NotificationLog
from ..database import AsyncSessionLocal


def generate_notification_hash(
    notification_type: str,
    entity_id: str,
    data: Optional[Dict[str, Any]] = None
) -> str:
    """
    Генерирует уникальный хеш уведомления на основе типа, ID сущности и данных.
    
    Args:
        notification_type: Тип уведомления (redate, rating, call, manager_reassignment, queue_update)
        entity_id: ID сущности (обычно cons_id)
        data: Дополнительные данные уведомления (дата, время, менеджер и т.д.)
    
    Returns:
        SHA256 хеш для идентификации уведомления
    """
    # Создаем строку для хеширования
    # ВАЖНО: Нормализуем данные для стабильного хеша
    # None значения заменяем на пустые строки, чтобы хеш был стабильным
    normalized_data = None
    if data:
        normalized_data = {}
        for key, value in data.items():
            if value is None:
                normalized_data[key] = ""
            elif isinstance(value, dict):
                # Рекурсивно нормализуем вложенные словари
                normalized_data[key] = {k: ("" if v is None else v) for k, v in value.items()}
            else:
                normalized_data[key] = value
    
    key_data = {
        "type": notification_type,
        "entity_id": entity_id,
    }
    if normalized_data:
        key_data["data"] = normalized_data
    
    key_string = json.dumps(key_data, sort_keys=True, ensure_ascii=False)
    
    # Генерируем SHA256 хеш
    return hashlib.sha256(key_string.encode('utf-8')).hexdigest()


async def check_and_log_notification(
    db: AsyncSession,
    notification_type: str,
    entity_id: str,
    data: Optional[Dict[str, Any]] = None,
    use_separate_transaction: bool = False
) -> bool:
    """
    Проверяет, было ли уже отправлено уведомление, и логирует его если нет.
    
    Args:
        db: Сессия БД (основная транзакция)
        notification_type: Тип уведомления (redate, rating, call, manager_reassignment, queue_update)
        entity_id: ID сущности (обычно cons_id)
        data: Дополнительные данные уведомления (опционально)
        use_separate_transaction: Если True, использует отдельную транзакцию для сохранения NotificationLog
                                  Это предотвращает потерю записи при rollback основной транзакции
    
    Returns:
        True если уведомление уже было отправлено (дубликат), False если новое (нужно отправить)
    """
    # Генерируем уникальный хеш
    unique_hash = generate_notification_hash(notification_type, entity_id, data)
    
    # Проверяем, существует ли уже такое уведомление
    # ВАЖНО: Проверяем в основной транзакции, чтобы видеть незакоммиченные записи
    result = await db.execute(
        select(NotificationLog).where(
            NotificationLog.unique_hash == unique_hash
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        # Уведомление уже было отправлено
        return True
    
    # Логируем новое уведомление
    if use_separate_transaction:
        # Используем отдельную транзакцию для гарантии сохранения записи
        # Это предотвращает потерю записи при rollback основной транзакции
        notification_db = None
        try:
            async with AsyncSessionLocal() as notification_db:
                # Проверяем еще раз в отдельной транзакции (на случай race condition)
                check_result = await notification_db.execute(
                    select(NotificationLog).where(
                        NotificationLog.unique_hash == unique_hash
                    )
                )
                if check_result.scalar_one_or_none():
                    return True
                
                notification_log = NotificationLog(
                    notification_type=notification_type,
                    entity_id=entity_id,
                    unique_hash=unique_hash
                )
                notification_db.add(notification_log)
                await notification_db.commit()
        finally:
            # Явно закрываем сессию для гарантии освобождения соединения
            if notification_db:
                try:
                    await notification_db.close()
                except Exception:
                    pass
    else:
        # Сохраняем в основной транзакции
        notification_log = NotificationLog(
            notification_type=notification_type,
            entity_id=entity_id,
            unique_hash=unique_hash
        )
        db.add(notification_log)
        await db.flush()  # Сохраняем в текущей транзакции
    
    # Возвращаем False - уведомление новое, нужно отправить
    return False


