"""
Утилиты для работы с idempotency keys.
"""
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta, date, time
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from ..models import IdempotencyKey


def generate_request_hash(request_data: Dict[str, Any]) -> str:
    """
    Генерирует хеш запроса для проверки идентичности.
    
    Args:
        request_data: Данные запроса
    
    Returns:
        SHA256 хеш запроса
    """
    def json_serializer(obj):
        """Кастомный сериализатор для JSON (поддержка datetime, date, time, bytes)"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        raise TypeError(f"Type {type(obj)} not serializable")
    
    # Сортируем ключи для консистентности
    sorted_data = json.dumps(request_data, sort_keys=True, ensure_ascii=False, default=json_serializer)
    return hashlib.sha256(sorted_data.encode('utf-8')).hexdigest()


async def check_idempotency_key(
    db: AsyncSession,
    key: str,
    operation_type: str,
    request_hash: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Проверяет idempotency key и возвращает кэшированный ответ если есть.
    
    Args:
        db: Сессия БД
        key: Idempotency key
        operation_type: Тип операции
        request_hash: Хеш запроса для проверки идентичности (опционально)
    
    Returns:
        Кэшированный response_data если ключ найден и валиден, None иначе
    """
    # Удаляем истекшие ключи
    await cleanup_expired_keys(db)
    
    # Ищем ключ
    result = await db.execute(
        select(IdempotencyKey).where(
            IdempotencyKey.key == key,
            IdempotencyKey.operation_type == operation_type,
            IdempotencyKey.expires_at > datetime.now(timezone.utc)
        )
    )
    idempotency_record = result.scalar_one_or_none()
    
    if not idempotency_record:
        return None
    
    # Если передан request_hash, проверяем идентичность запроса
    if request_hash and idempotency_record.request_hash:
        if idempotency_record.request_hash != request_hash:
            # Запрос отличается от оригинального - это ошибка
            return None
    
    # Возвращаем кэшированный ответ
    return idempotency_record.response_data


async def store_idempotency_key(
    db: AsyncSession,
    key: str,
    operation_type: str,
    resource_id: Optional[str] = None,
    request_hash: Optional[str] = None,
    response_data: Optional[Dict[str, Any]] = None,
    ttl_hours: int = 24
) -> None:
    """
    Сохраняет idempotency key в БД.
    Использует ON CONFLICT DO UPDATE для предотвращения дубликатов.
    
    Args:
        db: Сессия БД
        key: Idempotency key
        operation_type: Тип операции
        resource_id: ID созданного/обновленного ресурса
        request_hash: Хеш запроса
        response_data: Данные ответа для кэширования (будет сериализован в JSON)
        ttl_hours: Время жизни ключа в часах (по умолчанию 24 часа)
    """
    from sqlalchemy.dialects.postgresql import insert
    
    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
    
    # Сериализуем response_data в JSON-совместимый формат для JSONB поля
    serialized_response_data = None
    if response_data:
        def json_serializer(obj):
            """Кастомный сериализатор для JSON (поддержка datetime, date, bytes)"""
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, date):
                return obj.isoformat()
            elif isinstance(obj, bytes):
                return obj.decode('utf-8')
            raise TypeError(f"Type {type(obj)} not serializable")
        
        try:
            # Сериализуем в JSON строку и обратно в dict для очистки datetime объектов
            json_str = json.dumps(response_data, default=json_serializer)
            serialized_response_data = json.loads(json_str)
        except Exception as e:
            # Если не удалось сериализовать, логируем и не сохраняем response_data
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to serialize response_data for idempotency key {key}: {e}")
            serialized_response_data = None
    
    # Используем INSERT ... ON CONFLICT DO UPDATE для предотвращения дубликатов
    stmt = insert(IdempotencyKey).values(
        key=key,
        operation_type=operation_type,
        resource_id=resource_id,
        request_hash=request_hash,
        response_data=serialized_response_data,
        expires_at=expires_at
    )
    
    # При конфликте обновляем существующую запись
    stmt = stmt.on_conflict_do_update(
        constraint='uq_idempotency_key',
        set_=dict(
            resource_id=stmt.excluded.resource_id,
            request_hash=stmt.excluded.request_hash,
            response_data=stmt.excluded.response_data,
            expires_at=stmt.excluded.expires_at
        )
    )
    
    await db.execute(stmt)
    await db.flush()


async def cleanup_expired_keys(db: AsyncSession) -> int:
    """
    Удаляет истекшие idempotency keys.
    
    Args:
        db: Сессия БД
    
    Returns:
        Количество удаленных ключей
    """
    result = await db.execute(
        delete(IdempotencyKey).where(
            IdempotencyKey.expires_at < datetime.now(timezone.utc)
        )
    )
    deleted_count = result.rowcount
    await db.flush()
    return deleted_count

