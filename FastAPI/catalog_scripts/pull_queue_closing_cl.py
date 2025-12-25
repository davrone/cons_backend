#!/usr/bin/env python3
"""
Скрипт для загрузки закрытия очереди для консультантов из 1C:ЦЛ через OData.

Загружает InformationRegister_ЗакрытиеОчередиНаКонсультанта с пагинацией.
При обнаружении закрытия очереди отправляет уведомления клиентам о скором переназначении.
"""
import os
import sys
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from urllib.parse import quote
import requests
from sqlalchemy import select, text, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import QueueClosing, Consultation, User
from FastAPI.services.chatwoot_client import ChatwootClient
from FastAPI.services.manager_notifications import send_queue_update_notification

# Конфигурация
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_DATE = os.getenv("ETL_QUEUE_CLOSING_INITIAL_FROM", "2025-12-01")
MAX_ERROR_LOGS = int(os.getenv("ETL_MAX_ERROR_LOGS", "5"))

ENTITY = "InformationRegister_ЗакрытиеОчередиНаКонсультанта"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_queue_closing")

# OData настройки
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Подключение к БД
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

HEADERS = {
    "User-Agent": "cons-middleware/queue-closing-loader",
    "Accept": "application/json",
}


def clean_uuid(val: Optional[str]) -> Optional[str]:
    """Очистка UUID"""
    if not val or val == "00000000-0000-0000-0000-000000000000":
        return None
    return val


def clean_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Преобразует Edm.DateTime в datetime"""
    if not dt_str or dt_str.startswith("0001-01-01"):
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120):
    """HTTP GET с retry и backoff"""
    session = requests.Session()
    attempt = 0
    while True:
        try:
            resp = session.get(url, auth=auth, headers=HEADERS, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("HTTP %s — retry in %s sec (attempt %s)", resp.status_code, wait, attempt + 1)
                time.sleep(wait)
                attempt += 1
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as ex:
            if attempt >= max_retries:
                logger.error("HTTP error after %s attempts: %s", attempt + 1, ex)
                raise
            wait = min(2 ** attempt, 60)
            logger.warning("Request failed: %s — retry in %s sec (attempt %s)", ex, wait, attempt + 1)
            time.sleep(wait)
            attempt += 1


async def get_last_sync_date(db: AsyncSession) -> Optional[datetime]:
    """Получить дату последней синхронизации"""
    result = await db.execute(
        text("SELECT last_synced_at FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY}
    )
    row = result.first()
    if row and row[0]:
        return row[0]
    return None


async def save_sync_date(db: AsyncSession, sync_date: datetime):
    """Сохранить дату синхронизации"""
    await db.execute(
        text("""
            INSERT INTO sys.sync_state (entity_name, last_synced_at)
            VALUES (:entity, :date)
            ON CONFLICT (entity_name) DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at
        """),
        {"entity": ENTITY, "date": sync_date}
    )


async def process_queue_closing_item(
    db: AsyncSession,
    item: Dict[str, Any],
    chatwoot_client: Optional[ChatwootClient] = None,
    current_date: Optional[datetime] = None
):
    """
    Обработать одну запись закрытия очереди.
    
    ВАЖНО: Обрабатываем только записи, где:
    - Дата соответствует текущему дню
    - Закрыт = true (очередь закрыта)
    Одна запись в регистре действует ровно на один день.
    """
    if current_date is None:
        current_date = datetime.now(timezone.utc)
    
    # Используем поле "Дата" вместо "Period"
    date_str = item.get("Дата")
    if not date_str:
        return  # Пропускаем записи без даты
    
    date_dt = clean_datetime(date_str)
    if not date_dt:
        return  # Пропускаем записи с невалидной датой
    
    # Нормализуем дату до начала дня для сравнения
    date_only = date_dt.date()
    current_date_only = current_date.date()
    
    # Обрабатываем только записи для текущего дня
    if date_only != current_date_only:
        return  # Пропускаем записи не для текущего дня
    
    manager_key = clean_uuid(item.get("Менеджер_Key"))
    if not manager_key:
        return  # Пропускаем записи без менеджера
    
    # Проверяем поле "Закрыт" - если false или отсутствует, очередь открыта
    closed = item.get("Закрыт", False)
    if not closed:
        # Если Закрыт = false, удаляем запись о закрытии (если была)
        # Используем начало дня для поиска записи
        period_start_of_day = datetime.combine(date_only, datetime.min.time()).replace(tzinfo=timezone.utc)
        await db.execute(
            text("""
                DELETE FROM cons.queue_closing 
                WHERE date_trunc('day', period) = date_trunc('day', :period_start) 
                  AND manager_key = :manager_key
            """),
            {"period_start": period_start_of_day, "manager_key": manager_key}
        )
        logger.debug(f"Queue opened for manager {manager_key} on {date_only} (Закрыт=false)")
        return
    
    # Закрыт = true - сохраняем запись о закрытии
    # Используем начало дня для period
    period_start_of_day = datetime.combine(date_only, datetime.min.time()).replace(tzinfo=timezone.utc)
    
    # Проверяем, существует ли уже такая запись
    existing = await db.execute(
        select(QueueClosing).where(
            func.date_trunc('day', QueueClosing.period) == func.date_trunc('day', period_start_of_day),
            QueueClosing.manager_key == manager_key
        ).limit(1)
    )
    is_new = existing.scalar_one_or_none() is None
    
    # Подготавливаем данные для вставки
    values = {
        "period": period_start_of_day,
        "manager_key": manager_key
    }
    
    stmt = insert(QueueClosing).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["period", "manager_key"],
        set_={"period": stmt.excluded.period}
    )
    await db.execute(stmt)
    
    # Если это новая запись о закрытии, отправляем уведомления клиентам
    if is_new:
        # Находим все активные консультации этого менеджера
        consultations_result = await db.execute(
            select(Consultation).where(
                Consultation.manager == manager_key,
                Consultation.status.in_(["open", "pending"]),
                Consultation.denied == False
            )
        )
        consultations = consultations_result.scalars().all()
        
        # Получаем имя менеджера для уведомления
        manager_result = await db.execute(
            select(User).where(User.cl_ref_key == manager_key).limit(1)
        )
        manager = manager_result.scalar_one_or_none()
        manager_name = manager.description if manager else "менеджера"
        
        # Отправляем уведомления клиентам
        for consultation in consultations:
            if not consultation.cons_id or consultation.cons_id.startswith("cl_"):
                continue
            
            try:
                if chatwoot_client:
                    date_str = date_only.strftime("%d.%m.%Y")
                    message_content = (
                        f"⚠️ Очередь для {manager_name} закрыта на {date_str}. "
                        f"В скором времени ваша консультация будет переназначена другому менеджеру."
                    )
                    
                    # Используем send_message вместо send_note, так как note сообщения не видны клиенту
                    await chatwoot_client.send_message(
                        conversation_id=consultation.cons_id,
                        content=message_content,
                        message_type="outgoing"
                    )
                    logger.info(f"Sent queue closing notification to Chatwoot for consultation {consultation.cons_id}")
            except Exception as e:
                logger.warning(f"Failed to send queue closing notification for consultation {consultation.cons_id}: {e}")


async def pull_queue_closing():
    """Основная функция загрузки закрытия очереди"""
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)
    
    auth = (ODATA_USER, ODATA_PASSWORD)
    # ВАЖНО: Настраиваем пул соединений для ETL скрипта
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30
    )
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
    
    try:
        async with AsyncSessionLocal() as db:
            # Получаем дату последней синхронизации
            last_sync = await get_last_sync_date(db)
            
            if last_sync:
                from_dt = last_sync - timedelta(days=1)  # Буфер 1 день
                from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
                logger.info("Incremental sync from %s (last sync: %s)", from_date, last_sync)
            else:
                from_date = f"{INITIAL_FROM_DATE}T00:00:00"
                logger.info("First run — loading from %s", from_date)
            
            # ВАЖНО: Получаем текущую дату ДО инициализации last_processed_at
            # Это нужно для ограничения last_sync текущей датой (не используем будущие даты)
            current_date = datetime.now(timezone.utc)
            current_date_str = current_date.strftime("%Y-%m-%d")
            
            total_processed = 0
            skip = 0
            error_logs = 0
            last_processed_at: Optional[datetime] = None
            # ВАЖНО: Инициализируем last_processed_at текущей датой, если нет last_sync
            # Это гарантирует, что мы не будем использовать будущие даты
            if last_sync:
                # Если есть last_sync, используем его, но ограничиваем текущей датой
                last_processed_at = min(last_sync, current_date)
            else:
                # Если нет last_sync, используем текущую дату
                last_processed_at = current_date
            
            # Инициализируем ChatwootClient для отправки уведомлений
            chatwoot_client = None
            try:
                if settings.CHATWOOT_API_URL and settings.CHATWOOT_API_TOKEN:
                    chatwoot_client = ChatwootClient()
                    logger.info("Chatwoot client initialized for sending queue closing notifications")
                else:
                    logger.warning("Chatwoot credentials not configured, skipping notifications")
            except Exception as e:
                logger.warning(f"Failed to initialize Chatwoot client: {e}, continuing without notifications")
            
            while True:
                # Формируем фильтр по полю "Дата" (не Period)
                # Загружаем записи за последние 7 дней для проверки изменений
                filter_part = f"Дата ge datetime'{from_date}'"
                
                # ВАЖНО: Правильное кодирование кириллицы для OData
                encoded_filter = quote(filter_part, safe="'()=<>", encoding='utf-8')
                encoded_orderby = quote("Дата asc", safe=",", encoding='utf-8')
                
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={encoded_filter}"
                    f"&$orderby={encoded_orderby}"
                    f"&$top={PAGE_SIZE}&$skip={skip}"
                )
                
                try:
                    resp = http_get_with_backoff(url, auth, timeout=120)
                except Exception as e:
                    logger.exception("Failed to fetch batch: %s", e)
                    error_logs += 1
                    if error_logs >= MAX_ERROR_LOGS:
                        break
                    skip += PAGE_SIZE
                    continue
                
                batch = resp.json().get("value", [])
                if not batch:
                    break
                
                logger.info("Processing batch: %s items (skip=%s)", len(batch), skip)
                
                # Обрабатываем каждую запись
                for item in batch:
                    try:
                        await process_queue_closing_item(db, item, chatwoot_client, current_date)
                        
                        # Обновляем last_processed_at по полю "Дата"
                        # ВАЖНО: Ограничиваем last_processed_at текущей датой (не используем будущие даты)
                        # Это предотвращает сдвиг last_sync в будущее из-за запланированных закрытий очереди
                        date_value = clean_datetime(item.get("Дата"))
                        if date_value:
                            # Ограничиваем дату текущей датой
                            if date_value > current_date:
                                logger.debug(
                                    f"Skipping future date for last_sync: {date_value} (current: {current_date}) "
                                    f"for queue closing item"
                                )
                                # Если last_processed_at еще не установлен, устанавливаем текущую дату
                                if last_processed_at is None:
                                    last_processed_at = current_date
                                # Если last_processed_at уже установлен, но меньше current_date, обновляем до current_date
                                elif last_processed_at < current_date:
                                    last_processed_at = current_date
                            else:
                                # Дата в прошлом или настоящем - используем её
                                if last_processed_at is None or date_value > last_processed_at:
                                    last_processed_at = date_value
                        
                        total_processed += 1
                    except Exception as e:
                        logger.exception("Failed to process queue closing item: %s", e)
                        error_logs += 1
                        if error_logs >= MAX_ERROR_LOGS:
                            logger.error("Too many errors, stopping")
                            break
                
                await db.commit()
                
                # ВАЖНО: Сохраняем sync_state после каждого батча для устойчивости при прерывании
                # ВАЖНО: Ограничиваем last_processed_at текущей датой перед сохранением
                if last_processed_at:
                    # Ограничиваем текущей датой (не используем будущие даты)
                    sync_date_to_save = min(last_processed_at, current_date)
                    try:
                        await save_sync_date(db, sync_date_to_save)
                        await db.commit()
                        logger.debug(f"✓ Sync state saved after batch: {sync_date_to_save}")
                    except Exception as sync_error:
                        logger.warning(f"Failed to save sync state after batch: {sync_error}")
                        # Не прерываем выполнение, продолжаем обработку
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            # Финальное сохранение даты синхронизации (на случай если последний батч не сохранил)
            # ВАЖНО: Ограничиваем last_processed_at текущей датой перед сохранением
            if last_processed_at:
                # Ограничиваем текущей датой (не используем будущие даты)
                sync_date_to_save = min(last_processed_at, current_date)
                await save_sync_date(db, sync_date_to_save)
                await db.commit()
                logger.info("✓ Final sync date saved: %s", sync_date_to_save)
            
            logger.info("Queue closing sync completed. Total processed: %s", total_processed)
    
    except Exception as e:
        logger.exception("Fatal error in pull_queue_closing: %s", e)
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(pull_queue_closing())

