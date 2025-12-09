#!/usr/bin/env python3
"""
Скрипт для загрузки дозвонов из 1C:ЦЛ через OData.

Загружает InformationRegister_РегистрацияДозвона с пагинацией.
Дозвоны показывают попытки менеджера дозвониться до клиента.
"""
import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from urllib.parse import quote
import requests
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from FastAPI.config import settings
from FastAPI.models import Call, Consultation, Client, User
from FastAPI.services.chatwoot_client import ChatwootClient
from FastAPI.utils.notification_helpers import check_and_log_notification
from FastAPI.utils.etl_logging import ETLLogger

# Конфигурация
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_DATE = os.getenv("ETL_INITIAL_FROM_DATE", "2025-01-01")
MAX_ERROR_LOGS = int(os.getenv("ETL_MAX_ERROR_LOGS", "5"))

ENTITY = "InformationRegister_РегистрацияДозвона"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_calls_cl")

# OData настройки (используем URL для ЦЛ)
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Подключение к БД
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


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
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except:
        return None


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120):
    """HTTP GET с retry и backoff. Детальное логирование только для ошибок."""
    headers = {
        "User-Agent": "ETL-Calls/1.0",
        "Accept": "application/json",
    }
    s = requests.Session()
    attempt = 0
    
    while True:
        try:
            r = s.get(url, auth=auth, headers=headers, timeout=timeout)
            
            if r.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    r.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("⚠ HTTP %s — retry in %s sec (attempt %s/%s)", r.status_code, wait, attempt+1, max_retries+1)
                import time
                time.sleep(wait)
                attempt += 1
                continue
            
            r.raise_for_status()
            return r
        except requests.RequestException as ex:
            if attempt >= max_retries:
                logger.error("✗ HTTP error after %s attempts: %s", attempt+1, ex)
                logger.error("  URL: %s", url[:500])
                raise
            wait = min(2 ** attempt, 60)
            logger.debug("Request failed (attempt %s/%s): %s — retry in %s sec", attempt+1, max_retries+1, ex, wait)
            import time
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


async def process_call_item(db: AsyncSession, item: Dict[str, Any], chatwoot_client: Optional[ChatwootClient] = None):
    """Обработать одну запись дозвона"""
    period = clean_datetime(item.get("Period"))
    doc_key = clean_uuid(item.get("ДокументОбращения_Key"))  # cons_key
    client_key = clean_uuid(item.get("Абонент_Key"))
    manager_key = clean_uuid(item.get("Менеджер_Key"))
    
    if period and period.tzinfo is None:
        period = period.replace(tzinfo=timezone.utc)
    
    if not period or not doc_key:
        return  # Пропускаем записи без обязательных полей
    
    # Находим консультацию по ДокументОбращения_Key (cl_ref_key)
    result = await db.execute(
        select(Consultation).where(Consultation.cl_ref_key == doc_key).limit(1)
    )
    consultation = result.scalar_one_or_none()
    
    cons_id = consultation.cons_id if consultation else None
    
    # ВАЖНО: Если консультация не найдена, это может быть потому что:
    # 1. Консультация еще не загружена из 1C (ETL еще не успел)
    # 2. Консультация не относится к нашему сервису (нет Parent_Key)
    # В любом случае, сохраняем дозвон, но без cons_id (для статистики)
    if not consultation:
        logger.debug(f"Consultation not found for call: doc_key={doc_key[:20] if doc_key else 'N/A'}, period={period}")
    
    # Находим клиента по client_key
    client_id = None
    if client_key:
        result = await db.execute(
            select(Client.client_id).where(Client.cl_ref_key == client_key).limit(1)
        )
        row = result.first()
        if row:
            client_id = row[0]
    
    # Проверяем, существует ли уже такая запись
    existing_call = await db.execute(
        select(Call).where(
            Call.period == period,
            Call.cons_key == doc_key,
            Call.manager == manager_key
        ).limit(1)
    )
    is_new = existing_call.scalar_one_or_none() is None
    
    # Подготавливаем данные для вставки
    values = {
        "period": period,
        "cons_key": doc_key,
        "cons_id": cons_id,
        "client_key": client_key,
        "client_id": client_id,
        "manager": manager_key
    }
    
    stmt = insert(Call).values(**values)
    stmt = stmt.on_conflict_do_nothing(index_elements=["period", "cons_key", "manager"])
    await db.execute(stmt)
    
    # Если это новая запись и есть cons_id, отправляем note в Chatwoot
    if is_new and cons_id and chatwoot_client:
        # Проверяем, не было ли уже отправлено такое уведомление
        # ВАЖНО: Используем отдельную транзакцию для сохранения NotificationLog,
        # чтобы запись не потерялась при rollback основной транзакции
        notification_data = {
            "period": period.isoformat() if period else None,
            "cons_key": doc_key,
            "manager_key": manager_key
        }
        already_sent = await check_and_log_notification(
            db=db,
            notification_type="call",
            entity_id=cons_id,
            data=notification_data,
            use_separate_transaction=True  # Используем отдельную транзакцию для надежности
        )
        if already_sent:
            logger.debug(f"Call notification already sent for cons_id={cons_id}, period={period}, skipping")
            return
        
        try:
            # Получаем ФИО менеджера из БД
            manager_name = None
            if manager_key:
                try:
                    manager_result = await db.execute(
                        select(User.description)
                        .where(User.cl_ref_key == manager_key)
                        .where(User.deletion_mark == False)
                        .limit(1)
                    )
                    manager_name = manager_result.scalar_one_or_none()
                except Exception as e:
                    logger.warning(f"Failed to get manager name for {manager_key}: {e}")
            
            period_str = period.strftime("%d.%m.%Y %H:%M")
            note_content = f"📞 Попытка дозвона\nДата/время: {period_str}"
            if manager_name:
                note_content += f"\nМенеджер: {manager_name}"
            elif manager_key:
                # Fallback на UUID, если не удалось получить ФИО
                note_content += f"\nМенеджер: {manager_key[:8]}..."
            
            # Используем send_message вместо send_note, так как note сообщения не видны клиенту
            await chatwoot_client.send_message(
                conversation_id=cons_id,
                content=note_content,
                message_type="outgoing"
            )
            logger.debug(f"[pull_calls_cl] Sent call message to Chatwoot for consultation {cons_id}")
        except Exception as e:
            logger.debug(f"[pull_calls_cl] Failed to send call note to Chatwoot for consultation {cons_id}: {e}")


async def pull_calls():
    """Основная функция загрузки дозвонов"""
    etl_logger = ETLLogger("pull_calls_cl", ENTITY)
    
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        etl_logger.critical_error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)
    
    etl_logger.start({
        "ODATA_BASEURL": ODATA_BASEURL,
        "ENTITY": ENTITY,
        "INITIAL_FROM_DATE": INITIAL_FROM_DATE,
        "PAGE_SIZE": PAGE_SIZE
    })
    
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
                from_dt = last_sync - timedelta(hours=12)
                from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
                etl_logger.sync_info(last_sync, from_date, buffer_days=None)  # Буфер в часах, не днях
            else:
                from_date = f"{INITIAL_FROM_DATE}T00:00:00"
                etl_logger.sync_info(None, from_date)
            
            skip = 0
            error_logs = 0
            last_processed_period: Optional[datetime] = last_sync
            
            # Инициализируем ChatwootClient для отправки уведомлений
            chatwoot_client = None
            try:
                if settings.CHATWOOT_API_URL and settings.CHATWOOT_API_TOKEN:
                    chatwoot_client = ChatwootClient()
                    logger.debug("[pull_calls_cl] Chatwoot client initialized")
                else:
                    logger.debug("[pull_calls_cl] Chatwoot credentials not configured, skipping call notes")
            except Exception as e:
                logger.warning(f"[pull_calls_cl] Failed to initialize Chatwoot client: {e}")
            
            batch_created = 0
            batch_errors = 0
            
            while True:
                batch_num = skip // PAGE_SIZE + 1
                filter_part = f"Period ge datetime'{from_date}'"
                
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={quote(filter_part)}"
                    f"&$orderby=Period asc"
                    f"&$top={PAGE_SIZE}&$skip={skip}"
                )
                
                etl_logger.batch_start(batch_num, skip, PAGE_SIZE)
                
                try:
                    resp = http_get_with_backoff(url, auth, timeout=120)
                except Exception as e:
                    etl_logger.batch_error(batch_num, e, skip)
                    break
                
                try:
                    response_data = resp.json()
                    batch = response_data.get("value", [])
                except Exception as json_error:
                    etl_logger.batch_error(batch_num, json_error, skip)
                    break
                
                if not batch:
                    break
                
                batch_created = 0
                batch_errors = 0
                
                # Обрабатываем каждый дозвон
                for item in batch:
                    try:
                        await process_call_item(db, item, chatwoot_client)
                        batch_created += 1
                        
                        # Отслеживаем последний обработанный период
                        period = clean_datetime(item.get("Period"))
                        if period:
                            if period.tzinfo is None:
                                period = period.replace(tzinfo=timezone.utc)
                            if last_processed_period is None or period > last_processed_period:
                                last_processed_period = period
                    except Exception as e:
                        batch_errors += 1
                        if error_logs < MAX_ERROR_LOGS:
                            etl_logger.item_error(item.get("Period", "N/A"), e, "call")
                        elif error_logs == MAX_ERROR_LOGS:
                            logger.warning("[pull_calls_cl] Further call processing errors suppressed")
                        error_logs += 1
                        continue
                
                # Коммитим транзакцию после обработки всего батча
                try:
                    await db.commit()
                except Exception as commit_error:
                    etl_logger.batch_error(batch_num, commit_error, skip)
                    await db.rollback()
                    raise
                
                # Логируем прогресс батча
                etl_logger.batch_progress(batch_num, len(batch), created=batch_created, errors=batch_errors)
                
                # Сохраняем sync_state после каждого батча
                if last_processed_period:
                    try:
                        await save_sync_date(db, last_processed_period)
                        await db.commit()
                        etl_logger.sync_state_saved(last_processed_period, batch_num)
                    except Exception as sync_error:
                        logger.warning(f"[pull_calls_cl] Failed to save sync state after batch: {sync_error}")
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            # Финальное сохранение даты синхронизации
            sync_date_to_save = last_processed_period or datetime.now(timezone.utc)
            await save_sync_date(db, sync_date_to_save)
            await db.commit()
            etl_logger.sync_state_saved(sync_date_to_save)
            etl_logger.finish(success=True)
    except Exception as e:
        etl_logger.finish(success=False, error=e)
        sys.exit(1)
    finally:
        await engine.dispose()


async def ensure_support_tables():
    """Создаем вспомогательные таблицы и индексы при необходимости."""
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=1,
        max_overflow=1,
        pool_pre_ping=True
    )
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sys.sync_state (
                entity_name TEXT PRIMARY KEY,
                last_synced_at TIMESTAMPTZ
            )
        """))
        await conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_calls_period_cons_manager
            ON cons.calls (period, cons_key, manager)
        """))
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(ensure_support_tables())
    asyncio.run(pull_calls())

