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
import requests
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from FastAPI.config import settings
from FastAPI.models import Call, Consultation, Client

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
    """HTTP GET с retry и backoff"""
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
                logger.warning("HTTP %s — retry in %s sec (attempt %s)", r.status_code, wait, attempt+1)
                import time
                time.sleep(wait)
                attempt += 1
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as ex:
            if attempt >= max_retries:
                logger.error("HTTP error after %s attempts: %s", attempt+1, ex)
                raise
            wait = min(2 ** attempt, 60)
            logger.warning("Request failed: %s — retry in %s sec (attempt %s)", ex, wait, attempt+1)
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


async def process_call_item(db: AsyncSession, item: Dict[str, Any]):
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
    
    # Находим клиента по client_key
    client_id = None
    if client_key:
        result = await db.execute(
            select(Client.client_id).where(Client.cl_ref_key == client_key).limit(1)
        )
        row = result.first()
        if row:
            client_id = row[0]
    
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


async def pull_calls():
    """Основная функция загрузки дозвонов"""
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)
    
    auth = (ODATA_USER, ODATA_PASSWORD)
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
    
    try:
        async with AsyncSessionLocal() as db:
            # Получаем дату последней синхронизации
            last_sync = await get_last_sync_date(db)
            
            if last_sync:
                # Инкрементальная загрузка: -7 дней буфер
                from_date = (last_sync - timedelta(days=7)).strftime("%Y-%m-%d")
                logger.info("Incremental sync from %s (last sync: %s)", from_date, last_sync)
            else:
                from_date = INITIAL_FROM_DATE
                logger.info("First run — loading from %s", from_date)
            
            total_processed = 0
            skip = 0
            error_logs = 0
            
            while True:
                # Формируем фильтр по дате
                filter_part = f"Period ge datetime'{from_date}T00:00:00'"
                
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={filter_part}"
                    f"&$orderby=Period asc"
                    f"&$top={PAGE_SIZE}&$skip={skip}"
                )
                
                try:
                    resp = http_get_with_backoff(url, auth, timeout=120)
                except Exception as e:
                    logger.exception("Failed to fetch batch: %s", e)
                    break
                
                batch = resp.json().get("value", [])
                if not batch:
                    break
                
                logger.info("Processing batch: %s items (skip=%s)", len(batch), skip)
                
                # Обрабатываем каждый дозвон
                for item in batch:
                    try:
                        await process_call_item(db, item)
                    except Exception as e:
                        if error_logs < MAX_ERROR_LOGS:
                            logger.error("Error processing call %s: %s", item.get("Period"), e)
                        elif error_logs == MAX_ERROR_LOGS:
                            logger.error("Further call processing errors suppressed to avoid log spam")
                        error_logs += 1
                        await db.rollback()
                        continue
                
                await db.commit()
                total_processed += len(batch)
                logger.info("Processed %s calls (total: %s)", len(batch), total_processed)
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            # Сохраняем дату синхронизации
            await save_sync_date(db, datetime.now(timezone.utc))
            await db.commit()
            
            logger.info("✓ Sync completed. Total processed: %s", total_processed)
    finally:
        await engine.dispose()


async def ensure_support_tables():
    """Создаем вспомогательные таблицы и индексы при необходимости."""
    engine = create_async_engine(DATABASE_URL, echo=False)
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

