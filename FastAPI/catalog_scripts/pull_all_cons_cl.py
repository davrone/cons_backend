#!/usr/bin/env python3
"""
Скрипт для загрузки ВСЕХ консультаций из 1C:ЦЛ через OData (без фильтра по Parent_Key).

Используется для расчета очереди консультантов, так как консультанты обслуживают
не только клиентов нашего сервиса, но и других клиентов ЦЛ.

Загружает Document_ТелефонныйЗвонок с пагинацией и обновляет:
- cons.cons (консультации) - только для расчета очереди, не для отображения

Использует инкрементальную загрузку по дате изменения.
ВАЖНО: При первоначальной загрузке (пустая БД) загружает все консультации с момента INITIAL_FROM_DATE,
включая те, которые не являются родителем клиентов по нашему сервису.
"""
import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from urllib.parse import quote
import requests
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from FastAPI.config import settings
from FastAPI.models import Consultation, QAndA, Client

# Конфигурация
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_DATE = os.getenv("ETL_INITIAL_FROM_DATE", "2025-01-01")

ENTITY = "Document_ТелефонныйЗвонок_ALL"  # Отдельная сущность для отслеживания синхронизации

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_all_cons_cl")

# OData настройки (используем отдельный URL для ЦЛ)
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Подключение к БД
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


def map_status(vid_obrascheniya: Optional[str], end_date: Optional[datetime] = None) -> str:
    """
    Маппинг ВидОбращения в статус с учетом поля Конец.
    """
    if end_date:
        return "closed"
    
    if not vid_obrascheniya:
        return "new"
    
    vid = vid_obrascheniya.strip()
    if vid == "КонсультацияИТС":
        return "open"
    elif vid == "ВОчередьНаКонсультацию":
        return "pending"
    elif vid == "Другое":
        return "other"
    return "new"


def map_consultation_type(vid_obrascheniya: Optional[str]) -> str:
    """Все консультации из ЦЛ - это "Консультация по ведению учёта" """
    return "Консультация по ведению учёта"


def clean_uuid(val: Optional[str]) -> Optional[str]:
    """Очистка UUID"""
    if not val or val == "00000000-0000-0000-0000-000000000000":
        return None
    return val


def clean_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Преобразует Edm.DateTime в datetime, 0001-01-01 → None"""
    if not dt_str or dt_str.startswith("0001-01-01"):
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except:
        return None


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120):
    """HTTP GET с retry и backoff"""
    headers = {
        "User-Agent": "ETL-Consultations-All/1.0",
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
    """Получить дату последней синхронизации для всех консультаций"""
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


async def process_consultation_item(
    db: AsyncSession,
    item: Dict[str, Any],
):
    """Обработать один документ консультации (только для расчета очереди)"""
    ref_key = item.get("Ref_Key")
    if not ref_key:
        logger.warning(f"Skipping item without Ref_Key: {item.get('Number', 'N/A')}")
        return None
    
    # Маппинг основных полей
    number = item.get("Number")
    client_key = clean_uuid(item.get("Абонент_Key"))
    manager_key = clean_uuid(item.get("Менеджер_Key"))
    
    # Даты
    create_date = clean_datetime(item.get("ДатаСоздания"))
    start_date = clean_datetime(item.get("ДатаКонсультации"))
    end_date = clean_datetime(item.get("Конец"))
    
    # Статус
    vid_obrascheniya = item.get("ВидОбращения")
    status = map_status(vid_obrascheniya, end_date)
    consultation_type = map_consultation_type(vid_obrascheniya)
    
    # Найти или создать консультацию
    result = await db.execute(
        select(Consultation).where(Consultation.cl_ref_key == ref_key)
    )
    consultation = result.scalar_one_or_none()
    
    # Если нет, создаем новую (только для расчета очереди)
    if not consultation:
        consultation = Consultation(
            cons_id=f"cl_all_{ref_key}",  # Префикс для идентификации что это для очереди
            cl_ref_key=ref_key,
            client_key=client_key,
            client_id=None,  # Не связываем с клиентами нашего сервиса
            number=number,
            status=status,
            org_inn=None,  # Не заполняем для консультаций вне нашего сервиса
            consultation_type=consultation_type,
            denied=False,
            create_date=create_date or datetime.now(timezone.utc),
            start_date=start_date,
            end_date=end_date,
            comment="",
            manager=str(manager_key) if manager_key else None,
            author=None,
            online_question_cat=None,
            online_question=None,
            source="1C_CL_ALL",  # Указываем источник - все консультации
        )
        db.add(consultation)
        logger.debug(f"Created consultation for queue calculation: cl_ref_key={ref_key}, number={number}, status={status}")
    else:
        # Обновляем существующую только если поля изменились
        has_changes = False
        
        if consultation.number != number:
            consultation.number = number
            has_changes = True
        
        if consultation.status != status:
            consultation.status = status
            has_changes = True
        
        if consultation.start_date != start_date:
            consultation.start_date = start_date
            has_changes = True
        
        if consultation.end_date != end_date:
            consultation.end_date = end_date
            has_changes = True
        
        new_manager = str(manager_key) if manager_key else None
        if consultation.manager != new_manager:
            consultation.manager = new_manager or consultation.manager
            has_changes = True
        
        # Если изменений нет, пропускаем обновление
        if not has_changes:
            return create_date or start_date or datetime.now(timezone.utc)
    
    await db.flush()
    
    return create_date or start_date or datetime.now(timezone.utc)


async def pull_all_consultations():
    """Основная функция загрузки всех консультаций (для расчета очереди)"""
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
                # Инкрементальная загрузка с буфером
                from_dt = last_sync - timedelta(days=7)
                from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
                logger.info("Incremental sync from %s (last sync: %s)", from_date, last_sync)
            else:
                from_date = f"{INITIAL_FROM_DATE}T00:00:00"
                logger.info("First run — loading ALL consultations from %s (for queue calculation)", from_date)
            
            # Логируем информацию о фильтре
            logger.info(f"Loading ALL consultations (no Parent_Key filter) for queue calculation")
            logger.info(f"Date filter: ДатаСоздания ge datetime'{from_date}'")
            
            total_processed = 0
            skip = 0
            last_processed_at: Optional[datetime] = last_sync
            
            while True:
                # Формируем фильтр только по ДатаСоздания
                # ВАЖНО: Используем только ДатаСоздания для фильтрации
                filter_part = f"ДатаСоздания ge datetime'{from_date}'"
                
                # ВАЖНО: Правильное кодирование кириллицы для OData
                encoded_filter = quote(filter_part, safe="'()=<>", encoding='utf-8')
                encoded_orderby = quote("ДатаСоздания asc", safe=",", encoding='utf-8')
                
                url = (
                    f"{ODATA_BASEURL}Document_ТелефонныйЗвонок?$format=json"
                    f"&$filter={encoded_filter}"
                    f"&$orderby={encoded_orderby}"
                    f"&$top={PAGE_SIZE}&$skip={skip}"
                )
                
                logger.debug(f"Fetching URL: {url}")
                
                try:
                    resp = http_get_with_backoff(url, auth, timeout=120)
                except Exception as e:
                    logger.exception("Failed to fetch batch: %s", e)
                    break
                
                response_data = resp.json()
                batch = response_data.get("value", [])
                
                logger.info(f"📥 Fetched batch: {len(batch)} items (skip={skip})")
                if len(batch) == 0:
                    logger.info(f"⚠ No more items found. Filter: {filter_part[:100]}...")
                    break
                
                logger.info(f"🔄 Processing batch: {len(batch)} items (skip={skip})")
                
                # Обрабатываем каждую консультацию
                batch_created = 0
                batch_updated = 0
                batch_errors = 0
                for idx, item in enumerate(batch):
                    try:
                        ref_key = item.get("Ref_Key")
                        if not ref_key:
                            logger.warning(f"⚠ Item {idx+1} in batch has no Ref_Key, skipping")
                            batch_errors += 1
                            continue
                        
                        processed_at = await process_consultation_item(db, item)
                        
                        if processed_at and (
                            last_processed_at is None or processed_at > last_processed_at
                        ):
                            last_processed_at = processed_at
                    except Exception as e:
                        batch_errors += 1
                        logger.error(f"✗ Error processing consultation {item.get('Ref_Key', 'N/A')[:20]}: {e}", exc_info=True)
                        continue
                
                if batch_errors > 0:
                    logger.error(f"✗ Batch had {batch_errors} errors out of {len(batch)} items")
                
                await db.commit()
                total_processed += len(batch)
                logger.info("✓ Processed batch: %s items (total: %s)", len(batch), total_processed)
                
                # ВАЖНО: Сохраняем sync_state после каждого батча для устойчивости при прерывании
                if last_processed_at:
                    try:
                        await save_sync_date(db, last_processed_at)
                        await db.commit()
                        logger.debug(f"✓ Sync state saved after batch: {last_processed_at}")
                    except Exception as sync_error:
                        logger.warning(f"Failed to save sync state after batch: {sync_error}")
                        # Не прерываем выполнение, продолжаем обработку
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            # Финальное сохранение даты синхронизации (на случай если последний батч не сохранил)
            if last_processed_at:
                await save_sync_date(db, last_processed_at)
                await db.commit()
                logger.info(f"✓ Final sync date saved: {last_processed_at}")
            
            if total_processed == 0:
                logger.warning("⚠ No consultations were processed.")
            else:
                logger.info("✓ Sync completed. Total processed: %s (for queue calculation)", total_processed)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    # Создаем таблицу sync_state если её нет
    async def ensure_sync_state_table():
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
        await engine.dispose()
    
    asyncio.run(ensure_sync_state_table())
    asyncio.run(pull_all_consultations())
