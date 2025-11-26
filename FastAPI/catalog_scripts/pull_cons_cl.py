#!/usr/bin/env python3
"""
Скрипт для загрузки консультаций из 1C:ЦЛ через OData.

Загружает Document_ТелефонныйЗвонок с пагинацией и обновляет:
- cons.cons (консультации)
- cons.q_and_a (вопросы и ответы)

Использует инкрементальную загрузку по дате изменения.
"""
import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
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

ENTITY = "Document_ТелефонныйЗвонок"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_cons_cl")

# OData настройки (используем отдельный URL для ЦЛ)
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Подключение к БД
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


def map_status(vid_obrascheniya: Optional[str]) -> str:
    """Маппинг ВидОбращения в статус"""
    if not vid_obrascheniya:
        return "new"
    vid = vid_obrascheniya.strip()
    if vid == "КонсультацияИТС":
        return "closed"  # Завершено
    elif vid == "ВОчередьНаКонсультацию":
        return "pending"  # Ожидание
    elif vid == "Другое":
        return "other"  # Другое
    return "new"


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
        # OData формат: "2025-10-20T09:28:15"
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except:
        return None


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120):
    """HTTP GET с retry и backoff"""
    headers = {
        "User-Agent": "ETL-Consultations/1.0",
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


async def find_client_by_key(db: AsyncSession, client_key: Optional[str]) -> Optional[str]:
    """Найти client_id по client_key из ЦЛ"""
    if not client_key:
        return None
    # Пока просто возвращаем ключ, в будущем можно добавить маппинг через отдельную таблицу
    # или искать по cl_ref_key в clients
    result = await db.execute(
        select(Client.client_id).where(Client.cl_ref_key == client_key).limit(1)
    )
    row = result.first()
    return str(row[0]) if row else None


async def process_consultation_item(
    db: AsyncSession,
    item: Dict[str, Any],
    chatwoot_cons_id: Optional[str] = None
):
    """Обработать один документ консультации"""
    ref_key = item.get("Ref_Key")
    if not ref_key:
        return
    
    # Маппинг основных полей
    number = item.get("Number")
    client_key = clean_uuid(item.get("Абонент_Key"))
    manager_key = clean_uuid(item.get("Менеджер_Key"))
    author_key = clean_uuid(item.get("Автор_Key"))
    
    # Статус из ВидОбращения
    status = map_status(item.get("ВидОбращения"))
    
    # Даты
    create_date = clean_datetime(item.get("ДатаСоздания"))
    start_date = clean_datetime(item.get("ДатаКонсультации"))
    end_date = clean_datetime(item.get("Конец"))
    
    # Другие поля
    comment = item.get("Описание") or item.get("Вопрос") or ""
    topic = item.get("Тема")
    online_question_cat = clean_uuid(item.get("КатегорияВопроса_Key"))
    online_question = clean_uuid(item.get("ВопросНаКонсультацию_Key"))
    
    # Найти или создать консультацию
    # Ищем по cl_ref_key (Ref_Key из ЦЛ)
    result = await db.execute(
        select(Consultation).where(Consultation.cl_ref_key == ref_key)
    )
    consultation = result.scalar_one_or_none()
    
    # Если нет, ищем по cons_id (если есть chatwoot_cons_id)
    if not consultation and chatwoot_cons_id:
        result = await db.execute(
            select(Consultation).where(Consultation.cons_id == chatwoot_cons_id)
        )
        consultation = result.scalar_one_or_none()
    
    # Если нет, создаем новую (cons_id будет временный, обновится при синхронизации с Chatwoot)
    if not consultation:
        consultation = Consultation(
            cons_id=f"cl_{ref_key}",  # Временный ID
            cl_ref_key=ref_key,
            number=number,
            status=status,
            create_date=create_date or datetime.now(timezone.utc),
            start_date=start_date,
            end_date=end_date,
            comment=comment,
            manager=str(manager_key) if manager_key else None,
            author=str(author_key) if author_key else None,
            online_question_cat=str(online_question_cat) if online_question_cat else None,
            online_question=str(online_question) if online_question else None,
        )
        db.add(consultation)
    else:
        # Обновляем существующую
        consultation.number = number
        consultation.status = status
        consultation.start_date = start_date
        consultation.end_date = end_date
        consultation.comment = comment or consultation.comment
        consultation.manager = str(manager_key) if manager_key else consultation.manager
        consultation.author = str(author_key) if author_key else consultation.author
        consultation.online_question_cat = str(online_question_cat) if online_question_cat else consultation.online_question_cat
        consultation.online_question = str(online_question) if online_question else consultation.online_question
    
    await db.flush()
    
    # Обработка КонсультацииИТС и ВопросыИОтветы → q_and_a
    # Удаляем старые записи для этой консультации
    await db.execute(
        text("DELETE FROM cons.q_and_a WHERE cons_ref_key = :ref_key"),
        {"ref_key": ref_key}
    )
    
    # Добавляем КонсультацииИТС
    for idx, consult in enumerate(item.get("КонсультацииИТС", []), 1):
        qa = QAndA(
            cons_ref_key=ref_key,
            cons_id=consultation.cons_id,
            line_number=int(consult.get("LineNumber", idx)),
            po_type_key=str(clean_uuid(consult.get("ВидПО_Key"))) if clean_uuid(consult.get("ВидПО_Key")) else None,
            po_section_key=str(clean_uuid(consult.get("РазделПО_Key"))) if clean_uuid(consult.get("РазделПО_Key")) else None,
            con_blocks_key=str(clean_uuid(consult.get("НаличиеПомех_Key"))) if clean_uuid(consult.get("НаличиеПомех_Key")) else None,
            manager_help_key=str(clean_uuid(consult.get("ПомощьМенеджера_Key"))) if clean_uuid(consult.get("ПомощьМенеджера_Key")) else None,
            is_repeat=consult.get("ПовторноеОбращение", False),
            question=consult.get("Вопрос"),
            answer=consult.get("Ответ"),
        )
        db.add(qa)
    
    # Добавляем ВопросыИОтветы
    for idx, qa_item in enumerate(item.get("ВопросыИОтветы", []), 1000):  # Начинаем с 1000 чтобы не пересекаться
        qa = QAndA(
            cons_ref_key=ref_key,
            cons_id=consultation.cons_id,
            line_number=int(qa_item.get("LineNumber", idx)),
            question=qa_item.get("Вопрос"),
            answer=qa_item.get("Ответ"),
        )
        db.add(qa)


async def pull_consultations():
    """Основная функция загрузки консультаций"""
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
            
            while True:
                # Формируем фильтр по дате изменения
                filter_part = f"ДатаСоздания ge datetime'{from_date}T00:00:00'"
                
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={filter_part}"
                    f"&$orderby=ДатаСоздания asc"
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
                
                # Обрабатываем каждую консультацию
                for item in batch:
                    try:
                        await process_consultation_item(db, item)
                    except Exception as e:
                        logger.error("Error processing consultation %s: %s", item.get("Ref_Key"), e)
                        continue
                
                await db.commit()
                total_processed += len(batch)
                logger.info("Processed %s consultations (total: %s)", len(batch), total_processed)
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            # Сохраняем дату синхронизации
            await save_sync_date(db, datetime.now(timezone.utc))
            await db.commit()
            
            logger.info("✓ Sync completed. Total processed: %s", total_processed)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    # Создаем таблицу sync_state если её нет
    async def ensure_sync_state_table():
        engine = create_async_engine(DATABASE_URL, echo=False)
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sys.sync_state (
                    entity_name TEXT PRIMARY KEY,
                    last_synced_at TIMESTAMPTZ
                )
            """))
        await engine.dispose()
    
    asyncio.run(ensure_sync_state_table())
    asyncio.run(pull_consultations())

