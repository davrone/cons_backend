#!/usr/bin/env python3
"""
Загрузка регистра переносов консультаций из 1C:ЦЛ (OData) прямо в cons.cons_redate.

Источник: InformationRegister_РегистрацияПереносаКонсультации
Особенности:
- по одной консультации может быть несколько переносов
- инкремент ведется по дате Period (через sys.sync_state)
- при появлении новой записи дополнительно обновляем поля redate / redate_time в cons.cons
- отправляет уведомления о переносах в Chatwoot
- синхронизирует изменения даты консультации обратно в 1C:ЦЛ
"""
from __future__ import annotations

import os
import sys
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Set
from urllib.parse import quote

import requests
import asyncpg
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import OperationalError

# Добавляем корень проекта для корректного импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import ConsRedate, Consultation, User
from FastAPI.services.chatwoot_client import ChatwootClient
from FastAPI.services.onec_client import OneCClient
from FastAPI.utils.etl_logging import ETLLogger

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
INITIAL_FROM_DATE = os.getenv("ETL_REDATE_INITIAL_FROM", "2025-12-01")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
MAX_ERROR_LOGS = int(os.getenv("ETL_MAX_ERROR_LOGS", "5"))

ENTITY = "InformationRegister_РегистрацияПереносаКонсультации"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_cons_redate")

# Настройки OData
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Database URL
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

HEADERS = {
    "User-Agent": "cons-middleware/redate-loader",
    "Accept": "application/json",
}


def clean_uuid(value: Optional[str]) -> Optional[str]:
    if not value or value == "00000000-0000-0000-0000-000000000000":
        return None
    return value


def clean_datetime(value: Optional[str]) -> Optional[datetime]:
    """Преобразует строку в datetime с timezone (offset-aware)"""
    if not value or value.startswith("0001-01-01"):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        # ВАЖНО: Убеждаемся, что datetime имеет timezone (offset-aware)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        logger.warning("Failed to parse datetime: %s", value)
        return None


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120) -> requests.Response:
    session = requests.Session()
    attempt = 0
    while True:
        try:
            resp = session.get(url, auth=auth, headers=HEADERS, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    resp.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("HTTP %s — retry in %s sec (attempt=%s)", resp.status_code, wait, attempt + 1)
                time.sleep(wait)
                attempt += 1
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt >= max_retries:
                logger.error("HTTP error after %s attempts: %s", attempt + 1, exc)
                raise
            wait = min(2 ** attempt, 60)
            logger.warning("Request failed: %s — retry in %s sec (attempt=%s)", exc, wait, attempt + 1)
            time.sleep(wait)
            attempt += 1


async def get_last_sync_date(db: AsyncSession) -> Optional[datetime]:
    """Получить дату последней синхронизации, убедившись что она offset-aware"""
    result = await db.execute(
        text("SELECT last_synced_at FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY},
    )
    row = result.first()
    if row and row[0]:
        dt = row[0]
        # ВАЖНО: Убеждаемся, что datetime имеет timezone (offset-aware)
        if isinstance(dt, datetime) and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


async def save_sync_date(db: AsyncSession, value: datetime):
    await db.execute(
        text(
            """
            INSERT INTO sys.sync_state (entity_name, last_synced_at)
            VALUES (:entity, :date)
            ON CONFLICT (entity_name) DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at
            """
        ),
        {"entity": ENTITY, "date": value},
    )


async def ensure_support_objects():
    """Создает необходимые индексы/таблицы перед загрузкой."""
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=1,
        max_overflow=1,
        pool_pre_ping=True
    )
    async with engine.begin() as conn:
        # sync_state уже создается в init_db, но продублируем на всякий случай
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sys.sync_state (
                    entity_name TEXT PRIMARY KEY,
                    last_synced_at TIMESTAMPTZ
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_cons_redate_keys
                ON cons.cons_redate (cons_key, clients_key, manager_key, period)
                """
            )
        )
    await engine.dispose()


async def upsert_redate_rows(db: AsyncSession, rows: List[Dict[str, Any]]) -> Set[tuple]:
    """Вставляет записи переносов и возвращает множество ключей новых записей"""
    if not rows:
        return set()
    
    # Проверяем, какие записи уже существуют
    existing_keys = set()
    for row in rows:
        result = await db.execute(
            select(ConsRedate).where(
                ConsRedate.cons_key == row["cons_key"],
                ConsRedate.clients_key == row["clients_key"],
                ConsRedate.manager_key == row["manager_key"],
                ConsRedate.period == row["period"]
            ).limit(1)
        )
        if result.scalar_one_or_none():
            existing_keys.add((row["cons_key"], row["clients_key"], row["manager_key"], row["period"]))
    
    # Вставляем только новые записи
    new_rows = [row for row in rows if (row["cons_key"], row["clients_key"], row["manager_key"], row["period"]) not in existing_keys]
    if new_rows:
        stmt = insert(ConsRedate).values(new_rows)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_cons_redate_keys")
        await db.execute(stmt)
    
    # Возвращаем ключи новых записей для отправки уведомлений
    return {(row["cons_key"], row["clients_key"], row["manager_key"], row["period"]) for row in new_rows}


async def update_consultation_schedule(
    db: AsyncSession,
    cons_key: str,
    new_date: Optional[datetime],
):
    if not new_date or not cons_key:
        return
    await db.execute(
        update(Consultation)
        .where(Consultation.cl_ref_key == cons_key)
        .values(
            redate=new_date.date(),
            redate_time=new_date.time(),
            updated_at=datetime.now(timezone.utc),
        )
    )


async def notify_chatwoot_redate(
    cons_id: str,
    old_date: Optional[datetime],
    new_date: Optional[datetime],
    manager_key: Optional[str] = None,
    db: Optional[AsyncSession] = None,
):
    """
    Отправка уведомления о переносе консультации в Chatwoot (как note).
    
    Проверяет, не было ли уже отправлено такое уведомление, чтобы избежать дублирования.
    """
    if not cons_id or cons_id.startswith(("temp_", "cl_")):
        # Пропускаем временные ID
        return
    
    if not new_date:
        return
    
    # Импортируем утилиту для проверки дублирования
    from ..utils.notification_helpers import check_and_log_notification
    
    # Проверяем, не было ли уже отправлено такое уведомление
    if db:
        # ВАЖНО: Нормализуем manager_key для стабильного хеша (None -> "")
        # Это предотвращает разные хеши для одного и того же переноса
        normalized_manager_key = manager_key if manager_key else ""
        notification_data = {
            "old_date": old_date.isoformat() if old_date else None,
            "new_date": new_date.isoformat(),
            "manager_key": normalized_manager_key  # Всегда строка, не None
        }
        # ВАЖНО: Используем отдельную транзакцию для сохранения NotificationLog,
        # чтобы запись не потерялась при rollback основной транзакции ETL
        already_sent = await check_and_log_notification(
            db=db,
            notification_type="redate",
            entity_id=cons_id,
            data=notification_data,
            use_separate_transaction=True  # Используем отдельную транзакцию для надежности
        )
        if already_sent:
            logger.debug(f"Redate notification already sent for cons_id={cons_id}, skipping")
            return
    
    try:
        chatwoot_client = ChatwootClient()
        
        # Получаем ФИО менеджера из БД
        manager_name = None
        if manager_key and db:
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
        
        # Формируем сообщение о переносе
        old_date_str = old_date.strftime("%d.%m.%Y %H:%M") if old_date else "не указана"
        new_date_str = new_date.strftime("%d.%m.%Y %H:%M")
        
        message = f"📅 Консультация перенесена\n"
        message += f"Старая дата: {old_date_str}\n"
        message += f"Новая дата: {new_date_str}"
        if manager_name:
            message += f"\nМенеджер: {manager_name}"
        elif manager_key:
            # Fallback на UUID, если не удалось получить ФИО
            message += f"\nМенеджер: {manager_key[:8]}..."
        
        # Используем send_message вместо send_note, так как note сообщения не видны клиенту
        await chatwoot_client.send_message(
            conversation_id=cons_id,
            content=message,
            message_type="outgoing"
        )
        
        # Уведомление уже залогировано в check_and_log_notification
        
        logger.info(f"Sent redate message to Chatwoot for cons_id={cons_id}")
    except Exception as e:
        logger.warning(f"Failed to notify Chatwoot about redate (cons_id={cons_id}): {e}")


async def update_cl_consultation_date(
    cl_ref_key: str,
    new_date: Optional[datetime],
):
    """Обновление даты консультации в 1C:ЦЛ через OData"""
    if not cl_ref_key or not new_date:
        return
    
    try:
        onec_client = OneCClient()
        
        # Обновляем дату консультации в ЦЛ
        await onec_client.update_consultation_odata(
            ref_key=cl_ref_key,
            start_date=new_date,
        )
        logger.debug(f"[pull_cons_redate_cl] Updated consultation date in CL for cl_ref_key={cl_ref_key[:20]}, new_date={new_date}")
    except Exception as e:
        logger.warning(f"Failed to update CL consultation date (cl_ref_key={cl_ref_key}): {e}")


async def process_batch(db: AsyncSession, batch: List[Dict[str, Any]]):
    """
    Обрабатывает батч записей о переносах консультаций.
    
    ВАЖНО: Сортирует записи по cons_key для предотвращения deadlocks при параллельной обработке.
    """
    rows: List[Dict[str, Any]] = []
    latest_period: Optional[datetime] = None
    for item in batch:
        cons_key = clean_uuid(item.get("ДокументОбращения_Key"))
        client_key = clean_uuid(item.get("Абонент_Key"))
        manager_key = clean_uuid(item.get("Менеджер_Key"))
        period_dt = clean_datetime(item.get("Period"))

        if not (cons_key and period_dt):
            continue

        row = {
            "cons_key": cons_key,
            "clients_key": client_key,
            "manager_key": manager_key,
            "period": period_dt,
            "old_date": clean_datetime(item.get("СтараяДата")),
            "new_date": clean_datetime(item.get("НоваяДата")),
        }
        rows.append(row)
        # ВАЖНО: Убеждаемся, что оба datetime имеют timezone перед сравнением
        if latest_period is None:
            latest_period = period_dt
        elif period_dt:
            # Нормализуем оба datetime к UTC для сравнения
            if period_dt.tzinfo is None:
                period_dt = period_dt.replace(tzinfo=timezone.utc)
            if latest_period.tzinfo is None:
                latest_period = latest_period.replace(tzinfo=timezone.utc)
            if period_dt > latest_period:
                latest_period = period_dt

    if rows:
        # ВАЖНО: Сортируем по cons_key для предотвращения deadlocks
        # Это гарантирует, что все процессы обрабатывают записи в одинаковом порядке
        rows.sort(key=lambda r: r["cons_key"] or "")
        
        # Вставляем записи и получаем ключи новых записей
        new_keys = await upsert_redate_rows(db, rows)
        
        for row in rows:
            row_key = (row["cons_key"], row["clients_key"], row["manager_key"], row["period"])
            cons_key = row["cons_key"]
            old_date = row["old_date"]
            new_date = row["new_date"]
            manager_key = row["manager_key"]
            
            # Обновляем расписание в БД
            await update_consultation_schedule(db, cons_key, new_date)
            
            # Отправляем уведомление только для новых записей
            # ВАЖНО: Проверка дубликатов выполняется внутри notify_chatwoot_redate
            # Это предотвращает отправку дубликатов даже если запись в cons_redate еще не в БД
            if row_key in new_keys:
                # Получаем консультацию для отправки в Chatwoot и ЦЛ
                result = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == cons_key)
                )
                consultation = result.scalar_one_or_none()
                
                if consultation:
                    # Отправляем уведомление в Chatwoot (проверка дубликатов внутри функции)
                    if consultation.cons_id:
                        await notify_chatwoot_redate(
                            consultation.cons_id, old_date, new_date, manager_key, db=db
                        )
                    
                    # Обновляем дату в ЦЛ (если есть cl_ref_key)
                    if consultation.cl_ref_key:
                        await update_cl_consultation_date(consultation.cl_ref_key, new_date)
    
    return len(rows), latest_period


async def pull_cons_redate():
    etl_logger = ETLLogger("pull_cons_redate_cl", ENTITY)
    
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
            last_sync = await get_last_sync_date(db)
            if last_sync:
                from_dt = last_sync - timedelta(hours=6)
                from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
                etl_logger.sync_info(last_sync, from_date, buffer_days=None)  # Буфер в часах
            else:
                from_date = f"{INITIAL_FROM_DATE}T00:00:00"
                etl_logger.sync_info(None, from_date)

            skip = 0
            error_logs = 0
            last_period_processed: Optional[datetime] = last_sync

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
                    resp = http_get_with_backoff(url, auth)
                except Exception as exc:
                    etl_logger.batch_error(batch_num, exc, skip)
                    break

                batch = resp.json().get("value", [])
                if not batch:
                    break

                # Обрабатываем батч с retry для deadlock ошибок
                max_deadlock_retries = 3
                deadlock_retry_delay = 0.1  # Начальная задержка в секундах
                processed = None
                latest_period = None
                
                for deadlock_attempt in range(1, max_deadlock_retries + 1):
                    try:
                        processed, latest_period = await process_batch(db, batch)
                        if latest_period:
                            # Нормализуем оба datetime к UTC перед сравнением
                            if latest_period.tzinfo is None:
                                latest_period = latest_period.replace(tzinfo=timezone.utc)
                            if last_period_processed is None:
                                last_period_processed = latest_period
                            else:
                                if last_period_processed.tzinfo is None:
                                    last_period_processed = last_period_processed.replace(tzinfo=timezone.utc)
                                if latest_period > last_period_processed:
                                    last_period_processed = latest_period
                        
                        # Успешно обработали, выходим из retry цикла
                        break
                    except Exception as exc:
                        # Проверяем, является ли это deadlock ошибкой
                        is_deadlock = False
                        
                        # Проверяем различные варианты deadlock ошибок
                        if isinstance(exc, OperationalError):
                            # SQLAlchemy оборачивает asyncpg исключения в OperationalError
                            orig_exc = getattr(exc, 'orig', None)
                            if isinstance(orig_exc, asyncpg.exceptions.DeadlockDetectedError):
                                is_deadlock = True
                        elif isinstance(exc, asyncpg.exceptions.DeadlockDetectedError):
                            # Прямое исключение asyncpg (маловероятно, но возможно)
                            is_deadlock = True
                        elif "deadlock detected" in str(exc).lower():
                            # Fallback: проверка по тексту ошибки
                            is_deadlock = True
                        
                        if is_deadlock and deadlock_attempt < max_deadlock_retries:
                            # Deadlock - делаем rollback и retry с задержкой
                            await db.rollback()
                            wait_time = deadlock_retry_delay * (2 ** (deadlock_attempt - 1))
                            logger.warning(
                                f"[pull_cons_redate_cl] Deadlock detected in batch {batch_num} "
                                f"(attempt {deadlock_attempt}/{max_deadlock_retries}). "
                                f"Retrying in {wait_time:.2f}s..."
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            # Не deadlock или исчерпаны попытки - логируем и прерываем
                            if error_logs < MAX_ERROR_LOGS:
                                etl_logger.batch_error(batch_num, exc, skip)
                            elif error_logs == MAX_ERROR_LOGS:
                                logger.warning("[pull_cons_redate_cl] Further redate processing errors suppressed")
                            error_logs += 1
                            await db.rollback()
                            break
                
                # Если не удалось обработать батч после всех retry, прерываем цикл
                if processed is None:
                    break

                await db.commit()

                # Логируем прогресс батча
                etl_logger.batch_progress(batch_num, len(batch), created=processed, updated=0, errors=0)

                # Сохраняем sync_state после каждого батча
                if last_period_processed:
                    try:
                        await save_sync_date(db, last_period_processed)
                        await db.commit()
                        etl_logger.sync_state_saved(last_period_processed, batch_num)
                    except Exception as sync_error:
                        logger.warning(f"[pull_cons_redate_cl] Failed to save sync state after batch: {sync_error}")

                if len(batch) < PAGE_SIZE:
                    break
                skip += PAGE_SIZE

            # Финальное сохранение даты синхронизации
            if last_period_processed:
                await save_sync_date(db, last_period_processed)
                await db.commit()
                etl_logger.sync_state_saved(last_period_processed)
            
            etl_logger.finish(success=True)
    except Exception as e:
        etl_logger.finish(success=False, error=e)
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(ensure_support_objects())
    asyncio.run(pull_cons_redate())

