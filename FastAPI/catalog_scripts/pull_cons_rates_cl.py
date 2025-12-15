#!/usr/bin/env python3
"""
Загрузка оценок консультаций из 1C:ЦЛ (OData) и обновление агрегатов в cons.cons.

Источник: InformationRegister_ОценкаКонсультацийПоЗаявкам.
Механика:
- каждая строка оценки сохраняется в cons.cons_rating_answers (на уровне вопроса)
- для каждого cons_key пересчитывается средняя оценка и сохраняется в cons.cons.con_rates
- прогресс фиксируется в sys.sync_state по последнему обработанному Обращение_Key
  (используется ключ вместо ДатаОценки, так как ДатаОценки может быть не заполнена)
"""
from __future__ import annotations

import os
import sys
import asyncio
import logging
import time
import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import quote

import requests
from sqlalchemy import select, text, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import (
    ConsRatingAnswer,
    Consultation,
    Client,
    User,
)
from FastAPI.services.consultation_ratings import recalc_consultation_ratings
from FastAPI.services.chatwoot_client import ChatwootClient
from FastAPI.utils.notification_helpers import check_and_log_notification

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_DATE = os.getenv("ETL_RATES_INITIAL_FROM", "2025-01-01")
MAX_ERROR_LOGS = int(os.getenv("ETL_MAX_ERROR_LOGS", "5"))

ENTITY = "InformationRegister_ОценкаКонсультацийПоЗаявкам"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_cons_rates")

ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

HEADERS = {
    "User-Agent": "cons-middleware/rates-loader",
    "Accept": "application/json",
}


def clean_uuid(value: Optional[str]) -> Optional[str]:
    if not value or value == "00000000-0000-0000-0000-000000000000":
        return None
    return value


def clean_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def clean_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value or value.startswith("0001-01-01"):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
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


async def ensure_support_objects():
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=1,
        max_overflow=1,
        pool_pre_ping=True
    )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sys.sync_state (
                    entity_name TEXT PRIMARY KEY,
                    last_synced_at TIMESTAMPTZ,
                    last_synced_key TEXT
                )
                """
            )
        )
        # Добавляем колонку last_synced_key если её нет
        await conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'sys' 
                        AND table_name = 'sync_state' 
                        AND column_name = 'last_synced_key'
                    ) THEN
                        ALTER TABLE sys.sync_state ADD COLUMN last_synced_key TEXT;
                    END IF;
                END $$;
                """
            )
        )
        await conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_cons_rating_answer
                ON cons.cons_rating_answers (cons_key, manager_key, question_number)
                """
            )
        )
    await engine.dispose()


async def get_last_sync_key(db: AsyncSession) -> Optional[str]:
    """Получает последний обработанный Обращение_Key из sync_state"""
    result = await db.execute(
        text("SELECT last_synced_key FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY},
    )
    row = result.first()
    if row and row[0]:
        return row[0]
    return None


async def get_last_sync_date(db: AsyncSession) -> Optional[datetime]:
    """Получает последнюю дату синхронизации (используется как fallback)"""
    result = await db.execute(
        text("SELECT last_synced_at FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY},
    )
    row = result.first()
    if row and row[0]:
        return row[0]
    return None


async def save_sync_state(db: AsyncSession, last_key: Optional[str] = None, last_date: Optional[datetime] = None):
    """Сохраняет состояние синхронизации (ключ и/или дату)"""
    await db.execute(
        text(
            """
            INSERT INTO sys.sync_state (entity_name, last_synced_at, last_synced_key)
            VALUES (:entity, :date, :key)
            ON CONFLICT (entity_name) DO UPDATE SET 
                last_synced_at = COALESCE(EXCLUDED.last_synced_at, sys.sync_state.last_synced_at),
                last_synced_key = COALESCE(EXCLUDED.last_synced_key, sys.sync_state.last_synced_key)
            """
        ),
        {"entity": ENTITY, "date": last_date, "key": last_key},
    )


async def fetch_consultation_map(db: AsyncSession, cons_keys: Set[str]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    if not cons_keys:
        return {}
    result = await db.execute(
        select(Consultation.cl_ref_key, Consultation.cons_id, Consultation.client_id).where(
            Consultation.cl_ref_key.in_(cons_keys)
        )
    )
    return {row[0]: (row[1], str(row[2]) if row[2] else None) for row in result.all()}


async def fetch_client_map(db: AsyncSession, client_keys: Set[str]) -> Dict[str, Optional[str]]:
    if not client_keys:
        return {}
    result = await db.execute(
        select(Client.cl_ref_key, Client.client_id).where(Client.cl_ref_key.in_(client_keys))
    )
    return {row[0]: str(row[1]) if row[1] else None for row in result.all()}


async def upsert_answers(db: AsyncSession, rows: List[Dict[str, Any]]) -> Set[tuple]:
    """Вставляет/обновляет записи оценок и возвращает множество ключей новых записей"""
    if not rows:
        return set()
    
    # Проверяем, какие записи уже существуют
    existing_keys = set()
    for row in rows:
        result = await db.execute(
            select(ConsRatingAnswer).where(
                ConsRatingAnswer.cons_key == row["cons_key"],
                ConsRatingAnswer.manager_key == row["manager_key"],
                ConsRatingAnswer.question_number == row["question_number"]
            ).limit(1)
        )
        if result.scalar_one_or_none():
            existing_keys.add((row["cons_key"], row["manager_key"], row["question_number"]))
    
    # Вставляем/обновляем записи
    stmt = insert(ConsRatingAnswer).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_cons_rating_answer",
        set_={
            "rating": stmt.excluded.rating,
            "question_text": stmt.excluded.question_text,
            "comment": stmt.excluded.comment,
            "sent_to_base": stmt.excluded.sent_to_base,
            "rating_date": stmt.excluded.rating_date,  # Обновляем ДатаОценки
            "cons_id": stmt.excluded.cons_id,
            "client_id": stmt.excluded.client_id,
            "updated_at": func.now(),
        },
    )
    await db.execute(stmt)
    
    # Возвращаем ключи новых записей для отправки уведомлений
    return {(row["cons_key"], row["manager_key"], row["question_number"]) for row in rows if (row["cons_key"], row["manager_key"], row["question_number"]) not in existing_keys}


async def notify_chatwoot_rating(
    cons_id: str,
    rating: Optional[int],
    question_text: Optional[str] = None,
    manager_key: Optional[str] = None,
    db: Optional[AsyncSession] = None,
):
    """
    Отправка уведомления об оценке консультации в Chatwoot (как note).
    
    Args:
        cons_id: ID консультации
        rating: Оценка (1-5)
        question_text: Текст вопроса (опционально)
        manager_key: UUID менеджера (cl_ref_key)
        db: Сессия БД для получения ФИО менеджера (опционально)
    """
    if not cons_id or cons_id.startswith(("temp_", "cl_")):
        # Пропускаем временные ID
        return
    
    if rating is None:
        return
    
    # Проверяем, не было ли уже отправлено такое уведомление
    if db:
        # ВАЖНО: Нормализуем manager_key для стабильного хеша (None -> "")
        # Ограничиваем question_text до 100 символов для стабильности хеша
        normalized_manager_key = manager_key if manager_key else ""
        normalized_question_text = question_text[:100] if question_text else None
        
        notification_data = {
            "rating": rating,
            "question_text": normalized_question_text,
            "manager_key": normalized_manager_key
        }
        # ВАЖНО: Используем отдельную транзакцию для сохранения NotificationLog,
        # чтобы запись не потерялась при rollback основной транзакции ETL
        already_sent = await check_and_log_notification(
            db=db,
            notification_type="rating",
            entity_id=cons_id,
            data=notification_data,
            use_separate_transaction=True  # Используем отдельную транзакцию для надежности
        )
        if already_sent:
            logger.debug(f"Rating notification already sent for cons_id={cons_id}, rating={rating}, skipping")
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
        
        # Формируем сообщение об оценке
        message = f"⭐ Оценка консультации получена\nОценка: {rating}/5"
        if question_text:
            message += f"\nВопрос: {question_text[:100]}"  # Ограничиваем длину
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
        logger.info(f"Sent rating message to Chatwoot for cons_id={cons_id}, rating={rating}")
    except Exception as e:
        logger.warning(f"Failed to notify Chatwoot about rating (cons_id={cons_id}): {e}")


async def process_batch(
    db: AsyncSession,
    batch: List[Dict[str, Any]],
    last_synced_key: Optional[str],
) -> Tuple[int, Optional[str], Optional[datetime], Dict[str, int]]:
    """
    Обрабатывает батч записей оценок.
    Возвращает: (количество обработанных, последний обработанный ключ, последняя дата, статистика)
    
    ВАЖНО: Пропуск уже обработанных записей происходит на основе last_synced_key.
    Если ключ меньше или равен last_synced_key, запись пропускается.
    Это работает потому что записи сортируются по ключу в OData запросе.
    """
    cons_keys: Set[str] = set()
    client_keys: Set[str] = set()
    latest_period: Optional[datetime] = None
    last_processed_key: Optional[str] = None
    stats = {
        "rows_in_batch": len(batch),
        "skipped_missing_cons": 0,
        "skipped_missing_question": 0,
        "skipped_before_key": 0,
    }

    # Собираем все ключи из батча
    for item in batch:
        cons_key = clean_uuid(item.get("Обращение_Key"))
        client_key = clean_uuid(item.get("Контрагент_Key"))
        if cons_key:
            cons_keys.add(cons_key)
        if client_key:
            client_keys.add(client_key)

    cons_map = await fetch_consultation_map(db, cons_keys)
    client_map = await fetch_client_map(db, client_keys)

    rows: List[Dict[str, Any]] = []
    affected_cons_keys: Set[str] = set()
    
    # Флаг для отслеживания, прошли ли мы уже точку last_synced_key
    # Это позволяет пропускать записи до нужной точки, а затем обрабатывать все остальные
    passed_sync_point = last_synced_key is None

    for item in batch:
        cons_key = clean_uuid(item.get("Обращение_Key"))
        client_key = clean_uuid(item.get("Контрагент_Key"))
        manager_key = clean_uuid(item.get("Менеджер_Key"))
        question_number = clean_int(item.get("НомерВопроса"))
        
        # Используем ДатаОценки только для сохранения, не для синхронизации
        rating_date_dt = clean_datetime(item.get("ДатаОценки"))
        if rating_date_dt and rating_date_dt.year == 1:
            rating_date_dt = None  # Не сохраняем невалидную дату

        if not cons_key:
            stats["skipped_missing_cons"] += 1
            continue
        if question_number is None:
            stats["skipped_missing_question"] += 1
            continue

        # Пропускаем записи до последнего обработанного ключа
        # Используем сравнение строк для GUID (OData сортирует по ключу, так что порядок стабильный)
        if not passed_sync_point and last_synced_key:
            # Сравниваем ключи как строки (GUID в формате UUID сортируются лексикографически)
            if cons_key == last_synced_key:
                # Достигли точки синхронизации, начинаем обрабатывать следующие записи
                passed_sync_point = True
            elif cons_key < last_synced_key:
                # Еще не достигли точки синхронизации, пропускаем
                stats["skipped_before_key"] += 1
                continue
            else:
                # Ключ больше last_synced_key, начинаем обрабатывать
                passed_sync_point = True

        # Обновляем последний обработанный ключ (используем максимальный по строковому сравнению)
        if last_processed_key is None or cons_key > last_processed_key:
            last_processed_key = cons_key
        if rating_date_dt and (latest_period is None or rating_date_dt > latest_period):
            latest_period = rating_date_dt

        rating_value = clean_int(item.get("Оценка"))
        cons_info = cons_map.get(cons_key, (None, None))
        cons_id, fallback_client_id = cons_info
        client_id = client_map.get(client_key) if client_key else fallback_client_id

        rows.append(
            {
                "cons_key": cons_key,
                "cons_id": cons_id,
                "client_key": client_key,
                "client_id": client_id,
                "manager_key": manager_key,
                "question_number": question_number,
                "rating": rating_value,
                "question_text": item.get("Вопрос"),
                "comment": item.get("Комментарий"),
                "sent_to_base": item.get("ОтправленаБаза"),
                "rating_date": rating_date_dt,  # Сохраняем ДатаОценки
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        )
        affected_cons_keys.add(cons_key)

    if not rows:
        return 0, last_processed_key, latest_period, stats

    # Вставляем/обновляем записи и получаем ключи новых записей
    new_keys = await upsert_answers(db, rows)
    await recalc_consultation_ratings(db, affected_cons_keys)
    
    # Отправляем уведомления в Chatwoot для новых оценок
    for row in rows:
        row_key = (row["cons_key"], row["manager_key"], row["question_number"])
        if row_key in new_keys:
            cons_id = row.get("cons_id")
            if cons_id:
                await notify_chatwoot_rating(
                    cons_id=cons_id,
                    rating=row.get("rating"),
                    question_text=row.get("question_text"),
                    manager_key=row.get("manager_key"),
                    db=db
                )
    
    return len(rows), last_processed_key, latest_period, stats


async def pull_cons_rates():
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
            # Получаем последний обработанный ключ для инкрементальной загрузки
            last_synced_key = await get_last_sync_key(db)
            last_synced_date = await get_last_sync_date(db)
            
            if last_synced_key:
                logger.info("Incremental sync from key: %s (last_sync_date=%s)", last_synced_key[:8] + "...", last_synced_date)
            else:
                logger.info("First run — loading all records")

            skip = 0
            total_processed = 0
            error_logs = 0
            last_processed_key: Optional[str] = last_synced_key
            last_period_processed: Optional[datetime] = last_synced_date

            while True:
                # ВАЖНО: Не используем фильтрацию по GUID через $filter, так как GUID нельзя сравнивать через gt/lt
                # Используем только сортировку по Обращение_Key, а пропуск уже обработанных записей
                # происходит на стороне приложения в функции process_batch
                encoded_orderby = quote("Обращение_Key asc", safe=",", encoding='utf-8')
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$orderby={encoded_orderby}"
                    f"&$top={PAGE_SIZE}&$skip={skip}"
                )

                try:
                    resp = http_get_with_backoff(url, auth)
                except Exception as exc:
                    logger.exception("Failed to fetch batch: %s", exc)
                    break

                batch = resp.json().get("value", [])
                if not batch:
                    break

                try:
                    processed, batch_last_key, latest_period, stats = await process_batch(db, batch, last_synced_key)
                except Exception as exc:
                    if error_logs < MAX_ERROR_LOGS:
                        logger.error("Error processing rates batch (skip=%s): %s", skip, exc)
                    elif error_logs == MAX_ERROR_LOGS:
                        logger.error("Further rate processing errors suppressed")
                    error_logs += 1
                    await db.rollback()
                    break

                await db.commit()
                total_processed += processed
                
                # Обновляем последний обработанный ключ
                if batch_last_key and (last_processed_key is None or batch_last_key > last_processed_key):
                    last_processed_key = batch_last_key
                
                # Обновляем последнюю дату
                if latest_period and (
                    last_period_processed is None or latest_period > last_period_processed
                ):
                    last_period_processed = latest_period

                if processed or stats["rows_in_batch"] == 0:
                    logger.info(
                        "Processed %s rate rows (total=%s, skip=%s, last_key=%s)",
                        processed,
                        total_processed,
                        skip,
                        batch_last_key[:8] + "..." if batch_last_key else "None",
                    )
                else:
                    logger.warning(
                        "Batch skip=%s dropped entirely (rows=%s, missing_cons=%s, missing_q=%s, before_key=%s)",
                        skip,
                        stats["rows_in_batch"],
                        stats["skipped_missing_cons"],
                        stats["skipped_missing_question"],
                        stats["skipped_before_key"],
                    )

                # ВАЖНО: Сохраняем sync_state после каждого батча для устойчивости при прерывании
                if last_processed_key:
                    try:
                        await save_sync_state(db, last_key=last_processed_key, last_date=last_period_processed)
                        await db.commit()
                        logger.debug(f"✓ Sync state saved after batch: key={last_processed_key[:8]}..., date={last_period_processed}")
                    except Exception as sync_error:
                        logger.warning(f"Failed to save sync state after batch: {sync_error}")
                        # Не прерываем выполнение, продолжаем обработку

                if len(batch) < PAGE_SIZE:
                    break
                skip += PAGE_SIZE

            # Финальное сохранение состояния синхронизации
            if last_processed_key:
                await save_sync_state(db, last_key=last_processed_key, last_date=last_period_processed)
                await db.commit()
                logger.info(
                    "✓ Rate sync completed. Total processed: %s (last_key=%s, last_date=%s)",
                    total_processed,
                    last_processed_key[:8] + "..." if last_processed_key else "None",
                    last_period_processed,
                )
            else:
                logger.info(
                    "✓ Rate sync completed. Total processed: %s (no new rows, sync_state unchanged)",
                    total_processed,
                )
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(ensure_support_objects())
    asyncio.run(pull_cons_rates())

