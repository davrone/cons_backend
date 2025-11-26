#!/usr/bin/env python3
"""
Загрузка регистра переносов консультаций из 1C:ЦЛ (OData) прямо в cons.cons_redate.

Источник: InformationRegister_РегистрацияПереносаКонсультации
Особенности:
- по одной консультации может быть несколько переносов
- инкремент ведется по дате Period (через sys.sync_state)
- при появлении новой записи дополнительно обновляем поля redate / redate_time в cons.cons
"""
from __future__ import annotations

import os
import sys
import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import requests
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Добавляем корень проекта для корректного импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import ConsRedate, Consultation

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
INITIAL_FROM_DATE = os.getenv("ETL_REDATE_INITIAL_FROM", "2025-01-01")
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


async def get_last_sync_date(db: AsyncSession) -> Optional[datetime]:
    result = await db.execute(
        text("SELECT last_synced_at FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY},
    )
    row = result.first()
    if row and row[0]:
        return row[0]
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
    engine = create_async_engine(DATABASE_URL, echo=False)
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


async def upsert_redate_rows(db: AsyncSession, rows: List[Dict[str, Any]]):
    if not rows:
        return
    stmt = insert(ConsRedate).values(rows)
    stmt = stmt.on_conflict_do_nothing(constraint="uq_cons_redate_keys")
    await db.execute(stmt)


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


async def process_batch(db: AsyncSession, batch: List[Dict[str, Any]]):
    rows: List[Dict[str, Any]] = []
    for item in batch:
        cons_key = clean_uuid(item.get("ДокументОбращения_Key"))
        client_key = clean_uuid(item.get("Абонент_Key"))
        manager_key = clean_uuid(item.get("Менеджер_Key"))
        period_dt = clean_datetime(item.get("Period"))

        if not (cons_key and client_key and manager_key and period_dt):
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

    if not rows:
        return

    await upsert_redate_rows(db, rows)

    # Обновляем расписание консультаций по свежим переносам
    for row in rows:
        await update_consultation_schedule(db, row["cons_key"], row["new_date"])


async def pull_cons_redate():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)

    auth = (ODATA_USER, ODATA_PASSWORD)
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with AsyncSessionLocal() as db:
            last_sync = await get_last_sync_date(db)
            if last_sync:
                from_date = (last_sync - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info("Incremental sync from %s (last_sync=%s)", from_date, last_sync)
            else:
                from_date = INITIAL_FROM_DATE
                logger.info("First run — loading from %s", from_date)

            skip = 0
            total_processed = 0
            error_logs = 0

            while True:
                filter_part = f"Period ge datetime'{from_date}T00:00:00'"
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={filter_part}"
                    f"&$orderby=Period asc"
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
                    await process_batch(db, batch)
                except Exception as exc:
                    if error_logs < MAX_ERROR_LOGS:
                        logger.error("Error processing redate batch (skip=%s): %s", skip, exc)
                    elif error_logs == MAX_ERROR_LOGS:
                        logger.error("Further redate processing errors suppressed")
                    error_logs += 1
                    await db.rollback()
                    break

                await db.commit()

                total_processed += len(batch)
                logger.info("Processed %s rows (total=%s, skip=%s)", len(batch), total_processed, skip)

                if len(batch) < PAGE_SIZE:
                    break
                skip += PAGE_SIZE

            await save_sync_date(db, datetime.now(timezone.utc))
            await db.commit()
            logger.info("✓ Redate sync completed. Total processed: %s", total_processed)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(ensure_support_objects())
    asyncio.run(pull_cons_redate())

