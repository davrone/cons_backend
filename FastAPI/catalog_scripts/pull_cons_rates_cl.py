#!/usr/bin/env python3
"""
Загрузка оценок консультаций из 1C:ЦЛ (OData) и обновление агрегатов в cons.cons.

Источник: InformationRegister_ОценкаКонсультацийПоЗаявкам.
Механика:
- каждая строка оценки сохраняется в cons.cons_rating_answers (на уровне вопроса)
- для каждого cons_key пересчитывается средняя оценка и сохраняется в cons.cons.con_rates
- прогресс фиксируется в sys.sync_state по максимальной дате Period
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
)
from FastAPI.services.consultation_ratings import recalc_consultation_ratings

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
    engine = create_async_engine(DATABASE_URL, echo=False)
    async with engine.begin() as conn:
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
                CREATE UNIQUE INDEX IF NOT EXISTS uq_cons_rating_answer
                ON cons.cons_rating_answers (cons_key, manager_key, question_number)
                """
            )
        )
    await engine.dispose()


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


async def upsert_answers(db: AsyncSession, rows: List[Dict[str, Any]]):
    if not rows:
        return
    stmt = insert(ConsRatingAnswer).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_cons_rating_answer",
        set_={
            "rating": stmt.excluded.rating,
            "question_text": stmt.excluded.question_text,
            "comment": stmt.excluded.comment,
            "sent_to_base": stmt.excluded.sent_to_base,
            "cons_id": stmt.excluded.cons_id,
            "client_id": stmt.excluded.client_id,
            "updated_at": func.now(),
        },
    )
    await db.execute(stmt)


async def process_batch(
    db: AsyncSession,
    batch: List[Dict[str, Any]],
    min_period: Optional[datetime],
) -> Tuple[int, Optional[datetime], Dict[str, int]]:
    cons_keys: Set[str] = set()
    client_keys: Set[str] = set()
    latest_period: Optional[datetime] = None
    stats = {
        "rows_in_batch": len(batch),
        "skipped_missing_cons": 0,
        "skipped_missing_question": 0,
        "skipped_missing_period": 0,
        "skipped_before_min": 0,
    }

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

    for item in batch:
        cons_key = clean_uuid(item.get("Обращение_Key"))
        client_key = clean_uuid(item.get("Контрагент_Key"))
        manager_key = clean_uuid(item.get("Менеджер_Key"))
        question_number = clean_int(item.get("НомерВопроса"))
        period_dt = clean_datetime(item.get("Period"))

        if not cons_key:
            stats["skipped_missing_cons"] += 1
            continue
        if question_number is None:
            stats["skipped_missing_question"] += 1
            continue
        if not period_dt:
            stats["skipped_missing_period"] += 1
            continue

        if min_period and period_dt < min_period:
            stats["skipped_before_min"] += 1
            continue

        if latest_period is None or period_dt > latest_period:
            latest_period = period_dt

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
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        )
        affected_cons_keys.add(cons_key)

    if not rows:
        return 0, latest_period, stats

    await upsert_answers(db, rows)
    await recalc_consultation_ratings(db, affected_cons_keys)
    return len(rows), latest_period, stats


async def pull_cons_rates():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)

    auth = (ODATA_USER, ODATA_PASSWORD)
    engine = create_async_engine(DATABASE_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with AsyncSessionLocal() as db:
            last_sync = await get_last_sync_date(db)
            min_period = None
            if last_sync:
                min_period = last_sync - timedelta(days=1)
                logger.info("Incremental sync from %s (last_sync=%s)", min_period.date(), last_sync)
            else:
                logger.info("First run — loading from %s", INITIAL_FROM_DATE)

            skip = 0
            total_processed = 0
            error_logs = 0
            last_period_processed: Optional[datetime] = None

            while True:
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
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
                    processed, latest_period, stats = await process_batch(db, batch, min_period)
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
                if processed or stats["rows_in_batch"] == 0:
                    logger.info("Processed %s rate rows (total=%s, skip=%s)", processed, total_processed, skip)
                else:
                    logger.warning(
                        "Batch skip=%s dropped entirely (rows=%s, missing_cons=%s, missing_q=%s, missing_period=%s, before_min=%s)",
                        skip,
                        stats["rows_in_batch"],
                        stats["skipped_missing_cons"],
                        stats["skipped_missing_question"],
                        stats["skipped_missing_period"],
                        stats["skipped_before_min"],
                    )
                    if (
                        stats["rows_in_batch"] > 0
                        and stats["skipped_missing_period"] == stats["rows_in_batch"]
                        and batch
                    ):
                        sample = batch[0]
                        preview = {k: sample.get(k) for k in sample.keys()}
                        logger.warning(
                            "Sample row for missing Period (skip=%s): %s | keys=%s",
                            skip,
                            json.dumps(preview, ensure_ascii=False),
                            list(sample.keys()),
                        )

                if latest_period and (
                    last_period_processed is None or latest_period > last_period_processed
                ):
                    last_period_processed = latest_period

                if len(batch) < PAGE_SIZE:
                    break
                skip += PAGE_SIZE

            if last_period_processed:
                await save_sync_date(db, last_period_processed)
                await db.commit()
                logger.info(
                    "✓ Rate sync completed. Total processed: %s (last_period=%s)",
                    total_processed,
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

