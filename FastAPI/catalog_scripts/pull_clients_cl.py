#!/usr/bin/env python3
"""
Загрузка клиентов из 1C:ЦЛ (OData) в таблицу cons.clients.

Источник: Catalog_Контрагенты
Особенности:
- инкрементальная загрузка по полю Code (через sys.sync_state)
- загружаем всех клиентов (без фильтра по Parent_Key)
- обновляем существующих клиентов по cl_ref_key
- заполняем org_inn из поля ИНН
- заполняем code_abonent из поля КодАбонентаClobus
- для всех клиентов из ЦЛ устанавливаем is_parent=true (в ЦЛ создаются только владельцы)
"""
from __future__ import annotations

import os
import sys
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote

import requests
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import Client

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_CODE = os.getenv("ETL_CLIENTS_INITIAL_FROM_CODE", "000000000")

ENTITY = "Catalog_Контрагенты"
# PARENT_KEY_FILTER больше не используется - загружаем всех клиентов

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_clients_cl")

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
    "User-Agent": "cons-middleware/clients-loader",
    "Accept": "application/json",
}


def clean_uuid(value: Optional[str]) -> Optional[str]:
    """Очистка UUID"""
    if not value or value == "00000000-0000-0000-0000-000000000000":
        return None
    return value


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


async def get_last_sync_code(db: AsyncSession) -> Optional[str]:
    """Получить последний синхронизированный Code"""
    result = await db.execute(
        text("SELECT last_synced_code FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY}
    )
    row = result.first()
    if row and row[0]:
        return row[0]
    return None


async def save_sync_code(db: AsyncSession, code: str):
    """Сохранить последний синхронизированный Code"""
    await db.execute(
        text("""
            INSERT INTO sys.sync_state (entity_name, last_synced_code)
            VALUES (:entity, :code)
            ON CONFLICT (entity_name) DO UPDATE SET last_synced_code = EXCLUDED.last_synced_code
        """),
        {"entity": ENTITY, "code": code}
    )


def http_get_with_backoff(url: str, auth: Tuple[str, str], max_retries: int = 6, timeout: int = 120):
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


def extract_contact_info(contact_list: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """Извлечь email и телефон из КонтактнаяИнформация"""
    email = None
    phone = None
    for contact in contact_list or []:
        ctype = contact.get("Тип")
        if ctype == "АдресЭлектроннойПочты":
            email = contact.get("АдресЭП")
        elif ctype == "Телефон":
            phone = contact.get("НомерТелефона")
    return email, phone


async def upsert_client(db: AsyncSession, item: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Создать или обновить клиента.
    Возвращает (is_new, is_updated)
    
    ВАЖНО: Обрабатывает дубли по code_abonent:
    - Если клиент с таким code_abonent уже существует, обновляем его вместо создания нового
    - Это предотвращает потерю истории заявок из-за дублей
    """
    ref_key = clean_uuid(item.get("Ref_Key"))
    if not ref_key:
        return False, False
    
    # Извлекаем code_abonent для проверки дублей
    code_abonent = item.get("КодАбонентаClobus")
    if code_abonent == "0" or not code_abonent:
        code_abonent = None
    
    # Проверяем, существует ли клиент по cl_ref_key
    result = await db.execute(
        select(Client).where(Client.cl_ref_key == ref_key).limit(1)
    )
    existing_client = result.scalar_one_or_none()
    
    # ВАЖНО: Если клиент не найден по cl_ref_key, но есть code_abonent,
    # проверяем, нет ли уже клиента с таким code_abonent (дубль)
    if not existing_client and code_abonent:
        result = await db.execute(
            select(Client).where(
                Client.code_abonent == code_abonent,
                Client.is_parent == True
            ).order_by(Client.created_at.asc()).limit(1)
        )
        existing_client_by_code = result.scalar_one_or_none()
        
        if existing_client_by_code:
            # Найден дубль по code_abonent - обновляем существующего клиента
            # вместо создания нового, чтобы сохранить историю заявок
            logger.warning(
                f"Duplicate code_abonent '{code_abonent}' detected: "
                f"client with cl_ref_key={ref_key} will update existing client "
                f"client_id={existing_client_by_code.client_id} (cl_ref_key={existing_client_by_code.cl_ref_key})"
            )
            existing_client = existing_client_by_code
            # Обновляем cl_ref_key на новый, если он более свежий
            # (приоритет отдаем клиенту из ЦЛ с более свежими данными)
            if not existing_client.cl_ref_key or existing_client.cl_ref_key != ref_key:
                existing_client.cl_ref_key = ref_key
    
    # Извлекаем данные
    email, phone = extract_contact_info(item.get("КонтактнаяИнформация", []))
    org_inn = item.get("ИНН") or item.get("ИННФизЛица")  # Может быть в разных полях
    name = item.get("Description") or item.get("Наименование")
    # ВАЖНО: Заполняем code_abonent из поля КодАбонентаClobus
    code_abonent = item.get("КодАбонентаClobus")
    # Если КодАбонентаClobus пустой или "0", не заполняем code_abonent
    if code_abonent == "0" or not code_abonent:
        code_abonent = None
    
    # География (если есть в данных)
    country = item.get("Страна")
    region = item.get("Регион")
    city = item.get("Город")
    
    # Подписка (если есть в данных)
    subs_id = item.get("Подписка")
    subs_start = clean_datetime(item.get("ДатаНачалаПодписки"))
    subs_end = clean_datetime(item.get("ДатаОкончанияПодписки"))
    
    # Тариф (если есть в данных)
    tariff_id = clean_uuid(item.get("Тариф_Key"))
    tariffperiod_id = clean_uuid(item.get("ТарифныйПериод_Key"))
    
    # Организация
    org_id = clean_uuid(item.get("Организация_Key"))
    
    # ВАЖНО: Для всех клиентов из ЦЛ устанавливаем is_parent=true
    # В ЦЛ создаются только владельцы, пользователи создаются только через фронтенд
    is_parent = True
    
    if existing_client:
        # Обновляем существующего клиента
        updated = False
        if email and existing_client.email != email:
            existing_client.email = email
            updated = True
        if phone and existing_client.phone_number != phone:
            existing_client.phone_number = phone
            updated = True
        if org_inn and existing_client.org_inn != org_inn:
            existing_client.org_inn = org_inn
            updated = True
        if name and existing_client.name != name:
            existing_client.name = name
            updated = True
        if code_abonent and existing_client.code_abonent != code_abonent:
            existing_client.code_abonent = code_abonent
            updated = True
        if country and existing_client.country != country:
            existing_client.country = country
            updated = True
        if region and existing_client.region != region:
            existing_client.region = region
            updated = True
        if city and existing_client.city != city:
            existing_client.city = city
            updated = True
        if subs_id and existing_client.subs_id != subs_id:
            existing_client.subs_id = subs_id
            updated = True
        if subs_start and existing_client.subs_start != subs_start:
            existing_client.subs_start = subs_start
            updated = True
        if subs_end and existing_client.subs_end != subs_end:
            existing_client.subs_end = subs_end
            updated = True
        if tariff_id and existing_client.tariff_id != tariff_id:
            existing_client.tariff_id = tariff_id
            updated = True
        if tariffperiod_id and existing_client.tariffperiod_id != tariffperiod_id:
            existing_client.tariffperiod_id = tariffperiod_id
            updated = True
        if org_id and existing_client.org_id != org_id:
            existing_client.org_id = org_id
            updated = True
        # ВАЖНО: Обновляем is_parent только если клиент пришел из ЦЛ (есть cl_ref_key)
        # Если клиент был создан через фронтенд (нет cl_ref_key), не трогаем is_parent
        if existing_client.cl_ref_key and existing_client.is_parent != is_parent:
            existing_client.is_parent = is_parent
            updated = True
        
        return False, updated
    else:
        # Создаем нового клиента
        # ВАЖНО: Для клиентов из ЦЛ всегда is_parent=true и parent_id=None
        new_client = Client(
            cl_ref_key=ref_key,
            email=email,
            phone_number=phone,
            org_inn=org_inn,
            name=name,
            code_abonent=code_abonent,
            country=country,
            region=region,
            city=city,
            subs_id=subs_id,
            subs_start=subs_start,
            subs_end=subs_end,
            tariff_id=tariff_id,
            tariffperiod_id=tariffperiod_id,
            org_id=org_id,
            is_parent=is_parent,  # Всегда true для клиентов из ЦЛ
            parent_id=None,  # Всегда None для клиентов из ЦЛ
        )
        db.add(new_client)
        return True, False


async def pull_clients():
    """Основная функция загрузки клиентов"""
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
            # Получаем последний синхронизированный Code
            last_sync_code = await get_last_sync_code(db)
            
            if last_sync_code:
                from_code = last_sync_code
                logger.info("Incremental sync from Code >= %s (last sync: %s)", from_code, last_sync_code)
            else:
                from_code = INITIAL_FROM_CODE
                logger.info("First run — loading from Code >= %s", from_code)
            
            total_inserted = 0
            total_updated = 0
            skip = 0
            last_code_in_batch = None
            
            while True:
                # Инкрементальная загрузка по полю Code
                filter_part = f"Code ge '{from_code}'"
                url = (
                    f"{ODATA_BASEURL}{ENTITY}?$format=json"
                    f"&$filter={quote(filter_part)}"
                    f"&$orderby=Code asc"
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
                
                # Обрабатываем каждый клиент
                for item in batch:
                    try:
                        is_new, is_updated = await upsert_client(db, item)
                        if is_new:
                            total_inserted += 1
                        elif is_updated:
                            total_updated += 1
                        
                        # Сохраняем последний обработанный Code
                        code = item.get("Code")
                        if code:
                            last_code_in_batch = code
                    except Exception as e:
                        logger.error("Error processing client %s: %s", item.get("Ref_Key"), e)
                        continue
                
                await db.commit()
                
                # Сохраняем последний синхронизированный Code
                if last_code_in_batch:
                    await save_sync_code(db, last_code_in_batch)
                    await db.commit()
                
                if len(batch) < PAGE_SIZE:
                    break
                
                skip += PAGE_SIZE
            
            logger.info("✓ Clients sync completed. Inserted: %s, Updated: %s", total_inserted, total_updated)
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
        # Создаем таблицу sync_state если её нет
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sys.sync_state (
                entity_name TEXT PRIMARY KEY,
                last_synced_at TIMESTAMPTZ
            )
        """))
        # Добавляем поле last_synced_code если его нет
        await conn.execute(text("""
            ALTER TABLE sys.sync_state 
            ADD COLUMN IF NOT EXISTS last_synced_code TEXT
        """))
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(ensure_support_tables())
    asyncio.run(pull_clients())

