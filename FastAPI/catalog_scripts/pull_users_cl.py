#!/usr/bin/env python3
"""
Загрузка пользователей и их навыков из 1C:ЦЛ (OData) в таблицы cons.users и cons.users_skill.

Логика:
- каталоги и регистры подтягиваются полностью (объём умеренный, инкремент не критичен);
- пользователи апсертом обновляются по cl_ref_key (Ref_Key из 1C);
- навыки пересобираются с нуля (таблица cons.users_skill очищается перед вставкой).
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, time as dtime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from FastAPI.config import settings
from FastAPI.models import User, UserSkill

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
LANG_RU = "15d38cda-1812-11ef-b824-c67597d01fa8"
LANG_UZ = "15d38cdb-1812-11ef-b824-c67597d01fa8"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_users_cl")

ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

HEADERS = {
    "User-Agent": "cons-middleware/users-loader",
    "Accept": "application/json",
}


def clean_uuid(value: Optional[str]) -> Optional[str]:
    if not value or value == "00000000-0000-0000-0000-000000000000":
        return None
    return value


def parse_time_field(raw: Optional[str]) -> Optional[dtime]:
    if not raw or not raw.startswith("0001-01-01T"):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).time()
    except ValueError:
        try:
            return datetime.strptime(raw.split("+")[0], "%Y-%m-%dT%H:%M:%S").time()  # type: ignore[attr-defined]
        except Exception:
            return None


def http_get_with_backoff(url: str, auth: Tuple[str, str], max_retries: int = 6, timeout: int = 120):
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


def fetch_entity(entity: str, auth: Tuple[str, str], orderby: Optional[str] = None) -> List[Dict[str, Any]]:
    if not ODATA_BASEURL:
        raise RuntimeError("ODATA_BASEURL_CL is not configured")
    base = f"{ODATA_BASEURL}{entity}?$format=json"
    if orderby:
        base += f"&$orderby={orderby}"

    result: List[Dict[str, Any]] = []
    skip = 0
    while True:
        page_url = f"{base}&$top={PAGE_SIZE}&$skip={skip}"
        resp = http_get_with_backoff(page_url, auth)
        batch = resp.json().get("value", [])
        if not batch:
            break
        result.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    logger.info("Fetched %s rows from %s", len(result), entity)
    return result


def extract_contact_info(contact_list: Sequence[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    email = None
    phone = None
    for contact in contact_list or []:
        ctype = contact.get("Тип")
        if ctype == "АдресЭлектроннойПочты":
            email = contact.get("АдресЭП")
        elif ctype == "Телефон":
            phone = contact.get("НомерТелефона")
    return email, phone


def build_reference_maps(auth: Tuple[str, str]) -> Dict[str, Any]:
    departments = fetch_entity("Catalog_Отделы", auth)
    dept_map = {item["Ref_Key"]: item.get("Description") for item in departments if not item.get("DeletionMark")}

    user_dept = fetch_entity("InformationRegister_ОтделыПользователей", auth)
    user_dept_map = {
        item.get("Менеджер_Key"): item.get("Отдел_Key")
        for item in user_dept
        if item.get("Менеджер_Key") and item.get("Отдел_Key")
    }

    user_lang = fetch_entity("InformationRegister_ЯзыкиПользователей", auth)
    user_lang_map: Dict[str, set] = {}
    for item in user_lang:
        user_key = item.get("Менеджер_Key")
        lang_key = item.get("Язык_Key")
        if user_key and lang_key:
            user_lang_map.setdefault(user_key, set()).add(lang_key)

    consultant_rows = fetch_entity(
        "InformationRegister_СписокКонсультантовДляЗаявок",
        auth,
        orderby="Менеджер_Key asc, Period desc",
    )
    consultant_map: Dict[str, Dict[str, Any]] = {}
    seen: set = set()
    for row in consultant_rows:
        user_key = row.get("Менеджер_Key")
        if not user_key or user_key in seen:
            continue
        seen.add(user_key)
        limit_raw = row.get("ЛимитКонсультаций")
        consultant_map[user_key] = {
            "con_limit": int(limit_raw) if isinstance(limit_raw, str) and limit_raw.isdigit() else None,
            "start_hour": parse_time_field(row.get("ВремяРаботыНачало")),
            "end_hour": parse_time_field(row.get("ВремяРаботыКонец")),
        }

    return {
        "dept_map": dept_map,
        "user_dept_map": user_dept_map,
        "user_lang_map": user_lang_map,
        "consultant_map": consultant_map,
    }


def transform_users(
    rows: Iterable[Dict[str, Any]],
    refs: Dict[str, Any],
) -> List[Dict[str, Any]]:
    dept_map = refs["dept_map"]
    user_dept_map = refs["user_dept_map"]
    user_lang_map = refs["user_lang_map"]
    consultant_map = refs["consultant_map"]

    transformed: List[Dict[str, Any]] = []
    for row in rows:
        cl_ref_key = clean_uuid(row.get("Ref_Key"))
        if not cl_ref_key:
            continue
        
        # Фильтрация: не загружаем пользователей с DeletionMark=true, Недействителен=true или Служебный=true
        deletion_mark = bool(row.get("DeletionMark"))
        invalid = bool(row.get("Недействителен"))
        service = bool(row.get("Служебный"))
        
        if deletion_mark or invalid or service:
            logger.debug(f"Skipping user {cl_ref_key}: DeletionMark={deletion_mark}, Недействителен={invalid}, Служебный={service}")
            continue

        lang_keys = user_lang_map.get(cl_ref_key, set())
        consultant = consultant_map.get(cl_ref_key, {})
        dept_key = user_dept_map.get(cl_ref_key)

        email, phone = extract_contact_info(row.get("КонтактнаяИнформация", []))

        # Определяем команду (team) на основе department
        department_name = dept_map.get(dept_key)
        chatwoot_team = None
        if department_name:
            # Маппинг department -> команда в Chatwoot
            # "ИТС консультанты" и "1С:УК 3.0 и Розница 3.0" -> "консультация по ведению учета"
            if department_name in ("ИТС консультанты", "1С:УК 3.0 и Розница 3.0"):
                chatwoot_team = "консультация по ведению учета"
            # Для технической поддержки можно не назначать команду, Chatwoot сам назначит
        
        transformed.append(
            {
                "user_id": row.get("Code") or row.get("Description"),
                "chatwoot_team": chatwoot_team,
                "avatar_url": None,
                "confirmed": False,
                "cl_ref_key": cl_ref_key,
                "deletion_mark": bool(row.get("DeletionMark")),
                "description": row.get("Description"),
                "invalid": bool(row.get("Недействителен")),
                "ru": LANG_RU in lang_keys,
                "uz": LANG_UZ in lang_keys,
                "department": department_name,
                "con_limit": consultant.get("con_limit"),
                "start_hour": consultant.get("start_hour"),
                "end_hour": consultant.get("end_hour"),
                "phone_number": phone,
                "email": email,
            }
        )
    return transformed


def transform_skills(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
    seen: set = set()
    result: List[Dict[str, str]] = []
    for row in rows:
        user_key = clean_uuid(row.get("Менеджер_Key"))
        category_key = clean_uuid(row.get("КатегорияВопроса_Key"))
        if not (user_key and category_key):
            continue
        combo = (user_key, category_key)
        if combo in seen:
            continue
        seen.add(combo)
        result.append({"user_key": user_key, "category_key": category_key})
    return result


async def upsert_users(db: AsyncSession, users: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not users:
        return 0, 0

    existing_map: Dict[str, Any] = {}
    result = await db.execute(select(User.account_id, User.cl_ref_key))
    for account_id, cl_ref_key in result:
        if cl_ref_key:
            existing_map[cl_ref_key] = account_id

    inserted = 0
    updated = 0
    for payload in users:
        cl_ref_key = payload["cl_ref_key"]
        if cl_ref_key and cl_ref_key in existing_map:
            stmt = (
                update(User)
                .where(User.account_id == existing_map[cl_ref_key])
                .values(
                    user_id=payload["user_id"],
                    chatwoot_team=payload["chatwoot_team"],
                    avatar_url=None,
                    confirmed=False,
                    deletion_mark=payload["deletion_mark"],
                    description=payload["description"],
                    invalid=payload["invalid"],
                    ru=payload["ru"],
                    uz=payload["uz"],
                    department=payload["department"],
                    con_limit=payload["con_limit"],
                    start_hour=payload["start_hour"],
                    end_hour=payload["end_hour"],
                )
            )
            await db.execute(stmt)
            updated += 1
        else:
            db.add(
                User(
                    user_id=payload["user_id"],
                    chatwoot_team=payload["chatwoot_team"],
                    avatar_url=None,
                    confirmed=False,
                    cl_ref_key=cl_ref_key,
                    deletion_mark=payload["deletion_mark"],
                    description=payload["description"],
                    invalid=payload["invalid"],
                    ru=payload["ru"],
                    uz=payload["uz"],
                    department=payload["department"],
                    con_limit=payload["con_limit"],
                    start_hour=payload["start_hour"],
                    end_hour=payload["end_hour"],
                )
            )
            inserted += 1

    return inserted, updated


async def rebuild_user_skills(db: AsyncSession, skills: List[Dict[str, str]]) -> int:
    await db.execute(delete(UserSkill))
    if not skills:
        return 0
    db.add_all(UserSkill(**row) for row in skills)
    return len(skills)


async def pull_users():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)

    auth = (ODATA_USER, ODATA_PASSWORD)
    refs = build_reference_maps(auth)
    users_raw = fetch_entity("Catalog_Пользователи", auth)
    skills_raw = fetch_entity("InformationRegister_КатегорииВопросовМенеджеров", auth)

    user_rows = transform_users(users_raw, refs)
    skill_rows = transform_skills(skills_raw)
    logger.info("Prepared %s users and %s skill links", len(user_rows), len(skill_rows))

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
    Session = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with Session() as db:
            inserted, updated = await upsert_users(db, user_rows)
            skills_inserted = await rebuild_user_skills(db, skill_rows)
            await db.commit()
        logger.info("Users sync completed. Inserted=%s, Updated=%s, Skills=%s", inserted, updated, skills_inserted)
        
        # После загрузки пользователей синхронизируем их с Chatwoot
        logger.info("Starting Chatwoot synchronization for users...")
        try:
            # Импортируем и запускаем синхронизацию
            from FastAPI.catalog_scripts.sync_users_to_chatwoot import sync_all_users
            # sync_all_users - это async функция, запускаем её через asyncio.run
            # Но так как мы уже в async контексте, используем await
            await sync_all_users()
            logger.info("✓ Chatwoot synchronization completed")
        except Exception as sync_error:
            logger.error(f"Failed to sync users with Chatwoot: {sync_error}", exc_info=True)
            # Не прерываем выполнение, так как основная задача (загрузка из ЦЛ) выполнена
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(pull_users())

