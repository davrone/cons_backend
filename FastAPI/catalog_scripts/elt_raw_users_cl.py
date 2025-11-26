#!/usr/bin/env python3
# etl_raw_users_enriched_v4.py
# 1. raw.users (обогащённые пользователи)
# 2. raw.users_know (знания по категориям) — в отдельной таблице
import os
import sys
import time
import uuid
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote
import requests
import psycopg2
from psycopg2.extras import execute_values

# ---- config ----
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
RAW_DB_CONN = os.getenv("POSTGRES_RAW_CONN")
ODATA_BASEURL = os.getenv("ODATA_BASEURL_CL")
ODATA_USER = os.getenv("ODATA_USER", "odata")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
PAGE_SIZE = 5000
ETL_RUN_ID = str(uuid.uuid4())

# Фиксированные языки
LANG_RU = "15d38cda-1812-11ef-b824-c67597d01fa8"
LANG_UZ = "15d38cdb-1812-11ef-b824-c67597d01fa8"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_raw_users_enriched_v4")

# ---- DDL ----
DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

-- Основная таблица пользователей
CREATE TABLE IF NOT EXISTS raw.users (
    ref_key UUID PRIMARY KEY,
    deletion_mark BOOLEAN,
    description TEXT,
    user_ib_id UUID,
    user_service_id UUID,
    invalid BOOLEAN,
    service_user BOOLEAN,
    predefined BOOLEAN,
    email TEXT,
    phone TEXT,
    ru BOOLEAN,
    uz BOOLEAN,
    department TEXT,
    con_limit INTEGER,
    start_hour TIME,
    end_hour TIME,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- Таблица знаний: какие категории знает каждый оператор
CREATE TABLE IF NOT EXISTS raw.users_know (
    user_key UUID NOT NULL,
    category_key UUID NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT,
    PRIMARY KEY (user_key, category_key)
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_raw_users_description ON raw.users(description);
CREATE INDEX IF NOT EXISTS idx_raw_users_know_category ON raw.users_know(category_key);
"""

TRUNCATE_USERS_SQL = "TRUNCATE TABLE raw.users;"
TRUNCATE_USERS_KNOW_SQL = "TRUNCATE TABLE raw.users_know;"

UPSERT_USERS_SQL = """
INSERT INTO raw.users (
    ref_key, deletion_mark, description, user_ib_id, user_service_id,
    invalid, service_user, predefined, email, phone,
    ru, uz, department, con_limit, start_hour, end_hour, etl_run_id
) VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    deletion_mark = EXCLUDED.deletion_mark,
    description = EXCLUDED.description,
    user_ib_id = EXCLUDED.user_ib_id,
    user_service_id = EXCLUDED.user_service_id,
    invalid = EXCLUDED.invalid,
    service_user = EXCLUDED.service_user,
    predefined = EXCLUDED.predefined,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    ru = EXCLUDED.ru,
    uz = EXCLUDED.uz,
    department = EXCLUDED.department,
    con_limit = EXCLUDED.con_limit,
    start_hour = EXCLUDED.start_hour,
    end_hour = EXCLUDED.end_hour,
    loaded_at = EXCLUDED.loaded_at,
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_USERS_KNOW_SQL = """
INSERT INTO raw.users_know (user_key, category_key, etl_run_id)
VALUES %s
ON CONFLICT (user_key, category_key) DO NOTHING;
"""

def pg_connect_from_url(conn_str: str):
    if not conn_str:
        raise RuntimeError("POSTGRES_RAW_CONN not set")
    if conn_str.strip().startswith("dbname="):
        return psycopg2.connect(conn_str)
    parsed = urlparse(conn_str)
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    dbname = parsed.path.lstrip("/") if parsed.path else ""
    dsn = f"host={host} port={port} dbname={dbname}"
    if user: dsn += f" user={user}"
    if password: dsn += f" password={password}"
    return psycopg2.connect(dsn)

HEADERS = {
    "User-Agent": "ETL-Users-V4/1.0",
    "Accept": "application/json",
}

def http_get_with_backoff(url, auth, max_retries=6, timeout=120):
    s = requests.Session()
    attempt = 0
    while True:
        try:
            r = s.get(url, auth=auth, headers=HEADERS, timeout=timeout)
            if r.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    r.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("HTTP %s — retry in %s sec (attempt %s)", r.status_code, wait, attempt+1)
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
            time.sleep(wait)
            attempt += 1

def clean_uuid(val):
    if not val or val == "00000000-0000-0000-0000-000000000000":
        return None
    return val

def extract_contact_fields(contact_list):
    email = None
    phone = None
    for contact in contact_list:
        if contact.get("Тип") == "АдресЭлектроннойПочты":
            email = contact.get("АдресЭП")
        elif contact.get("Тип") == "Телефон":
            phone = contact.get("НомерТелефона")
    return email, phone

def parse_time_field(time_str):
    if not time_str or not time_str.startswith("0001-01-01T"):
        return None
    try:
        return time_str.split("T")[1][:8]
    except:
        return None

def load_odata_entity(entity_name: str, auth, orderby=None):
    url = f"{ODATA_BASEURL}{entity_name}?$format=json"
    if orderby:
        url += f"&$orderby={orderby}"
    data = []
    skip = 0
    while True:
        page_url = f"{url}&$top={PAGE_SIZE}&$skip={skip}"
        resp = http_get_with_backoff(page_url, auth)
        batch = resp.json().get("value", [])
        if not batch:
            break
        data.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    logger.info("Loaded %s records from %s", len(data), entity_name)
    return data

def etl_users_enriched_v4():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing")
        sys.exit(1)
    if RAW_DB_CONN is None:
        raise ValueError("RAW_DB_CONN is not set")

    auth = (ODATA_USER, ODATA_PASSWORD)
    conn = pg_connect_from_url(RAW_DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL_CREATE)
    conn.commit()

    logger.info("Starting full enriched ETL v4")

    # === 1. Загрузка справочников и регистров в память ===

    # 1.1 Отделы
    logger.info("Loading departments...")
    departments_raw = load_odata_entity("Catalog_Отделы", auth)
    dept_map = {d["Ref_Key"]: d["Description"] for d in departments_raw if not d.get("DeletionMark")}

    # 1.2 Пользователь → отдел
    logger.info("Loading user → department...")
    user_dept_raw = load_odata_entity("InformationRegister_ОтделыПользователей", auth)
    user_dept_map = {}
    for item in user_dept_raw:
        user_key = item.get("Менеджер_Key")
        dept_key = item.get("Отдел_Key")
        if user_key and dept_key:
            user_dept_map[user_key] = dept_key

    # 1.3 Пользователь → языки
    logger.info("Loading user → languages...")
    user_lang_raw = load_odata_entity("InformationRegister_ЯзыкиПользователей", auth)
    user_lang_map = {}
    for item in user_lang_raw:
        user_key = item.get("Менеджер_Key")
        lang_key = item.get("Язык_Key")
        if user_key and lang_key:
            user_lang_map.setdefault(user_key, set()).add(lang_key)

    # 1.4 Консультанты: срез последних
    logger.info("Loading consultant data (latest by Period)...")
    consultant_raw = load_odata_entity(
        entity_name="InformationRegister_СписокКонсультантовДляЗаявок",
        auth=auth,
        orderby="Менеджер_Key asc, Period desc"
    )
    user_consultant_map = {}
    seen = set()
    for item in consultant_raw:
        user_key = item.get("Менеджер_Key")
        if not user_key or user_key in seen:
            continue
        seen.add(user_key)
        limit_str = item.get("ЛимитКонсультаций")
        con_limit = int(limit_str) if limit_str and limit_str.isdigit() else None
        start_hour = parse_time_field(item.get("ВремяРаботыНачало"))
        end_hour = parse_time_field(item.get("ВремяРаботыКонец"))
        user_consultant_map[user_key] = {
            "con_limit": con_limit,
            "start_hour": start_hour,
            "end_hour": end_hour
        }

    # === 2. Загрузка пользователей ===
    cur.execute(TRUNCATE_USERS_SQL)
    conn.commit()
    logger.info("Table raw.users truncated")

    entity = "Catalog_Пользователи"
    skip = 0
    total_users = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    while True:
        url = f"{ODATA_BASEURL}{entity}?$format=json&$top={PAGE_SIZE}&$skip={skip}"
        try:
            resp = http_get_with_backoff(url, auth)
        except Exception as e:
            logger.exception("Failed to fetch users page")
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        user_values = []
        for item in batch:
            ref_key = item.get("Ref_Key")
            if not ref_key:
                continue

            email, phone = extract_contact_fields(item.get("КонтактнаяИнформация", []))

            dept_key = user_dept_map.get(ref_key)
            department = dept_map.get(dept_key) if dept_key else None

            lang_keys = user_lang_map.get(ref_key, set())
            ru = LANG_RU in lang_keys
            uz = LANG_UZ in lang_keys

            consult = user_consultant_map.get(ref_key, {})
            con_limit = consult.get("con_limit")
            start_hour = consult.get("start_hour")
            end_hour = consult.get("end_hour")

            user_values.append((
                ref_key,
                item.get("DeletionMark"),
                item.get("Description"),
                clean_uuid(item.get("ИдентификаторПользователяИБ")),
                clean_uuid(item.get("ИдентификаторПользователяСервиса")),
                item.get("Недействителен"),
                item.get("Служебный"),
                item.get("Predefined"),
                email,
                phone,
                ru,
                uz,
                department,
                con_limit,
                start_hour,
                end_hour,
                ETL_RUN_ID
            ))

        if user_values:
            execute_values(cur, UPSERT_USERS_SQL, user_values, page_size=1000)
            total_users += len(user_values)

        conn.commit()
        logger.info("Loaded users batch: %s (total: %s)", len(user_values), total_users)

        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    # === 3. Загрузка знаний (users_know) ===
    logger.info("Loading users_know (КатегорииВопросовМенеджеров)...")
    cur.execute(TRUNCATE_USERS_KNOW_SQL)
    conn.commit()
    logger.info("Table raw.users_know truncated")

    know_raw = load_odata_entity("InformationRegister_КатегорииВопросовМенеджеров", auth)
    know_values = []
    for item in know_raw:
        user_key = item.get("Менеджер_Key")
        cat_key = item.get("КатегорияВопроса_Key")
        if user_key and cat_key:
            know_values.append((user_key, cat_key, ETL_RUN_ID))

    if know_values:
        execute_values(cur, UPSERT_USERS_KNOW_SQL, know_values, page_size=1000)
        logger.info("Inserted %s rows into raw.users_know", len(know_values))
    else:
        logger.info("No data in raw.users_know")

    conn.commit()
    cur.close()
    conn.close()

    logger.info("ETL v4 finished. Users: %s, Know rows: %s", total_users, len(know_values))

if __name__ == "__main__":
    try:
        etl_users_enriched_v4()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)