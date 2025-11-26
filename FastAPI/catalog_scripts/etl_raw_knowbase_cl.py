#!/usr/bin/env python3
# etl_raw_knowbase_cl.py
# Full load 6 справочников → 6 raw-таблиц
# raw.knowledge_base, raw.po_types, raw.po_sections,
# raw.question_categories, raw.consultation_questions, raw.consultation_blocks
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
logger = logging.getLogger("etl_raw_knowbase_cl")

# ---- DDL: 6 таблиц ----
DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

-- 1. База знаний
CREATE TABLE IF NOT EXISTS raw.knowledge_base (
    ref_key UUID PRIMARY KEY,
    description TEXT,
    po_type_key UUID,
    po_section_key UUID,
    author_key UUID,
    question TEXT,
    answer TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- 2. Виды ПО
CREATE TABLE IF NOT EXISTS raw.po_types (
    ref_key UUID PRIMARY KEY,
    description TEXT,
    details TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- 3. Разделы ПО
CREATE TABLE IF NOT EXISTS raw.po_sections (
    ref_key UUID PRIMARY KEY,
    owner_key UUID,
    description TEXT,
    details TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- 4. Категории вопросов
CREATE TABLE IF NOT EXISTS raw.question_categories (
    ref_key UUID PRIMARY KEY,
    code TEXT,
    description TEXT,
    language TEXT CHECK (language IN ('ru', 'uz')),
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- 5. Вопросы на консультацию
CREATE TABLE IF NOT EXISTS raw.consultation_questions (
    ref_key UUID PRIMARY KEY,
    code TEXT,
    description TEXT,
    language TEXT CHECK (language IN ('ru', 'uz')),
    category_key UUID,
    useful_info TEXT,
    question TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- 6. Помехи для консультаций
CREATE TABLE IF NOT EXISTS raw.consultation_blocks (
    ref_key UUID PRIMARY KEY,
    description TEXT,
    details TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_knowledge_base_po_type ON raw.knowledge_base(po_type_key);
CREATE INDEX IF NOT EXISTS idx_knowledge_base_po_section ON raw.knowledge_base(po_section_key);
CREATE INDEX IF NOT EXISTS idx_question_categories_lang ON raw.question_categories(language);
CREATE INDEX IF NOT EXISTS idx_consultation_questions_lang ON raw.consultation_questions(language);
"""

TRUNCATE_SQL = """
TRUNCATE TABLE
    raw.knowledge_base,
    raw.po_types,
    raw.po_sections,
    raw.question_categories,
    raw.consultation_questions,
    raw.consultation_blocks;
"""

# ---- UPSERT SQL ----
UPSERT_KB = """
INSERT INTO raw.knowledge_base (ref_key, description, po_type_key, po_section_key, author_key, question, answer, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    description = EXCLUDED.description,
    po_type_key = EXCLUDED.po_type_key,
    po_section_key = EXCLUDED.po_section_key,
    author_key = EXCLUDED.author_key,
    question = EXCLUDED.question,
    answer = EXCLUDED.answer,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_PO_TYPES = """
INSERT INTO raw.po_types (ref_key, description, details, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    description = EXCLUDED.description,
    details = EXCLUDED.details,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_PO_SECTIONS = """
INSERT INTO raw.po_sections (ref_key, owner_key, description, details, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    owner_key = EXCLUDED.owner_key,
    description = EXCLUDED.description,
    details = EXCLUDED.details,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_QUEST_CAT = """
INSERT INTO raw.question_categories (ref_key, code, description, language, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    code = EXCLUDED.code,
    description = EXCLUDED.description,
    language = EXCLUDED.language,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_QUESTIONS = """
INSERT INTO raw.consultation_questions (ref_key, code, description, language, category_key, useful_info, question, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    code = EXCLUDED.code,
    description = EXCLUDED.description,
    language = EXCLUDED.language,
    category_key = EXCLUDED.category_key,
    useful_info = EXCLUDED.useful_info,
    question = EXCLUDED.question,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_BLOCKS = """
INSERT INTO raw.consultation_blocks (ref_key, description, details, etl_run_id)
VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    description = EXCLUDED.description,
    details = EXCLUDED.details,
    loaded_at = NOW(),
    etl_run_id = EXCLUDED.etl_run_id;
"""

# ---- helpers ----
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
    "User-Agent": "ETL-Knowbase/1.0",
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

def get_language(lang_key):
    if lang_key == LANG_RU:
        return "ru"
    if lang_key == LANG_UZ:
        return "uz"
    return None

def load_odata_entity(entity_name: str, auth):
    url = f"{ODATA_BASEURL}{entity_name}?$format=json"
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

# ---- main ETL ----
def etl_knowbase():
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

    logger.info("Starting full ETL for 6 knowbase tables")

    # === TRUNCATE ALL ===
    cur.execute(TRUNCATE_SQL)
    conn.commit()
    logger.info("All raw knowbase tables truncated")

    now_iso = datetime.now(timezone.utc).isoformat()

    # === 1. Catalog_БазаЗнанийДляКонсультаций → raw.knowledge_base ===
    logger.info("Loading knowledge_base...")
    kb_raw = load_odata_entity("Catalog_БазаЗнанийДляКонсультаций", auth)
    kb_values = []
    for item in kb_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        kb_values.append((
            ref_key,
            item.get("Description"),
            clean_uuid(item.get("ВидПО_Key")),
            clean_uuid(item.get("РазделПО_Key")),
            clean_uuid(item.get("Автор_Key")),
            item.get("Вопрос"),
            item.get("Ответ"),
            ETL_RUN_ID
        ))
    if kb_values:
        execute_values(cur, UPSERT_KB, kb_values, page_size=1000)
        logger.info("Inserted %s into raw.knowledge_base", len(kb_values))

    # === 2. Catalog_ВидыПОДляКонсультаций → raw.po_types ===
    logger.info("Loading po_types...")
    po_types_raw = load_odata_entity("Catalog_ВидыПОДляКонсультаций", auth)
    po_types_values = []
    for item in po_types_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        po_types_values.append((
            ref_key,
            item.get("Description"),
            item.get("Описание"),
            ETL_RUN_ID
        ))
    if po_types_values:
        execute_values(cur, UPSERT_PO_TYPES, po_types_values, page_size=1000)
        logger.info("Inserted %s into raw.po_types", len(po_types_values))

    # === 3. Catalog_РазделыПОДляКонсультаций → raw.po_sections ===
    logger.info("Loading po_sections...")
    po_sections_raw = load_odata_entity("Catalog_РазделыПОДляКонсультаций", auth)
    po_sections_values = []
    for item in po_sections_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        po_sections_values.append((
            ref_key,
            clean_uuid(item.get("Owner_Key")),
            item.get("Description"),
            item.get("Описание"),
            ETL_RUN_ID
        ))
    if po_sections_values:
        execute_values(cur, UPSERT_PO_SECTIONS, po_sections_values, page_size=1000)
        logger.info("Inserted %s into raw.po_sections", len(po_sections_values))

    # === 4. Catalog_КатегорииВопросов → raw.question_categories ===
    logger.info("Loading question_categories...")
    qc_raw = load_odata_entity("Catalog_КатегорииВопросов", auth)
    qc_values = []
    for item in qc_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        lang = get_language(item.get("Язык_Key"))
        if not lang:
            continue
        qc_values.append((
            ref_key,
            item.get("Code"),
            item.get("Description"),
            lang,
            ETL_RUN_ID
        ))
    if qc_values:
        execute_values(cur, UPSERT_QUEST_CAT, qc_values, page_size=1000)
        logger.info("Inserted %s into raw.question_categories", len(qc_values))

    # === 5. Catalog_ВопросыНаКонсультацию → raw.consultation_questions ===
    logger.info("Loading consultation_questions...")
    cq_raw = load_odata_entity("Catalog_ВопросыНаКонсультацию", auth)
    cq_values = []
    for item in cq_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        lang = get_language(item.get("Язык_Key"))
        if not lang:
            continue
        cq_values.append((
            ref_key,
            item.get("Code"),
            item.get("Description"),
            lang,
            clean_uuid(item.get("Категория_Key")),
            item.get("ПолезнаяИнформация"),
            item.get("Вопрос"),
            ETL_RUN_ID
        ))
    if cq_values:
        execute_values(cur, UPSERT_QUESTIONS, cq_values, page_size=1000)
        logger.info("Inserted %s into raw.consultation_questions", len(cq_values))

    # === 6. Catalog_ПомехиДляКонсультаций → raw.consultation_blocks ===
    logger.info("Loading consultation_blocks...")
    cb_raw = load_odata_entity("Catalog_ПомехиДляКонсультаций", auth)
    cb_values = []
    for item in cb_raw:
        ref_key = item.get("Ref_Key")
        if not ref_key:
            continue
        cb_values.append((
            ref_key,
            item.get("Description"),
            item.get("Описание"),
            ETL_RUN_ID
        ))
    if cb_values:
        execute_values(cur, UPSERT_BLOCKS, cb_values, page_size=1000)
        logger.info("Inserted %s into raw.consultation_blocks", len(cb_values))

    conn.commit()
    cur.close()
    conn.close()
    logger.info("ETL knowbase finished successfully")

if __name__ == "__main__":
    try:
        etl_knowbase()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)