#!/usr/bin/env python3
# etl_raw_cons_rates_cl.py
# InformationRegister_ОценкаКонсультацийПоЗаявкам → raw.cons_rates
# Full load при пустой таблице, иначе — skip/top + ON CONFLICT
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

LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
RAW_DB_CONN = os.getenv("POSTGRES_RAW_CONN")
ODATA_BASEURL = os.getenv("ODATA_BASEURL_CL")
ODATA_USER = os.getenv("ODATA_USER", "odata")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")
PAGE_SIZE = 500
ETL_RUN_ID = str(uuid.uuid4())

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_raw_cons_rates")

DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cons_rates (
    calls_key UUID NOT NULL,
    clients_key UUID NOT NULL,
    manager_key UUID NOT NULL,
    question_number INTEGER NOT NULL,
    rating INTEGER,
    question_text TEXT,
    comment TEXT,
    sent_to_base BOOLEAN,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT,
    PRIMARY KEY (calls_key, clients_key, manager_key, question_number)
);

CREATE INDEX IF NOT EXISTS idx_cons_rates_calls ON raw.cons_rates(calls_key);
CREATE INDEX IF NOT EXISTS idx_cons_rates_manager ON raw.cons_rates(manager_key);
"""

TRUNCATE_SQL = "TRUNCATE TABLE raw.cons_rates;"
UPSERT_SQL = """
INSERT INTO raw.cons_rates (
    calls_key, clients_key, manager_key, question_number,
    rating, question_text, comment, sent_to_base, etl_run_id
) VALUES %s
ON CONFLICT (calls_key, clients_key, manager_key, question_number) DO NOTHING;
"""

def pg_connect_from_url(conn_str: str):
    if not conn_str: raise RuntimeError("POSTGRES_RAW_CONN not set")
    if conn_str.strip().startswith("dbname="): return psycopg2.connect(conn_str)
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

HEADERS = {"User-Agent": "ETL-ConsRates/1.0", "Accept": "application/json"}

def http_get_with_backoff(url, auth, max_retries=6, timeout=120):
    s = requests.Session()
    attempt = 0
    while True:
        try:
            r = s.get(url, auth=auth, headers=HEADERS, timeout=timeout)
            if r.status_code in (429, 502, 503, 504):
                if attempt >= max_retries: r.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("HTTP %s — retry in %s sec", r.status_code, wait)
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
            time.sleep(wait)
            attempt += 1

def clean_uuid(val):
    if not val or val == "00000000-0000-0000-0000-000000000000": return None
    return val

def load_odata_page(url, auth):
    resp = http_get_with_backoff(url, auth)
    return resp.json().get("value", [])

def etl_cons_rates():
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

    cur.execute("SELECT COUNT(*) FROM raw.cons_rates")
    is_empty = cur.fetchone()[0] == 0
    if is_empty:
        cur.execute(TRUNCATE_SQL)
        conn.commit()
        logger.info("Table raw.cons_rates is empty → full load")
    else:
        logger.info("Table raw.cons_rates has data → incremental via skip/top + ON CONFLICT")

    entity = "InformationRegister_ОценкаКонсультацийПоЗаявкам"
    base_url = f"{ODATA_BASEURL}{entity}?$format=json"
    skip = 0
    total_loaded = 0

    while True:
        page_url = f"{base_url}&$top={PAGE_SIZE}&$skip={skip}"
        batch = load_odata_page(page_url, auth)
        if not batch:
            break

        values = []
        for item in batch:
            calls_key = clean_uuid(item.get("Обращение_Key"))
            clients_key = clean_uuid(item.get("Контрагент_Key"))
            manager_key = clean_uuid(item.get("Менеджер_Key"))
            question_number = item.get("НомерВопроса")
            if not all([calls_key, clients_key, manager_key, question_number is not None]):
                continue
            values.append((
                calls_key,
                clients_key,
                manager_key,
                question_number,
                item.get("Оценка"),
                item.get("Вопрос"),
                item.get("Комментарий"),
                item.get("ОтправленаБаза"),
                ETL_RUN_ID
            ))

        if values:
            execute_values(cur, UPSERT_SQL, values, page_size=1000)
            total_loaded += len(values)

        conn.commit()
        logger.info("skip=%s | batch=%s | new=%s | total=%s", skip, len(batch), len(values), total_loaded)

        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    cur.close()
    conn.close()
    logger.info("ETL cons_rates finished. Total new records: %s", total_loaded)

if __name__ == "__main__":
    try:
        etl_cons_rates()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)