#!/usr/bin/env python3
# etl_raw_cons_redate_cl.py
# InformationRegister_РегистрацияПереносаКонсультации → raw.cons_redate
# PK: (calls_key, clients_key, manager_key, period)
# Фильтр: Period gt datetime'2025-11-12T12:05:05' (без Z/+05:00)
import os
import sys
import time
import uuid
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote, quote
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
logger = logging.getLogger("etl_raw_cons_redate")

DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cons_redate (
    calls_key UUID NOT NULL,
    clients_key UUID NOT NULL,
    manager_key UUID NOT NULL,
    period TIMESTAMPTZ NOT NULL,
    old_date TIMESTAMPTZ,
    new_date TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT,
    PRIMARY KEY (calls_key, clients_key, manager_key, period)
);

CREATE INDEX IF NOT EXISTS idx_cons_redate_period ON raw.cons_redate(period);
CREATE INDEX IF NOT EXISTS idx_cons_redate_calls ON raw.cons_redate(calls_key);
"""

UPSERT_SQL = """
INSERT INTO raw.cons_redate (
    calls_key, clients_key, manager_key, period,
    old_date, new_date, etl_run_id
) VALUES %s
ON CONFLICT (calls_key, clients_key, manager_key, period) DO NOTHING;
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

HEADERS = {"User-Agent": "ETL-ConsRedate/1.0", "Accept": "application/json"}

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

def parse_datetime(dt_str):
    if not dt_str: return None
    try:
        dt_str = dt_str.replace("Z", "")
        return datetime.fromisoformat(dt_str)
    except Exception as e:
        logger.warning("Failed to parse datetime: %s | %s", dt_str, e)
        return None

def load_odata_page(url, auth):
    resp = http_get_with_backoff(url, auth)
    return resp.json().get("value", [])

def etl_cons_redate():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing")
        sys.exit(1)
    if RAW_DB_CONN is None:
        raise ValueError("RAW_DB_CONN is not set")

    auth = (ODATA_USER, ODATA_PASSWORD)
    conn = pg_connect_from_url(RAW_DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    # --- Инкремент по MAX(period) ---
    cur.execute("SELECT MAX(period) FROM raw.cons_redate")
    last_period = cur.fetchone()[0]

    if last_period is None:
        # Первый запуск
        url = f"{ODATA_BASEURL}InformationRegister_РегистрацияПереносаКонсультации?$format=json&$orderby=Period asc"
        logger.info("First run → full load")
    else:
        # Конвертируем в строку БЕЗ таймзоны: 2025-11-12T12:05:05
        filter_dt = last_period.astimezone(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')
        filter_str = f"Period gt datetime'{filter_dt}'"
        url = f"{ODATA_BASEURL}InformationRegister_РегистрацияПереносаКонсультации?$format=json&$orderby=Period asc&$filter={quote(filter_str)}"
        logger.info("Incremental load: %s", filter_str)

    skip = 0
    total_new = 0

    while True:
        page_url = f"{url}&$top={PAGE_SIZE}&$skip={skip}"
        logger.info("Request: %s", page_url)
        batch = load_odata_page(page_url, auth)
        if not batch:
            break

        values = []
        for item in batch:
            calls_key = clean_uuid(item.get("ДокументОбращения_Key"))
            clients_key = clean_uuid(item.get("Абонент_Key"))
            manager_key = clean_uuid(item.get("Менеджер_Key"))
            period = parse_datetime(item.get("Period"))
            if not all([calls_key, clients_key, manager_key, period]):
                continue
            values.append((
                calls_key,
                clients_key,
                manager_key,
                period,
                parse_datetime(item.get("СтараяДата")),
                parse_datetime(item.get("НоваяДата")),
                ETL_RUN_ID
            ))

        if values:
            execute_values(cur, UPSERT_SQL, values, page_size=1000)
            total_new += len(values)

        conn.commit()
        logger.info("skip=%s | batch=%s | new=%s | total_new=%s", skip, len(batch), len(values), total_new)

        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    cur.close()
    conn.close()
    logger.info("ETL cons_redate finished. Total new records: %s", total_new)

if __name__ == "__main__":
    try:
        etl_cons_redate()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)