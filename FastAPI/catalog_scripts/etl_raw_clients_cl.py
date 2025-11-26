#!/usr/bin/env python3
# etl_raw_clients.py
# Load Catalog_Контрагенты → raw.clients (append-only) + raw.client_contacts
# Runs every 15 min: first run = full-load, next runs = new records only.

import os
import sys
import time
import uuid
import hashlib
import json
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

ENTITY = "Catalog_Контрагенты"
PAGE_SIZE = 5000
ETL_RUN_ID = str(uuid.uuid4())

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_raw_clients")


def compute_row_hash(item: dict) -> str:
    """Вычисляет детерминированный MD5-хеш от скалярных полей (без вложенных коллекций)."""
    clean = {}
    for k, v in item.items():
        if k in (
            "Ref_Key",
            "КонтактнаяИнформация",
            "АктивированныеКлючи",
            "ДополнительныеРеквизиты",
            "ЛицензииФармМодуля"
        ):
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            clean[k] = v
        elif hasattr(v, "isoformat"):  # datetime
            clean[k] = v.isoformat() if v else None
    s = json.dumps(clean, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# ---- DDL ----
DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.etl_state (
    entity_name TEXT PRIMARY KEY,
    last_number TEXT,
    last_run_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS raw.clients (
    ref_key UUID PRIMARY KEY,
    data_version TEXT,
    deletion_mark BOOLEAN,
    parent_key UUID,
    is_folder BOOLEAN,
    code TEXT,
    description TEXT,
    responsible_person TEXT,
    legal_entity_type TEXT,
    comment TEXT,
    inn TEXT,
    is_branch BOOLEAN,
    address_repr TEXT,
    main_contact_person_key UUID,
    password_access TEXT,
    do_not_notify_new_versions BOOLEAN,
    personal_message_user_key UUID,
    issue_message_once BOOLEAN,
    bank_key UUID,
    bank_account TEXT,
    main_1c_key TEXT,
    call_early BOOLEAN,
    is_partner BOOLEAN,
    personal_message_user_pharm_key UUID,
    issue_message_once_pharm BOOLEAN,
    clobus_subscriber_id BIGINT,
    row_hash TEXT NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT
);

CREATE TABLE IF NOT EXISTS raw.client_contacts (
    counterparty_ref_key UUID NOT NULL,
    line_number BIGINT NOT NULL,
    type TEXT,
    country TEXT,
    region TEXT,
    city TEXT,
    ep_address TEXT,
    phone_number TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    etl_run_id TEXT,
    PRIMARY KEY (counterparty_ref_key, line_number),
    FOREIGN KEY (counterparty_ref_key) REFERENCES raw.clients(ref_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_clients_code ON raw.clients(code);
CREATE INDEX IF NOT EXISTS idx_raw_client_contacts_counterparty ON raw.client_contacts(counterparty_ref_key);
"""

# ---- SQL ----
UPSERT_CLIENTS_SQL = """
INSERT INTO raw.clients (
    ref_key, data_version, deletion_mark, parent_key, is_folder,
    code, description, responsible_person, legal_entity_type, comment,
    inn, is_branch, address_repr, main_contact_person_key, password_access,
    do_not_notify_new_versions, personal_message_user_key, issue_message_once,
    bank_key, bank_account, main_1c_key, call_early,
    is_partner, personal_message_user_pharm_key, issue_message_once_pharm,
    clobus_subscriber_id, row_hash, etl_run_id
) VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    data_version = EXCLUDED.data_version,
    deletion_mark = EXCLUDED.deletion_mark,
    parent_key = EXCLUDED.parent_key,
    is_folder = EXCLUDED.is_folder,
    code = EXCLUDED.code,
    description = EXCLUDED.description,
    responsible_person = EXCLUDED.responsible_person,
    legal_entity_type = EXCLUDED.legal_entity_type,
    comment = EXCLUDED.comment,
    inn = EXCLUDED.inn,
    is_branch = EXCLUDED.is_branch,
    address_repr = EXCLUDED.address_repr,
    main_contact_person_key = EXCLUDED.main_contact_person_key,
    password_access = EXCLUDED.password_access,
    do_not_notify_new_versions = EXCLUDED.do_not_notify_new_versions,
    personal_message_user_key = EXCLUDED.personal_message_user_key,
    issue_message_once = EXCLUDED.issue_message_once,
    bank_key = EXCLUDED.bank_key,
    bank_account = EXCLUDED.bank_account,
    main_1c_key = EXCLUDED.main_1c_key,
    call_early = EXCLUDED.call_early,
    is_partner = EXCLUDED.is_partner,
    personal_message_user_pharm_key = EXCLUDED.personal_message_user_pharm_key,
    issue_message_once_pharm = EXCLUDED.issue_message_once_pharm,
    clobus_subscriber_id = EXCLUDED.clobus_subscriber_id,
    row_hash = EXCLUDED.row_hash,
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_CONTACTS_SQL = """
INSERT INTO raw.client_contacts (
    counterparty_ref_key, line_number, type, country, region,
    city, ep_address, phone_number, etl_run_id
) VALUES %s
ON CONFLICT (counterparty_ref_key, line_number) DO NOTHING;
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
    if user:
        dsn += f" user={user}"
    if password:
        dsn += f" password={password}"
    return psycopg2.connect(dsn)


HEADERS = {
    "User-Agent": "ETL-Client/1.0",
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


def etl_clients():
    if not (ODATA_BASEURL and ODATA_USER and ODATA_PASSWORD):
        logger.error("ODATA config missing. Check ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD")
        sys.exit(1)
    if RAW_DB_CONN is None:
        raise ValueError("RAW_DB_CONN is not set")
    conn = pg_connect_from_url(RAW_DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL_CREATE)
    conn.commit()

    # Получаем последний last_number
    cur.execute("SELECT last_number, last_run_at FROM raw.etl_state WHERE entity_name = %s", (ENTITY,))
    state = cur.fetchone()
    last_number = state[0] if state else None
    logger.info("Starting ETL: last_number = %s", last_number)

    now = datetime.now(timezone.utc)
    total_clients = 0
    total_contacts = 0

    while True:
        if last_number:
            filter_part = f"Code gt '{last_number}'"
        else:
            filter_part = "Code gt ''"
        url = f"{ODATA_BASEURL}{ENTITY}?$format=json&$filter={filter_part}&$orderby=Code asc&$top={PAGE_SIZE}"

        try:
            resp = http_get_with_backoff(url, auth=(ODATA_USER, ODATA_PASSWORD), timeout=120)
        except Exception as e:
            logger.exception("Failed to fetch batch: %s", e)
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        client_values = []
        contact_values = []
        now_iso = now.isoformat()

        for item in batch:
            ref_key = item.get("Ref_Key")
            if not ref_key:
                continue

            row_hash = compute_row_hash(item)

            client_values.append((
                ref_key,
                item.get("DataVersion"),
                item.get("DeletionMark"),
                clean_uuid(item.get("Parent_Key")),
                item.get("IsFolder"),
                item.get("Code"),
                item.get("Description"),
                item.get("ОтветственноеЛицо"),
                item.get("ЮридическоеФизическоеЛицо"),
                item.get("Комментарий"),
                item.get("ИНН"),
                item.get("ОбособленноеПодразделение"),
                item.get("ПредставлениеАдресаДляСравнения"),
                clean_uuid(item.get("ОсновноеКонтактноеЛицо_Key")),
                item.get("ПарольДоступа"),
                item.get("НеОтправлятьУведомленияОНовыхВерсияхПО"),
                clean_uuid(item.get("ПерсональноеСообщениеПользователю_Key")),
                item.get("ВыдоватьСообщениеОдинРаз"),
                clean_uuid(item.get("Банк_Key")),
                item.get("БанковскийСчет"),
                item.get("ОсновнойКлюч1С"),
                item.get("ЗвонитьПораньше"),
                item.get("Партнер"),
                clean_uuid(item.get("ПерсональноеСообщениеПользователюФарм_Key")),
                item.get("ВыдоватьСообщениеОдинРазФарм"),
                item.get("КодАбонентаClobus"),
                row_hash,
                now_iso
            ))

            for contact in item.get("КонтактнаяИнформация", []):
                contact_values.append((
                    ref_key,
                    contact.get("LineNumber"),
                    contact.get("Тип"),
                    contact.get("Страна"),
                    contact.get("Регион"),
                    contact.get("Город"),
                    contact.get("АдресЭП"),
                    contact.get("НомерТелефона"),
                    now_iso
                ))

        if client_values:
            execute_values(cur, UPSERT_CLIENTS_SQL, client_values, page_size=1000)
            total_clients += len(client_values)

        if contact_values:
            execute_values(cur, UPSERT_CONTACTS_SQL, contact_values, page_size=1000)
            total_contacts += len(contact_values)

        conn.commit()
        logger.info("Upserted: %s clients, %s contacts", len(client_values), len(contact_values))

        if batch:
            last_number = batch[-1].get("Code")

        if len(batch) < PAGE_SIZE:
            break

    # Сохраняем прогресс
    cur.execute(
        """
        INSERT INTO raw.etl_state (entity_name, last_number, last_run_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (entity_name) DO UPDATE
        SET last_number = EXCLUDED.last_number, last_run_at = EXCLUDED.last_run_at
        """,
        (ENTITY, last_number, now)
    )
    conn.commit()

    cur.close()
    conn.close()
    logger.info("ETL finished. Total: %s clients, %s contacts", total_clients, total_contacts)


if __name__ == "__main__":
    try:
        etl_clients()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)