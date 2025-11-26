#!/usr/bin/env python3
# etl_raw_calls.py
# Load Document_ТелефонныйЗвонок → raw.calls + raw.call_consultations

import os
import sys
import time
import uuid
import json
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, unquote

import requests
import psycopg2
from psycopg2.extras import execute_values

# ---- конфиг ----
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
RAW_DB_CONN = os.getenv("POSTGRES_RAW_CONN")
ODATA_BASEURL = os.getenv("ODATA_BASEURL_CL")
ODATA_USER = os.getenv("ODATA_USER", "odata")
ODATA_PASSWORD = os.getenv("ODATA_PASSWORD")

# Дата для first run (из .env, fallback на 2025-01-01)
INITIAL_FROM_DATE = os.getenv("ETL_INITIAL_FROM_DATE", "2025-01-01")

ENTITY = "Document_ТелефонныйЗвонок"
PAGE_SIZE = 5000
ETL_RUN_ID = str(uuid.uuid4())

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_raw_calls")

# ---- SQL DDL ----
DDL_CREATE = """
CREATE SCHEMA IF NOT EXISTS raw;

-- Таблица состояния ETL
CREATE TABLE IF NOT EXISTS raw.etl_state (
    entity_name TEXT PRIMARY KEY,
    last_number TEXT,
    last_run_at TIMESTAMPTZ
);

-- Основная таблица звонков
CREATE TABLE IF NOT EXISTS raw.calls (
    ref_key UUID PRIMARY KEY,
    data_version TEXT,
    deletion_mark BOOLEAN,
    number TEXT,
    date_document TIMESTAMPTZ,
    posted BOOLEAN,
    contact_way TEXT,
    contact_person_key UUID,
    contact_repr TEXT,
    author_key UUID,
    importance TEXT,
    incoming BOOLEAN,
    description TEXT,
    responsible_key UUID,
    reviewed BOOLEAN,
    review_after TIMESTAMPTZ,
    subject_key UUID,
    interaction_basis TEXT,
    topic TEXT,
    comment TEXT,
    client_key UUID,
    question TEXT,
    answer TEXT,
    call_end TIMESTAMPTZ,
    call_type TEXT,
    manager_key UUID,
    notify_on_save BOOLEAN,
    date_creation TIMESTAMPTZ,
    date_consultation TIMESTAMPTZ,
    is_temp BOOLEAN,
    lang_key UUID,
    question_category_key UUID,
    consultation_request_key UUID,
    contact_method TEXT,
    closed_without_consult BOOLEAN,
    sent_base BOOLEAN,
    time_spent TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    etl_run_id TEXT
);

-- Таблица консультаций (1:N)
CREATE TABLE IF NOT EXISTS raw.call_consultations (
    call_ref_key UUID NOT NULL,
    line_number TEXT NOT NULL,
    po_type_key UUID,
    po_section_key UUID,
    obstacle_key UUID,
    manager_help_key UUID,
    is_repeat BOOLEAN,
    question TEXT,
    answer TEXT,
    PRIMARY KEY (call_ref_key, line_number),
    FOREIGN KEY (call_ref_key) REFERENCES raw.calls(ref_key)
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_raw_calls_number ON raw.calls(number);
CREATE INDEX IF NOT EXISTS idx_raw_calls_date_creation ON raw.calls(date_creation);
CREATE INDEX IF NOT EXISTS idx_raw_calls_date_consultation ON raw.calls(date_consultation);
CREATE INDEX IF NOT EXISTS idx_raw_calls_call_end ON raw.calls(call_end);
CREATE INDEX IF NOT EXISTS idx_raw_calls_etl_run_id ON raw.calls(etl_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_call_consultations_call_ref ON raw.call_consultations(call_ref_key);
"""

# ---- SQL DML ----
UPSERT_CALLS_SQL = """
INSERT INTO raw.calls (
    ref_key, data_version, deletion_mark, number, date_document, posted,
    contact_way, contact_person_key, contact_repr, author_key, importance,
    incoming, description, responsible_key, reviewed, review_after,
    subject_key, interaction_basis, topic, comment, client_key,
    question, answer, call_end, call_type, manager_key,
    notify_on_save, date_creation, date_consultation, is_temp,
    lang_key, question_category_key, consultation_request_key,
    contact_method, closed_without_consult, sent_base, time_spent,
    loaded_at, updated_at, etl_run_id
) VALUES %s
ON CONFLICT (ref_key) DO UPDATE SET
    data_version = EXCLUDED.data_version,
    deletion_mark = EXCLUDED.deletion_mark,
    number = EXCLUDED.number,
    date_document = EXCLUDED.date_document,
    posted = EXCLUDED.posted,
    contact_way = EXCLUDED.contact_way,
    contact_person_key = EXCLUDED.contact_person_key,
    contact_repr = EXCLUDED.contact_repr,
    author_key = EXCLUDED.author_key,
    importance = EXCLUDED.importance,
    incoming = EXCLUDED.incoming,
    description = EXCLUDED.description,
    responsible_key = EXCLUDED.responsible_key,
    reviewed = EXCLUDED.reviewed,
    review_after = EXCLUDED.review_after,
    subject_key = EXCLUDED.subject_key,
    interaction_basis = EXCLUDED.interaction_basis,
    topic = EXCLUDED.topic,
    comment = EXCLUDED.comment,
    client_key = EXCLUDED.client_key,
    question = EXCLUDED.question,
    answer = EXCLUDED.answer,
    call_end = EXCLUDED.call_end,
    call_type = EXCLUDED.call_type,
    manager_key = EXCLUDED.manager_key,
    notify_on_save = EXCLUDED.notify_on_save,
    date_creation = EXCLUDED.date_creation,
    date_consultation = EXCLUDED.date_consultation,
    is_temp = EXCLUDED.is_temp,
    lang_key = EXCLUDED.lang_key,
    question_category_key = EXCLUDED.question_category_key,
    consultation_request_key = EXCLUDED.consultation_request_key,
    contact_method = EXCLUDED.contact_method,
    closed_without_consult = EXCLUDED.closed_without_consult,
    sent_base = EXCLUDED.sent_base,
    time_spent = EXCLUDED.time_spent,
    updated_at = EXCLUDED.updated_at,
    etl_run_id = EXCLUDED.etl_run_id;
"""

UPSERT_CONSULTATIONS_SQL = """
INSERT INTO raw.call_consultations (
    call_ref_key, line_number,
    po_type_key, po_section_key, obstacle_key, manager_help_key,
    is_repeat, question, answer
) VALUES %s
ON CONFLICT (call_ref_key, line_number) DO UPDATE SET
    po_type_key = EXCLUDED.po_type_key,
    po_section_key = EXCLUDED.po_section_key,
    obstacle_key = EXCLUDED.obstacle_key,
    manager_help_key = EXCLUDED.manager_help_key,
    is_repeat = EXCLUDED.is_repeat,
    question = EXCLUDED.question,
    answer = EXCLUDED.answer;
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

def clean_datetime(dt_str):
    """Преобразует Edm.DateTime в TIMESTAMPTZ, 0001-01-01 → None"""
    if not dt_str or dt_str.startswith("0001-01-01"):
        return None
    return dt_str  # psycopg2 конвертирует автоматически

def clean_uuid(val):
    """Преобразует строку GUID в UUID, None → None"""
    if not val or val == "00000000-0000-0000-0000-000000000000":
        return None
    return val  # psycopg2 принимает строку, но лучше UUID — но для скорости оставим str

def etl_calls():
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

    # Получить last state
    cur.execute("SELECT last_number, last_run_at FROM raw.etl_state WHERE entity_name = %s", (ENTITY,))
    state = cur.fetchone()
    if state:
        last_number, last_run = state
        logger.info("Incremental run: last_number=%s, last_run=%s", last_number, last_run)
        is_first_run = False
    else:
        last_number = None
        logger.info("First run — loading from %s", INITIAL_FROM_DATE)
        is_first_run = True

    now = datetime.now(timezone.utc)
    total_calls = 0
    total_consults = 0

    while True:
        # Формируем фильтр
        if is_first_run:
            filter_part = f"ДатаСоздания ge datetime'{INITIAL_FROM_DATE}T00:00:00'"
        else:
            # Буфер: -7 дней от последнего запуска, чтобы не пропустить
            buffer_date = (last_run - timedelta(days=7)).date().isoformat()
            filter_part = f"ДатаСоздания ge datetime'{buffer_date}T00:00:00'"

        if last_number:
            filter_part += f" and Number gt '{last_number}'"

        url = f"{ODATA_BASEURL}{ENTITY}?$format=json&$filter={filter_part}&$orderby=Number asc&$top={PAGE_SIZE}"

        try:
            resp = http_get_with_backoff(url, auth=(ODATA_USER, ODATA_PASSWORD), timeout=120)
        except Exception as e:
            logger.exception("Failed to fetch batch: %s", e)
            break

        batch = resp.json().get("value", [])
        if not batch:
            break

        # Подготавливаем данные
        call_values = []
        consult_values = []
        now_iso = now.isoformat()

        for item in batch:
            # Основные поля
            ref_key = item.get("Ref_Key")
            if not ref_key:
                continue

            call_values.append((
                ref_key,
                item.get("DataVersion"),
                item.get("DeletionMark"),
                item.get("Number"),
                clean_datetime(item.get("Date")),
                item.get("Posted"),
                item.get("АбонентКакСвязаться"),
                clean_uuid(item.get("АбонентКонтакт_Key")),
                item.get("АбонентПредставление"),
                clean_uuid(item.get("Автор_Key")),
                item.get("Важность"),
                item.get("Входящий"),
                item.get("Описание"),
                clean_uuid(item.get("Ответственный_Key")),
                item.get("Рассмотрено"),
                clean_datetime(item.get("РассмотретьПосле")),
                clean_uuid(item.get("Предмет")),
                item.get("ВзаимодействиеОснование"),
                item.get("Тема"),
                item.get("Комментарий"),
                clean_uuid(item.get("Абонент_Key")),
                item.get("Вопрос"),
                item.get("Ответ"),
                clean_datetime(item.get("Конец")),
                item.get("ВидОбращения"),
                clean_uuid(item.get("Менеджер_Key")),
                item.get("УведомлятьПриЗаписи"),
                clean_datetime(item.get("ДатаСоздания")),
                clean_datetime(item.get("ДатаКонсультации")),
                item.get("ВременныйРеквизит"),
                clean_uuid(item.get("Язык_Key")),
                clean_uuid(item.get("КатегорияВопроса_Key")),
                clean_uuid(item.get("ВопросНаКонсультацию_Key")),
                item.get("СпособСвязи"),
                item.get("ЗакрытоБезКонсультации"),
                item.get("ОтправленаБаза"),
                clean_datetime(item.get("ЗатраченноеВремя")),
                now_iso,
                now_iso,
                ETL_RUN_ID
            ))

            # КонсультацииИТС
            for consult in item.get("КонсультацииИТС", []):
                consult_values.append((
                    ref_key,
                    consult.get("LineNumber"),
                    clean_uuid(consult.get("ВидПО_Key")),
                    clean_uuid(consult.get("РазделПО_Key")),
                    clean_uuid(consult.get("НаличиеПомех_Key")),
                    clean_uuid(consult.get("ПомощьМенеджера_Key")),
                    consult.get("ПовторноеОбращение"),
                    consult.get("Вопрос"),
                    consult.get("Ответ")
                ))

        # Вставка
        if call_values:
            execute_values(cur, UPSERT_CALLS_SQL, call_values, page_size=1000)
            total_calls += len(call_values)

        if consult_values:
            execute_values(cur, UPSERT_CONSULTATIONS_SQL, consult_values, page_size=1000)
            total_consults += len(consult_values)

        conn.commit()
        last_number = batch[-1].get("Number")
        logger.info("Upserted: %s calls, %s consultations (last Number=%s)", len(call_values), len(consult_values), last_number)

        # Сохраняем прогресс каждые 50 тыс.
        if total_calls % 50000 == 0:
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

        # Выход, если меньше PAGE_SIZE
        if len(batch) < PAGE_SIZE:
            break

    # Финальное сохранение состояния
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
    logger.info("ETL finished. Total: %s calls, %s consultations", total_calls, total_consults)

if __name__ == "__main__":
    try:
        etl_calls()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(2)