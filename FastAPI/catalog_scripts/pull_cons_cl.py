#!/usr/bin/env python3
"""
Скрипт для загрузки консультаций из 1C:ЦЛ через OData.

Загружает Document_ТелефонныйЗвонок с пагинацией и обновляет:
- cons.cons (консультации)
- cons.q_and_a (вопросы и ответы)

Использует инкрементальную загрузку по полю ДатаИзменения.
Это позволяет эффективно загружать только измененные документы,
а не все новые и будущие консультации.
"""
import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from urllib.parse import quote, quote_plus
import requests
import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from FastAPI.config import settings
from FastAPI.models import Consultation, QAndA, Client
from FastAPI.services.chatwoot_client import ChatwootClient
from FastAPI.utils.etl_logging import ETLLogger

# Конфигурация
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
PAGE_SIZE = int(os.getenv("ODATA_PAGE_SIZE", "1000"))
INITIAL_FROM_DATE = os.getenv("ETL_INITIAL_FROM_DATE", "2025-01-01")
MAX_ERROR_LOGS = int(os.getenv("ETL_MAX_ERROR_LOGS", "10"))  # Максимум ошибок для логирования
INCREMENTAL_BUFFER_DAYS = int(os.getenv("ETL_CONS_INCREMENTAL_BUFFER_DAYS", "7"))  # Буфер для инкремента
REF_KEY_BATCH_SIZE = int(os.getenv("ETL_CONS_REF_KEY_BATCH_SIZE", "50"))  # Размер батча для запросов по Ref_Key
# Ограничиваем длину URL: при слишком большом батче IIS/1C возвращает 404. Значение 40 ~ 1.6KB фильтр.
MAX_REF_KEYS_PER_REQUEST = int(os.getenv("ETL_CONS_MAX_KEYS_PER_REQUEST", "40"))

# Режим работы ETL: "incremental" (по умолчанию) или "open_update"
ETL_MODE = os.getenv("ETL_CONS_MODE", "incremental")

ENTITY = "Document_ТелефонныйЗвонок"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("pull_cons_cl")

# OData настройки (используем отдельный URL для ЦЛ)
ODATA_BASEURL = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
ODATA_USER = settings.ODATA_USER
ODATA_PASSWORD = settings.ODATA_PASSWORD

# Подключение к БД
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


def map_status(
    vid_obrascheniya: Optional[str], 
    end_date: Optional[datetime] = None,
    denied: bool = False
) -> str:
    """
    Маппинг ВидОбращения в статус с учетом поля Конец и ЗакрытоБезКонсультации.
    
    Логика:
    - ВАЖНО: Если ЗакрытоБезКонсультации = True → статус "cancelled" (отменено)
      независимо от других полей (даже если Конец заполнен)
    - Если заполнено поле Конец (end_date) и НЕ закрыто без консультации → статус "closed" (завершено/закрыто)
    - Если Конец не заполнено:
      - "КонсультацияИТС" → "open" (в работе)
      - "ВОчередьНаКонсультацию" → "pending" (в очереди)
      - "Другое" → "other"
    """
    # ВАЖНО: Если закрыто без консультации, статус всегда "cancelled"
    if denied:
        return "cancelled"
    
    # Если заполнено поле Конец, консультация завершена
    if end_date:
        return "closed"
    
    # Иначе определяем статус по ВидОбращения
    if not vid_obrascheniya:
        return "new"
    
    vid = vid_obrascheniya.strip()
    if vid == "КонсультацияИТС":
        return "open"  # В работе
    elif vid == "ВОчередьНаКонсультацию":
        return "pending"  # В очереди
    elif vid == "Другое":
        return "other"  # Другое
    return "new"


def map_consultation_type(vid_obrascheniya: Optional[str]) -> str:
    """
    Маппинг ВидОбращения в consultation_type.
    
    ВАЖНО: Все консультации из ЦЛ имеют тип "Консультация по ведению учёта",
    так как в ЦЛ хранятся только такие консультации.
    "Техническая поддержка" не отправляется в ЦЛ, только в Chatwoot.
    """
    # Все консультации из ЦЛ - это "Консультация по ведению учёта"
    return "Консультация по ведению учёта"


def clean_uuid(val: Optional[str]) -> Optional[str]:
    """Очистка UUID"""
    if not val or val == "00000000-0000-0000-0000-000000000000":
        return None
    return val


def clean_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Преобразует Edm.DateTime в datetime, 0001-01-01 → None. Всегда возвращает offset-aware datetime (UTC)."""
    if not dt_str or dt_str.startswith("0001-01-01"):
        return None
    try:
        # OData формат: "2025-10-20T09:28:15" или "2025-10-20T09:28:15Z"
        dt_str_normalized = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(dt_str_normalized)
        # ВАЖНО: Убеждаемся, что datetime всегда offset-aware (UTC)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        return None


def is_valid_chatwoot_conversation_id(cons_id: Optional[str]) -> bool:
    """
    Проверяет, является ли cons_id валидным числовым ID Chatwoot.
    
    Chatwoot использует числовые ID для conversations (например, 12345),
    а не UUID. Если cons_id это UUID или временный ID (temp_, cl_), 
    то это невалидный ID для запросов к Chatwoot.
    
    Args:
        cons_id: ID консультации из БД
        
    Returns:
        True если cons_id валидный числовой ID Chatwoot, False иначе
    """
    if not cons_id:
        return False
    
    # Пропускаем временные ID
    if cons_id.startswith(("temp_", "cl_")):
        return False
    
    # Проверяем, что это числовой ID (не UUID)
    # UUID имеет формат: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 символов с дефисами)
    # Числовой ID: только цифры
    if len(cons_id) > 10:  # UUID обычно длиннее 10 символов
        # Проверяем, не UUID ли это (содержит дефисы)
        if '-' in cons_id:
            return False
    
    # Проверяем, что это число (или строка из цифр)
    return cons_id.isdigit()


def http_get_with_backoff(url: str, auth: tuple, max_retries: int = 6, timeout: int = 120):
    """HTTP GET с retry и backoff. Детальное логирование только для ошибок."""
    headers = {
        "User-Agent": "ETL-Consultations/1.0",
        "Accept": "application/json",
    }
    s = requests.Session()
    attempt = 0
    
    while True:
        try:
            r = s.get(url, auth=auth, headers=headers, timeout=timeout)
            
            # Ошибки 4xx (кроме 429) - это клиентские ошибки, не ретраим
            if 400 <= r.status_code < 500 and r.status_code != 429:
                logger.error("✗ HTTP %s Client Error (no retry): %s", r.status_code, r.reason)
                logger.error("  URL: %s", url[:500])
                try:
                    error_body = r.text[:1000]
                    logger.error("  Error response: %s", error_body)
                except:
                    pass
                r.raise_for_status()
            
            # Ошибки 5xx и 429 - ретраим
            if r.status_code in (429, 502, 503, 504):
                if attempt >= max_retries:
                    r.raise_for_status()
                wait = min(2 ** attempt, 60)
                logger.warning("⚠ HTTP %s — retry in %s sec (attempt %s/%s)", r.status_code, wait, attempt+1, max_retries+1)
                import time
                time.sleep(wait)
                attempt += 1
                continue
            
            r.raise_for_status()
            return r
        except requests.RequestException as ex:
            # Для 4xx ошибок не ретраим - логируем детально
            if hasattr(ex, 'response') and ex.response is not None and 400 <= ex.response.status_code < 500:
                logger.error("✗ HTTP %s Client Error: %s", ex.response.status_code, ex)
                logger.error("  URL: %s", url[:500])
                if hasattr(ex.response, 'text'):
                    logger.error("  Response: %s", ex.response.text[:500])
                raise
            if attempt >= max_retries:
                logger.error("✗ HTTP error after %s attempts: %s", attempt+1, ex)
                logger.error("  URL: %s", url[:500])
                raise
            wait = min(2 ** attempt, 60)
            logger.debug("Request failed (attempt %s/%s): %s — retry in %s sec", attempt+1, max_retries+1, ex, wait)
            import time
            time.sleep(wait)
            attempt += 1


async def get_last_sync_date(db: AsyncSession) -> Optional[datetime]:
    """Получить дату последней синхронизации. Всегда возвращает offset-aware datetime (UTC)."""
    result = await db.execute(
        text("SELECT last_synced_at FROM sys.sync_state WHERE entity_name = :entity"),
        {"entity": ENTITY}
    )
    row = result.first()
    if row and row[0]:
        dt = row[0]
        # ВАЖНО: Нормализуем к UTC если datetime offset-naive
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


async def save_sync_date(db: AsyncSession, sync_date: datetime):
    """Сохранить дату синхронизации"""
    await db.execute(
        text("""
            INSERT INTO sys.sync_state (entity_name, last_synced_at)
            VALUES (:entity, :date)
            ON CONFLICT (entity_name) DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at
        """),
        {"entity": ENTITY, "date": sync_date}
    )


async def find_client_by_key(db: AsyncSession, client_key: Optional[str]) -> Optional[str]:
    """Найти client_id по client_key из ЦЛ"""
    if not client_key:
        return None
    # Пока просто возвращаем ключ, в будущем можно добавить маппинг через отдельную таблицу
    # или искать по cl_ref_key в clients
    result = await db.execute(
        select(Client.client_id).where(Client.cl_ref_key == client_key).limit(1)
    )
    row = result.first()
    return str(row[0]) if row else None


async def get_client_org_inn(db: AsyncSession, client_id: Optional[str]) -> Optional[str]:
    """Получить org_inn клиента по client_id"""
    if not client_id:
        return None
    try:
        result = await db.execute(
            select(Client.org_inn).where(Client.client_id == client_id).limit(1)
        )
        row = result.first()
        return row[0] if row and row[0] else None
    except Exception:
        return None


async def get_con_blocks_from_q_and_a(db: AsyncSession, cons_ref_key: str) -> Optional[str]:
    """Получить con_blocks из q_and_a (берем первый непустой con_blocks_key)"""
    try:
        result = await db.execute(
            text("SELECT con_blocks_key FROM cons.q_and_a WHERE cons_ref_key = :ref_key AND con_blocks_key IS NOT NULL LIMIT 1"),
            {"ref_key": cons_ref_key}
        )
        row = result.first()
        return row[0] if row and row[0] else None
    except Exception:
        return None


async def get_con_calls_aggregated(db: AsyncSession, cons_ref_key: str) -> Optional[Dict[str, Any]]:
    """Агрегировать дозвоны из cons.calls в JSON"""
    try:
        result = await db.execute(
            text("""
                SELECT json_agg(
                    json_build_object(
                        'period', period,
                        'manager', manager
                    )
                    ORDER BY period
                )
                FROM cons.calls
                WHERE cons_key = :cons_key
            """),
            {"cons_key": cons_ref_key}
        )
        row = result.first()
        calls_data = row[0] if row and row[0] else None
        return calls_data if calls_data != [None] else None
    except Exception:
        return None




async def process_consultation_item(
    db: AsyncSession,
    item: Dict[str, Any],
    chatwoot_cons_id: Optional[str] = None,
):
    """Обработать один документ консультации"""
    ref_key = item.get("Ref_Key")
    if not ref_key:
        # Не логируем каждую пропущенную запись - это создает шум
        return None
    
    # Маппинг основных полей
    number = item.get("Number")
    client_key = clean_uuid(item.get("Абонент_Key"))
    
    # Фильтрация по Parent_Key теперь происходит на уровне OData запроса
    # Загружаем все консультации, которые пришли из OData (они уже отфильтрованы)
    # Это позволяет загружать консультации даже если клиент еще не создан в БД
    
    client_id = await find_client_by_key(db, client_key)
    # Получаем org_inn из клиента
    org_inn = await get_client_org_inn(db, client_id) if client_id else None
    
    # Не логируем каждое отсутствие клиента - это создает шум
    # Логируем только если это критично для бизнес-логики
    
    manager_key = clean_uuid(item.get("Менеджер_Key"))
    author_key = clean_uuid(item.get("Автор_Key"))
    
    # Даты (нужны для определения статуса)
    create_date = clean_datetime(item.get("ДатаСоздания"))
    start_date = clean_datetime(item.get("ДатаКонсультации"))
    end_date = clean_datetime(item.get("Конец"))
    # ВАЖНО: ДатаИзменения используется для инкрементального обновления
    change_date = clean_datetime(item.get("ДатаИзменения"))
    
    # Закрыто без консультации (ВАЖНО: проверяем ДО определения статуса)
    denied = bool(item.get("ЗакрытоБезКонсультации", False))
    
    # Статус из ВидОбращения с учетом поля Конец и ЗакрытоБезКонсультации
    vid_obrascheniya = item.get("ВидОбращения")
    status = map_status(vid_obrascheniya, end_date, denied)
    consultation_type = map_consultation_type(vid_obrascheniya)
    
    # Другие поля
    comment = item.get("Описание") or item.get("Вопрос") or ""
    topic = item.get("Тема")
    online_question_cat = clean_uuid(item.get("КатегорияВопроса_Key"))
    online_question = clean_uuid(item.get("ВопросНаКонсультацию_Key"))
    
    # Найти или создать консультацию
    # Ищем по cl_ref_key (Ref_Key из ЦЛ)
    result = await db.execute(
        select(Consultation).where(Consultation.cl_ref_key == ref_key)
    )
    consultation = result.scalar_one_or_none()
    
    # Если нет, ищем по cons_id (если есть chatwoot_cons_id)
    if not consultation and chatwoot_cons_id:
        result = await db.execute(
            select(Consultation).where(Consultation.cons_id == chatwoot_cons_id)
        )
        consultation = result.scalar_one_or_none()
    
    # Если нет, создаем новую (cons_id будет временный, обновится при синхронизации с Chatwoot)
    if not consultation:
        # Убрали избыточное логирование каждого создания - это логируется на уровне батча
        consultation = Consultation(
            cons_id=f"cl_{ref_key}",  # временный ID до синхронизации с Chatwoot
            cl_ref_key=ref_key,
            client_key=client_key,
            client_id=client_id,
            number=number,
            status=status,
            org_inn=org_inn,
            consultation_type=consultation_type,
            denied=denied,
            create_date=create_date or datetime.now(timezone.utc),
            start_date=start_date,
            end_date=end_date,
            comment=comment,
            manager=str(manager_key) if manager_key else None,
            author=str(author_key) if author_key else None,
            online_question_cat=str(online_question_cat) if online_question_cat else None,
            online_question=str(online_question) if online_question else None,
            source="1C_CL",  # Указываем источник создания
        )
        db.add(consultation)
    else:
        # Обновляем существующую только если поля изменились
        # Это оптимизация для уменьшения количества обновлений в БД
        has_changes = False
        old_manager = consultation.manager
        
        # Сравниваем и обновляем только измененные поля
        if consultation.number != number:
            consultation.number = number
            has_changes = True
        
        old_status = consultation.status
        # ═══════════════════════════════════════════════════════════════════════
        # GUARD CLAUSE: Терминальные статусы НЕ МЕНЯЕМ из ЦЛ
        # ═══════════════════════════════════════════════════════════════════════
        terminal_statuses = {"closed", "resolved", "cancelled"}
        
        # Если консультация уже в терминальном статусе, не меняем его
        if old_status in terminal_statuses:
            logger.debug(
                f"Status update skipped for consultation {ref_key}: "
                f"current status '{old_status}' is terminal, not updating to '{status}' from ЦЛ"
            )
            # Пропускаем обновление статуса, но продолжаем обновлять другие поля
        elif consultation.status != status:
            consultation.status = status
            has_changes = True
            
            # ВАЖНО: Синхронизируем статус с Chatwoot при изменении
            # Если статус изменился и есть валидный cons_id, обновляем в Chatwoot
            if is_valid_chatwoot_conversation_id(consultation.cons_id):
                try:
                    chatwoot_client = ChatwootClient()
                    # Если статус "closed" - закрываем в Chatwoot с сообщением о длительности разговора
                    if status == "closed" and old_status != "closed":
                        # Вычисляем длительность разговора
                        duration_message = ""
                        if consultation.start_date and consultation.end_date:
                            duration = consultation.end_date - consultation.start_date
                            duration_minutes = int(duration.total_seconds() / 60)
                            if duration_minutes > 0:
                                duration_message = f" Заявка была закрыта менеджером. Разговор состоялся {duration_minutes} минут."
                            else:
                                duration_message = " Заявка была закрыта менеджером."
                        else:
                            duration_message = " Заявка была закрыта менеджером."
                        
                        # Закрываем беседу через toggle_status
                        await chatwoot_client.toggle_conversation_status(
                            conversation_id=consultation.cons_id,
                            status="resolved"
                        )
                        
                        # Отправляем сообщение о закрытии
                        if duration_message:
                            await chatwoot_client.send_message(
                                conversation_id=consultation.cons_id,
                                content=duration_message.strip(),
                                message_type="outgoing"
                            )
                        
                        logger.info(f"Automatically closed consultation {consultation.cons_id} in Chatwoot (closed in 1C:ЦЛ) with message")
                    # Если статус "open" (КонсультацияИТС с пустым Конец) - открываем в Chatwoot
                    elif status == "open" and old_status != "open":
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status="open"  # Открываем заявку в Chatwoot
                        )
                        logger.info(f"Automatically opened consultation {consultation.cons_id} in Chatwoot (opened in 1C:ЦЛ)")
                    # Если статус "pending" - устанавливаем pending в Chatwoot
                    elif status == "pending" and old_status != "pending":
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status="pending"  # Устанавливаем статус pending в Chatwoot
                        )
                        logger.info(f"Automatically set pending status for consultation {consultation.cons_id} in Chatwoot")
                except Exception as sync_error:
                    # ВАЖНО: Не прерываем обработку консультации из-за ошибок Chatwoot
                    # Логируем ошибку, но продолжаем обработку остальных полей
                    error_msg = str(sync_error)
                    if "ConnectError" in error_msg or "connection" in error_msg.lower():
                        logger.warning(
                            f"Chatwoot connection error while syncing consultation status {consultation.cons_id}: {sync_error}. "
                            f"Chatwoot may be unavailable. Continuing ETL process..."
                        )
                    else:
                        logger.warning(f"Failed to sync consultation status {consultation.cons_id} in Chatwoot: {sync_error}")
            elif consultation.cons_id:
                # Логируем, если cons_id есть, но невалидный (UUID или временный)
                logger.debug(
                    f"Skipping Chatwoot status sync for consultation {consultation.cl_ref_key}: "
                    f"cons_id={consultation.cons_id} is not a valid Chatwoot conversation ID (UUID or temporary)"
                )
        
        # Синхронизируем номер консультации и другие поля в Chatwoot custom_attributes
        if is_valid_chatwoot_conversation_id(consultation.cons_id):
            try:
                chatwoot_client = ChatwootClient()
                custom_attrs_to_update = {}
                
                # Номер консультации (используем правильный ключ number_con)
                if consultation.number:
                    custom_attrs_to_update["number_con"] = str(consultation.number)
                
                # Дата консультации (date_con) - всегда обновляем если есть
                if consultation.start_date:
                    # Проверяем тип: может быть datetime или date
                    dt = consultation.start_date
                    if isinstance(dt, datetime):
                        # Это datetime - обрабатываем timezone
                        if dt.tzinfo:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        custom_attrs_to_update["date_con"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    elif hasattr(dt, 'strftime'):
                        # Это date - просто форматируем
                        custom_attrs_to_update["date_con"] = dt.strftime("%Y-%m-%dT00:00:00")
                
                # Дата окончания (con_end из etl_cons_cl) - всегда обновляем если есть
                if consultation.end_date:
                    dt = consultation.end_date
                    if isinstance(dt, datetime):
                        if dt.tzinfo:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        custom_attrs_to_update["con_end"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    elif hasattr(dt, 'strftime'):
                        custom_attrs_to_update["con_end"] = dt.strftime("%Y-%m-%dT00:00:00")
                
                # Перенос (дата) - redate_con из etl_redate_cl
                # ВАЖНО: redate имеет тип Date (не DateTime), поэтому нет tzinfo
                if consultation.redate:
                    # redate - это date объект, не datetime
                    if isinstance(consultation.redate, datetime):
                        dt = consultation.redate
                        if dt.tzinfo:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        custom_attrs_to_update["redate_con"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    elif hasattr(consultation.redate, 'strftime'):
                        # Это date объект
                        custom_attrs_to_update["redate_con"] = consultation.redate.strftime("%Y-%m-%dT00:00:00")
                
                # Перенос (время) - retime_con из etl_redate_cl - всегда обновляем если есть
                if consultation.redate_time:
                    if hasattr(consultation.redate_time, 'strftime'):
                        custom_attrs_to_update["retime_con"] = consultation.redate_time.strftime("%H:%M")
                    else:
                        # Если это строка или другой формат
                        custom_attrs_to_update["retime_con"] = str(consultation.redate_time)
                
                # Вид обращения (consultation_type) - всегда обновляем если есть
                if consultation.consultation_type:
                    custom_attrs_to_update["consultation_type"] = str(consultation.consultation_type)
                
                # Закрыто без консультации - closed_without_con из etl_cons_cl
                if consultation.denied is not None:
                    custom_attrs_to_update["closed_without_con"] = bool(consultation.denied)
                
                # Обновляем только если есть изменения
                if custom_attrs_to_update:
                    try:
                        await chatwoot_client.update_conversation_custom_attributes(
                            conversation_id=consultation.cons_id,
                            custom_attributes=custom_attrs_to_update
                        )
                        logger.info(f"Synced consultation fields to Chatwoot for {consultation.cons_id}: {list(custom_attrs_to_update.keys())}")
                    except httpx.HTTPStatusError as http_error:
                        # Обработка ошибок HTTP (404, 403 и т.д.)
                        if http_error.response.status_code == 404:
                            # Conversation не найдена в Chatwoot (возможно, была удалена)
                            logger.warning(
                                f"Conversation {consultation.cons_id} not found in Chatwoot (404). "
                                f"Possibly deleted. Skipping custom_attributes update."
                            )
                        else:
                            # Другие HTTP ошибки
                            logger.warning(
                                f"HTTP error {http_error.response.status_code} while updating custom_attributes "
                                f"for conversation {consultation.cons_id}: {http_error}"
                            )
                    except Exception as update_error:
                        logger.warning(
                            f"Failed to update custom_attributes for conversation {consultation.cons_id}: {update_error}",
                            exc_info=True
                        )
            except Exception as sync_error:
                # ВАЖНО: Не прерываем обработку консультации из-за ошибок Chatwoot
                # Логируем ошибку, но продолжаем обработку остальных полей
                error_msg = str(sync_error)
                if "ConnectError" in error_msg or "connection" in error_msg.lower():
                    logger.warning(
                        f"Chatwoot connection error while syncing consultation fields {consultation.cons_id}: {sync_error}. "
                        f"Chatwoot may be unavailable. Continuing ETL process..."
                    )
                else:
                    logger.warning(f"Failed to sync consultation fields to Chatwoot {consultation.cons_id}: {sync_error}", exc_info=True)
        elif consultation.cons_id:
            # Логируем, если cons_id есть, но невалидный (UUID или временный)
            logger.debug(
                f"Skipping Chatwoot custom_attributes sync for consultation {consultation.cl_ref_key}: "
                f"cons_id={consultation.cons_id} is not a valid Chatwoot conversation ID (UUID or temporary)"
            )
        
        if client_key and consultation.client_key != client_key:
            consultation.client_key = client_key
            has_changes = True
        
        if client_id and consultation.client_id != client_id:
            consultation.client_id = client_id
            has_changes = True
        
        # Обновляем org_inn если его нет или если клиент изменился
        if org_inn and (not consultation.org_inn or consultation.client_id != client_id):
            if consultation.org_inn != org_inn:
                consultation.org_inn = org_inn
                has_changes = True
        
        if consultation.consultation_type != consultation_type:
            consultation.consultation_type = consultation_type
            has_changes = True
        
        if consultation.denied != denied:
            consultation.denied = denied
            has_changes = True
        
        if consultation.start_date != start_date:
            consultation.start_date = start_date
            has_changes = True
        
        if consultation.end_date != end_date:
            consultation.end_date = end_date
            has_changes = True
        
        if comment and consultation.comment != comment:
            consultation.comment = comment
            has_changes = True
        
        new_manager = str(manager_key) if manager_key else None
        # ВАЖНО: Сохраняем старое значение менеджера для проверки изменений
        # (old_manager уже сохранен выше, но нужно убедиться что сравнение правильное)
        manager_changed = False
        if consultation.manager != new_manager:
            # Если new_manager None, но consultation.manager не None, это тоже изменение
            if new_manager is None and consultation.manager is not None:
                manager_changed = True
            elif new_manager is not None and consultation.manager != new_manager:
                manager_changed = True
            
            consultation.manager = new_manager or consultation.manager
            has_changes = True
        
        if author_key:
            author_str = str(author_key)
            if consultation.author != author_str:
                consultation.author = author_str
                has_changes = True
        
        if online_question_cat:
            question_cat_str = str(online_question_cat)
            if consultation.online_question_cat != question_cat_str:
                consultation.online_question_cat = question_cat_str
                has_changes = True
        
        if online_question:
            question_str = str(online_question)
            if consultation.online_question != question_str:
                consultation.online_question = question_str
                has_changes = True
        
        # Обновляем source если не установлен
        if not consultation.source:
            consultation.source = "ETL"
            has_changes = True
        
        # Если изменений нет, пропускаем обновление
        if not has_changes:
            return
        
        # Если менеджер изменился, отправляем уведомление
        # ВАЖНО: Проверяем изменение менеджера ДО обновления consultation.manager
        # Используем manager_changed флаг и проверяем что новый менеджер не None
        if manager_changed and consultation.manager:
            try:
                from ..services.manager_notifications import (
                    send_manager_reassignment_notification,
                    send_queue_update_notification
                )
                await send_manager_reassignment_notification(
                    db=db,
                    consultation=consultation,
                    old_manager_key=old_manager,
                    new_manager_key=consultation.manager,
                    reason="Переназначено в ЦЛ"
                )
                
                # Отправляем информацию об изменении очереди
                await send_queue_update_notification(
                    db=db,
                    consultation=consultation,
                    manager_key=consultation.manager,
                )
            except Exception as e:
                logger.warning(f"Failed to send manager reassignment notification: {e}")
    
    await db.flush()
    
    # Обработка КонсультацииИТС и ВопросыИОтветы → q_and_a
    # Удаляем старые записи для этой консультации
    await db.execute(
        text("DELETE FROM cons.q_and_a WHERE cons_ref_key = :ref_key"),
        {"ref_key": ref_key}
    )
    
    # Добавляем КонсультацииИТС
    for idx, consult in enumerate(item.get("КонсультацииИТС", []), 1):
        qa = QAndA(
            cons_ref_key=ref_key,
            cons_id=consultation.cons_id,
            line_number=int(consult.get("LineNumber", idx)),
            po_type_key=str(clean_uuid(consult.get("ВидПО_Key"))) if clean_uuid(consult.get("ВидПО_Key")) else None,
            po_section_key=str(clean_uuid(consult.get("РазделПО_Key"))) if clean_uuid(consult.get("РазделПО_Key")) else None,
            con_blocks_key=str(clean_uuid(consult.get("НаличиеПомех_Key"))) if clean_uuid(consult.get("НаличиеПомех_Key")) else None,
            manager_help_key=str(clean_uuid(consult.get("ПомощьМенеджера_Key"))) if clean_uuid(consult.get("ПомощьМенеджера_Key")) else None,
            is_repeat=consult.get("ПовторноеОбращение", False),
            question=consult.get("Вопрос"),
            answer=consult.get("Ответ"),
        )
        db.add(qa)
    
    # Добавляем ВопросыИОтветы
    for idx, qa_item in enumerate(item.get("ВопросыИОтветы", []), 1000):  # Начинаем с 1000 чтобы не пересекаться
        qa = QAndA(
            cons_ref_key=ref_key,
            cons_id=consultation.cons_id,
            line_number=int(qa_item.get("LineNumber", idx)),
            question=qa_item.get("Вопрос"),
            answer=qa_item.get("Ответ"),
        )
        db.add(qa)
    
    # Обновляем con_blocks из q_and_a
    con_blocks = await get_con_blocks_from_q_and_a(db, ref_key)
    if con_blocks:
        consultation.con_blocks = con_blocks
    
    # Обновляем con_calls (агрегация из cons.calls)
    con_calls = await get_con_calls_aggregated(db, ref_key)
    if con_calls:
        consultation.con_calls = con_calls
    
    await db.flush()

    # ВАЖНО: Возвращаем ДатаИзменения для отслеживания последней синхронизации
    # Это позволяет эффективно отслеживать инкрементальные обновления
    # Если ДатаИзменения отсутствует, используем create_date или start_date как fallback
    result_date = change_date or create_date or start_date or datetime.now(timezone.utc)
    if result_date.tzinfo is None:
        result_date = result_date.replace(tzinfo=timezone.utc)
    return result_date


async def pull_open_consultations_by_ref_key(db: AsyncSession, auth: tuple):
    """
    Обновление открытых консультаций по Ref_Key из БД.
    
    Получает список всех открытых консультаций из БД и обновляет их через OData запросы.
    Это позволяет узнавать о закрытии старых открытых заявок.
    """
    etl_logger = ETLLogger("pull_cons_cl_open_update", ENTITY)
    
    # Получаем список открытых консультаций из БД
    result = await db.execute(
        text("""
            SELECT DISTINCT cl_ref_key 
            FROM cons.cons 
            WHERE cl_ref_key IS NOT NULL 
            AND cl_ref_key != ''
            AND status NOT IN ('closed', 'resolved', 'cancelled')
            ORDER BY cl_ref_key
        """)
    )
    open_ref_keys = [row[0] for row in result.fetchall()]
    
    if not open_ref_keys:
        logger.info("No open consultations found in database")
        return
    
    logger.info(f"Found {len(open_ref_keys)} open consultations to update")
    etl_logger.start({
        "mode": "open_update",
        "open_consultations_count": len(open_ref_keys),
        "batch_size": REF_KEY_BATCH_SIZE
    })
    
    # Батчим запросы по Ref_Key
    total_updated = 0
    total_created = 0
    total_errors = 0
    
    # Берем минимальный батч между настроечным и безопасным порогом по длине URL
    effective_batch_size = max(1, min(REF_KEY_BATCH_SIZE, MAX_REF_KEYS_PER_REQUEST))
    for batch_start in range(0, len(open_ref_keys), effective_batch_size):
        batch_ref_keys = open_ref_keys[batch_start:batch_start + effective_batch_size]
        batch_num = batch_start // effective_batch_size + 1
        
        # Формируем фильтр по Ref_Key: Ref_Key eq 'guid1' or Ref_Key eq 'guid2' or ...
        ref_key_filters = [f"Ref_Key eq guid'{key}'" for key in batch_ref_keys]
        filter_part = " or ".join(ref_key_filters)
        
        encoded_filter = quote(filter_part, safe="'()=<>", encoding='utf-8')
        
        url = (
            f"{ODATA_BASEURL}{ENTITY}?$format=json"
            f"&$filter={encoded_filter}"
            f"&$top={PAGE_SIZE}"
        )
        
        try:
            resp = http_get_with_backoff(url, auth, timeout=120)
            response_data = resp.json()
            batch = response_data.get("value", [])
            
            logger.info(f"Batch {batch_num}: fetched {len(batch)} consultations from OData")
            
            # Собираем Ref_Key из ответа
            returned_ref_keys = set()
            
            for item in batch:
                try:
                    ref_key = item.get("Ref_Key")
                    if not ref_key:
                        continue
                    
                    returned_ref_keys.add(ref_key)
                    
                    # Проверяем существование перед обработкой
                    existing_check = await db.execute(
                        select(Consultation).where(Consultation.cl_ref_key == ref_key).limit(1)
                    )
                    was_existing = existing_check.scalar_one_or_none() is not None
                    
                    await process_consultation_item(db, item)
                    
                    if was_existing:
                        total_updated += 1
                    else:
                        total_created += 1
                        
                except Exception as e:
                    total_errors += 1
                    logger.warning(f"Error processing consultation {item.get('Ref_Key', 'N/A')}: {e}")
                    continue
            
            # Проверяем консультации, которые были в БД, но не вернулись в ответе от ЦЛ
            # Это означает, что они были удалены в ЦЛ
            missing_ref_keys = set(batch_ref_keys) - returned_ref_keys
            if missing_ref_keys:
                logger.info(f"Batch {batch_num}: found {len(missing_ref_keys)} consultations missing from ЦЛ response (possibly deleted)")
                for ref_key in missing_ref_keys:
                    try:
                        # Находим консультацию в БД
                        result = await db.execute(
                            select(Consultation).where(Consultation.cl_ref_key == ref_key).limit(1)
                        )
                        consultation = result.scalar_one_or_none()
                        
                        if consultation and is_valid_chatwoot_conversation_id(consultation.cons_id):
                            # Обновляем статус на cancelled (удалено в ЦЛ)
                            old_status = consultation.status
                            if old_status not in ("closed", "resolved", "cancelled"):
                                consultation.status = "cancelled"
                                
                                # Закрываем в Chatwoot
                                try:
                                    chatwoot_client = ChatwootClient()
                                    await chatwoot_client.toggle_conversation_status(
                                        conversation_id=consultation.cons_id,
                                        status="resolved"
                                    )
                                    await chatwoot_client.send_message(
                                        conversation_id=consultation.cons_id,
                                        content="Заявка была удалена в системе.",
                                        message_type="outgoing"
                                    )
                                    logger.info(f"Marked consultation {ref_key} as cancelled and closed in Chatwoot (deleted in ЦЛ)")
                                except Exception as sync_error:
                                    logger.warning(f"Failed to close consultation {ref_key} in Chatwoot: {sync_error}")
                    except Exception as e:
                        logger.warning(f"Error processing deleted consultation {ref_key}: {e}")
            
            await db.commit()
            logger.info(f"Batch {batch_num}: updated {total_updated}, created {total_created}, errors {total_errors}")
            
        except Exception as e:
            logger.error(f"Error fetching batch {batch_num}: {e}")
            await db.rollback()
            total_errors += len(batch_ref_keys)
            continue
    
    etl_logger.finish(success=True)
    logger.info(f"Open consultations update completed: updated={total_updated}, created={total_created}, errors={total_errors}")


async def pull_consultations_incremental(db: AsyncSession, auth: tuple):
    """
    Инкрементальная загрузка консультаций по дате изменения.
    
    Использует новое поле ДатаИзменения из OData для эффективного инкрементального обновления.
    Загружает только те документы, которые были изменены с момента последней синхронизации.
    """
    etl_logger = ETLLogger("pull_cons_cl_incremental", ENTITY)
    
    # Получаем дату последней синхронизации
    last_sync = await get_last_sync_date(db)
    
    # ВАЖНО: Получаем текущую дату/время для ограничения last_sync
    # Это предотвращает проблемы с будущими датами в last_sync
    current_time = datetime.now(timezone.utc)
    
    # ВАЖНО: Если last_sync в будущем (из-за запланированных консультаций),
    # используем текущую дату вместо неё
    effective_last_sync: Optional[datetime] = None
    if last_sync:
        if last_sync > current_time:
            logger.warning(
                f"last_sync ({last_sync}) is in the future, using current time ({current_time}) instead. "
                f"This can happen if there are scheduled consultations with future ДатаИзменения."
            )
            effective_last_sync = current_time
        else:
            effective_last_sync = last_sync
    
    if effective_last_sync:
        # Инкрементальная загрузка с буфером (для надежности, на случай пропущенных изменений)
        from_dt = effective_last_sync - timedelta(days=INCREMENTAL_BUFFER_DAYS)
        from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(f"Incremental sync from {from_date} (effective last sync: {effective_last_sync}, buffer: {INCREMENTAL_BUFFER_DAYS} days)")
    else:
        from_date = f"{INITIAL_FROM_DATE}T00:00:00"
        logger.info(f"First run — loading from {from_date}")
    
    etl_logger.start({
        "mode": "incremental",
        "from_date": from_date,
        "buffer_days": INCREMENTAL_BUFFER_DAYS,
        "PAGE_SIZE": PAGE_SIZE,
        "field": "ДатаИзменения"
    })
    
    skip = 0
    last_processed_at: Optional[datetime] = None
    if effective_last_sync:
        # Используем effective_last_sync (уже ограниченный current_time выше)
        if effective_last_sync.tzinfo is None:
            last_processed_at = effective_last_sync.replace(tzinfo=timezone.utc)
        else:
            last_processed_at = effective_last_sync
    
    error_logs = 0
    
    while True:
        # Используем новое поле ДатаИзменения для инкрементального обновления
        # Это позволяет загружать только измененные документы, что более эффективно
        filter_part = f"ДатаИзменения ge datetime'{from_date}'"
        
        # ВАЖНО: OData требует правильного кодирования кириллицы
        encoded_filter = quote(filter_part, safe="'()=<>", encoding='utf-8')
        encoded_orderby = quote("ДатаИзменения asc", safe=",", encoding='utf-8')
        
        url = (
            f"{ODATA_BASEURL}{ENTITY}?$format=json"
            f"&$filter={encoded_filter}"
            f"&$orderby={encoded_orderby}"
            f"&$top={PAGE_SIZE}&$skip={skip}"
        )
        
        batch_num = skip // PAGE_SIZE + 1
        etl_logger.batch_start(batch_num, skip, PAGE_SIZE)
        
        try:
            resp = http_get_with_backoff(url, auth, timeout=120)
        except requests.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 400:
                etl_logger.critical_error("400 Bad Request - stopping execution. Check OData filter syntax.", e)
                sys.exit(1)
            etl_logger.batch_error(batch_num, e, skip)
            break
        except Exception as e:
            etl_logger.batch_error(batch_num, e, skip)
            break
        
        # Парсим JSON ответ
        try:
            response_data = resp.json()
            batch = response_data.get("value", [])
        except Exception as json_error:
            etl_logger.batch_error(batch_num, json_error, skip)
            logger.error(f"[pull_cons_cl] Response text (first 500 chars): {resp.text[:500]}")
            break
        
        if len(batch) == 0:
            if "error" in response_data:
                etl_logger.batch_error(batch_num, Exception(f"OData error: {response_data.get('error')}"), skip)
            break
        
        # Обрабатываем каждую консультацию
        batch_created = 0
        batch_updated = 0
        batch_errors = 0
        for idx, item in enumerate(batch):
            try:
                ref_key = item.get("Ref_Key")
                if not ref_key:
                    batch_errors += 1
                    continue
                
                # Проверяем существование перед обработкой
                existing_check = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == ref_key).limit(1)
                )
                was_existing = existing_check.scalar_one_or_none() is not None
                
                processed_at = await process_consultation_item(db, item)
                
                if was_existing:
                    batch_updated += 1
                else:
                    batch_created += 1
                
                if processed_at:
                    if processed_at.tzinfo is None:
                        processed_at = processed_at.replace(tzinfo=timezone.utc)
                    
                    # ВАЖНО: Ограничиваем last_processed_at текущей датой/временем
                    # Это предотвращает сдвиг last_sync в будущее из-за запланированных консультаций
                    # Если ДатаИзменения в будущем, мы обрабатываем запись, но не используем её дату для last_sync
                    if processed_at <= current_time:
                        if last_processed_at is None or processed_at > last_processed_at:
                            last_processed_at = processed_at
                    else:
                        # Если processed_at в будущем, логируем это для отладки
                        logger.debug(
                            f"Skipping future date for last_sync: {processed_at} (current: {current_time}) "
                            f"for consultation {item.get('Ref_Key', 'N/A')[:20]}"
                        )
                        # Если processed_at в будущем, но last_processed_at еще не установлен,
                        # устанавливаем его на текущее время (но не больше)
                        if last_processed_at is None:
                            last_processed_at = current_time
                        # Если last_processed_at уже установлен, но меньше current_time, обновляем до current_time
                        elif last_processed_at < current_time:
                            last_processed_at = current_time
                else:
                    batch_errors += 1
            except Exception as e:
                batch_errors += 1
                error_logs += 1
                if error_logs <= MAX_ERROR_LOGS:
                    etl_logger.item_error(item.get('Ref_Key', 'N/A'), e, "consultation", full_traceback=True)
                elif error_logs == MAX_ERROR_LOGS + 1:
                    logger.warning(f"[pull_cons_cl] Further processing errors suppressed (showing first {MAX_ERROR_LOGS} errors)")
                continue
        
        # Коммитим транзакцию
        try:
            await db.commit()
        except Exception as commit_error:
            etl_logger.batch_error(batch_num, commit_error, skip)
            await db.rollback()
            raise
        
        # Логируем прогресс батча
        etl_logger.batch_progress(batch_num, len(batch), batch_created, batch_updated, batch_errors)
        
        # ВАЖНО: Сохраняем sync_state после каждого батча для устойчивости при прерывании
        if last_processed_at:
            try:
                await save_sync_date(db, last_processed_at)
                await db.commit()
                etl_logger.sync_state_saved(last_processed_at, batch_num)
            except Exception as sync_error:
                logger.warning(f"[pull_cons_cl] Failed to save sync state after batch: {sync_error}")
        
        if len(batch) < PAGE_SIZE:
            break
        
        skip += PAGE_SIZE
    
    # Финальное сохранение даты синхронизации
    if last_processed_at:
        await save_sync_date(db, last_processed_at)
        try:
            await db.commit()
            etl_logger.sync_state_saved(last_processed_at)
        except Exception as commit_error:
            logger.error(f"[pull_cons_cl] Failed to save final sync date: {commit_error}", exc_info=True)
            await db.rollback()
    
    etl_logger.finish(success=True)


async def pull_consultations():
    """Основная функция загрузки консультаций"""
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
            if ETL_MODE == "open_update":
                # Режим обновления открытых консультаций
                await pull_open_consultations_by_ref_key(db, auth)
            else:
                # Режим инкрементальной загрузки (по умолчанию)
                await pull_consultations_incremental(db, auth)
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    # Создаем таблицу sync_state если её нет
    async def ensure_sync_state_table():
        engine = create_async_engine(
            DATABASE_URL,
            echo=False,
            pool_size=1,
            max_overflow=1,
            pool_pre_ping=True
        )
        async with engine.begin() as conn:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sys.sync_state (
                    entity_name TEXT PRIMARY KEY,
                    last_synced_at TIMESTAMPTZ
                )
            """))
        await engine.dispose()
    
    asyncio.run(ensure_sync_state_table())
    asyncio.run(pull_consultations())

