"""Роуты для создания консультаций и управления атрибутами (переносы, оценки)."""
import logging
import uuid
from datetime import datetime, timezone, timedelta, date, time
from typing import Optional, List, Dict, Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Header, Body, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select, func, cast, Date, case, or_, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.exc import IntegrityError

from ..database import get_db
from ..dependencies.security import verify_front_secret
from ..models import (
    Consultation,
    ConsRedate,
    ConsRatingAnswer,
    Client,
    OnlineQuestionCat,
    OnlineQuestion,
    Call,
    User,
    UserMapping,
    TelegramUser,
    QueueClosing,
)
from ..schemas.tickets import (
    ConsultationWithClient,
    ConsultationCreate,
    ConsultationResponse,
    ConsultationRead,
    ConsultationListResponse,
    ConsultationCreateSimple,  # Алиас для обратной совместимости
    ConsultationUpdate,
    parse_datetime_flexible,
)
from ..schemas.consultation_meta import (
    ConsultationRedateCreate,
    ConsultationRedateRead,
    ConsultationRatingRequest,
    ConsultationRatingResponse,
    ConsultationRatingAnswerPayload,
    CallRead,
)
from ..routers.clients import find_or_create_client
from ..services.chatwoot_client import ChatwootClient, is_valid_email
from ..services.onec_client import OneCClient, ConsultationLimitExceeded
from ..services.consultation_ratings import recalc_consultation_ratings
from ..services.manager_selector import ManagerSelector
from ..config import get_settings
from ..utils.idempotency import (
    check_idempotency_key,
    store_idempotency_key,
    generate_request_hash
)
from ..exceptions import (
    ConsultationError,
    ValidationError,
    ChatwootError,
    OneCError
)

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(verify_front_secret)])


async def _get_manager_name(db: AsyncSession, manager_key: Optional[str]) -> Optional[str]:
    """
    Получает ФИО менеджера из таблицы users по его cl_ref_key.
    
    Args:
        db: Сессия БД
        manager_key: cl_ref_key менеджера (UUID из 1C:ЦЛ)
    
    Returns:
        ФИО менеджера (users.description) или None, если менеджер не найден
    """
    if not manager_key:
        return None
    
    try:
        result = await db.execute(
            select(User.description)
            .where(User.cl_ref_key == manager_key)
            .where(User.deletion_mark == False)
            .limit(1)
        )
        manager_name = result.scalar_one_or_none()
        return manager_name
    except Exception as e:
        logger.warning(f"Failed to get manager name for {manager_key}: {e}")
        return None


def normalize_uuid(uuid_str: Optional[str]) -> Optional[str]:
    """Нормализует пустые UUID: "00000000-0000-0000-0000-000000000000" → None"""
    if not uuid_str or uuid_str == "00000000-0000-0000-0000-000000000000":
        return None
    return uuid_str


async def _get_default_manager_key(
    db: AsyncSession, 
    consultation_type: Optional[str] = None
) -> Optional[str]:
    """
    Получает cl_ref_key первого доступного менеджера из БД для использования по умолчанию.
    
    ВАЖНО: Использует ту же логику фильтрации, что и ManagerSelector:
    - Только менеджеры с лимитами (con_limit > 0)
    - Только менеджеры с разрешением на консультации (consultation_enabled = True)
    - Для консультаций по ведению учета: только отдел "ИТС консультанты" с установленными часами работы
    
    Args:
        db: Сессия БД
        consultation_type: Тип консультации ("Консультация по ведению учёта" или "Техническая поддержка")
    
    Returns:
        cl_ref_key менеджера или None если нет доступных менеджеров
    """
    query = select(User.cl_ref_key).where(
        User.cl_ref_key.isnot(None),
        User.cl_ref_key != "",
        User.deletion_mark == False,
        User.invalid == False,
        User.consultation_enabled == True,  # Только менеджеры с разрешением на консультации
        User.con_limit.isnot(None),
        User.con_limit > 0,  # Только менеджеры с установленными лимитами
    )
    
    # ВАЖНО: Для "Консультация по ведению учёта" применяем дополнительные фильтры:
    # - department = "ИТС консультанты"
    # - start_hour и end_hour обязательны (должно быть установлено рабочее время)
    if consultation_type == "Консультация по ведению учёта":
        query = query.where(
            User.department == "ИТС консультанты",
            User.start_hour.isnot(None),  # Обязательно должно быть установлено рабочее время начала
            User.end_hour.isnot(None),    # Обязательно должно быть установлено рабочее время окончания
        )
    
    result = await db.execute(query.limit(1))
    manager = result.scalar_one_or_none()
    
    if not manager:
        logger.warning(
            f"No default manager found with required criteria "
            f"(consultation_type={consultation_type})"
        )
    
    return manager


async def _adjust_scheduled_at_to_working_hours(
    db: AsyncSession,
    scheduled_at: datetime,
    manager_key: Optional[str] = None,
    consultation_type: Optional[str] = None,
) -> datetime:
    """
    Корректирует scheduled_at на ближайшее рабочее время.
    
    Учитывает:
    - Рабочие часы менеджеров (start_hour, end_hour)
    - Закрытые очереди (queue_closing)
    - Если время вне рабочего времени - переносит на следующее рабочее время
    
    Args:
        db: Сессия БД
        scheduled_at: Выбранное время
        manager_key: Ключ менеджера (если уже выбран)
        consultation_type: Тип консультации (для технической поддержки нужны все менеджеры)
    
    Returns:
        Скорректированное время
    """
    from datetime import time as dt_time
    
    # Нормализуем время к UTC
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
    else:
        scheduled_at = scheduled_at.astimezone(timezone.utc)
    
    current_time = datetime.now(timezone.utc)
    
    # Если выбранное время в прошлом, начинаем с текущего времени
    if scheduled_at < current_time:
        logger.info(f"Scheduled time {scheduled_at} is in the past, adjusting to current time {current_time}")
        scheduled_at = current_time
    
    # ВАЖНО: Для технической поддержки используем рабочее время из env, а не проверяем менеджеров из БД
    if consultation_type == "Техническая поддержка":
        # Получаем рабочее время из настроек
        settings = get_settings()
        try:
            # Парсим время начала и конца рабочего дня из env (формат: "HH:MM")
            start_hour_str = settings.TECH_SUPPORT_WORKING_HOURS_START
            end_hour_str = settings.TECH_SUPPORT_WORKING_HOURS_END
            
            start_hour_parts = start_hour_str.split(":")
            end_hour_parts = end_hour_str.split(":")
            
            work_start_hour = dt_time(int(start_hour_parts[0]), int(start_hour_parts[1]))
            work_end_hour = dt_time(int(end_hour_parts[0]), int(end_hour_parts[1]))
            
            logger.info(f"Technical support working hours: {work_start_hour} - {work_end_hour}")
        except (ValueError, IndexError, AttributeError) as e:
            logger.warning(f"Failed to parse working hours from env, using default 9:00-18:00: {e}")
            work_start_hour = dt_time(9, 0)
            work_end_hour = dt_time(18, 0)
        
        # Проверяем, попадает ли выбранное время в рабочее время
        scheduled_time_only = scheduled_at.time()
        scheduled_date = scheduled_at.date()
        
        if work_start_hour <= scheduled_time_only <= work_end_hour:
            # Время в пределах рабочего времени - оставляем как есть
            logger.info(f"Scheduled time {scheduled_at} is within working hours, no adjustment needed")
            return scheduled_at
        
        # Время вне рабочего времени - ищем ближайшее рабочее время
        # Если выбранное время до начала рабочего дня - переносим на начало рабочего дня
        if scheduled_time_only < work_start_hour:
            # Если сегодня еще не начался рабочий день, переносим на начало рабочего дня сегодня
            adjusted_time = datetime.combine(scheduled_date, work_start_hour).replace(tzinfo=timezone.utc)
            if adjusted_time < current_time:
                # Если начало рабочего дня сегодня уже прошло, переносим на завтра
                adjusted_time = datetime.combine(scheduled_date + timedelta(days=1), work_start_hour).replace(tzinfo=timezone.utc)
            logger.info(f"Adjusted scheduled_at from {scheduled_at} to {adjusted_time} (before working hours)")
            return adjusted_time
        
        # Если выбранное время после окончания рабочего дня - переносим на начало рабочего дня следующего дня
        adjusted_time = datetime.combine(scheduled_date + timedelta(days=1), work_start_hour).replace(tzinfo=timezone.utc)
        logger.info(f"Adjusted scheduled_at from {scheduled_at} to {adjusted_time} (after working hours)")
        return adjusted_time
    
    # Для консультаций по ведению учета проверяем менеджеров из БД
    # Получаем список доступных менеджеров для проверки рабочего времени
    if manager_key:
        # Если менеджер уже выбран, проверяем только его
        # ВАЖНО: Проверяем, что менеджер не удален и не недействителен
        result = await db.execute(
            select(User).where(
                User.cl_ref_key == manager_key,
                User.deletion_mark == False,
                User.invalid == False
            )
        )
        manager = result.scalar_one_or_none()
        managers = [manager] if manager else []
    else:
        # Для консультаций по ведению учета используем ManagerSelector (только менеджеры с лимитами)
        # ВАЖНО: Передаем consultation_type для правильной фильтрации менеджеров
        manager_selector = ManagerSelector(db)
        managers = await manager_selector.get_available_managers(
            current_time=scheduled_at,
            consultation_type=consultation_type,  # Передаем тип консультации для фильтрации
        )
    
    if not managers:
        # Если нет доступных менеджеров, переносим на следующее утро 9:00
        next_day = scheduled_at.date() + timedelta(days=1)
        if scheduled_at.date() == current_time.date():
            # Если сегодня, проверяем текущее время
            if current_time.time() >= dt_time(9, 0):
                # Уже после 9:00 - переносим на завтра
                next_day = current_time.date() + timedelta(days=1)
            else:
                # Еще до 9:00 - можно сегодня в 9:00
                next_day = current_time.date()
        
        adjusted_time = datetime.combine(next_day, dt_time(9, 0)).replace(tzinfo=timezone.utc)
        logger.info(f"No available managers, adjusting scheduled_at to {adjusted_time}")
        return adjusted_time
    
    # Проверяем рабочее время менеджеров
    scheduled_date = scheduled_at.date()
    scheduled_time_only = scheduled_at.time()
    
    # Ищем менеджера, который работает в выбранное время
    working_manager = None
    for manager in managers:
        if not manager.cl_ref_key:
            continue
        
        # Проверяем закрытие очереди
        queue_closing_result = await db.execute(
            select(QueueClosing).where(
                QueueClosing.manager_key == manager.cl_ref_key,
                func.date_trunc('day', QueueClosing.period) == func.date_trunc('day', scheduled_at)
            ).limit(1)
        )
        if queue_closing_result.scalar_one_or_none():
            continue  # Очередь закрыта
        
        # Проверяем рабочее время
        if manager.start_hour is None or manager.end_hour is None:
            # Менеджер работает всегда
            working_manager = manager
            break
        
        start_hour = manager.start_hour
        end_hour = manager.end_hour
        
        # Проверяем, попадает ли время в рабочие часы
        if start_hour <= end_hour:
            # Обычное рабочее время (например, 9:00-18:00)
            if start_hour <= scheduled_time_only <= end_hour:
                working_manager = manager
                break
        else:
            # Рабочее время переходит через полночь (например, 22:00-06:00)
            if scheduled_time_only >= start_hour or scheduled_time_only <= end_hour:
                working_manager = manager
                break
    
    if working_manager:
        # Найден работающий менеджер - время корректно
        logger.info(f"Found working manager {working_manager.cl_ref_key} for scheduled time {scheduled_at}")
        return scheduled_at
    else:
        logger.info(f"No working manager found for scheduled time {scheduled_at}, searching for nearest working time")
    
    # Не найден работающий менеджер - ищем ближайшее рабочее время
    # Начинаем с выбранной даты и ищем первое доступное время
    search_date = scheduled_date
    max_days_ahead = 7  # Максимум на неделю вперед
    
    for day_offset in range(max_days_ahead):
        check_date = search_date + timedelta(days=day_offset)
        check_datetime = datetime.combine(check_date, dt_time(9, 0)).replace(tzinfo=timezone.utc)
        
        # Проверяем всех менеджеров на эту дату
        for manager in managers:
            if not manager.cl_ref_key:
                continue
            
            # Проверяем закрытие очереди
            queue_closing_result = await db.execute(
                select(QueueClosing).where(
                    QueueClosing.manager_key == manager.cl_ref_key,
                    func.date_trunc('day', QueueClosing.period) == func.date_trunc('day', check_datetime)
                ).limit(1)
            )
            if queue_closing_result.scalar_one_or_none():
                continue  # Очередь закрыта
            
            # Проверяем рабочее время
            if manager.start_hour is None or manager.end_hour is None:
                # Менеджер работает всегда - используем 9:00
                adjusted_time = check_datetime
                logger.info(f"Adjusted scheduled_at to {adjusted_time} (manager {manager.cl_ref_key} works always)")
                return adjusted_time
            
            start_hour = manager.start_hour
            end_hour = manager.end_hour
            
            # Используем start_hour как время начала работы
            adjusted_time = datetime.combine(check_date, start_hour).replace(tzinfo=timezone.utc)
            
            # Проверяем, что это время не в прошлом (для первого дня)
            if day_offset == 0 and adjusted_time < current_time:
                # Если сегодня уже прошло start_hour, переносим на завтра
                continue
            
            logger.info(f"Adjusted scheduled_at to {adjusted_time} (manager {manager.cl_ref_key} working hours: {start_hour}-{end_hour})")
            return adjusted_time
    
    # Если не нашли за неделю, возвращаем время через неделю в 9:00
    fallback_time = datetime.combine(search_date + timedelta(days=7), dt_time(9, 0)).replace(tzinfo=timezone.utc)
    logger.warning(f"Could not find working time within {max_days_ahead} days, using fallback: {fallback_time}")
    return fallback_time


async def _check_consultation_limit(
    db: AsyncSession,
    code_abonent: Optional[str],
    org_inn: Optional[str],
    consultation_date: datetime,
) -> None:
    """
    Проверяет лимит консультаций типа "Консультация по ведению учёта" в бэкенде перед отправкой в ЦЛ.
    
    Лимит: максимум 3 открытых/завершенных консультации на один день (по дате консультации).
    Проверка выполняется по code_abonent для всех user_id этого абонента.
    Проверяет только консультации типа "Консультация по ведению учёта", которые были успешно созданы в ЦЛ (имеют cl_ref_key).
    Отмененные консультации (status="cancelled") НЕ учитываются в лимите.
    
    Логика проверки:
    1. Приоритетно проверяет по коду абонента (code_abonent), если он есть
    2. Если код абонента отсутствует, но есть ИНН - проверяет по ИНН (fallback)
       Это необходимо для случаев создания консультаций через колл-центр или расширение 1С,
       когда клиент может быть создан без кода абонента, но уже существует в ЦЛ по ИНН
    
    ВАЖНО: 
    - Проверяет ТОЛЬКО консультации типа "Консультация по ведению учёта".
    - Консультации типа "Техническая поддержка" НЕ учитываются в лимите.
    - Отмененные консультации (status="cancelled") НЕ учитываются в лимите.
    - Учитываются только открытые/завершенные заявки (status != "cancelled").
    - Проверка выполняется по коду абонента (приоритетно), так как код абонента выдается системой
      и не может быть изменен пользователем, в то время как ИНН может быть изменен во фронтенде.
    - Если код абонента отсутствует, используется проверка по ИНН для случаев создания через
      колл-центр или расширение 1С, когда клиент уже существует в ЦЛ, но еще не синхронизирован в БД.
    
    Args:
        db: Сессия БД
        code_abonent: Код абонента владельца клиента (приоритетно)
        org_inn: ИНН владельца клиента (fallback, если code_abonent отсутствует)
        consultation_date: Дата консультации (scheduled_at)
    
    Raises:
        HTTPException: Если лимит превышен (429 Too Many Requests)
    """
    if not consultation_date:
        logger.warning("Cannot check consultation limit: consultation_date is empty")
        return
    
    # Преобразуем дату в дату без времени для сравнения по дню
    # Учитываем timezone: если дата имеет timezone, нормализуем к UTC перед извлечением даты
    if consultation_date.tzinfo is not None:
        # Если есть timezone, нормализуем к UTC
        consultation_date_utc = consultation_date.astimezone(timezone.utc)
        consultation_date_only = consultation_date_utc.date()
    else:
        # Если нет timezone, используем как есть
        consultation_date_only = consultation_date.date()
    
    # Базовые условия для всех запросов
    # Проверяем ТОЛЬКО консультации типа "Консультация по ведению учёта"
    # Исключаем отмененные консультации (status != "cancelled")
    # Учитываем только открытые/завершенные заявки (не отмененные)
    base_conditions = [
        Consultation.start_date.isnot(None),
        cast(Consultation.start_date, Date) == consultation_date_only,
        Consultation.cl_ref_key.isnot(None),
        Consultation.cl_ref_key != "",
        Consultation.consultation_type == "Консультация по ведению учёта",
        (Consultation.status.is_(None)) | (Consultation.status != "cancelled")
    ]
    
    count = 0
    
    # Приоритетно проверяем по коду абонента (если есть)
    if code_abonent:
        OwnerClient = aliased(Client)
        
        result = await db.execute(
            select(func.count(Consultation.cons_id))
            .join(Client, Consultation.client_id == Client.client_id)
            .outerjoin(OwnerClient, Client.parent_id == OwnerClient.client_id)
            .where(
                case(
                    (Client.parent_id.isnot(None), OwnerClient.code_abonent),
                    else_=Client.code_abonent
                ) == code_abonent
            )
            .where(*base_conditions)
        )
        count = result.scalar() or 0
        
        logger.info(
            f"Consultation limit check for code_abonent {code_abonent} on date {consultation_date_only}: "
            f"{count} consultations found (limit: 3)"
        )
    
    # Если код абонента отсутствует, проверяем по ИНН (fallback)
    # Это необходимо для случаев создания через колл-центр или расширение 1С,
    # когда клиент уже существует в ЦЛ по ИНН, но еще не синхронизирован в БД с кодом абонента
    # ВАЖНО: Проверяем по ИНН только если код абонента отсутствует, чтобы не дублировать проверку
    if not code_abonent and org_inn:
        result = await db.execute(
            select(func.count(Consultation.cons_id))
            .where(Consultation.org_inn == org_inn)
            .where(*base_conditions)
        )
        count = result.scalar() or 0
        
        logger.info(
            f"Consultation limit check for org_inn {org_inn} (fallback, code_abonent absent) on date {consultation_date_only}: "
            f"{count} consultations found (limit: 3)"
        )
    
    # Если ни код абонента, ни ИНН не указаны - пропускаем проверку
    if not code_abonent and not org_inn:
        logger.warning(
            f"Cannot check consultation limit: both code_abonent and org_inn are empty. "
            f"Skipping limit check."
        )
        return
    
    # Лимит: максимум 3 консультации в день
    if count >= 3:
        identifier = code_abonent or org_inn
        identifier_type = "коду абонента" if code_abonent else "ИНН"
        raise HTTPException(
            status_code=429,  # Too Many Requests
            detail=(
                f"Превышен лимит создания консультаций. "
                f"Максимум 3 консультации на один день (по дате консультации: {consultation_date_only}). "
                f"Проверка выполнена по {identifier_type}: {identifier}. "
                f"Попробуйте выбрать другую дату."
            )
        )


async def _check_technical_support_limit(
    db: AsyncSession,
    client_id: uuid.UUID,
) -> None:
    """
    Проверяет ограничение на создание консультаций типа "Техническая поддержка".
    
    Ограничение: максимум 1 открытая консультация одновременно для одного user_id.
    Проверка выполняется по client_id (user_id = client_id создателя консультации).
    
    Статусы, считающиеся открытыми: "open", "pending", None (не закрытые).
    Статусы, считающиеся закрытыми: "closed", "cancelled", "resolved".
    
    Args:
        db: Сессия БД
        client_id: UUID клиента (user_id), который создает консультацию
    
    Raises:
        HTTPException: Если уже есть открытая консультация (409 Conflict)
    """
    # Проверяем наличие открытых консультаций типа "Техническая поддержка" для данного client_id
    # Открытые статусы: "open", "pending", None (не закрытые)
    # Закрытые статусы: "closed", "cancelled", "resolved"
    result = await db.execute(
        select(func.count(Consultation.cons_id))
        .where(Consultation.client_id == client_id)
        .where(Consultation.consultation_type == "Техническая поддержка")
        .where(
            (Consultation.status.is_(None)) |
            (Consultation.status == "open") |
            (Consultation.status == "pending")
        )
    )
    count = result.scalar() or 0
    
    logger.info(
        f"Technical support limit check for client_id {client_id}: "
        f"{count} open consultations found (limit: 1)"
    )
    
    if count >= 1:
        raise HTTPException(
            status_code=409,  # Conflict
            detail=(
                "Нельзя создать новую заявку на техническую поддержку, "
                "пока не закрыта предыдущая открытая заявка. "
                "Пожалуйста, закройте существующую заявку перед созданием новой."
            )
        )


async def _get_owner_client(db: AsyncSession, client: Client) -> Client:
    """Возвращает владельца абонента (сам клиент или его родитель)."""
    if not client:
        raise HTTPException(status_code=400, detail="Client is required")
    if not client.parent_id:
        return client
    result = await db.execute(
        select(Client).where(Client.client_id == client.parent_id)
    )
    owner = result.scalar_one_or_none()
    if not owner:
        raise HTTPException(status_code=400, detail="Owner client not found")
    return owner


def _build_client_display_name(client: Client) -> str:
    """
    Читаемое имя клиента для 1С по правилу: CLOBUS + Наименование + КодАбонентаClobus + ИНН.
    
    Использует company_name если есть, иначе name или contact_name.
    """
    # Используем company_name если есть, иначе name или contact_name
    base_name = client.company_name or client.name or client.contact_name or "Клиент"
    
    parts = ["Clobus", base_name]
    
    # Добавляем код абонента если есть
    if client.code_abonent:
        parts.append(client.code_abonent)
    
    # Добавляем ИНН если есть
    if client.org_inn:
        parts.append(f"({client.org_inn})")
    
    return " ".join(parts)


def _build_contact_hint(client: Client, owner: Client, source: Optional[str]) -> Optional[str]:
    """Формирует строку для поля 'КакСвязаться'."""
    phone = client.phone_number or owner.phone_number
    name = (
        client.contact_name
        or client.name
        or owner.contact_name
        or owner.name
    )
    source_label = source or "web"
    parts = [part for part in (phone, name, source_label) if part]
    return " / ".join(parts) if parts else None


async def _ensure_owner_synced_with_cl(
    db: AsyncSession,
    owner: Client,
    onec_client: OneCClient,
) -> Client:
    """Убеждаемся, что владелец создан в 1С и имеет Ref_Key."""
    logger.info(f"=== Ensuring owner client is synced with 1C ===")
    logger.info(f"  Owner client_id: {owner.client_id}")
    logger.info(f"  Owner cl_ref_key: {owner.cl_ref_key}")
    logger.info(f"  Owner org_inn: {owner.org_inn}")
    logger.info(f"  Owner code_abonent: {owner.code_abonent}")
    
    if owner.cl_ref_key:
        logger.info(f"✓ Owner already has cl_ref_key: {owner.cl_ref_key}")
        return owner
    
    if not owner.org_inn:
        logger.error(f"✗ Owner client {owner.client_id} has no org_inn - cannot sync with 1C")
        raise HTTPException(status_code=400, detail="Owner client requires INN")

    try:
        logger.info(f"Searching for existing client in 1C by INN: {owner.org_inn}")
        existing = await onec_client.find_client_by_inn(owner.org_inn)
    except Exception as e:
        logger.warning(
            "Failed to fetch client from 1C by INN %s: %s. Proceeding without sync.",
            owner.org_inn,
            e,
        )
        return owner

    if existing:
        ref_key = existing.get("Ref_Key")
        logger.info(f"✓ Found existing client in 1C with Ref_Key: {ref_key}")
        owner.cl_ref_key = ref_key
        owner.code_abonent = owner.code_abonent or existing.get("КодАбонентаClobus")
        await db.flush()
        logger.info(f"✓ Saved cl_ref_key to owner: {ref_key}")
        return owner

    if not owner.code_abonent:
        logger.warning(
            "Owner client %s has no code_abonent. Unable to create in 1C.",
            owner.client_id,
        )
        return owner

    try:
        logger.info(f"Creating new client in 1C: org_inn={owner.org_inn}, code_abonent={owner.code_abonent}")
        created = await onec_client.create_client_odata(
            name=_build_client_display_name(owner),
            org_inn=owner.org_inn,
            code_abonent=owner.code_abonent,
            phone=owner.phone_number,
            email=owner.email,
        )
        logger.info(f"✓ Created client in 1C, response: {created}")
    except Exception as e:
        logger.error(
            "Failed to create client in 1C for %s: %s. Proceeding without sync.",
            owner.client_id,
            e,
            exc_info=True
        )
        return owner

    ref_key = created.get("Ref_Key")
    if not ref_key:
        logger.error(f"✗ 1C returned response without Ref_Key: {created}")
        return owner
    
    owner.cl_ref_key = ref_key
    owner.code_abonent = created.get("КодАбонентаClobus") or owner.code_abonent
    await db.flush()
    logger.info(f"✓ Saved cl_ref_key to owner: {ref_key}")
    return owner


def _map_importance_to_priority(importance: Optional[int]) -> Optional[str]:
    """Маппинг importance (1-3) в priority Chatwoot (low/medium/high)."""
    if importance is None:
        return None
    if importance >= 3:
        return "high"
    elif importance >= 2:
        return "medium"
    return "low"


async def _build_chatwoot_custom_attrs(
    db: AsyncSession,
    owner: Client,
    payload: ConsultationWithClient,
    consultation: Optional[Consultation] = None,
) -> Dict[str, Any]:
    """
    Готовим custom attributes для Conversation в Chatwoot.
    
    ВАЖНО: 
    - НЕ включаем org_inn и client_type - они в Contact, не Conversation
    - Используем только поля, специфичные для Conversation
    
    Custom attributes для Conversation:
    - code_abonent (обязательное - всегда присутствует, даже если пустое)
    - number_con, category_name, question_name, date_con, con_end, redate_con, retime_con, 
      consultation_type, closed_without_con, subs_id, subs_start, subs_end, tariff_id, tariffperiod_id (опционально)
    """
    # Обязательное поле - всегда присутствует, даже если пустое
    attrs: Dict[str, Any] = {
        "code_abonent": owner.code_abonent or "",  # Всегда строка, не None
    }
    
    # Номер из ЦЛ (если есть) - опциональное поле
    if consultation and consultation.number:
        attrs["number_con"] = str(consultation.number)
    
    # Опциональные поля из payload
    # Нормализуем пустые UUID
    question_cat_key = normalize_uuid(payload.consultation.online_question_cat)
    question_key = normalize_uuid(payload.consultation.online_question)
    
    if question_cat_key:
        result = await db.execute(
            select(OnlineQuestionCat.description).where(
                OnlineQuestionCat.ref_key == question_cat_key
            )
        )
        category_name = result.scalar_one_or_none()
        if category_name:
            attrs["category_name"] = str(category_name)

    if question_key:
        result = await db.execute(
            select(OnlineQuestion.description).where(
                OnlineQuestion.ref_key == question_key
            )
        )
        question_name = result.scalar_one_or_none()
        if question_name:
            attrs["question_name"] = str(question_name)
    
    # Поля из consultation (если есть)
    # Заполняем даты/статусы: сначала берем из consultation, если его нет или поле пустое — берем из payload
    date_to_use = consultation.start_date if consultation else None
    if not date_to_use and payload.consultation.scheduled_at:
        date_to_use = payload.consultation.scheduled_at
    if date_to_use:
        from datetime import timezone
        dt = date_to_use
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        attrs["date_con"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    end_to_use = consultation.end_date if consultation and hasattr(consultation, "end_date") else None
    if not end_to_use and getattr(payload.consultation, "end_date", None):
        end_to_use = payload.consultation.end_date
    if end_to_use:
        from datetime import timezone
        dt = end_to_use
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        attrs["con_end"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    redate_to_use = consultation.redate if consultation and hasattr(consultation, "redate") else None
    if not redate_to_use and getattr(payload.consultation, "redate", None):
        redate_to_use = payload.consultation.redate
    if redate_to_use:
        from datetime import timezone
        dt = redate_to_use
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        attrs["redate_con"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    retime_to_use = consultation.redate_time if consultation and hasattr(consultation, "redate_time") else None
    if not retime_to_use and getattr(payload.consultation, "redate_time", None):
        retime_to_use = payload.consultation.redate_time
    if retime_to_use:
        attrs["retime_con"] = retime_to_use.strftime("%H:%M")
    
    # consultation_type: берем из consultation.consultation_type или из payload.consultation.consultation_type
    consultation_type_value = None
    if consultation and hasattr(consultation, "consultation_type") and consultation.consultation_type:
        consultation_type_value = consultation.consultation_type
    elif hasattr(payload.consultation, "consultation_type") and payload.consultation.consultation_type:
        consultation_type_value = payload.consultation.consultation_type
    if consultation_type_value:
        attrs["consultation_type"] = str(consultation_type_value)
    
    # Закрыто без консультации
    denied_value = None
    if consultation and hasattr(consultation, "denied"):
        denied_value = consultation.denied
    if denied_value is None and getattr(payload.consultation, "denied", None) is not None:
        denied_value = payload.consultation.denied
    if denied_value is not None:
        attrs["closed_without_con"] = bool(denied_value)
        
    # Данные о подписке из owner
    if owner.subs_id:
        attrs["subs_id"] = str(owner.subs_id)
    
    if owner.subs_start:
        from datetime import timezone
        dt = owner.subs_start
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        attrs["subs_start"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    if owner.subs_end:
        from datetime import timezone
        dt = owner.subs_end
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        attrs["subs_end"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    if owner.tariff_id:
        attrs["tariff_id"] = str(owner.tariff_id)
    
    if owner.tariffperiod_id:
        attrs["tariffperiod_id"] = str(owner.tariffperiod_id)

    # Фильтруем только опциональные поля (None, пустые строки, пустые списки)
    # Обязательное поле (code_abonent) всегда остается
    filtered = {}
    for key, value in attrs.items():
        # Обязательное поле всегда включаем
        if key == "code_abonent":
            filtered[key] = value
        # Опциональные поля включаем только если не пустые
        elif value not in (None, "", []):
            filtered[key] = value
    
    # Валидация размера и количества custom_attributes для Chatwoot
    # Chatwoot имеет лимиты на количество и размер custom_attributes
    # Проверяем перед возвратом
    total_size = sum(len(str(v)) for v in filtered.values())
    max_size = 10000  # Примерный лимит Chatwoot (может варьироваться)
    max_fields = 50  # Примерный лимит количества полей
    
    if len(filtered) > max_fields:
        logger.warning(f"Too many custom_attributes ({len(filtered)}), truncating to {max_fields}")
        # Оставляем обязательное поле и первые N опциональных
        required_keys = {"code_abonent"}
        optional_items = [(k, v) for k, v in filtered.items() if k not in required_keys]
        filtered = {k: v for k, v in filtered.items() if k in required_keys}
        filtered.update(dict(optional_items[:max_fields - len(required_keys)]))
    
    if total_size > max_size:
        logger.warning(f"Custom_attributes too large ({total_size} bytes), truncating values")
        # Укорачиваем значения полей
        for key, value in filtered.items():
            if key != "code_abonent" and len(str(value)) > 500:
                filtered[key] = str(value)[:500]
    
    return filtered


def _build_chatwoot_contact_custom_attrs(
    owner: Client,
    client: Client
) -> Dict[str, Any]:
    """
    Готовим custom attributes для Contact в Chatwoot.
    
    Custom attributes для Contact:
    - code_abonent, inn_pinfl, client_type (обязательные)
    - region, country (опционально, если есть в модели)
    
    Для пользователей (is_parent=false) используем данные владельца для region и country.
    """
    attrs: Dict[str, Any] = {
        "code_abonent": owner.code_abonent or "",
        "inn_pinfl": owner.org_inn or "",
        "client_type": "owner" if not client.parent_id else "user",
    }
    
    # Опциональные поля: для пользователей берем из владельца, для владельцев - из клиента
    region_to_use = client.region if not client.parent_id else (client.region or owner.region)
    country_to_use = client.country if not client.parent_id else (client.country or owner.country)
    
    if region_to_use:
        attrs["region"] = str(region_to_use)
    
    if country_to_use:
        attrs["country"] = str(country_to_use)
    
    # Фильтруем пустые значения для опциональных полей
    filtered = {}
    for key, value in attrs.items():
        # Обязательные поля всегда включаем (даже если пустые)
        if key in ("code_abonent", "inn_pinfl", "client_type"):
            filtered[key] = value if value else ""
        # Опциональные поля включаем только если не пустые
        elif value not in (None, "", []):
            filtered[key] = value
    
    return filtered


def _build_chatwoot_labels(
    language: Optional[str],
    source: Optional[str],
    consultation_type: Optional[str] = None
) -> List[str]:
    """
    Формируем labels для Conversation (типовое поле Chatwoot).
    Используем точные названия меток, которые должны быть созданы в Chatwoot.
    
    Метки в Chatwoot:
    - рус, узб - языки
    - сайт, тг - источники
    - тех - тип консультации (Техническая поддержка)
    - бух, рт, ук - продукты 1С (бухгалтерия, розница, управление компанией)
    
    ВАЖНО: Метки должны быть созданы заранее в Chatwoot с точными названиями.
    """
    labels = []
    
    # Маппинг языков
    if language:
        lang_map = {
            "ru": "рус",
            "uz": "узб",
        }
        label_name = lang_map.get(language.lower())
        if label_name:
            labels.append(label_name)
    
    # Маппинг источников
    if source:
        source_map = {
            "site": "сайт",
            "web": "сайт",
            "telegram": "тг",
            "tg": "тг",
            "TELEGRAM": "тг",
        }
        source_lower = source.lower()
        label_name = source_map.get(source_lower)
        if label_name:
            labels.append(label_name)
    
    # Маппинг типа консультации
    if consultation_type:
        # Если тип консультации содержит "Техническая поддержка" или похожее
        consultation_type_lower = consultation_type.lower()
        if "техническая" in consultation_type_lower or "тех" in consultation_type_lower:
            labels.append("тех")
    
    return labels


async def _process_onec_response(
    consultation: Consultation,
    onec_response: Dict[str, Any]
) -> None:
    """
    Обрабатывает ответ от 1C OData при создании/обновлении консультации.
    Сохраняет все важные поля в объект consultation.
    
    Args:
        consultation: Объект консультации для обновления
        onec_response: Ответ от 1C OData API
    """
    
    # Обязательные поля
    if "Ref_Key" in onec_response:
        consultation.cl_ref_key = onec_response["Ref_Key"]
    
    if "Number" in onec_response:
        consultation.number = onec_response["Number"]
    
    # Даты (1C возвращает даты без timezone, добавляем UTC)
    if "ДатаСоздания" in onec_response:
        try:
            # Парсим дату из формата "2025-11-19T09:00:00" (без timezone)
            date_str = onec_response["ДатаСоздания"]
            if date_str and date_str != "0001-01-01T00:00:00":
                # Если нет timezone, добавляем UTC
                if "Z" in date_str or "+" in date_str or date_str.count("-") > 2:
                    consultation.create_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    # Без timezone - добавляем UTC
                    dt = datetime.fromisoformat(date_str)
                    consultation.create_date = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse ДатаСоздания from 1C: {e}")
    
    if "ДатаКонсультации" in onec_response:
        try:
            date_str = onec_response["ДатаКонсультации"]
            if date_str and date_str != "0001-01-01T00:00:00":
                if "Z" in date_str or "+" in date_str or date_str.count("-") > 2:
                    consultation.start_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(date_str)
                    consultation.start_date = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse ДатаКонсультации from 1C: {e}")
    
    if "Конец" in onec_response:
        try:
            date_str = onec_response["Конец"]
            if date_str and date_str != "0001-01-01T00:00:00":
                if "Z" in date_str or "+" in date_str or date_str.count("-") > 2:
                    consultation.end_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.fromisoformat(date_str)
                    consultation.end_date = dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse Конец from 1C: {e}")
    
    # Статус (маппинг ВидОбращения в наш статус)
    if "ВидОбращения" in onec_response:
        vid_obrascheniya = onec_response["ВидОбращения"]
        # Обратный маппинг: ВидОбращения → наш status
        vid_to_status = {
            "ВОчередьНаКонсультацию": "open",
            "КонсультацияИТС": "resolved",
            "Другое": "pending",
        }
        consultation.status = vid_to_status.get(vid_obrascheniya, consultation.status)
    
    # Закрыто без консультации
    if "ЗакрытоБезКонсультации" in onec_response:
        consultation.denied = bool(onec_response["ЗакрытоБезКонсультации"])
    
    # Описание (если изменилось)
    if "Описание" in onec_response and onec_response["Описание"]:
        consultation.comment = onec_response["Описание"]
    
    # Тема (если изменилась)
    if "Тема" in onec_response and onec_response["Тема"]:
        # Тема хранится в comment или можно добавить отдельное поле
        pass  # Пока не храним тему отдельно
    
    logger.debug(f"Processed 1C response for consultation: Ref_Key={consultation.cl_ref_key}, Number={consultation.number}")


@router.post("/create", response_model=ConsultationResponse)
async def create_consultation(
    payload: ConsultationWithClient,
    request: Request,
    db: AsyncSession = Depends(get_db),
    authorization: Optional[str] = Header(None, description="Bearer токен (опционально)"),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key", description="Уникальный ключ для предотвращения дублирования")
):
    """
    Создание консультации с данными клиента.
    
    Основной endpoint для фронта. Принимает:
    - Данные клиента (если клиента еще нет)
    - Данные консультации
    
    Процесс:
    1. Проверяет idempotency key (если передан)
    2. Находит или создает клиента
    3. Создает консультацию в БД
    4. Отправляет в Chatwoot
    5. Отправляет в 1C:ЦЛ
    6. Обновляет запись с полученными ID
    
    Headers:
    - Authorization: Bearer <token> (опционально, для будущей валидации)
    - Idempotency-Key: <key> (опционально, для предотвращения дублирования)
    """
    # Проверяем idempotency key если передан
    if idempotency_key:
        request_hash = generate_request_hash(payload.dict())
        cached_response = await check_idempotency_key(
            db=db,
            key=idempotency_key,
            operation_type="create_consultation",
            request_hash=request_hash
        )
        if cached_response:
            logger.info(f"Returning cached response for idempotency key: {idempotency_key}")
            return ConsultationResponse(**cached_response)
    
    try:
        # 1. Находим или создаем клиента
        client = None
        if payload.client:
            try:
                client = await find_or_create_client(db, payload.client)
            except Exception as e:
                logger.error(f"Failed to find or create client: {e}", exc_info=True)
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to process client data: {str(e)}"
                )
        elif payload.consultation.client_id:
            # Если указан client_id, проверяем существование
            try:
                client_uuid = uuid.UUID(payload.consultation.client_id)
                result = await db.execute(
                    select(Client).where(Client.client_id == client_uuid)
                )
                client = result.scalar_one_or_none()
                if not client:
                    raise HTTPException(status_code=404, detail="Client not found")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid client_id format")
            except Exception as e:
                logger.error(f"Database error while fetching client: {e}", exc_info=True)
                raise HTTPException(
                    status_code=500,
                    detail="Internal server error while fetching client"
                )
        
        if not client:
            raise HTTPException(
                status_code=400,
                detail="Client data or client_id is required"
            )
        
        try:
            owner_client = await _get_owner_client(db, client)
            onec_client = OneCClient()
            owner_client = await _ensure_owner_synced_with_cl(db, owner_client, onec_client)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to process owner client: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process owner client: {str(e)}"
            )
        
        # ВАЖНО: Сохраняем значения атрибутов owner_client в переменные ДО возможного rollback
        # После rollback нельзя обращаться к атрибутам объектов БД, так как сессия будет в невалидном состоянии
        client_key = owner_client.cl_ref_key
        owner_client_id = str(owner_client.client_id)
        owner_client_org_inn = owner_client.org_inn
        owner_client_code_abonent = owner_client.code_abonent
        owner_client_name = owner_client.name
        owner_client_contact_name = owner_client.contact_name
        owner_client_company_name = owner_client.company_name
        
        if not client_key:
            logger.error(
                "✗ Owner client %s is not linked with 1C (cl_ref_key missing). "
                "This means the client was not created in 1C or sync failed. "
                "Consultation will be created in DB and Chatwoot, but NOT in 1C.",
                owner_client_id,
            )
            logger.error(f"Owner client details: org_inn={owner_client_org_inn}, code_abonent={owner_client_code_abonent}, name={owner_client_name}")
            logger.error(f"Please check logs above for errors during client creation in 1C")

        contact_hint = _build_contact_hint(client, owner_client, payload.source)

        # Проверка ограничений на создание консультаций в зависимости от типа
        consultation_type = payload.consultation.consultation_type
        if consultation_type == "Техническая поддержка":
            # Проверка: максимум 1 открытая консультация одновременно для одного user_id (client_id)
            await _check_technical_support_limit(db, client.client_id)
        # 2. Автоматически выбираем менеджера для консультации
        # ВАЖНО: Для "Техническая поддержка" не подбираем менеджера автоматически
        selected_manager_key = None
        
        if consultation_type == "Консультация по ведению учёта":
            # Автоподбор менеджера только для консультаций по ведению учета
            # Используются только менеджеры с лимитами (проверка в ManagerSelector)
            manager_selector = ManagerSelector(db)
            
            # Получаем раздел программы из консультации (если есть)
            # Пока используем online_question_cat как category_key для выбора менеджера
            category_key = normalize_uuid(payload.consultation.online_question_cat)
            
            try:
                # Получаем язык консультации для проверки соответствия языка менеджера
                consultation_language = payload.consultation.lang
                
                selected_manager_key = await manager_selector.select_manager_for_consultation(
                    consultation=None,  # Консультация еще не создана
                    category_key=category_key,
                    current_time=datetime.now(timezone.utc),
                    consultation_type=consultation_type,  # Передаем тип консультации для фильтрации
                    language=consultation_language,  # Передаем язык для проверки соответствия
                )
                
                if selected_manager_key:
                    logger.info(f"Auto-selected manager {selected_manager_key} for consultation (Консультация по ведению учёта)")
                else:
                    logger.warning("No manager selected automatically for consultation (Консультация по ведению учёта), will use default or manual assignment")
            except Exception as e:
                logger.error(f"Failed to auto-select manager: {e}", exc_info=True)
                # Продолжаем без автоматического выбора менеджера
        elif consultation_type == "Техническая поддержка":
            # Для технической поддержки не подбираем менеджера автоматически
            logger.info("Skipping auto-assignment for Техническая поддержка (no manager selection needed)")
        else:
            # Для других типов консультаций (если появятся) также не подбираем автоматически
            logger.info(f"Skipping auto-assignment for consultation_type={consultation_type}")
        
        # Если менеджер не выбран автоматически, используем дефолтного
        # ВАЖНО: Для "Техническая поддержка" менеджер не назначается автоматически
        if not selected_manager_key and consultation_type == "Консультация по ведению учёта":
            selected_manager_key = await _get_default_manager_key(db, consultation_type=consultation_type)
        
        # ВАЖНО: Корректируем scheduled_at на ближайшее рабочее время после выбора менеджера
        if payload.consultation.scheduled_at:
            original_scheduled_at = payload.consultation.scheduled_at
            payload.consultation.scheduled_at = await _adjust_scheduled_at_to_working_hours(
                db=db,
                scheduled_at=payload.consultation.scheduled_at,
                manager_key=selected_manager_key,  # Используем выбранного менеджера если есть
                consultation_type=consultation_type  # Передаем тип консультации для правильной фильтрации менеджеров
            )
            
            if payload.consultation.scheduled_at != original_scheduled_at:
                logger.info(
                    f"Adjusted scheduled_at from {original_scheduled_at} to {payload.consultation.scheduled_at} "
                    f"to match working hours"
                )
        
        # Проверка ограничения на создание заявок на будущее
        if payload.consultation.scheduled_at:
            settings = get_settings()
            max_future_date = datetime.now(timezone.utc) + timedelta(days=settings.MAX_FUTURE_CONSULTATION_DAYS)
            
            # Нормализуем scheduled_at к UTC (если нет timezone, считаем что это UTC)
            scheduled_at_utc = payload.consultation.scheduled_at
            if scheduled_at_utc.tzinfo is None:
                # Если нет timezone, добавляем UTC
                scheduled_at_utc = scheduled_at_utc.replace(tzinfo=timezone.utc)
            else:
                # Если есть timezone, конвертируем в UTC
                scheduled_at_utc = scheduled_at_utc.astimezone(timezone.utc)
            
            if scheduled_at_utc > max_future_date:
                raise HTTPException(
                    status_code=400,
                    detail=f"Нельзя создавать консультации более чем на {settings.MAX_FUTURE_CONSULTATION_DAYS} дней вперед. "
                           f"Максимальная дата: {max_future_date.strftime('%Y-%m-%d %H:%M:%S')}"
                )
        
        # Проверка: максимум 3 консультации в день по code_abonent для всех user_id этого абонента
        if consultation_type == "Консультация по ведению учёта" and payload.consultation.scheduled_at:
            await _check_consultation_limit(
                db,
                code_abonent=owner_client_code_abonent,
                org_inn=owner_client_org_inn,
                consultation_date=payload.consultation.scheduled_at
            )

        # 2. Обрабатываем Telegram пользователя если передан
        telegram_user_id = payload.telegram_user_id
        telegram_phone_number = payload.telegram_phone_number
        
        # Автоматически определяем Telegram Web App по User-Agent или другим признакам
        user_agent = request.headers.get("user-agent", "").lower()
        referer = request.headers.get("referer", "").lower()
        origin = request.headers.get("origin", "").lower()
        
        # Проверяем все возможные признаки Telegram Web App
        is_telegram_webapp = (
            "telegram" in user_agent or 
            "telegram" in referer or
            "telegram" in origin or
            telegram_user_id is not None or
            payload.source == "TELEGRAM"  # Если фронтенд явно указал source
        )
        
        logger.info(f"Telegram Web App detection: user_agent={user_agent[:50]}, referer={referer[:50]}, origin={origin[:50]}, is_telegram_webapp={is_telegram_webapp}, payload.source={payload.source}")
        
        # Если это Telegram Web App, но telegram_user_id не передан, пытаемся найти по номеру телефона
        if is_telegram_webapp and not telegram_user_id and client.phone_number:
            try:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.phone_number == client.phone_number)
                )
                telegram_user_by_phone = result.scalar_one_or_none()
                if telegram_user_by_phone:
                    telegram_user_id = telegram_user_by_phone.telegram_user_id
                    telegram_phone_number = telegram_user_by_phone.phone_number or client.phone_number
                    logger.info(f"Found Telegram user by phone_number: {telegram_user_id} for client {client.client_id}")
            except Exception as e:
                logger.warning(f"Failed to find Telegram user by phone_number: {e}", exc_info=True)
        
        # Определяем источник создания
        # Если это Telegram Web App или передан telegram_user_id, устанавливаем source = "TELEGRAM"
        if is_telegram_webapp or telegram_user_id or payload.source == "TELEGRAM":
            source = "TELEGRAM"
        else:
            source = payload.source or "BACKEND"
        
        logger.info(f"Consultation source: {source}, telegram_user_id: {telegram_user_id}, is_telegram_webapp: {is_telegram_webapp}, payload.source: {payload.source}")
        
        # Связываем Telegram пользователя с клиентом если передан telegram_user_id или найден по телефону
        if telegram_user_id:
            try:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.telegram_user_id == telegram_user_id)
                )
                telegram_user = result.scalar_one_or_none()
                
                if telegram_user:
                    # Обновляем существующего пользователя
                    telegram_user.client_id = client.client_id
                    if telegram_phone_number:
                        telegram_user.phone_number = telegram_phone_number
                else:
                    # Создаем нового пользователя
                    telegram_user = TelegramUser(
                        telegram_user_id=telegram_user_id,
                        client_id=client.client_id,
                        phone_number=telegram_phone_number
                    )
                    db.add(telegram_user)
                
                await db.flush()
                logger.info(f"Linked Telegram user {telegram_user_id} with client {client.client_id}")
            except Exception as e:
                logger.warning(f"Failed to link Telegram user: {e}", exc_info=True)
                # Продолжаем создание консультации даже если не удалось связать пользователя
        
        # 3. Создаем консультацию в БД
        # ВАЖНО: Используем транзакцию для атомарности операций
        # При ошибке в Chatwoot/1C - откатываем транзакцию
        temp_cons_id = f"temp_{uuid.uuid4()}"
        consultation = Consultation(
            cons_id=temp_cons_id,
            client_id=client.client_id,
            client_key=client_key,
            cl_ref_key=payload.consultation.cl_ref_key,
            org_inn=owner_client_org_inn,
            lang=payload.consultation.lang or "ru",
            consultation_type=payload.consultation.consultation_type,
            comment=payload.consultation.comment or "",
            online_question_cat=normalize_uuid(payload.consultation.online_question_cat),
            online_question=normalize_uuid(payload.consultation.online_question),
            importance=payload.consultation.importance,
            start_date=payload.consultation.scheduled_at,
            status="open",
            # ВАЖНО: Для технической поддержки менеджер не устанавливается - он будет назначен через Chatwoot webhook
            manager=None if consultation_type == "Техническая поддержка" else selected_manager_key,
            source=source,  # Указываем источник создания (TELEGRAM, SITE, BACKEND)
        )
        db.add(consultation)
        await db.flush()
        
        # Формируем custom_attrs с учетом созданной consultation
        custom_attrs = await _build_chatwoot_custom_attrs(db, owner_client, payload, consultation=consultation)
        
        # Валидация обязательного поля для Chatwoot
        # Если code_abonent пустой, используем дефолтное значение
        # ВАЖНО: это поле должно быть непустым, иначе Chatwoot может вернуть ошибку
        code_abonent_value = custom_attrs.get("code_abonent")
        if not code_abonent_value or code_abonent_value == "":
            logger.warning(f"code_abonent is empty for client {owner_client_id}, using default 'N/A'")
            custom_attrs["code_abonent"] = "N/A"  # Дефолтное значение вместо пустой строки
        
        # Логируем финальные custom_attrs перед отправкой
        logger.info(f"Final custom_attrs for Chatwoot: {custom_attrs}")
        
        # 4. Отправляем в Chatwoot и 1C
        # Отслеживаем успех создания хотя бы в одной системе
        chatwoot_success = False
        onec_success = False
        
        chatwoot_client = ChatwootClient()
        chatwoot_cons_id = None
        # ВАЖНО: source_id для conversation - это UUID, который МЫ генерируем на нашей стороне
        # Это уникальный идентификатор беседы для идемпотентности (чтобы Chatwoot не создавал дубликаты)
        # source_id НЕ берется из contact_inboxes - это разные идентификаторы
        conversation_source_id = str(uuid.uuid4())  # Генерируем UUID для conversation
        logger.info(f"Generated conversation source_id (UUID): {conversation_source_id} for consultation")
        
        chatwoot_source_id = None  # source_id для подключения виджета (будет установлен после создания conversation)
        pubsub_token = None  # pubsub_token для WebSocket подключения (будет извлечен из Public API ответа)
        
        try:
            from ..config import settings
            
            # 3. Создаем conversation в Chatwoot
            # ВАЖНО: Application API Chatwoot не позволяет создавать contacts напрямую
            # Поэтому создаем conversation только с source_id - Chatwoot автоматически создаст contact
            logger.info(
                f"Creating Chatwoot conversation: source_id={client.client_id}, "
                f"inbox_id={settings.CHATWOOT_INBOX_ID}, custom_attrs={custom_attrs}"
            )
            
            # Маппинг importance в priority (типовое поле Chatwoot)
            priority = _map_importance_to_priority(payload.consultation.importance)
            
            # Формируем labels для language, source и consultation_type (типовое поле Chatwoot)
            consultation_type_for_labels = None
            if consultation and consultation.consultation_type:
                consultation_type_for_labels = consultation.consultation_type
            elif payload.consultation.consultation_type:
                consultation_type_for_labels = payload.consultation.consultation_type
            
            labels = _build_chatwoot_labels(
                language=payload.consultation.lang,
                source=source,  # Используем определенный выше source (TELEGRAM, SITE, BACKEND)
                consultation_type=consultation_type_for_labels
            )
            
            # Подготавливаем данные контакта (используются для поиска, создания контакта и conversation)
            contact_name = client.name or client.contact_name or owner_client.name or owner_client.contact_name or "Клиент"
            contact_email = client.email or owner_client.email
            contact_phone = client.phone_number or owner_client.phone_number
            
            # Валидируем email перед использованием
            if contact_email and not is_valid_email(contact_email):
                logger.warning(f"Invalid email format '{contact_email}' for client {client.client_id}, skipping email field")
                contact_email = None  # Не отправляем невалидный email
            
            # Разделяем custom_attributes на contact и conversation согласно ТЗ
            from ..routers.clients import _build_chatwoot_contact_additional_attrs
            contact_custom_attrs = _build_chatwoot_contact_custom_attrs(owner_client, client)
            contact_additional_attrs = _build_chatwoot_contact_additional_attrs(owner_client, client)
            conversation_custom_attrs = custom_attrs
            
            # ВАЖНО: Используем source_id из БД клиента, если он уже есть
            # Контакт должен быть создан при создании клиента, и source_id сохранен в БД
            contact_id = None
            contact_source_id = None
            
            # Проверяем, есть ли source_id в БД клиента
            if client.source_id:
                logger.info(f"Using source_id from DB for client {client.client_id}: {client.source_id}")
                contact_source_id = client.source_id
                
                # Проверяем, что contact существует в Chatwoot (получаем contact_id)
                try:
                    existing_contact = await chatwoot_client.find_contact_by_identifier(str(client.client_id))
                    if existing_contact:
                        contact_id = existing_contact.get("id")
                        logger.info(f"Found existing Chatwoot contact by identifier: {contact_id} for client {client.client_id}")
                        
                        # ВАЖНО: Получаем pubsub_token для существующего contact через Public API
                        if not client.chatwoot_pubsub_token:
                            try:
                                logger.info(f"Getting pubsub_token for existing contact (found by identifier) via Public API: source_id={client.source_id}")
                                contact_public_data = await chatwoot_client.get_contact_via_public_api(client.source_id)
                                
                                existing_pubsub_token = chatwoot_client._extract_pubsub_token(contact_public_data)
                                if existing_pubsub_token:
                                    client.chatwoot_pubsub_token = existing_pubsub_token
                                    await db.flush()
                                    logger.info(f"✓ Retrieved pubsub_token for existing contact (found by identifier): {existing_pubsub_token[:20]}...")
                                else:
                                    logger.warning(f"⚠ pubsub_token not found in Public API response for existing contact (found by identifier)")
                            except Exception as get_pubsub_error:
                                logger.warning(f"Failed to get pubsub_token for existing contact (found by identifier) via Public API: {get_pubsub_error}")
                    else:
                        logger.warning(f"source_id exists in DB but contact not found in Chatwoot for client {client.client_id}")
                        # source_id может быть устаревшим, нужно обновить
                        contact_source_id = None
                except Exception as contact_error:
                    logger.error(f"Failed to verify contact in Chatwoot: {contact_error}", exc_info=True)
                    contact_source_id = None
            
            # Если source_id нет в БД или contact не найден, ищем существующий contact
            if not contact_source_id:
                try:
                    # Ищем контакт по identifier (client_id UUID)
                    existing_contact = await chatwoot_client.find_contact_by_identifier(str(client.client_id))
                    if existing_contact:
                        contact_id = existing_contact.get("id")
                        logger.info(f"Found existing Chatwoot contact by identifier: {contact_id} for client {client.client_id}")
                        
                        # Извлекаем source_id из существующего contact
                        contact_inboxes = existing_contact.get("contact_inboxes", [])
                        if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                            for ci in contact_inboxes:
                                if ci.get("inbox_id") == settings.CHATWOOT_INBOX_ID:
                                    contact_source_id = ci.get("source_id")
                                    break
                            if not contact_source_id and len(contact_inboxes) > 0:
                                contact_source_id = contact_inboxes[0].get("source_id")
                        
                        # Сохраняем source_id в БД клиента для будущего использования
                        if contact_source_id:
                            client.source_id = contact_source_id
                            await db.flush()
                            logger.info(f"✓ Saved source_id to DB: {contact_source_id} for client {client.client_id}")
                    else:
                        logger.warning(f"Contact not found in Chatwoot for client {client.client_id}. Contact should be created when client is created.")
                        # Пробуем найти по email или phone как fallback
                        
                        if contact_email:
                            existing_contact = await chatwoot_client.find_contact_by_email(contact_email)
                            if existing_contact:
                                contact_id = existing_contact.get("id")
                                logger.info(f"Found existing Chatwoot contact by email: {contact_id}")
                                
                                # Извлекаем source_id
                                contact_inboxes = existing_contact.get("contact_inboxes", [])
                                if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                                    for ci in contact_inboxes:
                                        if ci.get("inbox_id") == settings.CHATWOOT_INBOX_ID:
                                            contact_source_id = ci.get("source_id")
                                            break
                                    if not contact_source_id and len(contact_inboxes) > 0:
                                        contact_source_id = contact_inboxes[0].get("source_id")
                                
                                if contact_source_id:
                                    client.source_id = contact_source_id
                                    
                                    # ВАЖНО: Получаем pubsub_token для существующего contact через Public API
                                    if not client.chatwoot_pubsub_token:
                                        try:
                                            logger.info(f"Getting pubsub_token for existing contact (found by email) via Public API: source_id={contact_source_id}")
                                            contact_public_data = await chatwoot_client.get_contact_via_public_api(
                                                source_id=contact_source_id
                                            )
                                            
                                            existing_pubsub_token = chatwoot_client._extract_pubsub_token(contact_public_data)
                                            if existing_pubsub_token:
                                                client.chatwoot_pubsub_token = existing_pubsub_token
                                                logger.info(f"✓ Retrieved pubsub_token for existing contact (found by email): {existing_pubsub_token[:20]}...")
                                            else:
                                                logger.warning(f"⚠ pubsub_token not found in Public API response for existing contact (found by email)")
                                        except Exception as get_pubsub_error:
                                            logger.warning(f"Failed to get pubsub_token for existing contact (found by email) via Public API: {get_pubsub_error}")
                                    
                                    await db.flush()
                        
                        if not contact_id and contact_phone:
                            existing_contact = await chatwoot_client.find_contact_by_phone(contact_phone)
                            if existing_contact:
                                contact_id = existing_contact.get("id")
                                logger.info(f"Found existing Chatwoot contact by phone: {contact_id}")
                                
                                # Извлекаем source_id
                                contact_inboxes = existing_contact.get("contact_inboxes", [])
                                if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                                    for ci in contact_inboxes:
                                        if ci.get("inbox_id") == settings.CHATWOOT_INBOX_ID:
                                            contact_source_id = ci.get("source_id")
                                            break
                                    if not contact_source_id and len(contact_inboxes) > 0:
                                        contact_source_id = contact_inboxes[0].get("source_id")
                                
                                if contact_source_id:
                                    client.source_id = contact_source_id
                                    
                                    # ВАЖНО: Получаем pubsub_token для существующего contact через Public API
                                    if not client.chatwoot_pubsub_token:
                                        try:
                                            logger.info(f"Getting pubsub_token for existing contact (found by phone) via Public API: source_id={contact_source_id}")
                                            contact_public_data = await chatwoot_client.get_contact_via_public_api(
                                                source_id=contact_source_id
                                            )
                                            
                                            existing_pubsub_token = chatwoot_client._extract_pubsub_token(contact_public_data)
                                            if existing_pubsub_token:
                                                client.chatwoot_pubsub_token = existing_pubsub_token
                                                logger.info(f"✓ Retrieved pubsub_token for existing contact (found by phone): {existing_pubsub_token[:20]}...")
                                            else:
                                                logger.warning(f"⚠ pubsub_token not found in Public API response for existing contact (found by phone)")
                                        except Exception as get_pubsub_error:
                                            logger.warning(f"Failed to get pubsub_token for existing contact (found by phone) via Public API: {get_pubsub_error}")
                                    
                                    await db.flush()
                        
                        if not contact_id:
                            logger.warning(f"No contact found in Chatwoot for client {client.client_id}. Will create contact before conversation.")
                except Exception as contact_error:
                    logger.error(f"Failed to find contact in Chatwoot: {contact_error}", exc_info=True)
                    # Продолжаем - попробуем создать контакт явно
                    contact_id = None
            
            # ВАЖНО: Если контакт не найден, создаем его явно перед созданием conversation
            # Это более надежный подход, чем полагаться на автоматическое создание через payload
            if not contact_id:
                try:
                    logger.info(f"Contact not found, creating new contact in Chatwoot for client {client.client_id}")
                    # Данные контакта уже подготовлены выше
                    
                    # Проверяем, что есть хотя бы email или phone для создания контакта
                    if contact_email or contact_phone:
                        # ВАЖНО: Используем Public API для создания contact, чтобы получить pubsub_token
                        try:
                            new_contact = await chatwoot_client.create_contact_via_public_api(
                                name=contact_name,
                                identifier=str(client.client_id),  # Глобальный внешний ID (UUID)
                                email=contact_email,
                                phone_number=contact_phone,
                                custom_attributes=contact_custom_attrs,
                                additional_attributes=contact_additional_attrs
                            )
                            
                            # Извлекаем contact_id из ответа Public API
                            contact_id = new_contact.get("id")
                            if not contact_id:
                                contact_id = new_contact.get("payload", {}).get("contact", {}).get("id") if isinstance(new_contact.get("payload"), dict) else None
                            
                            # Извлекаем source_id из ответа Public API
                            # source_id создается автоматически Chatwoot при создании contact через Public API
                            new_contact_source_id = chatwoot_client._extract_source_id(
                                new_contact,
                                inbox_id=settings.CHATWOOT_INBOX_ID
                            )
                            
                            if new_contact_source_id:
                                logger.info(f"✓ Extracted source_id from Public API response: {new_contact_source_id}")
                            else:
                                # Если source_id не найден в ответе создания, получаем contact через GET для извлечения source_id
                                if contact_id:
                                    try:
                                        logger.info(f"source_id not found in create response, fetching contact {contact_id} to get source_id")
                                        fetched_contact = await chatwoot_client.get_contact(contact_id)
                                        
                                        # Извлекаем source_id из ответа GET запроса Application API
                                        new_contact_source_id = chatwoot_client._extract_source_id(
                                            fetched_contact,
                                            inbox_id=settings.CHATWOOT_INBOX_ID
                                        )
                                        
                                        if new_contact_source_id:
                                            logger.info(f"✓ Retrieved source_id from GET contact: {new_contact_source_id}")
                                    except Exception as get_contact_error:
                                        logger.warning(f"Failed to get contact {contact_id} to extract source_id: {get_contact_error}")
                            
                            # Извлекаем pubsub_token из ответа создания contact
                            # ВАЖНО: pubsub_token принадлежит контакту (Contact), а не беседе (Conversation)
                            logger.info(f"=== Extracting pubsub_token from contact creation response ===")
                            logger.info(f"  Contact response keys: {list(new_contact.keys()) if isinstance(new_contact, dict) else 'not a dict'}")
                            
                            contact_pubsub_token = chatwoot_client._extract_pubsub_token(new_contact)
                            if contact_pubsub_token:
                                logger.info(f"✓ Extracted pubsub_token from contact creation response: {contact_pubsub_token[:20]}...")
                                # Сохраняем pubsub_token в БД клиента (он принадлежит контакту)
                                client.chatwoot_pubsub_token = contact_pubsub_token
                                pubsub_token = contact_pubsub_token  # Используем для ответа
                                logger.info(f"✓ Saved pubsub_token to client DB: {contact_pubsub_token[:20]}...")
                            else:
                                logger.warning(f"⚠ pubsub_token not found in contact creation response")
                                # Логируем полную структуру ответа для отладки
                                import json
                                logger.warning(f"  Full contact response structure: {json.dumps(new_contact, ensure_ascii=False, indent=2)}")
                            
                            # Сохраняем source_id в БД клиента
                            if new_contact_source_id:
                                contact_source_id = new_contact_source_id
                                client.source_id = new_contact_source_id
                                await db.flush()
                                logger.info(f"✓ Created Chatwoot contact via Public API: {contact_id}, source_id: {new_contact_source_id} for client {client.client_id}")
                                logger.info(f"✓ Saved source_id to DB: {new_contact_source_id} for client {client.client_id}")
                            else:
                                logger.warning(f"Created Chatwoot contact {contact_id} but source_id not found in response or get_contact for client {client.client_id}")
                                logger.warning(f"Response structure: {list(new_contact.keys()) if isinstance(new_contact, dict) else 'not a dict'}")
                                logger.warning("source_id will remain null - frontend will handle this case")
                            
                            if not contact_id:
                                logger.warning(f"Failed to extract contact_id from Chatwoot response: {new_contact}")
                        except httpx.HTTPStatusError as http_error:
                            # Обработка ошибки 422 - контакт уже существует
                            if http_error.response.status_code == 422:
                                logger.warning(f"Contact already exists in Chatwoot (422), trying to find existing contact for client {client.client_id}")
                                
                                # Пытаемся найти существующий contact
                                existing_contact = None
                                if str(client.client_id):
                                    existing_contact = await chatwoot_client.find_contact_by_identifier(str(client.client_id))
                                
                                if not existing_contact and contact_email:
                                    existing_contact = await chatwoot_client.find_contact_by_email(contact_email)
                                
                                if not existing_contact and contact_phone:
                                    existing_contact = await chatwoot_client.find_contact_by_phone(contact_phone)
                                
                                if existing_contact:
                                    # Извлекаем contact_id из найденного контакта
                                    contact_id = existing_contact.get("id")
                                    if not contact_id:
                                        contact_id = existing_contact.get("payload", {}).get("contact", {}).get("id")
                                    
                                    # Извлекаем source_id из найденного contact
                                    # Используем универсальный метод для извлечения source_id
                                    contact_source_id = chatwoot_client._extract_source_id(
                                        existing_contact,
                                        inbox_id=settings.CHATWOOT_INBOX_ID
                                    )
                                    
                                    # Если source_id не найден в ответе find, получаем contact через GET
                                    if not contact_source_id and contact_id:
                                        try:
                                            logger.info(f"source_id not found in find response, fetching contact {contact_id} to get source_id")
                                            fetched_contact = await chatwoot_client.get_contact(contact_id)
                                            
                                            # Извлекаем source_id из ответа GET запроса Application API
                                            contact_source_id = chatwoot_client._extract_source_id(
                                                fetched_contact,
                                                inbox_id=settings.CHATWOOT_INBOX_ID
                                            )
                                            
                                            if contact_source_id:
                                                logger.info(f"✓ Retrieved source_id from GET contact: {contact_source_id}")
                                        except Exception as get_contact_error:
                                            logger.warning(f"Failed to get contact {contact_id} to extract source_id: {get_contact_error}")
                                    
                                    # Сохраняем source_id в БД клиента
                                    if contact_source_id:
                                        client.source_id = contact_source_id
                                        
                                        # ВАЖНО: Для существующего contact нужно получить pubsub_token через GET запрос к Public API
                                        # pubsub_token не возвращается в Application API ответе, только в Public API
                                        if not client.chatwoot_pubsub_token:
                                            try:
                                                logger.info(f"Getting pubsub_token for existing contact via Public API: source_id={contact_source_id}")
                                                contact_public_data = await chatwoot_client.get_contact_via_public_api(
                                                    source_id=contact_source_id
                                                )
                                                
                                                existing_pubsub_token = chatwoot_client._extract_pubsub_token(contact_public_data)
                                                if existing_pubsub_token:
                                                    client.chatwoot_pubsub_token = existing_pubsub_token
                                                    pubsub_token = existing_pubsub_token
                                                    logger.info(f"✓ Retrieved pubsub_token for existing contact: {existing_pubsub_token[:20]}...")
                                                else:
                                                    logger.warning(f"⚠ pubsub_token not found in Public API response for existing contact")
                                                    import json
                                                    logger.warning(f"  Full Public API response: {json.dumps(contact_public_data, ensure_ascii=False, indent=2)}")
                                            except Exception as get_pubsub_error:
                                                logger.warning(f"Failed to get pubsub_token for existing contact via Public API: {get_pubsub_error}")
                                                # Продолжаем без pubsub_token - frontend может получить его сам
                                        else:
                                            pubsub_token = client.chatwoot_pubsub_token
                                            logger.info(f"✓ Using existing pubsub_token from client DB: {pubsub_token[:20]}...")
                                        
                                        await db.flush()
                                        logger.info(f"✓ Found existing Chatwoot contact: {contact_id}, source_id: {contact_source_id} for client {client.client_id}")
                                        logger.info(f"✓ Saved source_id to DB: {contact_source_id} for client {client.client_id}")
                                    else:
                                        logger.warning(f"Found existing contact {contact_id} but source_id not found for client {client.client_id}")
                                else:
                                    logger.error(f"Contact exists (422) but cannot be found by identifier/email/phone for client {client.client_id}")
                                    raise ValueError("Contact exists but cannot be found")
                            else:
                                # Другие HTTP ошибки - пробрасываем дальше
                                raise
                    else:
                        logger.warning(f"Cannot create contact: no valid email or phone for client {client.client_id}")
                except Exception as create_contact_error:
                    logger.error(f"Failed to create contact in Chatwoot: {create_contact_error}", exc_info=True)
                    # Продолжаем без contact_id - попробуем создать conversation с объектом contact (fallback)
                    contact_id = None
            
            
            # Назначаем менеджера в Chatwoot (если выбран)
            assignee_id = None
            if selected_manager_key:
                # Ищем chatwoot_user_id через user_mapping или напрямую в users
                mapping_result = await db.execute(
                    select(UserMapping).where(UserMapping.cl_manager_key == selected_manager_key).limit(1)
                )
                mapping = mapping_result.scalar_one_or_none()
                if mapping:
                    assignee_id = mapping.chatwoot_user_id
                    logger.info(f"Mapped manager {selected_manager_key} to Chatwoot user {assignee_id}")
                else:
                    # Пробуем найти через users
                    user_result = await db.execute(
                        select(User).where(
                            User.cl_ref_key == selected_manager_key,
                            User.deletion_mark == False,
                            User.invalid == False
                        ).limit(1)
                    )
                    user = user_result.scalar_one_or_none()
                    if user and user.chatwoot_user_id:
                        assignee_id = user.chatwoot_user_id
                        logger.info(f"Found Chatwoot user {assignee_id} for manager {selected_manager_key}")
                    else:
                        # Менеджер не найден в Chatwoot - возможно, не синхронизирован
                        logger.warning(
                            f"Manager {selected_manager_key} not found in Chatwoot. "
                            f"User exists: {user is not None}, has chatwoot_user_id: {user.chatwoot_user_id if user else None}. "
                            f"Conversation will be created without assignee. "
                            f"Please run sync_users_to_chatwoot.py to sync this user."
                        )
            
            # Определяем команду (team) в зависимости от consultation_type
            # ВАЖНО: Сначала делаем GET команд, сравниваем что подходит, обновляем названия при необходимости
            team_id = None
            consultation_type = payload.consultation.consultation_type
            if consultation_type == "Консультация по ведению учёта":
                # Ищем команду с любым похожим названием и обновляем до правильного
                team_id = await chatwoot_client.find_team_by_name(
                    team_name="консультация по ведению учета",  # Для поиска
                    expected_name="консультация по ведению учета"  # Ожидаемое название
                )
                if not team_id:
                    logger.warning(f"Team 'консультация по ведению учета' not found in Chatwoot, conversation will be created without team")
            elif consultation_type == "Техническая поддержка":
                # Ищем команду с любым похожим названием и обновляем до правильного
                team_id = await chatwoot_client.find_team_by_name(
                    team_name="техническая поддержка",  # Для поиска
                    expected_name="техническая поддержка"  # Ожидаемое название
                )
                if not team_id:
                    logger.warning(f"Team 'техническая поддержка' not found in Chatwoot, conversation will be created without team")
            
            # ВАЖНО: Создание заявки/консультации/беседы происходит по Public API
            # После создания нужно правильно обновить лейблами, агентом и командой
            if not contact_source_id:
                raise ValueError("contact_source_id is required for creating conversation via Public API")
            
            logger.info(f"=== Creating conversation via Public API ===")
            logger.info(f"  Contact source_id: {contact_source_id}")
            logger.info(f"  Inbox identifier: {settings.CHATWOOT_INBOX_IDENTIFIER}")
            logger.info(f"  Message preview: {(payload.consultation.comment or '')[:100]}")
            
            chatwoot_response = await chatwoot_client.create_conversation_via_public_api(
                source_id=contact_source_id,
                message=payload.consultation.comment or "",
                custom_attributes=conversation_custom_attrs,
            )
            chatwoot_cons_id = str(chatwoot_response.get("id"))
            if not chatwoot_cons_id or chatwoot_cons_id == "None":
                chatwoot_cons_id = str(chatwoot_response.get("payload", {}).get("id", "")) if isinstance(chatwoot_response.get("payload"), dict) else None
                if not chatwoot_cons_id or chatwoot_cons_id == "None":
                    raise ValueError(f"Chatwoot returned invalid conversation ID: {chatwoot_response}")
            
            conversation_source_id_from_response = chatwoot_client._extract_source_id(
                chatwoot_response,
                inbox_id=settings.CHATWOOT_INBOX_ID
            )
            chatwoot_source_id = conversation_source_id_from_response if conversation_source_id_from_response else contact_source_id
            
            # ВАЖНО: После создания заявки назначаем команду и агента через assignments endpoint
            # Сначала команду, потом агента (раздельно)
            if team_id:
                try:
                    await chatwoot_client.assign_conversation_team(
                        conversation_id=chatwoot_cons_id,
                        team_id=team_id
                    )
                    logger.info(f"✓ Assigned team {team_id} to conversation {chatwoot_cons_id}")
                except Exception as team_error:
                    logger.warning(f"Failed to assign team to conversation: {team_error}")
            
            if assignee_id:
                try:
                    await chatwoot_client.assign_conversation_agent(
                        conversation_id=chatwoot_cons_id,
                        assignee_id=assignee_id
                    )
                    logger.info(f"✓ Assigned agent {assignee_id} to conversation {chatwoot_cons_id}")
                except Exception as agent_error:
                    logger.warning(f"Failed to assign agent to conversation: {agent_error}")
            
            # Добавляем labels отдельно (если нужно)
            if labels:
                try:
                    await chatwoot_client.add_conversation_labels(
                        conversation_id=chatwoot_cons_id,
                        labels=labels
                    )
                    logger.info(f"✓ Added labels to conversation {chatwoot_cons_id}")
                except Exception as labels_error:
                    logger.warning(f"Failed to add labels: {labels_error}")
            
            # ВАЖНО: pubsub_token НЕ возвращается в ответе создания conversation
            # pubsub_token возвращается ТОЛЬКО в ответе POST создания contact через Public API
            # pubsub_token уже сохранен в БД клиента при создании contact выше
            if client.chatwoot_pubsub_token:
                logger.info(f"✓ Using pubsub_token from client DB (saved during contact creation): {client.chatwoot_pubsub_token[:20]}...")
            else:
                logger.warning(f"⚠ pubsub_token not found in client DB - contact may not have been created via Public API or pubsub_token was not in response")
            
            # Сохраняем данные из ответа создания conversation
            # ВАЖНО: Проверяем, не существует ли уже консультация с таким cons_id
            # Если существует для того же клиента - возвращаем существующую (идемпотентность)
            # Если существует для другого клиента - это ошибка
            existing_consultation = await db.execute(
                select(Consultation).where(Consultation.cons_id == chatwoot_cons_id).limit(1)
            )
            existing = existing_consultation.scalar_one_or_none()
            
            if existing:
                # Консультация с таким cons_id уже существует
                # ВАЖНО: Сохраняем данные ДО rollback, так как после rollback existing будет недоступен
                existing_cons_id = existing.cons_id
                existing_client_id = str(existing.client_id)
                existing_status = existing.status
                
                # Проверяем, для того же ли клиента И не закрыта ли консультация
                if existing_client_id == str(owner_client_id):
                    # Проверяем статус - если консультация закрыта, создаем новую
                    closed_statuses = ["closed", "cancelled", "resolved"]
                    if existing_status in closed_statuses:
                        # Старая консультация закрыта - создаем новую
                        logger.info(
                            f"Consultation with cons_id={chatwoot_cons_id} exists but is {existing_status}. "
                            f"Creating new consultation for client {owner_client_id}."
                        )
                        # Продолжаем создание новой консультации (не возвращаем старую)
                    else:
                        # Консультация активна - это повторный запрос, возвращаем существующую
                        logger.info(
                            f"Consultation with cons_id={chatwoot_cons_id} already exists and is active for client {owner_client_id}. "
                            f"This is a duplicate request (idempotency). Returning existing consultation."
                        )
                        # Откатываем текущую транзакцию (мы не создавали новую консультацию)
                        await db.rollback()
                        # Загружаем существующую консультацию в новой сессии для ответа
                        from ..database import AsyncSessionLocal
                        async with AsyncSessionLocal() as new_db:
                            existing_loaded = await new_db.get(Consultation, existing_cons_id)
                            if existing_loaded:
                                # Загружаем связанные данные
                                manager_name = await _get_manager_name(new_db, existing_loaded.manager)
                                # Получаем pubsub_token из клиента
                                from ..models import Client
                                client_loaded = await new_db.get(Client, existing_loaded.client_id)
                                final_pubsub_token = None
                                if client_loaded and client_loaded.chatwoot_pubsub_token:
                                    if isinstance(client_loaded.chatwoot_pubsub_token, bytes):
                                        final_pubsub_token = client_loaded.chatwoot_pubsub_token.decode('utf-8')
                                    else:
                                        final_pubsub_token = str(client_loaded.chatwoot_pubsub_token)
                                # Формируем ответ с существующей консультацией
                                # ConsultationResponse и ConsultationRead уже импортированы в начале файла
                                from ..config import settings
                                response = ConsultationResponse(
                                    consultation=ConsultationRead.from_model(existing_loaded, manager_name=manager_name),
                                    client_id=str(existing_loaded.client_id),
                                    message="Consultation already exists (idempotency)",
                                    source=existing_loaded.source or "SITE",
                                    chatwoot_conversation_id=existing_loaded.cons_id,
                                    chatwoot_source_id=existing_loaded.chatwoot_source_id,
                                    chatwoot_account_id=str(settings.CHATWOOT_ACCOUNT_ID) if settings.CHATWOOT_ACCOUNT_ID else None,
                                    chatwoot_inbox_id=settings.CHATWOOT_INBOX_ID,
                                    chatwoot_pubsub_token=final_pubsub_token,
                                )
                                return response
                        # Если не удалось загрузить - продолжаем создание новой (fallback)
                        logger.warning(f"Failed to load existing consultation {chatwoot_cons_id}, continuing with new creation")
                else:
                    # Консультация существует, но для другого клиента - это ошибка
                    logger.error(
                        f"Consultation with cons_id={chatwoot_cons_id} already exists for different client. "
                        f"Existing client_id: {existing_client_id}, requested client_id: {owner_client_id}. "
                        f"This indicates a data inconsistency."
                    )
                    await db.rollback()
                    raise HTTPException(
                        status_code=409,
                        detail=f"Consultation with cons_id={chatwoot_cons_id} already exists for a different client. "
                               f"This indicates a data inconsistency. Please contact support."
                    )
            
            # Консультации с таким cons_id нет - безопасно обновляем
            consultation.cons_id = chatwoot_cons_id  # ID conversation из ответа
            consultation.chatwoot_source_id = chatwoot_source_id  # source_id из contact (для виджета)
            
            # ВАЖНО: pubsub_token принадлежит контакту и уже сохранен в БД клиента при создании contact
            # pubsub_token НЕ возвращается в ответе создания conversation
            # Используем pubsub_token из БД клиента
            if client.chatwoot_pubsub_token:
                logger.info(f"✓ Using pubsub_token from client DB (saved during contact creation): {client.chatwoot_pubsub_token[:20]}...")
            else:
                logger.warning(f"⚠ pubsub_token not found in client DB - contact may not have been created via Public API or pubsub_token was not in response")
            
            try:
                await db.flush()  # Сохраняем обновленный cons_id и source_id
            except IntegrityError as e:
                # Если все же возникла ошибка уникальности - это дубликат запроса
                await db.rollback()
                logger.error(
                    f"IntegrityError when updating cons_id to {chatwoot_cons_id}: {e}. "
                    f"This is a duplicate request. Consultation already exists."
                )
                # Возвращаем ошибку - фронтенд должен правильно обрабатывать кэш
                raise HTTPException(
                    status_code=409,
                    detail=f"Consultation with cons_id={chatwoot_cons_id} already exists. "
                           f"This is a duplicate request. Please check frontend cache."
                )
            chatwoot_success = True
            logger.info(f"✓ Created Chatwoot conversation via Public API: {chatwoot_cons_id}, source_id: {chatwoot_source_id}, contact_id: {contact_id}")
        except HTTPException:
            # Пробрасываем HTTPException как есть (это наша ошибка дубликата)
            raise
        except Exception as e:
            logger.error(
                f"✗ Failed to create Chatwoot conversation: {e}",
                exc_info=True
            )
            # ВАЖНО: Если Chatwoot упал, conversation НЕ создана
            # chatwoot_cons_id остается None, и это будет возвращено в ответе API
            # Фронтенд должен проверить chatwoot_conversation_id перед попыткой найти conversation
            chatwoot_cons_id = None  # Явно устанавливаем None, чтобы не было случайных значений
            logger.warning(
                f"⚠ Chatwoot conversation creation failed. "
                f"chatwoot_cons_id is None. "
                f"Frontend will receive chatwoot_conversation_id=None in API response. "
                f"Frontend should NOT attempt to find conversation by ID."
            )
            
            # ВАЖНО: Если Chatwoot упал, используем source_id из contact (если есть)
            # Для Public API source_id должен быть из contact, а не UUID conversation
            if not chatwoot_source_id:
                # Используем source_id из contact для будущей попытки создания через Public API
                chatwoot_source_id = contact_source_id if contact_source_id else None
                if chatwoot_source_id:
                    logger.warning(f"Chatwoot conversation creation failed. Will use contact source_id: {chatwoot_source_id} for retry")
                else:
                    logger.warning(f"Chatwoot conversation creation failed and no contact_source_id available")
            
            # Сохраняем source_id для будущего использования
            # Если conversation создана - это source_id из ответа или contact_source_id
            # Если не создана - это contact_source_id (для будущей попытки создания через Public API)
            # ВАЖНО: Проверяем, что consultation все еще в сессии перед обращением к нему
            try:
                consultation.chatwoot_source_id = chatwoot_source_id
                await db.flush()
            except Exception as flush_error:
                # Если consultation не persistent (после rollback), просто логируем
                logger.warning(f"Failed to update consultation.chatwoot_source_id after Chatwoot error: {flush_error}")
            # Продолжаем - попробуем создать в 1C
        
        # 4. Отправляем в 1C:ЦЛ через OData
        # ВАЖНО: Отправляем в ЦЛ только консультации с типом "Консультация по ведению учёта"
        consultation_type = payload.consultation.consultation_type
        should_send_to_cl = consultation_type == "Консультация по ведению учёта"
        
        logger.info(f"=== Preparing to send consultation to 1C ===")
        logger.info(f"  consultation_type: {consultation_type}")
        logger.info(f"  should_send_to_cl: {should_send_to_cl}")
        logger.info(f"  client_key: {client_key}")
        logger.info(f"  owner_client.cl_ref_key: {client_key}")
        logger.info(f"  owner_client.client_id: {owner_client_id}")
        logger.info(f"  owner_client.org_inn: {owner_client_org_inn}")
        logger.info(f"  owner_client.code_abonent: {owner_client_code_abonent}")
        
        if client_key and should_send_to_cl:
            try:
                # Проверяем лимит консультаций в бэкенде перед отправкой в ЦЛ
                # ВАЖНО: Приоритетно проверяем по code_abonent (выдается системой и не может быть изменен),
                # если код абонента отсутствует - проверяем по ИНН (fallback для случаев создания через
                # колл-центр или расширение 1С, когда клиент уже существует в ЦЛ, но еще не синхронизирован в БД)
                if payload.consultation.scheduled_at:
                    try:
                        await _check_consultation_limit(
                            db=db,
                            code_abonent=owner_client_code_abonent,
                            org_inn=owner_client_org_inn,
                            consultation_date=payload.consultation.scheduled_at,
                        )
                        identifier = owner_client_code_abonent or owner_client_org_inn
                        identifier_type = "code_abonent" if owner_client_code_abonent else "org_inn"
                        logger.info(f"✓ Consultation limit check passed for {identifier_type}: {identifier}")
                    except HTTPException:
                        # Пробрасываем HTTPException как есть (это наш лимит)
                        raise
                    except Exception as limit_check_error:
                        # Если проверка лимита упала с другой ошибкой, логируем и продолжаем
                        # (не блокируем создание консультации из-за ошибки проверки)
                        logger.warning(
                            f"Failed to check consultation limit by INN: {limit_check_error}. "
                            f"Proceeding with 1C creation (1C will check limit anyway)."
                        )
                
                # ВАЖНО: Для технической поддержки менеджер не назначается - он сам назначит себя в Chatwoot
                # Для консультаций по ведению учета используем выбранного менеджера (или дефолтного если не выбран)
                if consultation_type == "Техническая поддержка":
                    manager_key = None  # Не назначаем менеджера для технической поддержки
                    logger.info("Техническая поддержка: менеджер не назначается, будет назначен через Chatwoot")
                else:
                    manager_key = selected_manager_key or await _get_default_manager_key(db, consultation_type=consultation_type)
                    if not manager_key:
                        logger.warning("No manager found, consultation will be created without manager_key")
                
                # СпособСвязи - пока используем маппинг из source (TODO: добавить preferred_contact_method в Client)
                from ..services.onec_client import map_source_to_contact_method
                contact_method = map_source_to_contact_method(payload.source) if payload.source else "ПоТелефону"
                
                # Название клиента для АбонентПредставление
                # ВАЖНО: Используем сохраненные значения, а не обращаемся к owner_client напрямую
                # (после возможного rollback owner_client может быть не доступен)
                base_name = owner_client_company_name or owner_client_name or owner_client_contact_name or "Клиент"
                client_display_name_parts = ["Clobus", base_name]
                if owner_client_code_abonent:
                    client_display_name_parts.append(owner_client_code_abonent)
                if owner_client_org_inn:
                    client_display_name_parts.append(f"({owner_client_org_inn})")
                client_display_name = " ".join(client_display_name_parts)
                
                # Валидация перед отправкой в 1C
                if not client_key or len(client_key) != 36 or client_key.count("-") != 4:
                    raise ValueError(f"Invalid client_key format: '{client_key}'. Must be a valid GUID.")
                
                if manager_key and (len(manager_key) != 36 or manager_key.count("-") != 4):
                    logger.warning(f"Invalid manager_key format: '{manager_key}', proceeding without manager")
                    manager_key = None
                
                logger.info(f"Creating 1C consultation: client_key={client_key}, manager_key={manager_key}, client_display_name={client_display_name}")
                
                onec_response = await onec_client.create_consultation_odata(
                    client_key=client_key,
                    manager_key=manager_key,  # Менеджер из БД
                    description=payload.consultation.comment or "",
                    topic=payload.consultation.topic,
                    scheduled_at=payload.consultation.scheduled_at,
                    question_category_key=normalize_uuid(payload.consultation.online_question_cat),  # Нормализуем пустые UUID
                    question_key=normalize_uuid(payload.consultation.online_question),  # Нормализуем пустые UUID
                    language_code=payload.consultation.lang,
                    contact_method=contact_method,
                    contact_hint=contact_hint,
                    client_display_name=client_display_name,
                    importance=payload.consultation.importance,
                    comment=payload.consultation.comment,
                    db_session=db,  # Передаем сессию БД для поиска автора по имени
                )
                
                # Проверяем, что ответ от 1C содержит обязательные поля
                if not onec_response:
                    raise ValueError("1C returned empty response")
                
                if "Ref_Key" not in onec_response:
                    logger.warning(f"1C response missing Ref_Key: {onec_response}")
                else:
                    logger.debug(f"1C consultation created with Ref_Key: {onec_response.get('Ref_Key')}")
                
                # Обрабатываем полный ответ от 1C и сохраняем все важные поля
                await _process_onec_response(consultation, onec_response)
                await db.flush()  # Сохраняем данные из 1C
                
                # Проверяем, что cl_ref_key сохранен
                if not consultation.cl_ref_key:
                    logger.warning("cl_ref_key was not set from 1C response, consultation may not be properly synced")
                
                # Обновляем custom_attrs с номером из 1C
                if consultation.number:
                    custom_attrs["number_con"] = str(consultation.number)
                    # Обновляем в Chatwoot если conversation уже создан
                    if chatwoot_success and chatwoot_cons_id:
                        try:
                            # Обновляем custom_attributes в Chatwoot с номером консультации
                            await chatwoot_client.update_conversation(
                                conversation_id=chatwoot_cons_id,
                                custom_attributes={"number_con": str(consultation.number)}
                            )
                            logger.info(f"Updated Chatwoot conversation {chatwoot_cons_id} with 1C number: {consultation.number}")
                        except Exception as e:
                            logger.warning(f"Failed to update Chatwoot conversation with 1C number: {e}")
                
                onec_success = True
                logger.info(f"✓ Created 1C consultation: cl_ref_key={consultation.cl_ref_key}, number={consultation.number}, client_key={client_key}")
            except ConsultationLimitExceeded as e:
                # Специальная обработка ошибки превышения лимита консультаций
                logger.error(f"✗ Consultation limit exceeded in 1C: {e}")
                # Пробрасываем исключение дальше, чтобы вернуть понятное сообщение пользователю
                raise HTTPException(
                    status_code=429,  # Too Many Requests
                    detail=str(e)
                ) from e
            except Exception as e:
                logger.error(f"✗ Failed to create 1C consultation: {e}", exc_info=True)
        else:
            logger.warning("⚠ Skipping 1C consultation creation because client is not synced with 1C yet.")
            logger.warning(f"  Owner client {owner_client_id} has no cl_ref_key (client_key is None or empty)")
            logger.warning(f"  This means the client was not created in 1C or cl_ref_key was not saved to DB")
            logger.warning(f"  Owner client details: org_inn={owner_client_org_inn}, code_abonent={owner_client_code_abonent}, name={owner_client_name}")
        
        # ВАЖНО: Консультация всегда сохраняется в БД, даже если внешние сервисы недоступны
        # Это позволяет системе продолжать работать при проблемах с внешними сервисами
        # Синхронизация с внешними сервисами будет выполнена позже через retry механизм
        
        if not chatwoot_success and not onec_success:
            # Обе системы упали - сохраняем консультацию в БД с предупреждением
            logger.warning("Both Chatwoot and 1C failed to create consultation. Consultation will be saved in DB and synced later.")
            logger.warning("External services are unavailable, but consultation data is preserved in database.")
            
            # Генерируем UUID для консультации, если еще не создан
            if consultation.cons_id.startswith("temp_"):
                consultation.cons_id = str(uuid.uuid4())
                await db.flush()
                logger.info(f"Generated UUID for consultation: {consultation.cons_id} (external services failed)")
        
        elif not chatwoot_success:
            # Chatwoot упал - сохраняем консультацию в БД
            logger.warning("Chatwoot failed to create consultation. Consultation will be saved in DB and synced later.")
            
            # Генерируем UUID для консультации, если еще не создан
            if consultation.cons_id.startswith("temp_"):
                consultation.cons_id = str(uuid.uuid4())
                await db.flush()
                logger.info(f"Generated UUID for consultation: {consultation.cons_id} (Chatwoot failed)")
        
        # Если хотя бы одна система успешна, генерируем нормальный ID если нужно
        if not chatwoot_success and consultation.cons_id.startswith("temp_"):
            # Генерируем UUID вместо temp_ ID
            consultation.cons_id = str(uuid.uuid4())
            await db.flush()
            logger.info(f"Generated UUID for consultation: {consultation.cons_id} (Chatwoot failed, but 1C succeeded)")
        
        # ВАЖНО: Если chatwoot_source_id не установлен, используем source_id из contact (если есть)
        # Для Public API source_id должен быть из contact, а не UUID conversation
        if not consultation.chatwoot_source_id:
            # Используем contact_source_id для будущей попытки создания через Public API
            consultation.chatwoot_source_id = contact_source_id if contact_source_id else None
            if consultation.chatwoot_source_id:
                logger.info(f"Set chatwoot_source_id to contact source_id: {consultation.chatwoot_source_id} (for future retry via Public API)")
            else:
                logger.warning(f"chatwoot_source_id is null - Chatwoot conversation was not created and no contact_source_id available")
                logger.warning("Frontend will need to handle null chatwoot_source_id case")
            await db.flush()
        
        # Убеждаемся, что переменная chatwoot_source_id установлена для ответа
        # ВАЖНО: Не обращаемся к consultation.chatwoot_source_id напрямую, так как после rollback
        # consultation может быть не persistent. Используем сохраненную переменную или contact_source_id.
        if chatwoot_source_id is None:
            # Если chatwoot_source_id не был установлен, используем contact_source_id
            # (который был сохранен ранее в переменной contact_source_id)
            chatwoot_source_id = contact_source_id if 'contact_source_id' in locals() else None
            logger.debug(f"Using chatwoot_source_id from contact: {chatwoot_source_id}")
        
        # ВАЖНО: Коммитим транзакцию только если хотя бы одна внешняя система успешна
        # Если обе системы упали, все равно сохраняем в БД для последующей синхронизации
        try:
            await db.commit()
            # ВАЖНО: Проверяем, что consultation все еще в сессии перед refresh
            # После rollback consultation может быть не persistent
            try:
                await db.refresh(consultation)
            except Exception as refresh_error:
                # Если consultation не persistent, просто логируем и продолжаем
                logger.warning(f"Failed to refresh consultation after commit: {refresh_error}. Consultation may not be persistent in session.")
        except Exception as e:
            logger.error(f"Failed to commit consultation to database: {e}", exc_info=True)
            try:
                await db.rollback()
                # Если была ошибка коммита, пытаемся откатить изменения в Chatwoot/1C
                if chatwoot_success and chatwoot_cons_id:
                    try:
                        logger.warning(f"Attempting to delete Chatwoot conversation {chatwoot_cons_id} due to DB commit failure")
                        # Можно добавить удаление conversation если нужно
                    except Exception as cleanup_error:
                        logger.error(f"Failed to cleanup Chatwoot conversation: {cleanup_error}")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback after commit error: {rollback_error}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to save consultation to database: {str(e)}"
            )
    
        # Сохраняем idempotency key если передан
        if idempotency_key:
            request_hash = generate_request_hash(payload.dict())
            # Убеждаемся что pubsub_token это строка, а не bytes
            pubsub_token_str = None
            if pubsub_token:
                if isinstance(pubsub_token, bytes):
                    pubsub_token_str = pubsub_token.decode('utf-8')
                else:
                    pubsub_token_str = str(pubsub_token)
            
            # Формируем response_data с обязательным полем client_id
            # Получаем ФИО менеджера
            manager_name = await _get_manager_name(db, consultation.manager)
            consultation_read = ConsultationRead.from_model(consultation, manager_name=manager_name)
            response_data = ConsultationResponse(
                consultation=consultation_read,
                client_id=str(client.client_id),  # Добавляем обязательное поле client_id
                chatwoot_source_id=chatwoot_source_id,
                pubsub_token=pubsub_token_str
            ).dict()
            try:
                await store_idempotency_key(
                    db=db,
                    key=idempotency_key,
                    operation_type="create_consultation",
                    resource_id=consultation.cons_id,
                    request_hash=request_hash,
                    response_data=response_data
                )
            except Exception as e:
                # Если ошибка при сохранении idempotency key, логируем но не прерываем выполнение
                logger.warning(f"Failed to store idempotency key: {e}")
                # Откатываем только изменения idempotency key, не всю транзакцию
                try:
                    await db.rollback()
                    # Продолжаем работу - консультация уже создана
                except Exception:
                    pass
        
        # Отправляем информационное сообщение от имени компании в Chatwoot
        if chatwoot_success and chatwoot_cons_id:
            try:
                # Формируем информационное сообщение
                info_message_parts = ["Ваша заявка на консультацию принята."]
                
                if consultation.number:
                    info_message_parts.append(f"Номер заявки: {consultation.number}.")
                
                if consultation.start_date:
                    date_str = consultation.start_date.strftime("%d.%m.%Y %H:%M")
                    info_message_parts.append(f"Запланированная дата консультации: {date_str}.")
                
                # Добавляем информацию об очереди и времени ожидания
                # ВАЖНО: Для "Техническая поддержка" не показываем очередь, только сообщение о времени связи
                consultation_type = consultation.consultation_type or payload.consultation.consultation_type
                if consultation_type == "Техническая поддержка":
                    info_message_parts.append("Мы свяжемся с вами в течении 15-20 минут.")
                elif selected_manager_key:
                    try:
                        wait_info = await manager_selector.calculate_wait_time(selected_manager_key)
                        queue_position = wait_info["queue_position"]
                        wait_hours = wait_info["estimated_wait_hours"]
                        
                        if queue_position > 1:
                            info_message_parts.append(
                                f"Вы в очереди #{queue_position}. "
                                f"Примерное время ожидания: {wait_hours} {'час' if wait_hours == 1 else 'часа' if wait_hours < 5 else 'часов'}."
                            )
                    except Exception as e:
                        logger.warning(f"Failed to calculate wait time: {e}")
                
                info_message = " ".join(info_message_parts)
                
                # Отправляем через Application API как исходящее сообщение от системы
                await chatwoot_client.send_message(
                    conversation_id=chatwoot_cons_id,
                    content=info_message,
                    message_type="outgoing",  # Исходящее от системы
                    private=False  # Видно клиенту
                )
                logger.info(f"Sent info message to Chatwoot conversation {chatwoot_cons_id}")
                
                # Обновляем custom_attributes с номером консультации если он есть
                if consultation.number:
                    try:
                        await chatwoot_client.update_conversation_custom_attributes(
                            conversation_id=chatwoot_cons_id,
                            custom_attributes={"number_con": str(consultation.number)}
                        )
                        logger.info(f"Updated custom_attributes with number_con={consultation.number} for conversation {chatwoot_cons_id}")
                    except Exception as e:
                        logger.warning(f"Failed to update custom_attributes with number_con: {e}")
            except Exception as e:
                logger.warning(f"Failed to send info message to Chatwoot: {e}", exc_info=True)
        
        # Формируем сообщение об успехе
        success_parts = []
        if chatwoot_success:
            success_parts.append("Chatwoot")
        if onec_success:
            success_parts.append("1C:ЦЛ")
        
        message = f"Consultation created successfully in: {', '.join(success_parts) if success_parts else 'database only'}"
        
        # Получаем настройки Chatwoot для виджета
        from ..config import settings
        chatwoot_account_id = str(settings.CHATWOOT_ACCOUNT_ID) if settings.CHATWOOT_ACCOUNT_ID else None
        chatwoot_inbox_id = settings.CHATWOOT_INBOX_ID if settings.CHATWOOT_INBOX_ID else None
        
        # ВАЖНО: pubsub_token возвращается ТОЛЬКО в ответе POST создания contact через Public API
        # pubsub_token сохраняется в БД клиента при создании contact и больше не меняется
        # Берем pubsub_token из БД клиента (он был сохранен при создании contact)
        # Убеждаемся что pubsub_token это строка, а не bytes
        final_pubsub_token = None
        if client.chatwoot_pubsub_token:
            if isinstance(client.chatwoot_pubsub_token, bytes):
                final_pubsub_token = client.chatwoot_pubsub_token.decode('utf-8')
            else:
                final_pubsub_token = str(client.chatwoot_pubsub_token)
        
        # ВАЖНО ДЛЯ ФРОНТЕНДА:
        # - chatwoot_conversation_id возвращается ТОЛЬКО если conversation успешно создана в Chatwoot
        # - Если conversation не создана (chatwoot_cons_id is None), НЕ пытайтесь найти conversation по этому ID
        # - Используйте chatwoot_conversation_id из ответа, НЕ используйте consultation.cons_id для поиска conversation
        # - consultation.cons_id - это ID консультации в нашей БД, а не ID conversation в Chatwoot
        # - Если chatwoot_conversation_id is None, conversation будет создана позже через retry механизм
        if not chatwoot_success:
            logger.warning(
                f"⚠ Chatwoot conversation was NOT created. "
                f"chatwoot_conversation_id will be None in response. "
                f"Frontend should NOT attempt to find conversation. "
                f"Consultation cons_id: {consultation.cons_id}"
            )
        
        # Получаем ФИО менеджера
        manager_name = await _get_manager_name(db, consultation.manager)
        
        # Получаем bot_username для Telegram (если консультация создана через Telegram)
        bot_username = None
        if source == "TELEGRAM" and telegram_user_id:
            try:
                from ..services.telegram_bot import TelegramBotService
                telegram_bot_service = TelegramBotService()
                bot_info = await telegram_bot_service.bot.get_me()
                if bot_info:
                    bot_username = bot_info.username
                    logger.info(f"Got bot username for Telegram consultation: {bot_username}")
            except Exception as e:
                logger.warning(f"Failed to get bot username: {e}")
        
        # Формируем ответ
        response = ConsultationResponse(
            consultation=ConsultationRead.from_model(consultation, manager_name=manager_name),
            client_id=str(client.client_id),
            message=message,
            source=source,  # Источник создания (TELEGRAM, SITE, BACKEND)
            telegram_user_id=telegram_user_id if telegram_user_id else None,  # ID пользователя Telegram
            bot_username=bot_username,  # Username бота для Telegram
            # Поля для подключения чат-виджета Chatwoot
            # ВАЖНО: chatwoot_conversation_id будет None если conversation не создана
            chatwoot_conversation_id=chatwoot_cons_id if chatwoot_success else None,  # ID conversation (только если создана)
            chatwoot_source_id=chatwoot_source_id,  # source_id из contact (для идентификации пользователя)
            chatwoot_account_id=chatwoot_account_id,  # account_id для подключения виджета
            chatwoot_inbox_id=chatwoot_inbox_id,  # inbox_id для подключения виджета
            chatwoot_pubsub_token=final_pubsub_token,  # pubsub_token для WebSocket подключения (из контакта, не из беседы)
        )
        
        # Если консультация создана через Telegram, отправляем авто сообщение ботом
        if source == "TELEGRAM" and telegram_user_id and chatwoot_cons_id:
            try:
                from ..services.telegram_bot import TelegramBotService
                telegram_bot_service = TelegramBotService()
                # Отправляем сообщение пользователю о создании консультации
                consultation_message = (
                    f"✅ Ваша заявка #{consultation.number or consultation.cons_id} создана!\n\n"
                    f"Мы получили ваш запрос и скоро с вами свяжемся.\n\n"
                    f"Вы можете продолжить общение здесь в чате."
                )
                await telegram_bot_service.bot.send_message(
                    chat_id=telegram_user_id,
                    text=consultation_message
                )
                logger.info(f"Sent auto message to Telegram user {telegram_user_id} for consultation {consultation.cons_id}")
            except Exception as e:
                logger.error(f"Failed to send auto message to Telegram user {telegram_user_id}: {e}", exc_info=True)
                # Не блокируем ответ, если не удалось отправить сообщение
        
        # Сохраняем idempotency key если передан (после успешного создания)
        if idempotency_key:
            try:
                request_hash = generate_request_hash(payload.dict())
                await store_idempotency_key(
                    db=db,
                    key=idempotency_key,
                    operation_type="create_consultation",
                    resource_id=consultation.cons_id,
                    request_hash=request_hash,
                    response_data=response.dict()
                )
                logger.debug(f"Stored idempotency key: {idempotency_key} for consultation {consultation.cons_id}")
            except Exception as e:
                logger.warning(f"Failed to store idempotency key: {e}")
        
        return response
    except HTTPException:
        # Пробрасываем HTTPException как есть
        raise
    except Exception as e:
        # Логируем все остальные ошибки и возвращаем 500 с деталями
        logger.error(f"Unexpected error in create_consultation: {e}", exc_info=True)
        try:
            await db.rollback()
        except Exception as rollback_error:
            logger.error(f"Failed to rollback after unexpected error: {rollback_error}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.post("/simple", response_model=ConsultationResponse)
async def create_consultation_simple(
    payload: ConsultationCreate,
    db: AsyncSession = Depends(get_db),
    authorization: Optional[str] = Header(None)
):
    """
    Упрощенное создание консультации (только данные консультации).
    
    Используется когда клиент уже существует и известен client_id.
    """
    if not payload.client_id:
        raise HTTPException(status_code=400, detail="client_id is required")
    
    # Обертываем в ConsultationWithClient для переиспользования логики
    full_payload = ConsultationWithClient(
        client=None,
        consultation=payload,
        source="SITE"
    )
    
    return await create_consultation(full_payload, db, authorization)


async def _get_consultation_or_404(db: AsyncSession, cons_id: str) -> Consultation:
    result = await db.execute(select(Consultation).where(Consultation.cons_id == cons_id))
    consultation = result.scalar_one_or_none()
    if not consultation:
        raise HTTPException(status_code=404, detail="Consultation not found")
    return consultation


@router.get("/{cons_id}/calls", response_model=List[CallRead])
async def list_calls(
    cons_id: str,
    db: AsyncSession = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """
    Получение списка попыток дозвона для консультации.
    
    Возвращает список попыток дозвона, отсортированных по дате (новые первыми).
    Поддерживает пагинацию через параметры skip и limit.
    """
    consultation = await _get_consultation_or_404(db, cons_id)
    
    # Получаем дозвоны по cons_id или cons_key
    query = select(Call).where(
        (Call.cons_id == cons_id) | (Call.cons_key == consultation.cl_ref_key)
    ).order_by(Call.period.desc()).offset(skip).limit(limit)
    
    result = await db.execute(query)
    calls = result.scalars().all()
    
    return [CallRead.model_validate(call) for call in calls]


@router.get("/{cons_id}/redates", response_model=List[ConsultationRedateRead])
async def list_redates(cons_id: str, db: AsyncSession = Depends(get_db)):
    consultation = await _get_consultation_or_404(db, cons_id)
    result = await db.execute(
        select(ConsRedate)
        .where(ConsRedate.cons_key == consultation.cl_ref_key)
        .order_by(ConsRedate.period.desc())
    )
    return result.scalars().all()


@router.post("/{cons_id}/redates", response_model=ConsultationRedateRead)
async def create_redate(
    cons_id: str,
    payload: ConsultationRedateCreate,
    db: AsyncSession = Depends(get_db),
):
    consultation = await _get_consultation_or_404(db, cons_id)
    if not consultation.cl_ref_key:
        raise HTTPException(status_code=400, detail="Consultation not yet synced with 1C")

    clients_key = consultation.client_key or (str(consultation.client_id) if consultation.client_id else consultation.cl_ref_key)
    
    # Получаем тип консультации для правильной фильтрации менеджеров
    consultation_type = consultation.consultation_type
    
    # Получаем менеджера - используем из payload, consultation или дефолтного из БД
    manager_key = payload.manager_key or consultation.manager
    if not manager_key:
        # Получаем менеджера по умолчанию из БД с учетом типа консультации
        manager_key = await _get_default_manager_key(db, consultation_type=consultation_type)
    
    # Валидация: manager_key должен быть валидным GUID, не "FRONT" или пустой строкой
    if not manager_key or manager_key == "FRONT" or len(manager_key) != 36 or manager_key.count("-") != 4:
        logger.warning(f"Invalid manager_key '{manager_key}' for consultation {cons_id}, using default manager")
        manager_key = await _get_default_manager_key(db, consultation_type=consultation_type)
        if not manager_key:
            raise HTTPException(
                status_code=400,
                detail="No valid manager found. Please specify manager_key or ensure default manager exists in database."
            )
    
    old_date = consultation.start_date

    redate = ConsRedate(
        cons_key=consultation.cl_ref_key,
        clients_key=clients_key,
        manager_key=manager_key,
        period=datetime.now(timezone.utc),
        old_date=old_date,
        new_date=payload.new_date,
        comment=payload.comment,
    )
    db.add(redate)

    if payload.new_date:
        consultation.redate = payload.new_date.date()
        consultation.redate_time = payload.new_date.time()
        consultation.start_date = payload.new_date
        consultation.updated_at = datetime.now(timezone.utc)

    await db.flush()
    
    # Отправляем в 1C:ЦЛ
    onec_client = OneCClient()
    try:
        await onec_client.create_redate_odata(
            cons_key=consultation.cl_ref_key,
            client_key=clients_key,
            manager_key=manager_key,
            old_date=old_date,
            new_date=payload.new_date,
            comment=payload.comment,
            period=redate.period,
        )
        # Также обновляем дату в самом документе
        onec_response = await onec_client.update_consultation_odata(
            ref_key=consultation.cl_ref_key,
            start_date=payload.new_date,
        )
        # Обрабатываем ответ от 1C и обновляем локальную БД
        await _process_onec_response(consultation, onec_response)
        await db.flush()
        logger.info(f"Created redate in 1C for consultation {cons_id}")
    except Exception as e:
        logger.error(f"Failed to create redate in 1C: {e}", exc_info=True)
    
    # Отправляем note в Chatwoot
    chatwoot_client = ChatwootClient()
    try:
        old_date_str = old_date.strftime("%d.%m.%Y %H:%M") if old_date else "не указана"
        new_date_str = payload.new_date.strftime("%d.%m.%Y %H:%M") if payload.new_date else "не указана"
        note_content = f"📅 Консультация перенесена\nСтарая дата: {old_date_str}\nНовая дата: {new_date_str}"
        if payload.comment:
            note_content += f"\nКомментарий: {payload.comment}"
        
        await chatwoot_client.send_message(
            conversation_id=cons_id,
            content=note_content,
            message_type="outgoing",
            private=False
        )
        logger.info(f"Sent redate note to Chatwoot for consultation {cons_id}")
    except Exception as e:
        logger.error(f"Failed to send redate note to Chatwoot: {e}", exc_info=True)
    
    await db.commit()
    await db.refresh(redate)
    return redate


async def _build_rating_response(db: AsyncSession, cons_key: str) -> ConsultationRatingResponse:
    result = await db.execute(
        select(
            ConsRatingAnswer.question_number,
            ConsRatingAnswer.rating,
            ConsRatingAnswer.question_text,
            ConsRatingAnswer.comment,
            ConsRatingAnswer.manager_key,
        )
        .where(ConsRatingAnswer.cons_key == cons_key)
        .order_by(ConsRatingAnswer.question_number.asc())
    )
    rows = result.all()
    ratings = [row[1] for row in rows if row[1] is not None]
    average = round(sum(ratings) / len(ratings), 2) if ratings else None
    payload_answers = [
        ConsultationRatingAnswerPayload(
            question_number=row[0],
            rating=row[1],
            question=row[2],
            comment=row[3],
            manager_key=row[4],
        )
        for row in rows
    ]
    return ConsultationRatingResponse(average=average, count=len(ratings), answers=payload_answers)


@router.get("/{cons_id}/ratings", response_model=ConsultationRatingResponse)
async def get_ratings(cons_id: str, db: AsyncSession = Depends(get_db)):
    consultation = await _get_consultation_or_404(db, cons_id)
    if not consultation.cl_ref_key:
        return ConsultationRatingResponse(average=None, count=0, answers=[])
    return await _build_rating_response(db, consultation.cl_ref_key)


@router.post("/{cons_id}/ratings", response_model=ConsultationRatingResponse)
async def submit_ratings(
    cons_id: str,
    payload: ConsultationRatingRequest,
    db: AsyncSession = Depends(get_db),
):
    consultation = await _get_consultation_or_404(db, cons_id)
    if not consultation.cl_ref_key:
        raise HTTPException(status_code=400, detail="Consultation not yet synced with 1C")
    if not payload.answers:
        raise HTTPException(status_code=400, detail="Answers array is required")

    client_key = consultation.client_key
    client_id = str(consultation.client_id) if consultation.client_id else None

    rows = []
    rating_date = datetime.now(timezone.utc)  # Текущая дата для ДатаОценки
    for answer in payload.answers:
        rows.append(
            {
                "cons_key": consultation.cl_ref_key,
                "cons_id": consultation.cons_id,
                "client_key": client_key,
                "client_id": client_id,
                "manager_key": answer.manager_key or consultation.manager,
                "question_number": answer.question_number,
                "rating": answer.rating,
                "question_text": answer.question,
                "comment": answer.comment,
                "sent_to_base": False,
                "rating_date": rating_date,  # Сохраняем дату оценки
            }
        )

    stmt = insert(ConsRatingAnswer).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_cons_rating_answer",
        set_={
            "rating": stmt.excluded.rating,
            "question_text": stmt.excluded.question_text,
            "comment": stmt.excluded.comment,
            "manager_key": stmt.excluded.manager_key,
            "rating_date": stmt.excluded.rating_date,  # Обновляем дату оценки
            "updated_at": func.now(),
        },
    )
    await db.execute(stmt)
    await recalc_consultation_ratings(db, {consultation.cl_ref_key})
    await db.flush()
    
    # Отправляем оценки в 1C:ЦЛ
    onec_client = OneCClient()
    for answer in payload.answers:
        try:
            # Валидация manager_key - должен быть валидным GUID
            answer_manager_key = answer.manager_key or consultation.manager
            if not answer_manager_key or answer_manager_key == "FRONT" or len(answer_manager_key) != 36 or answer_manager_key.count("-") != 4:
                # Получаем менеджера по умолчанию из БД с учетом типа консультации
                consultation_type = consultation.consultation_type
                answer_manager_key = await _get_default_manager_key(db, consultation_type=consultation_type)
                if not answer_manager_key:
                    logger.warning(f"Invalid manager_key for rating question {answer.question_number}, skipping 1C sync")
                    continue
            
            await onec_client.create_rating_odata(
                cons_key=consultation.cl_ref_key,
                client_key=client_key or "",
                manager_key=answer_manager_key,
                question_number=answer.question_number,
                rating=answer.rating,
                question_text=answer.question,
                comment=answer.comment,
                period=datetime.now(timezone.utc),
            )
        except Exception as e:
            logger.error(f"Failed to create rating in 1C for question {answer.question_number}: {e}", exc_info=True)
    
    # Отправляем note в Chatwoot
    chatwoot_client = ChatwootClient()
    try:
        avg_rating = sum(a.rating for a in payload.answers if a.rating) / len([a for a in payload.answers if a.rating]) if payload.answers else None
        note_content = f"⭐ Оценка консультации получена\nСредняя оценка: {avg_rating:.1f}" if avg_rating else "⭐ Оценка консультации получена"
        if len(payload.answers) > 1:
            note_content += f"\nКоличество вопросов: {len(payload.answers)}"
        
        # Используем send_message вместо send_note, так как note сообщения не видны клиенту
        await chatwoot_client.send_message(
            conversation_id=cons_id,
            content=note_content,
            message_type="outgoing"
        )
        logger.info(f"Sent rating message to Chatwoot for consultation {cons_id}")
    except Exception as e:
        logger.error(f"Failed to send rating note to Chatwoot: {e}", exc_info=True)
    
    await db.commit()
    return await _build_rating_response(db, consultation.cl_ref_key)


@router.put("/{cons_id}", response_model=ConsultationRead)
async def update_consultation(
    cons_id: str,
    payload: ConsultationUpdate,
    db: AsyncSession = Depends(get_db)
):
    """
    Обновление консультации.
    
    Поддерживаемые поля:
    - status: Статус консультации
    - start_date: Дата консультации (ISO 8601 формат, например: "2025-12-04T11:40:00Z")
    - end_date: Конец консультации (ISO 8601 формат, например: "2025-12-04T11:40:00Z")
    - comment: Описание/комментарий
    - topic: Тема
    - importance: Важность (1-3)
    
    Обновляет данные в БД и синхронизирует с 1C:ЦЛ (если cl_ref_key есть).
    """
    consultation = await _get_consultation_or_404(db, cons_id)
    
    # Обновляем поля в БД (используем model_dump для получения только установленных полей)
    update_data = payload.model_dump(exclude_unset=True)
    
    if "status" in update_data:
        consultation.status = update_data["status"]
    if "start_date" in update_data:
        consultation.start_date = update_data["start_date"]
    if "end_date" in update_data:
        consultation.end_date = update_data["end_date"]
    if "comment" in update_data:
        consultation.comment = update_data["comment"]
    if "importance" in update_data:
        consultation.importance = update_data["importance"]
    
    await db.flush()
    
    # Синхронизируем с 1C:ЦЛ если есть cl_ref_key
    if consultation.cl_ref_key:
        onec_client = OneCClient()
        try:
            # Маппим данные для 1C
            status = update_data.get("status")
            start_date = update_data.get("start_date")
            end_date = update_data.get("end_date")
            description = update_data.get("comment")
            
            onec_response = await onec_client.update_consultation_odata(
                ref_key=consultation.cl_ref_key,
                status=status,
                start_date=start_date,
                end_date=end_date,
                description=description,
                is_chatwoot_status=True if status else False,
            )
            # Обрабатываем ответ от 1C и обновляем локальную БД
            await _process_onec_response(consultation, onec_response)
            await db.flush()
            logger.info(f"Updated consultation {cons_id} in 1C:ЦЛ")
        except Exception as e:
            logger.error(f"Failed to update consultation {cons_id} in 1C:ЦЛ: {e}", exc_info=True)
            # Продолжаем - данные в БД уже обновлены
    
    await db.commit()
    await db.refresh(consultation)
    
    # Получаем ФИО менеджера
    manager_name = await _get_manager_name(db, consultation.manager)
    return ConsultationRead.from_model(consultation, manager_name=manager_name)


@router.get("/{cons_id}", response_model=ConsultationRead)
async def get_consultation(
    cons_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Получение консультации по ID (cons_id из Chatwoot)"""
    consultation = await _get_consultation_or_404(db, cons_id)
    # Получаем ФИО менеджера
    manager_name = await _get_manager_name(db, consultation.manager)
    return ConsultationRead.from_model(consultation, manager_name=manager_name)


@router.post("/{cons_id}/cancel", response_model=ConsultationRead)
async def cancel_consultation(
    cons_id: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Аннулирование консультации пользователем.
    
    Аннулирование доступно только если:
    - Прошло не более CANCEL_CONSULTATION_TIMEOUT_MINUTES минут с момента создания консультации
    - Консультация еще не завершена (end_date не установлен)
    - Статус консультации позволяет аннулирование (open или pending)
    - Консультация не была ранее отменена или закрыта
    
    При аннулировании:
    - Документ ТелефонныйЗвонок удаляется в 1C:ЦЛ (освобождает лимит)
    - В Chatwoot беседа закрывается со статусом "resolved" и custom attribute "closed_without_con": true
    - Отправляется сообщение в чат о том, что заявка аннулирована
    - Статус консультации в БД обновляется на "cancelled"
    - Устанавливается флаг denied = True
    
    Настройка:
    - Время для аннулирования настраивается через переменную окружения CANCEL_CONSULTATION_TIMEOUT_MINUTES (по умолчанию 30 минут)
    """
    consultation = await _get_consultation_or_404(db, cons_id)
    now = datetime.now(timezone.utc)
    settings = get_settings()
    
    # Проверка 1: Консультация уже отменена или закрыта
    if consultation.status in ("cancelled", "closed", "resolved"):
        raise HTTPException(
            status_code=400,
            detail=f"Консультация уже имеет статус '{consultation.status}' и не может быть аннулирована. "
                   f"Аннулирование возможно только для консультаций со статусом 'open' или 'pending'."
        )
    
    # Проверка 2: Консультация уже завершена (end_date установлен)
    if consultation.end_date:
        # Если end_date без timezone, добавляем UTC
        end_date = consultation.end_date
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        
        raise HTTPException(
            status_code=400,
            detail=f"Консультация уже завершена (дата завершения: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}) "
                   f"и не может быть аннулирована."
        )
    
    # Проверка 3: Статус консультации позволяет аннулирование
    if consultation.status not in ("open", "pending", None):
        raise HTTPException(
            status_code=400,
            detail=f"Консультация со статусом '{consultation.status}' не может быть аннулирована. "
                   f"Аннулирование возможно только для консультаций со статусом 'open' или 'pending'."
        )
    
    # Проверка 4: Время с момента создания не превышает лимит
    create_date = consultation.create_date or consultation.created_at
    
    if not create_date:
        raise HTTPException(
            status_code=400,
            detail="Не удалось определить дату создания консультации"
        )
    
    # Если create_date без timezone, добавляем UTC
    if create_date.tzinfo is None:
        create_date = create_date.replace(tzinfo=timezone.utc)
    
    time_since_creation = now - create_date
    cancel_timeout = timedelta(minutes=settings.CANCEL_CONSULTATION_TIMEOUT_MINUTES)
    
    if time_since_creation > cancel_timeout:
        minutes_passed = int(time_since_creation.total_seconds() / 60)
        raise HTTPException(
            status_code=400,
            detail=f"Время для аннулирования консультации истекло. "
                   f"Аннулирование возможно только в течение {settings.CANCEL_CONSULTATION_TIMEOUT_MINUTES} минут с момента создания. "
                   f"Прошло: {minutes_passed} минут"
        )
    
    logger.info(
        f"Annulling consultation {cons_id}: "
        f"status={consultation.status}, "
        f"time_since_creation={time_since_creation.total_seconds() / 60:.1f} minutes, "
        f"cl_ref_key={consultation.cl_ref_key}"
    )
    
    # Помечаем документ в 1C:ЦЛ на удаление (DeletionMark=true) вместо удаления
    if consultation.cl_ref_key:
        onec_client = OneCClient()
        try:
            await onec_client.mark_consultation_deleted(consultation.cl_ref_key)
            logger.info(f"✓ Marked 1C consultation as deleted (DeletionMark=true): Ref_Key={consultation.cl_ref_key} for annulled consultation {cons_id}")
        except Exception as e:
            logger.error(f"✗ Failed to mark 1C consultation {consultation.cl_ref_key} as deleted: {e}", exc_info=True)
            # Продолжаем выполнение даже если пометка в 1C не удалась
    
    # Закрываем беседу в Chatwoot и отправляем сообщение
    chatwoot_client = ChatwootClient()
    try:
        # Закрываем беседу со статусом "resolved" и пометкой "closed_without_con": true
        await chatwoot_client.update_conversation(
            conversation_id=cons_id,
            status="resolved",
            custom_attributes={"closed_without_con": True}
        )
        logger.info(f"✓ Closed Chatwoot conversation {cons_id} with 'closed_without_con' flag")
        
        # Отправляем сообщение в чат о том, что заявка аннулирована
        try:
            await chatwoot_client.send_message(
                conversation_id=cons_id,
                content="Заявка аннулирована клиентом.",
                message_type="outgoing"
            )
            logger.info(f"✓ Sent cancellation message to Chatwoot conversation {cons_id}")
        except Exception as msg_error:
            logger.warning(f"Failed to send cancellation message to Chatwoot conversation {cons_id}: {msg_error}")
            # Не критично, продолжаем выполнение
    except Exception as e:
        logger.error(f"✗ Failed to update Chatwoot conversation {cons_id}: {e}", exc_info=True)
        # Продолжаем выполнение даже если обновление в Chatwoot не удалось
    
    # Обновляем статус в БД
    consultation.status = "cancelled"
    consultation.denied = True  # Флаг "закрыто без консультации"
    consultation.end_date = now  # Устанавливаем дату завершения при аннулировании
    
    await db.commit()
    await db.refresh(consultation)
    
    logger.info(f"✓ Annulled consultation {cons_id}: deleted from 1C, closed in Chatwoot, sent message, updated in DB")
    
    # Получаем ФИО менеджера
    manager_name = await _get_manager_name(db, consultation.manager)
    return ConsultationRead.from_model(consultation, manager_name=manager_name)


@router.get("/clients/{client_id}/consultations", response_model=ConsultationListResponse)
async def get_client_consultations(
    client_id: str,
    db: AsyncSession = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Получение всех консультаций клиента"""
    try:
        client_uuid = uuid.UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid client_id format")
    
    # Проверяем существование клиента
    result = await db.execute(
        select(Client).where(Client.client_id == client_uuid)
    )
    client = result.scalar_one_or_none()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Получаем консультации с JOIN к users для получения ФИО менеджеров
    user_alias = aliased(User)
    result = await db.execute(
        select(Consultation, user_alias.description)
        .outerjoin(user_alias, (Consultation.manager == user_alias.cl_ref_key) & (user_alias.deletion_mark == False))
        .where(Consultation.client_id == client_uuid)
        .order_by(Consultation.create_date.desc())
        .offset(skip)
        .limit(limit)
    )
    rows = result.all()
    
    # Формируем список консультаций с manager_name
    consultations_list = []
    for consultation, manager_name in rows:
        consultations_list.append(ConsultationRead.from_model(consultation, manager_name=manager_name))
    
    # Подсчитываем общее количество
    count_result = await db.execute(
        select(func.count(Consultation.cons_id))
        .where(Consultation.client_id == client_uuid)
    )
    total = count_result.scalar() or 0
    
    return ConsultationListResponse(
        consultations=consultations_list,
        total=total
    )


@router.post(
    "/{cons_id}/sync",
    response_model=ConsultationRead,
    summary="Принудительная синхронизация консультации",
    description="""
    Принудительная синхронизация консультации с Chatwoot и 1C:ЦЛ.
    
    Получает актуальные данные из обеих систем, обновляет БД и возвращает актуальное состояние.
    
    **Использование:**
    Вызывайте этот endpoint при необходимости принудительно обновить данные консультации,
    например, если заметили рассинхронизацию между системами.
    
    **Процесс:**
    1. Получает актуальные данные из Chatwoot (статус, менеджер, custom_attributes)
    2. Получает актуальные данные из 1C:ЦЛ (если доступно)
    3. Обновляет БД с полученными данными
    4. Возвращает обновленную консультацию
    """
)
async def sync_consultation(
    cons_id: str = ...,
    db: AsyncSession = Depends(get_db)
):
    # Получаем консультацию из БД
    result = await db.execute(
        select(Consultation).where(Consultation.cons_id == cons_id)
    )
    consultation = result.scalar_one_or_none()
    
    if not consultation:
        raise HTTPException(
            status_code=404,
            detail=f"Consultation {cons_id} not found"
        )
    
    # ═══════════════════════════════════════════════════════════════════════
    # GUARD CLAUSE: Терминальные статусы НЕ МЕНЯЕМ
    # ═══════════════════════════════════════════════════════════════════════
    terminal_statuses = {"closed", "resolved", "cancelled"}
    if consultation.status in terminal_statuses:
        logger.info(
            f"Sync skipped: consultation {cons_id} is in terminal state: {consultation.status}. "
            f"Terminal statuses cannot be changed by sync."
        )
        # Возвращаем консультацию как есть, не синхронизируем
        manager_name = await _get_manager_name(db, consultation.manager)
        return ConsultationRead.from_model(consultation, manager_name=manager_name)
    
    chatwoot_client = ChatwootClient()
    onec_client = OneCClient()
    
    sync_changes = []
    
    try:
        # Синхронизация с Chatwoot
        if consultation.cons_id and not consultation.cons_id.startswith(("temp_", "cl_")):
            try:
                # Получаем актуальные данные из Chatwoot
                conversation_response = await chatwoot_client._request(
                    "GET",
                    f"/api/v1/accounts/{chatwoot_client.account_id}/conversations/{cons_id}"
                )
                
                if conversation_response:
                    conversation = conversation_response
                    old_status = consultation.status
                    new_status = conversation.get("status")
                    
                    # Обновляем статус только если он не терминальный
                    # ВАЖНО: Не меняем терминальные статусы (closed, resolved, cancelled)
                    if new_status and old_status != new_status:
                        # Проверяем, что новый статус не является "откатом" терминального статуса
                        # Например, если в Chatwoot статус "open", а у нас "cancelled" - не меняем
                        if old_status not in terminal_statuses:
                            # Маппинг статусов Chatwoot → Clobus
                            # resolved → resolved (НЕ pending!)
                            mapped_status = new_status
                            if new_status == "resolved":
                                mapped_status = "resolved"
                            elif new_status == "open":
                                mapped_status = "open"
                            elif new_status == "pending":
                                mapped_status = "pending"
                            else:
                                # Для неизвестных статусов оставляем текущий
                                mapped_status = old_status
                            
                            consultation.status = mapped_status
                            sync_changes.append(f"status: {old_status} -> {mapped_status}")
                        else:
                            logger.info(
                                f"Status update skipped: consultation {cons_id} has terminal status '{old_status}', "
                                f"not updating to '{new_status}' from Chatwoot"
                            )
                        
                        # Логируем изменение
                        from ..utils.change_log import log_consultation_change
                        await log_consultation_change(
                            db=db,
                            cons_id=cons_id,
                            field_name="status",
                            old_value=old_status,
                            new_value=new_status,
                            source="API_SYNC"
                        )
                    
                    # Обновляем менеджера
                    assignee = conversation.get("assignee")
                    if assignee:
                        chatwoot_user_id = assignee.get("id")
                        if chatwoot_user_id:
                            # Пытаемся найти маппинг
                            mapping_result = await db.execute(
                                select(UserMapping).where(UserMapping.chatwoot_user_id == chatwoot_user_id).limit(1)
                            )
                            mapping = mapping_result.scalar_one_or_none()
                            if mapping:
                                old_manager = consultation.manager
                                consultation.manager = mapping.cl_manager_key
                                if old_manager != consultation.manager:
                                    sync_changes.append(f"manager: {old_manager} -> {consultation.manager}")
                    
                    # Обновляем custom_attributes
                    custom_attrs = conversation.get("custom_attributes", {})
                    if custom_attrs:
                        # Обновляем даты из custom_attributes
                        if "date_con" in custom_attrs and custom_attrs["date_con"]:
                            try:
                                from dateutil import parser as date_parser
                                date_str = str(custom_attrs["date_con"])
                                parsed_date = date_parser.parse(date_str)
                                if parsed_date.tzinfo is None:
                                    parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                                if consultation.start_date != parsed_date:
                                    consultation.start_date = parsed_date
                                    sync_changes.append(f"start_date updated")
                            except Exception as e:
                                logger.warning(f"Failed to parse date_con: {e}")
                
                logger.info(f"Synced consultation {cons_id} from Chatwoot. Changes: {', '.join(sync_changes) if sync_changes else 'none'}")
            except Exception as e:
                # ════════════════════════════════════════════════════════════════════
                # КРИТИЧЕСКИ ВАЖНО: При ошибке НЕ МЕНЯЕМ статус!
                # ════════════════════════════════════════════════════════════════════
                logger.warning(
                    f"Failed to sync from Chatwoot for consultation {cons_id}: {e}. "
                    f"Current status '{consultation.status}' preserved (not changed to fallback)."
                )
                # НЕ устанавливаем fallback "pending"!
                # Оставляем текущий статус без изменений
        
        # Синхронизация с 1C:ЦЛ
        if consultation.cl_ref_key:
            try:
                # Получаем актуальные данные из 1C через OData
                # Здесь можно добавить запрос к 1C API для получения актуальных данных
                # Пока просто логируем
                logger.info(f"Consultation {cons_id} has cl_ref_key={consultation.cl_ref_key}, 1C sync would be performed here")
            except Exception as e:
                logger.warning(f"Failed to sync from 1C: {e}")
        
        await db.commit()
        
        # Уведомляем WebSocket клиентов об обновлении
        try:
            from ..routers.websocket import notify_consultation_update
            await notify_consultation_update(cons_id, consultation)
        except Exception as ws_error:
            logger.debug(f"Failed to notify WebSocket clients: {ws_error}")
        
        # Возвращаем обновленную консультацию
        await db.refresh(consultation)
        # Получаем ФИО менеджера
        manager_name = await _get_manager_name(db, consultation.manager)
        return ConsultationRead.from_model(consultation, manager_name=manager_name)
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error syncing consultation {cons_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sync consultation: {str(e)}"
        )


@router.get(
    "/{cons_id}/updates",
    response_model=Dict[str, Any],
    summary="Polling обновлений консультации",
    description="""
    Polling endpoint для получения обновлений консультации.
    
    Возвращает только изменения с момента `last_updated`.
    Используется фронтендом для периодического обновления данных.
    
    **Рекомендуется:** Использовать SSE (`/stream`) или WebSocket (`/ws/consultations/{cons_id}`) для real-time обновлений.
    Polling endpoint предназначен как fallback или для простых случаев.
    
    **Query параметры:**
    - `last_updated` (опционально): ISO timestamp последнего обновления
    
    **Ответ:**
    - `has_updates: true` - есть обновления, поле `consultation` содержит актуальные данные
    - `has_updates: false` - обновлений нет с момента `last_updated`
    """
)
async def get_consultation_updates(
    cons_id: str = ...,
    last_updated: Optional[datetime] = None,
    db: AsyncSession = Depends(get_db)
):
    # Получаем консультацию
    result = await db.execute(
        select(Consultation).where(Consultation.cons_id == cons_id)
    )
    consultation = result.scalar_one_or_none()
    
    if not consultation:
        raise HTTPException(
            status_code=404,
            detail=f"Consultation {cons_id} not found"
        )
    
    # Если last_updated не указан, возвращаем все данные
    if not last_updated:
        # Получаем ФИО менеджера
        manager_name = await _get_manager_name(db, consultation.manager)
        return {
            "has_updates": True,
            "consultation": ConsultationRead.from_model(consultation, manager_name=manager_name).dict(),
            "updated_at": consultation.updated_at.isoformat() if consultation.updated_at else None
        }
    
    # Нормализуем last_updated к UTC
    if last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=timezone.utc)
    else:
        last_updated = last_updated.astimezone(timezone.utc)
    
    # Проверяем, были ли изменения
    consultation_updated_at = consultation.updated_at
    if consultation_updated_at:
        if consultation_updated_at.tzinfo is None:
            consultation_updated_at = consultation_updated_at.replace(tzinfo=timezone.utc)
        else:
            consultation_updated_at = consultation_updated_at.astimezone(timezone.utc)
        
        if consultation_updated_at <= last_updated:
            return {
                "has_updates": False,
                "updated_at": consultation_updated_at.isoformat()
            }
    
    # Есть изменения - возвращаем обновленные данные
    # Получаем ФИО менеджера
    manager_name = await _get_manager_name(db, consultation.manager)
    return {
        "has_updates": True,
        "consultation": ConsultationRead.from_model(consultation, manager_name=manager_name).dict(),
        "updated_at": consultation_updated_at.isoformat() if consultation_updated_at else None
    }


@router.get(
    "/{cons_id}/stream",
    summary="SSE stream обновлений консультации",
    description="""
    Server-Sent Events (SSE) endpoint для real-time обновлений консультации.
    
    **Использование:**
    ```javascript
    const eventSource = new EventSource('/api/consultations/12345/stream');
    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'update') {
        // Обновить UI
      }
    };
    ```
    
    **Формат сообщений:**
    - `{"type": "initial", "data": {...}}` - Начальное состояние консультации
    - `{"type": "update", "data": {...}}` - Обновление консультации
    - `{"type": "error", "message": "..."}` - Ошибка
    - `: heartbeat` - Keep-alive сообщения (каждые 3 секунды)
    
    **Альтернативы:**
    - WebSocket: `WS /ws/consultations/{cons_id}` - двусторонняя связь
    - Polling: `GET /api/consultations/{cons_id}/updates` - простой fallback
    """
)
async def stream_consultation_updates(
    cons_id: str = ...,
    db: AsyncSession = Depends(get_db)
):
    import asyncio
    import json
    
    async def event_generator():
        last_updated = None
        
        try:
            while True:
                # Получаем консультацию
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
                consultation = result.scalar_one_or_none()
                
                if not consultation:
                    yield f"data: {json.dumps({'error': 'Consultation not found'})}\n\n"
                    break
                
                # Проверяем изменения
                current_updated_at = consultation.updated_at
                if current_updated_at:
                    if current_updated_at.tzinfo is None:
                        current_updated_at = current_updated_at.replace(tzinfo=timezone.utc)
                    else:
                        current_updated_at = current_updated_at.astimezone(timezone.utc)
                    
                    if last_updated is None or current_updated_at > last_updated:
                        # Есть обновления
                        def json_serializer(obj):
                            """Кастомный сериализатор для JSON (поддержка datetime, date, time, bytes)"""
                            if isinstance(obj, datetime):
                                return obj.isoformat()
                            elif isinstance(obj, date):
                                return obj.isoformat()
                            elif isinstance(obj, time):
                                return obj.isoformat()
                            elif isinstance(obj, bytes):
                                return obj.decode('utf-8')
                            raise TypeError(f"Type {type(obj)} not serializable")
                        
                        # Получаем ФИО менеджера
                        manager_name = await _get_manager_name(db, consultation.manager)
                        consultation_dict = ConsultationRead.from_model(consultation, manager_name=manager_name).dict()
                        data = {
                            "has_updates": True,
                            "consultation": consultation_dict,
                            "updated_at": current_updated_at.isoformat()
                        }
                        yield f"data: {json.dumps(data, ensure_ascii=False, default=json_serializer)}\n\n"
                        last_updated = current_updated_at
                    else:
                        # Нет обновлений - отправляем heartbeat
                        yield f": heartbeat\n\n"
                else:
                    yield f": heartbeat\n\n"
                
                # Ждем перед следующей проверкой (2-5 секунд)
                await asyncio.sleep(3)
                
        except asyncio.CancelledError:
            logger.info(f"SSE stream cancelled for consultation {cons_id}")
        except Exception as e:
            logger.error(f"Error in SSE stream for consultation {cons_id}: {e}", exc_info=True)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Отключаем буферизацию в nginx
        }
    )
