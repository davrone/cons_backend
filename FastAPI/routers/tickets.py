"""
Роуты для работы с тикетами (консультациями).

⚠️ DEPRECATED: Этот модуль устарел. Используйте routers.consultations вместо него.
Все endpoints перенесены в /api/consultations.

Этот файл оставлен для обратной совместимости, но будет удален в будущих версиях.
"""
import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Optional
import uuid

logger = logging.getLogger(__name__)

from ..database import get_db
from ..models import Consultation, Client, User
from ..schemas.tickets import TicketCreate, TicketRead, TicketListResponse
from sqlalchemy.orm import aliased
from ..services.chatwoot_client import ChatwootClient
from ..services.onec_client import OneCClient

router = APIRouter()


@router.post("/create", response_model=TicketRead)
async def create_ticket(
    ticket: TicketCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Создание нового тикета.
    
    Процесс:
    1. Сохранение в БД (локальный UUID)
    2. Отправка в Chatwoot
    3. Отправка в 1C:ЦЛ
    4. Обновление записи с полученными ID
    """
    # 1. Проверяем/создаем клиента
    client_id = None
    if ticket.client_id:
        result = await db.execute(
            select(Client).where(Client.client_id == uuid.UUID(ticket.client_id))
        )
        client = result.scalar_one_or_none()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        client_id = client.client_id
    
    # 2. Создаем временную запись в БД (cons_id будет обновлен после создания в Chatwoot)
    temp_cons_id = f"temp_{uuid.uuid4()}"
    consultation = Consultation(
        cons_id=temp_cons_id,
        client_id=client_id,
        cl_ref_key=ticket.cl_ref_key,
        org_inn=ticket.org_inn,
        lang=ticket.lang,
        comment=ticket.comment,
        online_question_cat=ticket.online_question_cat,
        online_question=ticket.online_question,
        importance=ticket.importance,
        start_date=ticket.scheduled_at,
        status="new"
    )
    db.add(consultation)
    await db.flush()  # Получаем ID без коммита
    
    # 3. Отправляем в Chatwoot
    chatwoot_client = ChatwootClient()
    try:
        from ..config import settings
        chatwoot_response = await chatwoot_client.create_conversation(
            source_id=str(client_id) if client_id else None,
            inbox_id=settings.CHATWOOT_INBOX_ID,
            message=ticket.comment or "",
        )
        chatwoot_cons_id = str(chatwoot_response.get("id"))
        
        # Обновляем cons_id
        consultation.cons_id = chatwoot_cons_id
    except Exception as e:
        # Если Chatwoot недоступен, оставляем temp ID
        logger.error(f"Failed to create Chatwoot conversation: {e}", exc_info=True)
        # В реальной системе здесь должна быть retry логика
        pass
    
    # 4. Отправляем в 1C:ЦЛ через OData
    # ВАЖНО: Отправляем в ЦЛ только консультации с типом "Консультация по ведению учёта"
    # Техническая поддержка НЕ отправляется в ЦЛ
    consultation_type = getattr(ticket, 'consultation_type', None) or getattr(consultation, 'consultation_type', None)
    should_send_to_cl = consultation_type == "Консультация по ведению учёта"
    
    if ticket.cl_ref_key and should_send_to_cl:
        onec_client = OneCClient()
        try:
            # Формируем КонсультацииИТС если есть данные
            consultations_its = None
            if ticket.online_question_cat or ticket.online_question:
                consultations_its = [{
                    "LineNumber": "1",
                    "ВидПО_Key": None,  # Можно добавить если есть в ticket
                    "РазделПО_Key": None,
                    "Вопрос": ticket.comment or "",
                    "Ответ": ""
                }]
            
            onec_response = await onec_client.create_consultation_odata(
                client_key=ticket.cl_ref_key,  # Абонент_Key из ЦЛ
                description=ticket.comment or "",
                topic=None,  # Можно добавить в схему если нужно
                scheduled_at=ticket.scheduled_at,
                question_category_key=ticket.online_question_cat,
                question_key=ticket.online_question,
                consultations_its=consultations_its
            )
            # OData возвращает Ref_Key и Number
            consultation.cl_ref_key = onec_response.get("Ref_Key")
            consultation.number = onec_response.get("Number")
        except Exception as e:
            # Если ЦЛ недоступен, продолжаем без его данных
            logger.error(f"Failed to create consultation in 1C: {e}")
            pass
    else:
        if not should_send_to_cl:
            logger.info(f"Skipping 1C consultation creation: consultation_type='{consultation_type}' (only 'Консультация по ведению учёта' should be sent to ЦЛ)")
        elif not ticket.cl_ref_key:
            logger.warning("Skipping 1C consultation creation: client is not synced with 1C (cl_ref_key missing)")
    
    await db.commit()
    await db.refresh(consultation)
    
    # Получаем ФИО менеджера
    manager_name = None
    if consultation.manager:
        manager_result = await db.execute(
            select(User.description)
            .where(User.cl_ref_key == consultation.manager)
            .where(User.deletion_mark == False)
            .limit(1)
        )
        manager_name = manager_result.scalar_one_or_none()
    
    return TicketRead.from_model(consultation, manager_name=manager_name)


@router.get("/{cons_id}", response_model=TicketRead)
async def get_ticket(
    cons_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Получение тикета по ID (cons_id из Chatwoot)"""
    result = await db.execute(
        select(Consultation).where(Consultation.cons_id == cons_id)
    )
    consultation = result.scalar_one_or_none()
    
    if not consultation:
        raise HTTPException(status_code=404, detail="Ticket not found")
    
    # Получаем ФИО менеджера
    manager_name = None
    if consultation.manager:
        manager_result = await db.execute(
            select(User.description)
            .where(User.cl_ref_key == consultation.manager)
            .where(User.deletion_mark == False)
            .limit(1)
        )
        manager_name = manager_result.scalar_one_or_none()
    
    return TicketRead.from_model(consultation, manager_name=manager_name)


@router.get("/clients/{client_id}/tickets", response_model=TicketListResponse)
async def get_client_tickets(
    client_id: str,
    db: AsyncSession = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """Получение всех тикетов клиента"""
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
    
    # Получаем тикеты с JOIN к users для получения ФИО менеджеров
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
    
    # Формируем список тикетов с manager_name
    tickets_list = []
    for consultation, manager_name in rows:
        tickets_list.append(TicketRead.from_model(consultation, manager_name=manager_name))
    
    # Подсчитываем общее количество
    count_result = await db.execute(
        select(func.count(Consultation.cons_id))
        .where(Consultation.client_id == client_uuid)
    )
    total = count_result.scalar() or 0
    
    return TicketListResponse(
        tickets=tickets_list,
        total=total
    )

