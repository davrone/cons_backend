"""Роуты для работы с тикетами (консультациями)"""
import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Optional
import uuid

logger = logging.getLogger(__name__)

from ..database import get_db
from ..models import Consultation, Client
from ..schemas.tickets import TicketCreate, TicketRead, TicketListResponse
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
        chatwoot_response = await chatwoot_client.create_conversation(
            source_id=str(client_id) if client_id else None,
            inbox_id=None,  # Нужно получить из настроек
            message=ticket.comment or "",
        )
        chatwoot_cons_id = str(chatwoot_response.get("id"))
        
        # Обновляем cons_id
        consultation.cons_id = chatwoot_cons_id
    except Exception as e:
        # Если Chatwoot недоступен, оставляем temp ID
        # В реальной системе здесь должна быть retry логика
        pass
    
    # 4. Отправляем в 1C:ЦЛ через OData
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
    
    await db.commit()
    await db.refresh(consultation)
    
    return TicketRead.from_model(consultation)


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
    
    return TicketRead.from_model(consultation)


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
    
    # Получаем тикеты
    result = await db.execute(
        select(Consultation)
        .where(Consultation.client_id == client_uuid)
        .order_by(Consultation.create_date.desc())
        .offset(skip)
        .limit(limit)
    )
    consultations = result.scalars().all()
    
    # Подсчитываем общее количество
    count_result = await db.execute(
        select(func.count(Consultation.cons_id))
        .where(Consultation.client_id == client_uuid)
    )
    total = count_result.scalar()
    
    return TicketListResponse(
        tickets=[TicketRead.from_model(c) for c in consultations],
        total=total
    )

