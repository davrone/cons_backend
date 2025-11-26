"""Роуты для создания консультаций и управления атрибутами (переносы, оценки)."""
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from ..database import get_db
from ..models import Consultation, ConsRedate, ConsRatingAnswer, Client
from ..schemas.tickets import (
    ConsultationWithClient,
    ConsultationCreate,
    ConsultationResponse,
    TicketRead,
)
from ..schemas.consultation_meta import (
    ConsultationRedateCreate,
    ConsultationRedateRead,
    ConsultationRatingRequest,
    ConsultationRatingResponse,
    ConsultationRatingAnswerPayload,
)
from ..routers.clients import find_or_create_client
from ..services.chatwoot_client import ChatwootClient
from ..services.onec_client import OneCClient
from ..services.consultation_ratings import recalc_consultation_ratings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/create", response_model=ConsultationResponse)
async def create_consultation(
    payload: ConsultationWithClient,
    db: AsyncSession = Depends(get_db),
    authorization: Optional[str] = Header(None)
):
    """
    Создание консультации с данными клиента.
    
    Основной endpoint для фронта. Принимает:
    - Данные клиента (если клиента еще нет)
    - Данные консультации
    
    Процесс:
    1. Находит или создает клиента
    2. Создает консультацию в БД
    3. Отправляет в Chatwoot
    4. Отправляет в 1C:ЦЛ
    5. Обновляет запись с полученными ID
    
    Headers:
    - Authorization: Bearer <token> (опционально, для будущей валидации)
    """
    # 1. Находим или создаем клиента
    client = None
    if payload.client:
        client = await find_or_create_client(db, payload.client)
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
    
    if not client:
        raise HTTPException(
            status_code=400,
            detail="Client data or client_id is required"
        )
    
    # 2. Создаем консультацию в БД
    temp_cons_id = f"temp_{uuid.uuid4()}"
    consultation = Consultation(
        cons_id=temp_cons_id,
        client_id=client.client_id,
        cl_ref_key=payload.consultation.cl_ref_key,
        org_inn=payload.consultation.org_inn or client.org_inn,
        lang=payload.consultation.lang or "ru",
        comment=payload.consultation.comment or "",
        online_question_cat=payload.consultation.online_question_cat,
        online_question=payload.consultation.online_question,
        importance=payload.consultation.importance,
        start_date=payload.consultation.scheduled_at,
        status="new"
    )
    db.add(consultation)
    await db.flush()
    
    # 3. Отправляем в Chatwoot
    chatwoot_client = ChatwootClient()
    chatwoot_cons_id = None
    try:
        chatwoot_response = await chatwoot_client.create_conversation(
            source_id=str(client.client_id),
            inbox_id=None,  # Нужно получить из настроек
            message=payload.consultation.comment or "",
        )
        chatwoot_cons_id = str(chatwoot_response.get("id"))
        consultation.cons_id = chatwoot_cons_id
        logger.info(f"Created Chatwoot conversation: {chatwoot_cons_id}")
    except Exception as e:
        logger.error(f"Failed to create Chatwoot conversation: {e}")
        # Продолжаем без Chatwoot ID
    
    # 4. Отправляем в 1C:ЦЛ через OData
    onec_client = OneCClient()
    try:
        # Формируем КонсультацииИТС если есть данные
        consultations_its = None
        if payload.consultation.online_question_cat or payload.consultation.comment:
            consultations_its = [{
                "LineNumber": "1",
                "Вопрос": payload.consultation.comment or "",
                "Ответ": ""
            }]
            if payload.consultation.online_question_cat:
                consultations_its[0]["КатегорияВопроса_Key"] = payload.consultation.online_question_cat
        
        onec_response = await onec_client.create_consultation_odata(
            client_key=client.cl_ref_key,  # Абонент_Key из ЦЛ (если есть)
            description=payload.consultation.comment or "",
            topic=payload.consultation.topic,
            scheduled_at=payload.consultation.scheduled_at,
            question_category_key=payload.consultation.online_question_cat,
            question_key=payload.consultation.online_question,
            consultations_its=consultations_its
        )
        # OData возвращает Ref_Key и Number
        consultation.cl_ref_key = onec_response.get("Ref_Key")
        consultation.number = onec_response.get("Number")
        logger.info(f"Created 1C consultation: {consultation.cl_ref_key}, {consultation.number}")
    except Exception as e:
        logger.error(f"Failed to create 1C consultation: {e}")
        # Продолжаем без ЦЛ данных
    
    await db.commit()
    await db.refresh(consultation)
    
    return ConsultationResponse(
        consultation=TicketRead.from_model(consultation),
        client_id=str(client.client_id),
        message="Consultation created successfully"
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
    manager_key = payload.manager_key or consultation.manager or "FRONT"

    redate = ConsRedate(
        cons_key=consultation.cl_ref_key,
        clients_key=clients_key,
        manager_key=manager_key,
        period=datetime.now(timezone.utc),
        old_date=consultation.start_date,
        new_date=payload.new_date,
        comment=payload.comment,
    )
    db.add(redate)

    if payload.new_date:
        consultation.redate = payload.new_date.date()
        consultation.redate_time = payload.new_date.time()
        consultation.updated_at = datetime.now(timezone.utc)

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
            "updated_at": func.now(),
        },
    )
    await db.execute(stmt)
    await recalc_consultation_ratings(db, {consultation.cl_ref_key})
    await db.commit()
    return await _build_rating_response(db, consultation.cl_ref_key)
