"""Роуты для обработки вебхуков от внешних систем"""
from fastapi import APIRouter, Request, HTTPException, Header, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
import hmac
import hashlib
import json

from ..database import get_db
from ..models import WebhookLog, Consultation
from ..schemas.webhooks import WebhookResponse
from ..config import settings
from ..services.chatwoot_client import ChatwootClient
from ..services.onec_client import OneCClient

router = APIRouter()


def verify_chatwoot_signature(payload: bytes, signature: str) -> bool:
    """Проверка подписи вебхука от Chatwoot"""
    if not settings.CHATWOOT_API_TOKEN:
        return True  # В dev режиме пропускаем
    
    expected_signature = hmac.new(
        settings.CHATWOOT_API_TOKEN.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_signature, signature)


@router.post("/chatwoot", response_model=WebhookResponse)
async def chatwoot_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_chatwoot_signature: Optional[str] = Header(None)
):
    """
    Обработка вебхука от Chatwoot.
    
    События:
    - conversation.created
    - conversation.updated
    - message.created
    - message.updated
    """
    # Получаем payload
    body = await request.body()
    payload = json.loads(body)
    
    # Сохраняем в лог
    webhook_log = WebhookLog(
        source="CHATWOOT",
        payload=payload
    )
    db.add(webhook_log)
    await db.flush()
    
    # Проверяем подпись
    if x_chatwoot_signature:
        if not verify_chatwoot_signature(body, x_chatwoot_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Обрабатываем событие
    event_type = payload.get("event")
    event_data = payload.get("data", {})
    
    try:
        if event_type == "conversation.created":
            # Новая консультация создана в Chatwoot
            conversation = event_data.get("conversation", {})
            cons_id = str(conversation.get("id"))
            
            # Проверяем, есть ли уже в БД
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation:
                # Создаем новую запись
                consultation = Consultation(
                    cons_id=cons_id,
                    status=conversation.get("status"),
                    create_date=conversation.get("created_at"),
                )
                db.add(consultation)
        
        elif event_type == "conversation.updated":
            # Обновление консультации
            conversation = event_data.get("conversation", {})
            cons_id = str(conversation.get("id"))
            
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if consultation:
                # Обновляем поля
                if "status" in conversation:
                    consultation.status = conversation["status"]
                if "assignee" in conversation:
                    consultation.manager = str(conversation["assignee"].get("id")) if conversation["assignee"] else None
        
        elif event_type == "message.created":
            # Новое сообщение в консультации
            message = event_data.get("message", {})
            conversation_id = str(message.get("conversation_id"))
            
            # Можно обновить last_message_at или сохранить в q_and_a
            # В зависимости от бизнес-логики
        
        await db.commit()
        webhook_log.processed = True
        await db.commit()
        
        return WebhookResponse(status="ok", message=f"Processed {event_type}")
    
    except Exception as e:
        await db.rollback()
        webhook_log.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")


@router.post("/1c_cl", response_model=WebhookResponse)
async def onec_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Обработка вебхука от 1C:ЦЛ.
    
    События:
    - consultation.created
    - consultation.updated
    - consultation.closed
    - consultation.rescheduled
    """
    payload = await request.json()
    
    # Сохраняем в лог
    webhook_log = WebhookLog(
        source="1C_CL",
        payload=payload
    )
    db.add(webhook_log)
    await db.flush()
    
    event_type = payload.get("event")
    event_data = payload.get("data", {})
    
    try:
        if event_type == "consultation.created":
            # Новая консультация из ЦЛ
            cl_ref_key = event_data.get("cl_ref_key")
            number = event_data.get("number")
            
            # Ищем по cl_ref_key или создаем новую
            result = await db.execute(
                select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation:
                # Создаем новую (cons_id будет обновлен после синхронизации с Chatwoot)
                consultation = Consultation(
                    cons_id=f"cl_{cl_ref_key}",  # Временный ID
                    cl_ref_key=cl_ref_key,
                    number=number,
                    status=event_data.get("status", "new"),
                    org_inn=event_data.get("org_inn"),
                )
                db.add(consultation)
            
            # Отправляем в Chatwoot
            chatwoot_client = ChatwootClient()
            try:
                chatwoot_response = await chatwoot_client.create_conversation(
                    source_id=None,
                    inbox_id=None,
                    message=event_data.get("description", ""),
                )
                consultation.cons_id = str(chatwoot_response.get("id"))
            except Exception:
                pass  # Chatwoot недоступен
        
        elif event_type == "consultation.updated":
            # Обновление из ЦЛ
            cl_ref_key = event_data.get("cl_ref_key")
            cons_id = event_data.get("cons_id")
            
            # Ищем консультацию
            if cons_id:
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
            else:
                result = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
                )
            
            consultation = result.scalar_one_or_none()
            
            if consultation:
                # Обновляем поля
                if "status" in event_data:
                    consultation.status = event_data["status"]
                if "manager" in event_data:
                    consultation.manager = event_data["manager"]
                if "start_date" in event_data:
                    consultation.start_date = event_data["start_date"]
                if "end_date" in event_data:
                    consultation.end_date = event_data["end_date"]
                
                # Синхронизируем с Chatwoot
                if consultation.cons_id and consultation.cons_id.startswith("cl_"):
                    # Если еще нет Chatwoot ID, создаем
                    pass
                else:
                    # Обновляем в Chatwoot
                    chatwoot_client = ChatwootClient()
                    try:
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status=consultation.status,
                        )
                    except Exception:
                        pass
        
        elif event_type == "consultation.closed":
            # Закрытие консультации
            cl_ref_key = event_data.get("cl_ref_key")
            cons_id = event_data.get("cons_id")
            
            if cons_id:
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
            else:
                result = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
                )
            
            consultation = result.scalar_one_or_none()
            
            if consultation:
                consultation.status = "closed"
                consultation.end_date = event_data.get("end_date")
                
                # Обновляем в Chatwoot
                if consultation.cons_id and not consultation.cons_id.startswith("cl_"):
                    chatwoot_client = ChatwootClient()
                    try:
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status="resolved",
                        )
                    except Exception:
                        pass
        
        await db.commit()
        webhook_log.processed = True
        await db.commit()
        
        return WebhookResponse(status="ok", message=f"Processed {event_type}")
    
    except Exception as e:
        await db.rollback()
        webhook_log.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")
