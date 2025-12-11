"""Роутеры для работы с Telegram ботом"""
import logging
from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, Dict, Any
import json

from ..database import get_db
from ..models import Consultation, TelegramUser, Client
from ..schemas.telegram import (
    TelegramUserLink,
    TelegramUserLinkResponse,
    TelegramMessagesResponse,
    TelegramMessage,
    ConsultationInfoResponse
)
from ..services.chatwoot_client import ChatwootClient
from ..services.telegram_bot import TelegramBotService
from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

# Глобальный экземпляр бота (будет инициализирован в main.py)
telegram_bot_service: Optional[TelegramBotService] = None


def get_telegram_bot_service() -> TelegramBotService:
    """Получить экземпляр TelegramBotService"""
    global telegram_bot_service
    if not telegram_bot_service:
        raise HTTPException(
            status_code=503,
            detail="Telegram bot service not initialized"
        )
    return telegram_bot_service


@router.post("/webhook")
async def telegram_webhook(
    request: Request,
    bot_service: TelegramBotService = Depends(get_telegram_bot_service)
):
    """
    Webhook от Telegram для получения обновлений.
    
    Обрабатывает обновления от Telegram и передает их в бота.
    """
    try:
        # Получаем обновление от Telegram
        update_data = await request.json()
        
        # Проверяем secret token если указан
        if settings.TELEGRAM_WEBHOOK_SECRET:
            secret_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if secret_token != settings.TELEGRAM_WEBHOOK_SECRET:
                logger.warning("Invalid secret token in Telegram webhook")
                raise HTTPException(status_code=403, detail="Invalid secret token")
        
        # Обрабатываем обновление через бота
        from telegram import Update
        update = Update.de_json(update_data, bot_service.bot)
        
        if update:
            await bot_service.application.process_update(update)
        
        return {"ok": True}
        
    except Exception as e:
        logger.error(f"Error processing Telegram webhook: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/webhook/chatwoot")
async def chatwoot_webhook_for_telegram(
    request: Request,
    db: AsyncSession = Depends(get_db),
    bot_service: TelegramBotService = Depends(get_telegram_bot_service)
):
    """
    Webhook от Chatwoot для новых сообщений в консультациях.
    
    Отправляет сообщения менеджеров в Telegram пользователям.
    """
    try:
        payload = await request.json()
        event_type = payload.get("event")
        
        # Обрабатываем только события с сообщениями
        if event_type == "message_created":
            message_data = payload.get("message", {})
            conversation = payload.get("conversation", {})
            cons_id = str(conversation.get("id", ""))
            
            if not cons_id:
                return {"ok": True}
            
            # Получаем консультацию
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation or not consultation.client_id:
                return {"ok": True}
            
            # Получаем Telegram пользователя по client_id
            result = await db.execute(
                select(TelegramUser).where(TelegramUser.client_id == consultation.client_id)
            )
            telegram_user = result.scalar_one_or_none()
            
            if not telegram_user:
                return {"ok": True}
            
            # Проверяем, что сообщение от менеджера (не от клиента)
            sender = message_data.get("sender", {})
            sender_type = sender.get("type", "")
            
            if sender_type == "user":  # Сообщение от менеджера
                content = message_data.get("content", "")
                sender_name = sender.get("name", "Менеджер")
                
                # Форматируем сообщение для Telegram
                formatted_message = f"👤 {sender_name}:\n{content}"
                
                # Отправляем в Telegram
                await bot_service.send_message_to_telegram(
                    telegram_user_id=telegram_user.telegram_user_id,
                    message_text=formatted_message
                )
        
        return {"ok": True}
        
    except Exception as e:
        logger.error(f"Error processing Chatwoot webhook for Telegram: {e}", exc_info=True)
        return {"ok": False, "error": str(e)}


@router.get("/consultations/{cons_id}/messages", response_model=TelegramMessagesResponse)
async def get_consultation_messages(
    cons_id: str,
    page: int = 1,
    per_page: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """
    Получение истории сообщений из Chatwoot для консультации.
    
    Используется для загрузки истории при открытии чата в Telegram.
    """
    try:
        # Проверяем существование консультации
        result = await db.execute(
            select(Consultation).where(Consultation.cons_id == cons_id)
        )
        consultation = result.scalar_one_or_none()
        
        if not consultation:
            raise HTTPException(status_code=404, detail="Consultation not found")
        
        # Получаем сообщения из Chatwoot
        chatwoot_client = ChatwootClient()
        messages_response = await chatwoot_client.get_messages(cons_id, page=page, per_page=per_page)
        
        # Парсим ответ Chatwoot
        messages_data = messages_response.get("payload", []) if isinstance(messages_response, dict) else []
        total = messages_response.get("meta", {}).get("count", len(messages_data)) if isinstance(messages_response, dict) else len(messages_data)
        
        # Преобразуем в формат для ответа
        messages = []
        for msg in messages_data:
            sender = msg.get("sender", {})
            messages.append(TelegramMessage(
                id=str(msg.get("id", "")),
                content=msg.get("content", ""),
                message_type=msg.get("message_type", "incoming"),
                created_at=msg.get("created_at"),
                sender_name=sender.get("name") if sender else None,
                sender_type=sender.get("type") if sender else None
            ))
        
        return TelegramMessagesResponse(
            messages=messages,
            total=total,
            page=page,
            per_page=per_page
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting consultation messages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/consultations/{cons_id}", response_model=ConsultationInfoResponse)
async def get_consultation_info(
    cons_id: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Получение информации о консультации (статус, можно ли отправлять сообщения).
    """
    try:
        result = await db.execute(
            select(Consultation).where(Consultation.cons_id == cons_id)
        )
        consultation = result.scalar_one_or_none()
        
        if not consultation:
            raise HTTPException(status_code=404, detail="Consultation not found")
        
        # Определяем, открыта ли консультация
        is_open = consultation.status in (None, "open", "pending")
        
        message = None
        if not is_open:
            status_text = {
                "closed": "закрыта",
                "resolved": "решена",
                "cancelled": "отменена"
            }.get(consultation.status, "закрыта")
            message = f"Консультация {status_text}. Новые сообщения не принимаются."
        
        return ConsultationInfoResponse(
            cons_id=cons_id,
            status=consultation.status,
            is_open=is_open,
            message=message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting consultation info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/link-user", response_model=TelegramUserLinkResponse)
async def link_telegram_user(
    link_data: TelegramUserLink,
    db: AsyncSession = Depends(get_db)
):
    """
    Связывание Telegram пользователя с клиентом.
    
    Используется при создании консультации через Telegram Web App.
    """
    try:
        # Проверяем существование клиента если указан
        client_id = None
        if link_data.client_id:
            try:
                import uuid
                client_uuid = uuid.UUID(link_data.client_id)
                result = await db.execute(
                    select(Client).where(Client.client_id == client_uuid)
                )
                client = result.scalar_one_or_none()
                if not client:
                    raise HTTPException(status_code=404, detail="Client not found")
                client_id = client.client_id
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid client_id format")
        
        # Находим или создаем запись Telegram пользователя
        result = await db.execute(
            select(TelegramUser).where(TelegramUser.telegram_user_id == link_data.telegram_user_id)
        )
        telegram_user = result.scalar_one_or_none()
        
        if telegram_user:
            # Обновляем существующего пользователя
            telegram_user.client_id = client_id
            telegram_user.phone_number = link_data.phone_number or telegram_user.phone_number
            telegram_user.username = link_data.username or telegram_user.username
            telegram_user.first_name = link_data.first_name or telegram_user.first_name
            telegram_user.last_name = link_data.last_name or telegram_user.last_name
        else:
            # Создаем нового пользователя
            telegram_user = TelegramUser(
                telegram_user_id=link_data.telegram_user_id,
                client_id=client_id,
                phone_number=link_data.phone_number,
                username=link_data.username,
                first_name=link_data.first_name,
                last_name=link_data.last_name
            )
            db.add(telegram_user)
        
        await db.commit()
        await db.refresh(telegram_user)
        
        return TelegramUserLinkResponse(
            success=True,
            message="Telegram user linked successfully",
            telegram_user_id=link_data.telegram_user_id,
            client_id=str(telegram_user.client_id) if telegram_user.client_id else None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error linking Telegram user: {e}", exc_info=True)
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

