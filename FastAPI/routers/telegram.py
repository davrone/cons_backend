"""Роутеры для работы с Telegram ботом"""
import logging
from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
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
    # ЛОГИРУЕМ СРАЗУ, ДО ПАРСИНГА JSON
    # Используем print для гарантированного вывода
    print(f"[TELEGRAM WEBHOOK] === INCOMING WEBHOOK REQUEST ===")
    print(f"[TELEGRAM WEBHOOK] Method: {request.method}, URL: {request.url}")
    logger.info("=== INCOMING WEBHOOK REQUEST ===")
    logger.info(f"Method: {request.method}")
    logger.info(f"URL: {request.url}")
    logger.info(f"Headers: {dict(request.headers)}")
    
    try:
        # Получаем тело запроса как байты для логирования
        body_bytes = await request.body()
        logger.info(f"Body length: {len(body_bytes)} bytes")
        
        if len(body_bytes) == 0:
            logger.warning("Empty request body")
            return {"ok": True}
        
        # Парсим JSON
        import json
        payload = json.loads(body_bytes)
        event_type = payload.get("event")
        
        # Логируем все входящие webhook'и для отладки
        print(f"[TELEGRAM WEBHOOK] Received event: {event_type}, payload_keys: {list(payload.keys())}")
        logger.info(f"Received Chatwoot webhook: event={event_type}, payload_keys={list(payload.keys())}")
        
        # Обрабатываем событие создания conversation - просто логируем, связывание будет при первом сообщении в бот
        if event_type == "conversation_created":
            print(f"[TELEGRAM WEBHOOK] Processing conversation_created event")
            conversation = payload.get("conversation", {})
            cons_id = str(conversation.get("id", ""))
            logger.info(f"Conversation created: cons_id={cons_id}, will be linked when user sends first message to bot")
            return {"ok": True}
        
        # Обрабатываем изменение статуса conversation (закрытие заявки)
        if event_type == "conversation_status_changed" or event_type == "conversation_updated":
            print(f"[TELEGRAM WEBHOOK] Processing conversation_status_changed/updated event")
            conversation = payload.get("conversation", {})
            cons_id = str(conversation.get("id", ""))
            new_status = conversation.get("status", "")
            
            if not cons_id:
                logger.warning("No cons_id in conversation_status_changed webhook")
                return {"ok": True}
            
            # Получаем консультацию из БД для проверки старого статуса
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation or not consultation.client_id:
                logger.warning(f"Consultation {cons_id} not found or has no client_id")
                return {"ok": True}
            
            # Проверяем, что статус изменился на resolved или closed (не был уже закрыт)
            old_status = consultation.status
            if new_status in ("resolved", "closed") and old_status not in ("resolved", "closed"):
                print(f"[TELEGRAM WEBHOOK] Conversation {cons_id} closed/resolved (was {old_status}), notifying Telegram user")
                
                # Получаем Telegram пользователя по client_id
                # ВАЖНО: Может быть несколько записей с одним client_id, берем первую (самую свежую)
                result = await db.execute(
                    select(TelegramUser)
                    .where(TelegramUser.client_id == consultation.client_id)
                    .order_by(TelegramUser.created_at.desc())
                    .limit(1)
                )
                telegram_user = result.scalar_one_or_none()
                
                # Если нашли несколько записей, логируем предупреждение
                count_result = await db.execute(
                    select(func.count(TelegramUser.telegram_user_id))
                    .where(TelegramUser.client_id == consultation.client_id)
                )
                count = count_result.scalar() or 0
                if count > 1:
                    print(f"[TELEGRAM WEBHOOK] WARNING: Found {count} Telegram users for client_id {consultation.client_id}, using the most recent one")
                    logger.warning(f"Found {count} Telegram users for client_id {consultation.client_id}, using the most recent one")
                
                if telegram_user:
                    # Отправляем сообщение о закрытии заявки
                    status_text = "закрыта" if new_status == "closed" else "решена"
                    close_message = (
                        f"✅ Ваша заявка #{consultation.number or cons_id} {status_text}.\n\n"
                        f"Спасибо за обращение! Если у вас возникнут дополнительные вопросы, "
                        f"вы можете создать новую заявку через портал поддержки."
                    )
                    
                    # Добавляем кнопки: web app и ссылка на оценку
                    from telegram import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
                    from ..config import settings
                    
                    # Определяем URL для Web App
                    if settings.TELEGRAM_WEBAPP_URL:
                        web_app_url = settings.TELEGRAM_WEBAPP_URL.rstrip("/")
                        if "/subscriptions" not in web_app_url:
                            web_app_url = f"{web_app_url}/subscriptions"
                    elif settings.TELEGRAM_WEBHOOK_URL:
                        base_url = settings.TELEGRAM_WEBHOOK_URL.replace("/api/telegram/webhook", "").rstrip("/")
                        if "backdev" in base_url:
                            base_url = base_url.replace("backdev", "dev")
                        web_app_url = f"{base_url}/subscriptions"
                    else:
                        web_app_url = "https://dev.clobus.uz/subscriptions"
                    
                    # Формируем ссылку на оценку в Chatwoot
                    # Стандартный формат: https://{chatwoot_url}/public/conversations/{conversation_id}/rating
                    chatwoot_base_url = settings.CHATWOOT_API_URL.rstrip("/")
                    # Убираем /api/v1 если есть
                    if chatwoot_base_url.endswith("/api/v1"):
                        chatwoot_base_url = chatwoot_base_url.replace("/api/v1", "")
                    rating_url = f"{chatwoot_base_url}/public/conversations/{cons_id}/rating"
                    
                    keyboard = [
                        [InlineKeyboardButton(
                            "⭐ Оценить консультацию",
                            url=rating_url
                        )],
                        [InlineKeyboardButton(
                            "📱 Открыть портал поддержки",
                            web_app=WebAppInfo(url=web_app_url)
                        )]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    try:
                        await bot_service.send_message_to_telegram(
                            telegram_user_id=telegram_user.telegram_user_id,
                            message_text=close_message
                        )
                        
                        # Отправляем сообщение с кнопками (оценка и web app)
                        rating_message = (
                            "Пожалуйста, оцените качество нашей работы:\n\n"
                            "• Нажмите на кнопку ниже, чтобы оставить оценку\n"
                            "• Или откройте портал поддержки для создания новой заявки"
                        )
                        await bot_service.bot.send_message(
                            chat_id=telegram_user.telegram_user_id,
                            text=rating_message,
                            reply_markup=reply_markup
                        )
                        
                        print(f"[TELEGRAM WEBHOOK] Sent close notification and rating link to Telegram user {telegram_user.telegram_user_id}")
                        logger.info(f"Sent close notification and rating link to Telegram user {telegram_user.telegram_user_id} for consultation {cons_id}")
                    except Exception as e:
                        print(f"[TELEGRAM WEBHOOK] ERROR sending close notification: {e}")
                        logger.error(f"Error sending close notification to Telegram: {e}", exc_info=True)
                else:
                    logger.warning(f"No Telegram user found for client_id {consultation.client_id}")
            
            return {"ok": True}
        
        # Обрабатываем события с сообщениями
        # Chatwoot использует формат "message_created" (с подчеркиванием), а не "message.created"
        if event_type == "message_created" or event_type == "message.created":
            print(f"[TELEGRAM WEBHOOK] Processing message_created event")
            
            # ВАЖНО: В Chatwoot webhook данные находятся в корне payload, а не в payload.message!
            # Из логов видно: payload_keys: ['account', 'content', 'conversation', 'message_type', 'sender', ...]
            # Поэтому используем payload напрямую как message_data
            message_data = payload  # Данные сообщения находятся в корне payload
            
            # Логируем структуру attachments для отладки
            print(f"[TELEGRAM WEBHOOK] Full payload structure - attachments key exists: {'attachments' in payload}")
            if 'attachments' in payload:
                print(f"[TELEGRAM WEBHOOK] attachments value: {payload.get('attachments')}")
                print(f"[TELEGRAM WEBHOOK] attachments type: {type(payload.get('attachments'))}")
            
            conversation = payload.get("conversation", {})
            cons_id = str(conversation.get("id", ""))
            
            print(f"[TELEGRAM WEBHOOK] cons_id={cons_id}, message_id={message_data.get('id')}")
            print(f"[TELEGRAM WEBHOOK] payload keys: {list(payload.keys())}")
            logger.info(f"Processing message_created webhook: cons_id={cons_id}, message_id={message_data.get('id')}")
            
            if not cons_id:
                logger.warning("No cons_id in webhook payload")
                return {"ok": True}
            
            # Пропускаем системные сообщения (private notes, activity messages)
            private = message_data.get("private", False)
            message_type = message_data.get("message_type", "")
            # В логах Chatwoot видно, что message_type может быть числом (1 = outgoing, 0 = incoming)
            # Преобразуем в строку для сравнения
            if isinstance(message_type, int):
                message_type = "outgoing" if message_type == 1 else "incoming"
            
            # content может быть None, поэтому обрабатываем это
            content = message_data.get("content") or ""
            if content:
                content = str(content).strip()
            else:
                content = ""
            sender = message_data.get("sender", {})
            original_sender_type = sender.get("type", "") if sender else ""
            # Сохраняем оригинальный тип для проверки (может быть "User" с большой буквы)
            sender_type_lower = original_sender_type.lower() if original_sender_type else ""
            sender_id = sender.get("id") if sender else None
            
            print(f"[TELEGRAM WEBHOOK] Message details: private={private}, message_type={message_type}, original_sender_type={original_sender_type}, sender_id={sender_id}, content_length={len(content)}")
            logger.info(f"Message details: private={private}, message_type={message_type}, original_sender_type={original_sender_type}, sender_type_lower={sender_type_lower}, sender_id={sender_id}, content_length={len(content)}")
            
            # Проверяем наличие вложений ДО проверки content
            # Это нужно, чтобы не пропускать сообщения с файлами без текста
            has_attachments = bool(message_data.get("attachments")) or bool(message_data.get("content_attributes", {}).get("attachments"))
            
            # Пропускаем системные сообщения, но НЕ пропускаем сообщения с вложениями (даже если content пустой)
            if private or message_type == "activity":
                logger.debug(f"Skipping system message: private={private}, type={message_type}")
                return {"ok": True}
            
            # Пропускаем сообщения без текста и без вложений
            if not content and not has_attachments:
                logger.debug(f"Skipping empty message: content_length={len(content)}, has_attachments={has_attachments}")
                return {"ok": True}
            
            # Получаем консультацию
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation:
                logger.warning(f"Consultation {cons_id} not found in database")
                return {"ok": True}
            
            if not consultation.client_id:
                logger.warning(f"Consultation {cons_id} has no client_id")
                return {"ok": True}
            
            # Получаем Telegram пользователя по client_id
            # ВАЖНО: Может быть несколько записей с одним client_id, берем первую (самую свежую)
            result = await db.execute(
                select(TelegramUser)
                .where(TelegramUser.client_id == consultation.client_id)
                .order_by(TelegramUser.created_at.desc())
                .limit(1)
            )
            telegram_user = result.scalar_one_or_none()
            
            # Если нашли несколько записей, логируем предупреждение
            count_result = await db.execute(
                select(func.count(TelegramUser.telegram_user_id))
                .where(TelegramUser.client_id == consultation.client_id)
            )
            count = count_result.scalar() or 0
            if count > 1:
                print(f"[TELEGRAM WEBHOOK] WARNING: Found {count} Telegram users for client_id {consultation.client_id}, using the most recent one")
                logger.warning(f"Found {count} Telegram users for client_id {consultation.client_id}, using the most recent one")
            
            if not telegram_user:
                print(f"[TELEGRAM WEBHOOK] WARNING: No Telegram user found for client_id {consultation.client_id}")
                logger.warning(f"No Telegram user found for client_id {consultation.client_id}")
                # Пытаемся найти Telegram пользователя по phone_number из контакта Chatwoot
                # Это может помочь, если пользователь создал заявку через web app, но не связал Telegram
                conversation_meta = conversation.get("meta", {})
                sender = conversation_meta.get("sender", {})
                phone_number = sender.get("phone_number")
                
                print(f"[TELEGRAM WEBHOOK] conversation.meta structure: {list(conversation_meta.keys()) if conversation_meta else 'empty'}")
                print(f"[TELEGRAM WEBHOOK] sender structure: {list(sender.keys()) if sender else 'empty'}")
                print(f"[TELEGRAM WEBHOOK] phone_number from sender: {phone_number}")
                
                if phone_number:
                    print(f"[TELEGRAM WEBHOOK] Trying to find Telegram user by phone_number: {phone_number}")
                    # ВАЖНО: Может быть несколько записей с одним phone_number, берем первую (самую свежую)
                    result = await db.execute(
                        select(TelegramUser)
                        .where(TelegramUser.phone_number == phone_number)
                        .order_by(TelegramUser.created_at.desc())
                        .limit(1)
                    )
                    telegram_user = result.scalar_one_or_none()
                    if telegram_user:
                        # Связываем найденного Telegram пользователя с клиентом
                        telegram_user.client_id = consultation.client_id
                        await db.commit()
                        print(f"[TELEGRAM WEBHOOK] Linked Telegram user {telegram_user.telegram_user_id} with client_id {consultation.client_id} by phone_number")
                        logger.info(f"Linked Telegram user {telegram_user.telegram_user_id} with client_id {consultation.client_id} by phone_number")
                    else:
                        print(f"[TELEGRAM WEBHOOK] No Telegram user found by phone_number: {phone_number}")
                        logger.warning(f"No Telegram user found by phone_number: {phone_number} for conversation {cons_id}")
                        return {"ok": True}
                else:
                    print(f"[TELEGRAM WEBHOOK] No phone_number in conversation meta for cons_id {cons_id}")
                    print(f"[TELEGRAM WEBHOOK] Full conversation structure: {list(conversation.keys())}")
                    logger.warning(f"No phone_number in conversation meta for cons_id {cons_id}. Conversation keys: {list(conversation.keys())}")
                    return {"ok": True}
            
            print(f"[TELEGRAM WEBHOOK] Found Telegram user: telegram_user_id={telegram_user.telegram_user_id}, client_id={consultation.client_id}")
            logger.info(f"Found Telegram user: telegram_user_id={telegram_user.telegram_user_id}, client_id={consultation.client_id}")
            
            # В Chatwoot:
            # - message_type == "outgoing" означает сообщение от менеджера/бота
            # - message_type == "incoming" означает сообщение от клиента
            # - sender.type == "user" означает менеджер
            # - sender.type == "contact" означает клиент
            
            # Отправляем только сообщения от менеджеров (outgoing или sender_type == "user")
            # В логах Chatwoot видно: message_type: "outgoing", sender_type: "User"
            is_manager_message = (
                message_type == "outgoing" or 
                sender_type_lower == "user" or 
                original_sender_type == "User"  # Проверяем оригинальный тип (может быть с большой буквы)
            )
            
            # Используем print для гарантированного вывода в консоль
            print(f"[TELEGRAM WEBHOOK] Checking message: message_type={message_type}, original_sender_type={original_sender_type}, sender_type_lower={sender_type_lower}, is_manager={is_manager_message}")
            logger.info(f"Checking if manager message: message_type={message_type}, original_sender_type={original_sender_type}, sender_type_lower={sender_type_lower}, is_manager={is_manager_message}")
            
            if is_manager_message:
                print(f"[TELEGRAM WEBHOOK] Processing manager message, sending to Telegram user {telegram_user.telegram_user_id}")
                sender_name = sender.get("name", "Менеджер")
                
                # Проверяем наличие вложений
                attachments = message_data.get("attachments", [])
                print(f"[TELEGRAM WEBHOOK] Attachments check: attachments={attachments}, type={type(attachments)}, len={len(attachments) if attachments else 0}")
                print(f"[TELEGRAM WEBHOOK] Full message_data keys: {list(message_data.keys())}")
                logger.info(f"Attachments in message: {attachments}, type: {type(attachments)}")
                
                # Проверяем также content_attributes, там могут быть вложения
                content_attributes = message_data.get("content_attributes", {})
                print(f"[TELEGRAM WEBHOOK] content_attributes: {content_attributes}")
                if content_attributes and isinstance(content_attributes, dict) and "attachments" in content_attributes:
                    content_attrs_attachments = content_attributes.get("attachments", [])
                    print(f"[TELEGRAM WEBHOOK] Found attachments in content_attributes: {content_attrs_attachments}")
                    if content_attrs_attachments and (not attachments or len(attachments) == 0):
                        attachments = content_attrs_attachments
                        print(f"[TELEGRAM WEBHOOK] Using attachments from content_attributes: {attachments}")
                
                if attachments and len(attachments) > 0:
                    # Если есть вложения, отправляем их отдельно
                    print(f"[TELEGRAM WEBHOOK] Found {len(attachments)} attachments")
                    for idx, attachment in enumerate(attachments):
                        print(f"[TELEGRAM WEBHOOK] Processing attachment {idx+1}/{len(attachments)}: {attachment}")
                        logger.info(f"Processing attachment {idx+1}: {attachment}")
                        
                        # Пробуем разные варианты ключей для URL
                        attachment_url = (
                            attachment.get("data_url") or 
                            attachment.get("file_url") or 
                            attachment.get("url") or
                            attachment.get("download_url") or
                            attachment.get("file")
                        )
                        attachment_type = attachment.get("file_type") or attachment.get("type", "file")
                        attachment_name = attachment.get("name") or attachment.get("filename") or attachment.get("file_name", "file")
                        
                        print(f"[TELEGRAM WEBHOOK] Processing attachment: name={attachment_name}, type={attachment_type}, url={attachment_url}")
                        
                        if attachment_url:
                            logger.info(f"Sending attachment to Telegram: {attachment_name}, type={attachment_type}")
                            
                            # Формируем caption с именем отправителя и текстом сообщения (если есть)
                            caption_parts = []
                            if sender_name:
                                caption_parts.append(f"👤 {sender_name}")
                            if content:
                                caption_parts.append(content)
                            caption = "\n".join(caption_parts) if caption_parts else None
                            
                            try:
                                # Отправляем медиафайл через Telegram Bot API
                                await bot_service.send_media_to_telegram(
                                    telegram_user_id=telegram_user.telegram_user_id,
                                    file_url=attachment_url,
                                    file_type=attachment_type,
                                    caption=caption
                                )
                                print(f"[TELEGRAM WEBHOOK] Successfully sent attachment {attachment_name} to Telegram user {telegram_user.telegram_user_id}")
                            except Exception as attach_error:
                                print(f"[TELEGRAM WEBHOOK] ERROR sending attachment: {attach_error}")
                                logger.error(f"Error sending attachment to Telegram: {attach_error}", exc_info=True)
                                # Если не удалось отправить файл, отправляем ссылку на файл
                                file_message = f"👤 {sender_name} отправил файл: {attachment_name}\n{attachment_url}"
                                await bot_service.send_message_to_telegram(
                                    telegram_user_id=telegram_user.telegram_user_id,
                                    message_text=file_message
                                )
                        else:
                            logger.warning(f"Attachment has no URL: {attachment}")
                            print(f"[TELEGRAM WEBHOOK] Attachment has no URL: {attachment}")
                else:
                    # Обычное текстовое сообщение
                    formatted_message = f"👤 {sender_name}:\n{content}"
                    
                    logger.info(f"Sending message to Telegram: user_id={telegram_user.telegram_user_id}, sender={sender_name}, message_type={message_type}, original_sender_type={original_sender_type}, sender_type_lower={sender_type_lower}")
                    
                    # Отправляем в Telegram
                    print(f"[TELEGRAM WEBHOOK] Attempting to send message to Telegram user {telegram_user.telegram_user_id}")
                    try:
                        await bot_service.send_message_to_telegram(
                            telegram_user_id=telegram_user.telegram_user_id,
                            message_text=formatted_message
                        )
                        print(f"[TELEGRAM WEBHOOK] Successfully sent message to Telegram user {telegram_user.telegram_user_id}")
                        logger.info(f"Successfully sent message from Chatwoot to Telegram user {telegram_user.telegram_user_id}")
                    except Exception as send_error:
                        print(f"[TELEGRAM WEBHOOK] ERROR sending to Telegram: {send_error}")
                        logger.error(f"Error sending message to Telegram: {send_error}", exc_info=True)
                        raise
            else:
                logger.info(f"Skipping message from client: message_type={message_type}, original_sender_type={original_sender_type}, sender_type_lower={sender_type_lower}")
        else:
            logger.debug(f"Unhandled event type: {event_type}")
        
        return {"ok": True}
        
    except Exception as e:
        logger.error(f"Error processing Chatwoot webhook for Telegram: {e}", exc_info=True)
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
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

