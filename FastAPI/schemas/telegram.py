"""Схемы для работы с Telegram ботом"""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime


class TelegramUserLink(BaseModel):
    """Связывание Telegram пользователя с клиентом"""
    telegram_user_id: int
    client_id: Optional[str] = None  # UUID клиента
    phone_number: Optional[str] = None
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class TelegramUserLinkResponse(BaseModel):
    """Ответ при связывании пользователя"""
    success: bool
    message: str
    telegram_user_id: int
    client_id: Optional[str] = None


class TelegramMessage(BaseModel):
    """Сообщение из Chatwoot для отправки в Telegram"""
    id: str
    content: str
    message_type: str  # incoming, outgoing
    created_at: datetime
    sender_name: Optional[str] = None
    sender_type: Optional[str] = None  # user, contact, bot


class TelegramMessagesResponse(BaseModel):
    """Ответ с историей сообщений"""
    messages: List[TelegramMessage]
    total: int
    page: int
    per_page: int


class TelegramWebhookUpdate(BaseModel):
    """Webhook обновление от Telegram"""
    update_id: int
    message: Optional[Dict[str, Any]] = None
    edited_message: Optional[Dict[str, Any]] = None
    callback_query: Optional[Dict[str, Any]] = None
    # Другие типы обновлений можно добавить по необходимости


class ConsultationInfoResponse(BaseModel):
    """Информация о консультации для Telegram"""
    cons_id: str
    status: Optional[str] = None
    is_open: bool  # True если консультация открыта (можно отправлять сообщения)
    message: Optional[str] = None  # Сообщение для пользователя (если закрыта)


class TelegramUserContactCheckResponse(BaseModel):
    """Ответ при проверке наличия контакта у Telegram пользователя"""
    has_contact: bool  # True если у пользователя есть сохраненный контакт
    telegram_user_id: int
    phone_number: Optional[str] = None
    message: Optional[str] = None  # Сообщение для пользователя (если контакта нет)
