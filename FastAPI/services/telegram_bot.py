"""Сервис для работы с Telegram ботом"""
import logging
from typing import Optional, Dict, Any
from telegram import Bot, Update, WebAppInfo, MenuButtonWebApp
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.request import HTTPXRequest

from ..config import settings
from ..services.chatwoot_client import ChatwootClient
from ..database import AsyncSessionLocal
from ..models import Consultation, TelegramUser, Client
from sqlalchemy import select

logger = logging.getLogger(__name__)


class TelegramBotService:
    """Сервис для управления Telegram ботом"""
    
    def __init__(self):
        """Инициализация бота"""
        if not settings.TELEGRAM_BOT_TOKEN:
            logger.warning("TELEGRAM_BOT_TOKEN not set, Telegram bot will not be initialized")
            self.bot = None
            self.application = None
            return
        
        # Создаем приложение бота
        self.application = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).request(
            HTTPXRequest(connection_pool_size=8)
        ).build()
        
        self.bot = self.application.bot
        
        # Настраиваем обработчики
        self.setup_handlers()
        
        logger.info("Telegram bot service initialized")
    
    def setup_handlers(self):
        """Настройка обработчиков команд и сообщений"""
        if not self.application:
            return
        
        # Обработчик команды /start
        self.application.add_handler(CommandHandler("start", self.start_command))
        
        # Обработчик контакта
        self.application.add_handler(MessageHandler(filters.CONTACT, self.handle_contact))
        
        # Обработчик медиафайлов (фото, документы, аудио, видео)
        self.application.add_handler(MessageHandler(
            filters.PHOTO | filters.Document.ALL | filters.AUDIO | filters.VOICE | filters.VIDEO,
            self.handle_media
        ))
        
        # Обработчик текстовых сообщений (должен быть последним, чтобы не перехватывать медиа)
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        logger.info("Telegram bot handlers setup completed")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /start"""
        if not update.message:
            return
        
        # ВАЖНО: Обрабатываем только личные сообщения, игнорируем команды из групп
        if update.message.chat.type != "private":
            logger.debug(f"Ignoring /start command from non-private chat: chat_type={update.message.chat.type}, chat_id={update.message.chat.id}")
            return
        
        user = update.message.from_user
        telegram_user_id = user.id
        
        # Получаем параметр из команды (deep link: /start cons_123)
        command_args = context.args
        cons_id = None
        
        if command_args and len(command_args) > 0:
            # Парсим параметр вида "cons_123" или просто "123"
            param = command_args[0]
            if param.startswith("cons_"):
                cons_id = param[5:]  # Убираем префикс "cons_"
            else:
                cons_id = param
        
        # Если передан cons_id - открываем чат с консультацией
        if cons_id:
            await self.open_consultation_chat(update, context, cons_id, telegram_user_id)
        else:
            # Обычный старт - приветствие и запрос контакта
            await self.send_welcome_message(update, context, telegram_user_id)
    
    async def send_welcome_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, telegram_user_id: int):
        """Отправка приветственного сообщения с запросом контакта"""
        welcome_text = (
            "👋 Добро пожаловать!\n\n"
            "Для работы с консультациями нам нужен ваш контакт.\n"
            "Пожалуйста, нажмите кнопку ниже, чтобы отправить ваш номер телефона."
        )
        
        from telegram import ReplyKeyboardMarkup, KeyboardButton
        
        # Кнопка отправки контакта в ReplyKeyboardMarkup (основная кнопка)
        # ВАЖНО: request_contact работает только в ReplyKeyboardMarkup, не в InlineKeyboard
        keyboard = [[KeyboardButton("📱 Отправить контакт", request_contact=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup
        )
        
        # Дополнительное сообщение с подсказкой для тех, кто не видит кнопку
        hint_text = (
            "💡 Если вы не видите кнопку отправки контакта, "
            "попробуйте обновить приложение Telegram или используйте меню бота."
        )
        await update.message.reply_text(hint_text)
    
    async def open_consultation_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE, cons_id: str, telegram_user_id: int):
        """Открытие чата с консультацией и загрузка истории"""
        try:
            # Проверяем существование консультации
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
                consultation = result.scalar_one_or_none()
                
                if not consultation:
                    await update.message.reply_text(
                        "❌ Консультация не найдена. Пожалуйста, создайте новую заявку через портал поддержки."
                    )
                    return
                
                # Сохраняем активную консультацию в контексте пользователя
                # Это позволяет обрабатывать сообщения без проверки client_id
                if context.user_data is not None:
                    context.user_data["active_cons_id"] = cons_id
                    context.user_data["active_client_id"] = consultation.client_id
                
                # ВАЖНО: Связываем Telegram пользователя с клиентом из консультации
                # Это нужно для того, чтобы сообщения от операторов доходили до пользователя
                if consultation.client_id:
                    try:
                        result = await db.execute(
                            select(TelegramUser).where(TelegramUser.telegram_user_id == telegram_user_id)
                        )
                        telegram_user = result.scalar_one_or_none()
                        
                        if telegram_user:
                            # Обновляем существующего пользователя
                            telegram_user.client_id = consultation.client_id
                            logger.info(f"Linked Telegram user {telegram_user_id} with client {consultation.client_id} via /start command")
                        else:
                            # Создаем нового пользователя
                            telegram_user = TelegramUser(
                                telegram_user_id=telegram_user_id,
                                client_id=consultation.client_id
                            )
                            db.add(telegram_user)
                            logger.info(f"Created and linked Telegram user {telegram_user_id} with client {consultation.client_id} via /start command")
                        
                        await db.commit()
                    except Exception as e:
                        logger.error(f"Failed to link Telegram user via /start command: {e}", exc_info=True)
                        await db.rollback()
                
                # Проверяем статус консультации
                is_open = consultation.status in (None, "open", "pending")
                
                if not is_open:
                    status_text = {
                        "closed": "закрыта",
                        "resolved": "решена",
                        "cancelled": "отменена"
                    }.get(consultation.status, "закрыта")
                    
                    await update.message.reply_text(
                        f"ℹ️ Эта консультация {status_text}. Новые сообщения не принимаются.\n\n"
                        f"Вы можете создать новую заявку через портал поддержки."
                    )
                    return
                
                # Загружаем историю сообщений из Chatwoot
                await self.load_conversation_history(update, context, cons_id, telegram_user_id)
                
        except Exception as e:
            logger.error(f"Error opening consultation chat: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при открытии чата. Пожалуйста, попробуйте позже."
            )
    
    async def load_conversation_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE, cons_id: str, telegram_user_id: int):
        """Загрузка истории сообщений из Chatwoot и отправка в Telegram"""
        try:
            chatwoot_client = ChatwootClient()
            
            # Получаем сообщения (первая страница, 50 сообщений)
            messages_response = await chatwoot_client.get_messages(cons_id, page=1, per_page=50)
            
            # Парсим ответ Chatwoot
            messages = messages_response.get("payload", []) if isinstance(messages_response, dict) else []
            
            if not messages:
                await update.message.reply_text(
                    "💬 Чат открыт. Начните общение, отправив сообщение."
                )
                return
            
            # Фильтруем только пользовательские сообщения (не системные)
            user_messages = []
            for msg in messages:
                # Пропускаем системные сообщения (private notes, activity messages)
                private = msg.get("private", False)
                message_type = msg.get("message_type", "")
                # content может быть None, поэтому обрабатываем это
                content = msg.get("content") or ""
                if content:
                    content = str(content).strip()
                else:
                    content = ""
                
                # Пропускаем приватные заметки и пустые сообщения
                if private or not content:
                    continue
                
                # Пропускаем activity сообщения (системные события)
                if message_type == "activity":
                    continue
                
                user_messages.append(msg)
            
            if not user_messages:
                await update.message.reply_text(
                    "💬 Чат открыт. Начните общение, отправив сообщение."
                )
                return
            
            # Отправляем информацию о загрузке истории
            await update.message.reply_text(
                f"📜 Загружаю историю сообщений ({len(user_messages)} сообщений)..."
            )
            
            # Отправляем сообщения в хронологическом порядке (старые первыми)
            for msg in reversed(user_messages):
                # content может быть None, поэтому обрабатываем это
                content = msg.get("content") or ""
                if content:
                    content = str(content).strip()
                else:
                    content = ""
                
                message_type = msg.get("message_type", "incoming")
                sender = msg.get("sender", {})
                sender_name = sender.get("name", "Менеджер") if sender else "Менеджер"
                sender_type = sender.get("type", "") if sender else ""
                
                # В Chatwoot:
                # - message_type == "incoming" означает сообщение от клиента (входящее)
                # - message_type == "outgoing" означает сообщение от менеджера/бота (исходящее)
                # - sender.type == "user" означает менеджер
                # - sender.type == "contact" означает клиент
                
                if message_type == "outgoing" or sender_type == "user":
                    # Сообщение от менеджера
                    formatted_msg = f"👤 {sender_name}:\n{content}"
                else:
                    # Сообщение от клиента
                    formatted_msg = f"💬 Вы:\n{content}"
                
                await update.message.reply_text(formatted_msg)
            
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
                web_app_url = "https://your-domain.com/subscriptions"
            
            # Отправляем сообщение с кнопкой web app
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton
            keyboard = [[InlineKeyboardButton(
                "📱 Открыть портал поддержки",
                web_app=WebAppInfo(url=web_app_url)
            )]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "✅ История загружена. Теперь вы можете продолжить общение.",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error loading conversation history: {e}", exc_info=True)
            await update.message.reply_text(
                "⚠️ Не удалось загрузить историю сообщений, но вы можете начать общение."
            )
    
    async def handle_contact(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка отправки контакта"""
        if not update.message or not update.message.contact:
            return
        
        # ВАЖНО: Обрабатываем только личные сообщения, игнорируем сообщения из групп
        if update.message.chat.type != "private":
            logger.debug(f"Ignoring contact from non-private chat: chat_type={update.message.chat.type}, chat_id={update.message.chat.id}")
            return
        
        contact = update.message.contact
        # Получаем telegram_user_id из контакта (если это контакт пользователя) или из сообщения
        telegram_user_id = contact.user_id if contact.user_id else update.message.from_user.id
        phone_number = contact.phone_number
        
        # Username и другие данные получаем из from_user, а не из contact
        from_user = update.message.from_user
        
        try:
            # Сохраняем или обновляем информацию о пользователе Telegram
            # ВАЖНО: Эти данные сохраняются только в telegram_users для связи с клиентом
            # Данные клиента (имя, email и т.д.) заполняются на фронте через webapp
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.telegram_user_id == telegram_user_id)
                )
                telegram_user = result.scalar_one_or_none()
                
                # Проверяем, есть ли активная консультация для связывания
                active_client_id = None
                if context.user_data and context.user_data.get("active_client_id"):
                    active_client_id = context.user_data.get("active_client_id")
                
                if telegram_user:
                    # Обновляем существующего пользователя Telegram
                    telegram_user.phone_number = phone_number
                    telegram_user.first_name = from_user.first_name
                    telegram_user.last_name = from_user.last_name
                    telegram_user.username = from_user.username
                    
                    # ВАЖНО: Если есть активная консультация, связываем с клиентом
                    if active_client_id:
                        telegram_user.client_id = active_client_id
                        logger.info(f"Linked Telegram user {telegram_user_id} with client {active_client_id} via contact")
                else:
                    # Создаем нового пользователя Telegram
                    telegram_user = TelegramUser(
                        telegram_user_id=telegram_user_id,
                        phone_number=phone_number,
                        first_name=from_user.first_name,
                        last_name=from_user.last_name,
                        username=from_user.username,
                        client_id=active_client_id
                    )
                    db.add(telegram_user)
                    if active_client_id:
                        logger.info(f"Created and linked Telegram user {telegram_user_id} with client {active_client_id} via contact")
                
                await db.commit()
            
            # Отправляем сообщение с кнопкой для открытия Web App
            # Убираем кнопку отправки контакта и оставляем только web app
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
            
            # Определяем URL для Web App
            # ВАЖНО: Web App должен открываться на фронтенде, а не на бэкенде
            if settings.TELEGRAM_WEBAPP_URL:
                # Используем явно указанный URL для Web App
                web_app_url = settings.TELEGRAM_WEBAPP_URL.rstrip("/")
                # Если URL уже содержит путь /subscriptions, не добавляем его повторно
                if "/subscriptions" not in web_app_url:
                    web_app_url = f"{web_app_url}/subscriptions"
            elif settings.TELEGRAM_WEBHOOK_URL:
                # Если не указан TELEGRAM_WEBAPP_URL, используем базовый URL из webhook
                # Убираем путь /api/telegram/webhook если есть
                base_url = settings.TELEGRAM_WEBHOOK_URL.replace("/api/telegram/webhook", "").rstrip("/")
                # Если это бэкенд домен (backdev), пытаемся заменить на фронтенд домен
                # Заменяем backdev на dev (или можно настроить отдельную переменную)
                if "backdev" in base_url:
                    base_url = base_url.replace("backdev", "dev")
                web_app_url = f"{base_url}/subscriptions"
            else:
                # Для разработки можно использовать localhost или указать в .env
                web_app_url = "https://your-domain.com/subscriptions"  # Нужно настроить в .env
            
            logger.info(f"Web App URL: {web_app_url}")
            
            keyboard = [[InlineKeyboardButton(
                "📱 Открыть портал поддержки",
                web_app=WebAppInfo(url=web_app_url)
            )]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Убираем клавиатуру с кнопкой отправки контакта
            await update.message.reply_text(
                "✅ Контакт сохранен!\n\n"
                "Теперь вы можете создать заявку на консультацию через портал поддержки.",
                reply_markup=ReplyKeyboardRemove(),
                reply_to_message_id=update.message.message_id
            )
            
            # Отправляем отдельное сообщение с кнопкой web app
            # Это важно для тех, кто не видит menu button (кнопку меню)
            await update.message.reply_text(
                "📱 Нажмите кнопку ниже, чтобы открыть портал поддержки:\n\n"
                "💡 Также вы можете использовать кнопку меню (4 квадрата) рядом с чатом.",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error handling contact: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при сохранении контакта. Пожалуйста, попробуйте позже."
            )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений от пользователя"""
        if not update.message or not update.message.text:
            return
        
        # ВАЖНО: Обрабатываем только личные сообщения, игнорируем сообщения из групп
        if update.message.chat.type != "private":
            logger.debug(f"Ignoring message from non-private chat: chat_type={update.message.chat.type}, chat_id={update.message.chat.id}")
            return
        
        telegram_user_id = update.message.from_user.id
        message_text = update.message.text
        
        try:
            # Проверяем, есть ли активная консультация в контексте пользователя
            # (установлена при переходе через deep link /start cons_123)
            active_cons_id = None
            if context.user_data:
                active_cons_id = context.user_data.get("active_cons_id")
            
            async with AsyncSessionLocal() as db:
                consultation = None
                
                # Если есть активная консультация в контексте, используем её
                if active_cons_id:
                    result = await db.execute(
                        select(Consultation).where(Consultation.cons_id == active_cons_id)
                    )
                    consultation = result.scalar_one_or_none()
                
                # Если нет активной консультации в контексте, ищем по client_id или связываем по cons_id
                if not consultation:
                    result = await db.execute(
                        select(TelegramUser).where(TelegramUser.telegram_user_id == telegram_user_id)
                    )
                    telegram_user = result.scalar_one_or_none()
                    
                    # Если Telegram пользователь не связан с клиентом, пытаемся найти консультацию по cons_id из deep link
                    # или по последней открытой консультации для этого пользователя
                    if not telegram_user or not telegram_user.client_id:
                        # Пытаемся найти консультацию по cons_id из deep link (если пользователь перешел по /start cons_id=XXX)
                        # Или находим последнюю открытую консультацию, созданную недавно
                        from datetime import datetime, timezone, timedelta
                        recent_time = datetime.now(timezone.utc) - timedelta(hours=24)  # За последние 24 часа
                        
                        # Ищем последние открытые консультации
                        result = await db.execute(
                            select(Consultation)
                            .where(Consultation.status.in_([None, "open", "pending"]))
                            .where(Consultation.created_at >= recent_time)
                            .order_by(Consultation.created_at.desc())
                            .limit(10)
                        )
                        recent_consultations = result.scalars().all()
                        
                        # Пытаемся найти консультацию по номеру телефона из Telegram пользователя
                        consultation = None
                        if telegram_user and telegram_user.phone_number:
                            for cons in recent_consultations:
                                if cons.client_id:
                                    result = await db.execute(
                                        select(Client).where(Client.client_id == cons.client_id)
                                    )
                                    client = result.scalar_one_or_none()
                                    if client and client.phone_number == telegram_user.phone_number:
                                        consultation = cons
                                        # Связываем Telegram пользователя с клиентом
                                        telegram_user.client_id = cons.client_id
                                        await db.commit()
                                        logger.info(f"Auto-linked Telegram user {telegram_user_id} with client {cons.client_id} via phone_number in handle_message")
                                        break
                        
                        # Если не нашли по телефону, но есть консультации - берем самую последнюю
                        # Это работает только если пользователь создал одну консультацию
                        if not consultation and recent_consultations:
                            # Берем самую последнюю консультацию
                            consultation = recent_consultations[0]
                            if consultation.client_id:
                                # Создаем или обновляем Telegram пользователя
                                if not telegram_user:
                                    telegram_user = TelegramUser(
                                        telegram_user_id=telegram_user_id,
                                        client_id=consultation.client_id
                                    )
                                    db.add(telegram_user)
                                else:
                                    telegram_user.client_id = consultation.client_id
                                await db.commit()
                                logger.info(f"Auto-linked Telegram user {telegram_user_id} with client {consultation.client_id} via recent consultation in handle_message")
                        
                        if not consultation:
                            await update.message.reply_text(
                                "❌ Вы не связаны с клиентом. Пожалуйста, создайте заявку через портал поддержки или перейдите по ссылке из заявки."
                            )
                            return
                    else:
                        # Находим открытую консультацию для этого клиента
                        result = await db.execute(
                            select(Consultation)
                            .where(Consultation.client_id == telegram_user.client_id)
                            .where(Consultation.status.in_([None, "open", "pending"]))
                            .order_by(Consultation.created_at.desc())
                            .limit(1)
                        )
                        consultation = result.scalar_one_or_none()
                    
                    # Сохраняем найденную консультацию в контексте
                    if consultation and context.user_data is not None:
                        context.user_data["active_cons_id"] = consultation.cons_id
                        context.user_data["active_client_id"] = consultation.client_id
                
                if not consultation:
                    await update.message.reply_text(
                        "❌ У вас нет активных консультаций. Создайте новую заявку через портал поддержки."
                    )
                    return
                
                # Проверяем статус консультации
                if consultation.status in ("closed", "resolved", "cancelled"):
                    await update.message.reply_text(
                        "ℹ️ Эта консультация закрыта. Новые сообщения не принимаются.\n\n"
                        "Вы можете создать новую заявку через портал поддержки."
                    )
                    return
                
                # Отправляем сообщение в Chatwoot
                chatwoot_client = ChatwootClient()
                await chatwoot_client.send_message(
                    conversation_id=consultation.cons_id,
                    content=message_text,
                    message_type="incoming"
                )
                
                # Убрано подтверждение отправки сообщения
                
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при отправке сообщения. Пожалуйста, попробуйте позже."
            )
    
    async def send_message_to_telegram(self, telegram_user_id: int, message_text: str):
        """
        Отправка сообщения пользователю в Telegram.
        
        ВАЖНО: Обрабатывает ошибку "Chat not found" как нормальную ситуацию
        (пользователь заблокировал бота или удалил чат), не прерывает выполнение.
        """
        print(f"[TELEGRAM BOT] send_message_to_telegram called: user_id={telegram_user_id}, message_length={len(message_text)}")
        
        if not self.bot:
            print("[TELEGRAM BOT] ERROR: Bot not initialized")
            logger.warning("Bot not initialized, cannot send message")
            return
        
        try:
            print(f"[TELEGRAM BOT] Attempting to send message to chat_id={telegram_user_id}")
            result = await self.bot.send_message(
                chat_id=telegram_user_id,
                text=message_text
            )
            print(f"[TELEGRAM BOT] Message sent successfully: message_id={result.message_id if result else 'N/A'}")
            logger.info(f"Message sent to Telegram user {telegram_user_id}, message_id={result.message_id if result else 'N/A'}")
        except Exception as e:
            error_message = str(e)
            
            # Обработка ошибки "Chat not found" - чат недоступен (пользователь заблокировал бота, удалил чат и т.д.)
            if "Chat not found" in error_message or "chat not found" in error_message.lower():
                logger.warning(
                    f"Chat not found for Telegram user {telegram_user_id}. "
                    f"User may have blocked the bot or deleted the chat. "
                    f"Skipping message send."
                )
                print(f"[TELEGRAM BOT] WARNING: Chat not found for user {telegram_user_id}, skipping message")
                # Не пробрасываем ошибку - это нормальная ситуация, когда пользователь заблокировал бота
                return
            
            # Для других ошибок логируем и пробрасываем
            print(f"[TELEGRAM BOT] ERROR sending message: {e}")
            logger.error(f"Error sending message to Telegram: {e}", exc_info=True)
            raise  # Пробрасываем ошибку для других случаев
    
    async def send_media_to_telegram(
        self, 
        telegram_user_id: int, 
        file_url: str, 
        file_type: str = "file",
        caption: str = None
    ):
        """Отправка медиафайла (фото, документ, аудио, видео) пользователю в Telegram"""
        print(f"[TELEGRAM BOT] send_media_to_telegram called: user_id={telegram_user_id}, file_url={file_url}, file_type={file_type}")
        
        if not self.bot:
            print("[TELEGRAM BOT] ERROR: Bot not initialized")
            logger.warning("Bot not initialized, cannot send media")
            return
        
        try:
            import httpx
            from io import BytesIO
            from urllib.parse import urlparse, urljoin
            import mimetypes
            
            # Формируем полный URL файла (если это относительный путь)
            if not file_url.startswith("http"):
                # Если это относительный путь, добавляем базовый URL Chatwoot
                from ..config import settings
                base_url = settings.CHATWOOT_API_URL.rstrip("/")
                file_url = urljoin(base_url, file_url.lstrip("/"))
            
            print(f"[TELEGRAM BOT] Downloading file from: {file_url}")
            
            # Скачиваем файл из Chatwoot с поддержкой redirect
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                file_response = await client.get(file_url)
                file_response.raise_for_status()
                file_content = file_response.content
                
                # Определяем имя файла из URL или Content-Disposition заголовка
                file_name = None
                content_disposition = file_response.headers.get("Content-Disposition", "")
                if content_disposition:
                    # Пытаемся извлечь имя файла из Content-Disposition
                    import re
                    filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^\s;]+)', content_disposition)
                    if filename_match:
                        file_name = filename_match.group(1).strip('\'"')
                
                # Если не нашли в заголовке, берем из URL
                if not file_name:
                    file_name = file_url.split("/")[-1].split("?")[0] or "file"
                    # Убираем query параметры
                    if "?" in file_name:
                        file_name = file_name.split("?")[0]
                
                # Если имя файла все еще пустое, используем дефолтное
                if not file_name or file_name == "file":
                    # Определяем расширение по Content-Type
                    content_type = file_response.headers.get("Content-Type", "")
                    if content_type:
                        ext = mimetypes.guess_extension(content_type.split(";")[0])
                        if ext:
                            file_name = f"file{ext}"
                        else:
                            file_name = "file"
                    else:
                        file_name = "file"
            
            print(f"[TELEGRAM BOT] Downloaded file: {file_name}, size={len(file_content)} bytes")
            
            # Определяем тип файла по расширению
            file_name_lower = file_name.lower()
            file_ext = None
            if "." in file_name_lower:
                file_ext = file_name_lower.split(".")[-1]
            
            # Определяем, какой метод использовать для отправки
            is_image = (
                file_type == "image" or 
                file_ext in ('jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'ico', 'svg')
            )
            is_audio = (
                file_type == "audio" or 
                file_ext in ('mp3', 'ogg', 'wav', 'm4a', 'aac', 'flac', 'opus', 'wma')
            )
            is_video = (
                file_type == "video" or 
                file_ext in ('mp4', 'avi', 'mov', 'mkv', 'webm', 'flv', 'wmv', '3gp', 'mpeg', 'mpg')
            )
            
            # Создаем BytesIO объект с правильным именем файла
            file_obj = BytesIO(file_content)
            # ВАЖНО: Устанавливаем имя файла через атрибут name для правильной работы с Telegram API
            file_obj.name = file_name
            
            print(f"[TELEGRAM BOT] Sending file as: is_image={is_image}, is_audio={is_audio}, is_video={is_video}, file_ext={file_ext}")
            
            if is_image:
                # Отправляем как фото
                result = await self.bot.send_photo(
                    chat_id=telegram_user_id,
                    photo=file_obj,
                    caption=caption
                )
                print(f"[TELEGRAM BOT] Photo sent successfully: message_id={result.message_id if result else 'N/A'}")
            elif is_audio:
                # Отправляем как аудио
                result = await self.bot.send_audio(
                    chat_id=telegram_user_id,
                    audio=file_obj,
                    caption=caption,
                    title=file_name.rsplit('.', 1)[0] if '.' in file_name else file_name
                )
                print(f"[TELEGRAM BOT] Audio sent successfully: message_id={result.message_id if result else 'N/A'}")
            elif is_video:
                # Отправляем как видео
                result = await self.bot.send_video(
                    chat_id=telegram_user_id,
                    video=file_obj,
                    caption=caption
                )
                print(f"[TELEGRAM BOT] Video sent successfully: message_id={result.message_id if result else 'N/A'}")
            else:
                # Отправляем как документ
                result = await self.bot.send_document(
                    chat_id=telegram_user_id,
                    document=file_obj,
                    caption=caption
                )
                print(f"[TELEGRAM BOT] Document sent successfully: message_id={result.message_id if result else 'N/A'}")
            
            logger.info(f"Media sent to Telegram user {telegram_user_id}, message_id={result.message_id if result else 'N/A'}")
        except Exception as e:
            print(f"[TELEGRAM BOT] ERROR sending media: {e}")
            logger.error(f"Error sending media to Telegram: {e}", exc_info=True)
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def handle_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка медиафайлов (фото, документы, аудио, видео)"""
        if not update.message:
            return
        
        # ВАЖНО: Обрабатываем только личные сообщения, игнорируем сообщения из групп
        if update.message.chat.type != "private":
            logger.debug(f"Ignoring media from non-private chat: chat_type={update.message.chat.type}, chat_id={update.message.chat.id}")
            return
        
        telegram_user_id = update.message.from_user.id
        
        try:
            # Проверяем, есть ли активная консультация в контексте пользователя
            active_cons_id = None
            if context.user_data:
                active_cons_id = context.user_data.get("active_cons_id")
            
            async with AsyncSessionLocal() as db:
                consultation = None
                
                # Если есть активная консультация в контексте, используем её
                if active_cons_id:
                    result = await db.execute(
                        select(Consultation).where(Consultation.cons_id == active_cons_id)
                    )
                    consultation = result.scalar_one_or_none()
                
                # Если нет активной консультации в контексте, ищем по client_id
                if not consultation:
                    result = await db.execute(
                        select(TelegramUser).where(TelegramUser.telegram_user_id == telegram_user_id)
                    )
                    telegram_user = result.scalar_one_or_none()
                    
                    if not telegram_user or not telegram_user.client_id:
                        await update.message.reply_text(
                            "❌ Вы не связаны с клиентом. Пожалуйста, создайте заявку через портал поддержки."
                        )
                        return
                    
                    # Находим открытую консультацию для этого клиента
                    result = await db.execute(
                        select(Consultation)
                        .where(Consultation.client_id == telegram_user.client_id)
                        .where(Consultation.status.in_([None, "open", "pending"]))
                        .order_by(Consultation.create_date.desc())
                        .limit(1)
                    )
                    consultation = result.scalar_one_or_none()
                    
                    # Сохраняем найденную консультацию в контексте
                    if consultation and context.user_data is not None:
                        context.user_data["active_cons_id"] = consultation.cons_id
                        context.user_data["active_client_id"] = consultation.client_id
                
                if not consultation:
                    await update.message.reply_text(
                        "❌ У вас нет активных консультаций. Создайте новую заявку через портал поддержки."
                    )
                    return
                
                # Проверяем статус консультации
                if consultation.status in ("closed", "resolved", "cancelled"):
                    await update.message.reply_text(
                        "ℹ️ Эта консультация закрыта. Новые сообщения не принимаются.\n\n"
                        "Вы можете создать новую заявку через портал поддержки."
                    )
                    return
                
                # Получаем файл из сообщения
                file = None
                file_type = None
                caption = update.message.caption or ""
                
                if update.message.photo:
                    # Фото - берем самое большое
                    file = update.message.photo[-1].file_id
                    file_type = "image"
                elif update.message.document:
                    file = update.message.document.file_id
                    file_type = "file"
                elif update.message.audio:
                    file = update.message.audio.file_id
                    file_type = "audio"
                elif update.message.voice:
                    file = update.message.voice.file_id
                    file_type = "audio"
                elif update.message.video:
                    file = update.message.video.file_id
                    file_type = "video"
                
                if not file:
                    await update.message.reply_text(
                        "❌ Не удалось обработать файл. Пожалуйста, попробуйте отправить его снова."
                    )
                    return
                
                # Получаем файл от Telegram
                file_obj = await self.bot.get_file(file)
                # Получаем полный URL файла от Telegram
                # file_path уже содержит правильный путь, не нужно добавлять базовый URL дважды
                if file_obj.file_path.startswith("http"):
                    file_url = file_obj.file_path
                else:
                    file_url = f"https://api.telegram.org/file/bot{settings.TELEGRAM_BOT_TOKEN}/{file_obj.file_path}"
                
                # Отправляем медиафайл в Chatwoot
                chatwoot_client = ChatwootClient()
                await chatwoot_client.send_message_with_attachment(
                    conversation_id=consultation.cons_id,
                    content=caption or f"📎 Отправлен файл ({file_type})",
                    attachment_url=file_url,
                    attachment_type=file_type
                )
                
                # Убрано подтверждение отправки файла
                
        except Exception as e:
            logger.error(f"Error handling media: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при отправке файла. Пожалуйста, попробуйте позже."
            )
    
    async def start_polling(self):
        """Запуск polling (для разработки)"""
        if not self.application:
            logger.warning("Application not initialized, cannot start polling")
            return
        
        logger.info("Starting Telegram bot polling...")
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        logger.info("Telegram bot polling started")
    
    async def setup_webhook(self, webhook_url: str, secret_token: Optional[str] = None) -> bool:
        """
        Настройка webhook (для production)
        
        Returns:
            True если webhook успешно установлен, False если произошла ошибка
        """
        if not self.bot:
            logger.warning("Bot not initialized, cannot setup webhook")
            return False
        
        try:
            await self.bot.set_webhook(
                url=webhook_url,
                secret_token=secret_token
            )
            logger.debug(f"Telegram webhook setup at {webhook_url}")
            return True
        except Exception as e:
            logger.warning(f"Failed to setup webhook: {e}. Will fallback to polling.")
            return False
    
    async def setup_menu_button(self) -> bool:
        """
        Настройка кнопки меню (menu button) для открытия Web App.
        
        Кнопка меню отображается в интерфейсе Telegram рядом с чатом
        (4 маленьких квадрата в одном большом квадрате).
        
        Returns:
            True если кнопка успешно установлена, False если произошла ошибка
        """
        if not self.bot:
            logger.warning("Bot not initialized, cannot setup menu button")
            return False
        
        try:
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
                logger.warning("TELEGRAM_WEBAPP_URL or TELEGRAM_WEBHOOK_URL not set, cannot setup menu button")
                return False
            
            # Создаем кнопку меню с Web App
            menu_button = MenuButtonWebApp(
                text="📱 Портал поддержки",
                web_app=WebAppInfo(url=web_app_url)
            )
            
            # Устанавливаем кнопку меню для всех пользователей (chat_id=None означает глобальная настройка)
            await self.bot.set_chat_menu_button(chat_id=None, menu_button=menu_button)
            
            logger.info(f"Menu button setup successfully with Web App URL: {web_app_url}")
            return True
        except Exception as e:
            logger.warning(f"Failed to setup menu button: {e}", exc_info=True)
            return False
    
    async def shutdown(self):
        """Остановка бота"""
        if not self.application:
            return
        
        logger.info("Shutting down Telegram bot...")
        await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()
        logger.info("Telegram bot shut down")

