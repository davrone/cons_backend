"""Сервис для работы с Telegram ботом"""
import logging
from typing import Optional, Dict, Any
from telegram import Bot, Update, WebAppInfo
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
        
        # Обработчик текстовых сообщений
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        logger.info("Telegram bot handlers setup completed")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /start"""
        if not update.message:
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
        
        keyboard = [[KeyboardButton("📱 Отправить контакт", request_contact=True)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=reply_markup
        )
    
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
            
            # Отправляем информацию о загрузке истории
            await update.message.reply_text(
                f"📜 Загружаю историю сообщений ({len(messages)} сообщений)..."
            )
            
            # Отправляем сообщения в хронологическом порядке (старые первыми)
            for msg in reversed(messages):
                content = msg.get("content", "")
                message_type = msg.get("message_type", "incoming")
                sender = msg.get("sender", {})
                sender_name = sender.get("name", "Система") if sender else "Система"
                created_at = msg.get("created_at", "")
                
                # Форматируем сообщение для Telegram
                if message_type == "incoming":
                    # Сообщение от менеджера
                    formatted_msg = f"👤 {sender_name}:\n{content}"
                else:
                    # Сообщение от клиента
                    formatted_msg = f"💬 Вы:\n{content}"
                
                await update.message.reply_text(formatted_msg)
            
            await update.message.reply_text(
                "✅ История загружена. Теперь вы можете продолжить общение."
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
                
                if telegram_user:
                    # Обновляем существующего пользователя Telegram
                    telegram_user.phone_number = phone_number
                    telegram_user.first_name = from_user.first_name
                    telegram_user.last_name = from_user.last_name
                    telegram_user.username = from_user.username
                else:
                    # Создаем нового пользователя Telegram
                    telegram_user = TelegramUser(
                        telegram_user_id=telegram_user_id,
                        phone_number=phone_number,
                        first_name=from_user.first_name,
                        last_name=from_user.last_name,
                        username=from_user.username
                    )
                    db.add(telegram_user)
                
                await db.commit()
            
            # Отправляем сообщение с кнопкой для открытия Web App
            from telegram import InlineKeyboardMarkup, InlineKeyboardButton
            
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
            
            await update.message.reply_text(
                "✅ Контакт сохранен!\n\n"
                "Теперь вы можете создать заявку на консультацию через портал поддержки.",
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
        
        telegram_user_id = update.message.from_user.id
        message_text = update.message.text
        
        try:
            # Находим активную консультацию для пользователя
            async with AsyncSessionLocal() as db:
                # Получаем client_id пользователя
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
                
                await update.message.reply_text("✅ Сообщение отправлено менеджеру.")
                
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Произошла ошибка при отправке сообщения. Пожалуйста, попробуйте позже."
            )
    
    async def send_message_to_telegram(self, telegram_user_id: int, message_text: str):
        """Отправка сообщения пользователю в Telegram"""
        if not self.bot:
            logger.warning("Bot not initialized, cannot send message")
            return
        
        try:
            await self.bot.send_message(
                chat_id=telegram_user_id,
                text=message_text
            )
        except Exception as e:
            logger.error(f"Error sending message to Telegram: {e}", exc_info=True)
    
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
    
    async def shutdown(self):
        """Остановка бота"""
        if not self.application:
            return
        
        logger.info("Shutting down Telegram bot...")
        await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()
        logger.info("Telegram bot shut down")

