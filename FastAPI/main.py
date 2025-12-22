"""
Главный файл FastAPI приложения.

Middleware для интеграции:
- Сайта
- Telegram Mini App
- Chatwoot
- 1C:ЦЛ
"""
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from .config import settings
from .init_db import init_db, check_db_connection
from .routers import auth, webhooks, health, consultations, clients, dicts, managers, telegram
from .routers import websocket as ws_router
from .scheduler import setup_scheduler, start_scheduler, shutdown_scheduler
from .services.chatwoot_client import ChatwootClient
from .services.telegram_bot import TelegramBotService
from .exceptions import (
    ConsultationError,
    ConsultationNotFoundError,
    ConsultationLimitExceededError,
    ClientNotFoundError,
    SyncError,
    ChatwootError,
    OneCError,
    ValidationError,
    NotificationError
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle events для приложения.
    
    При старте:
    - Проверяет подключение к БД
    - Инициализирует БД (схемы, таблицы)
    
    При остановке:
    - Закрывает соединения с БД
    """
    # Startup
    print("🚀 Запуск приложения...")
    
    # Проверка подключения к БД
    if await check_db_connection():
        # Инициализация БД (идемпотентная)
        await init_db()
    else:
        print("⚠️  Предупреждение: не удалось подключиться к БД")
    
    # Инициализация labels в Chatwoot (создаем заранее с человеко-читаемыми названиями)
    try:
        chatwoot_client = ChatwootClient()
        # Список всех необходимых labels с человеко-читаемыми названиями
        required_labels = [
            "Русский",
            "Узбекский",
            "Сайт",
            "Telegram",
            "Телефон",
        ]
        for label_title in required_labels:
            try:
                await chatwoot_client.ensure_label_exists(label_title)
            except Exception as label_error:
                # Игнорируем ошибки "already exists" - это нормально при повторном запуске
                error_str = str(label_error).lower()
                if "already" in error_str or "422" in error_str or "409" in error_str:
                    logger.debug(f"Label '{label_title}' already exists (expected)")
                else:
                    logger.warning(f"Failed to initialize label '{label_title}': {label_error}")
        print("✓ Labels инициализированы в Chatwoot")
    except Exception as e:
        logger.warning(f"Ошибка инициализации labels: {e}", exc_info=True)
        print(f"⚠️  Предупреждение: не удалось инициализировать labels: {e}")
    
    # Запускаем планировщик задач (альтернатива cron)
    # ВАЖНО: Если ENABLE_SCHEDULER=false, scheduler запускается в отдельном контейнере
    enable_scheduler = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
    if enable_scheduler:
        try:
            setup_scheduler()
            start_scheduler()
            print("✓ Планировщик задач запущен")
        except Exception as e:
            logger.error(f"Ошибка запуска планировщика: {e}", exc_info=True)
            print(f"⚠️  Предупреждение: не удалось запустить планировщик задач: {e}")
    else:
        print("ℹ️  Планировщик задач отключен в этом контейнере (запущен в отдельном контейнере cons_scheduler)")
    
    # Инициализация Telegram бота
    telegram_bot_service = None
    if settings.TELEGRAM_BOT_TOKEN:
        try:
            telegram_bot_service = TelegramBotService()
            
            # Устанавливаем глобальный экземпляр для роутера
            # Устанавливаем глобальную переменную в модуле telegram
            import FastAPI.routers.telegram as telegram_module
            telegram_module.telegram_bot_service = telegram_bot_service
            
            # Инициализируем application бота (нужно для webhook режима)
            if telegram_bot_service.application:
                await telegram_bot_service.application.initialize()
                await telegram_bot_service.application.start()
                logger.info("Telegram bot application initialized")
                
                # Настраиваем кнопку меню для Web App
                menu_button_success = await telegram_bot_service.setup_menu_button()
                if menu_button_success:
                    print("✓ Кнопка меню для Web App настроена")
                else:
                    print("⚠️  Не удалось настроить кнопку меню для Web App")
            
            # Настраиваем webhook или polling
            if settings.TELEGRAM_WEBHOOK_URL:
                # Production: пытаемся использовать webhook
                # Если webhook не установится (домен недоступен и т.д.), переключаемся на polling
                # Проверяем, есть ли уже путь в URL
                if '/api/telegram/webhook' in settings.TELEGRAM_WEBHOOK_URL:
                    webhook_url = settings.TELEGRAM_WEBHOOK_URL
                else:
                    base_url = settings.TELEGRAM_WEBHOOK_URL.rstrip('/')
                    webhook_url = f"{base_url}/api/telegram/webhook"
                
                logger.info(f"Attempting to setup webhook at: {webhook_url}")
                webhook_success = await telegram_bot_service.setup_webhook(
                    webhook_url=webhook_url,
                    secret_token=settings.TELEGRAM_WEBHOOK_SECRET
                )
                
                if webhook_success:
                    print(f"✓ Telegram bot webhook настроен: {webhook_url}")
                else:
                    # Webhook не установился, переключаемся на polling
                    print(f"⚠️  Webhook не установлен, переключаемся на polling")
                    import asyncio
                    asyncio.create_task(telegram_bot_service.start_polling())
                    print("✓ Telegram bot polling запущен")
            else:
                # Development: используем polling
                # Запускаем polling в фоне
                import asyncio
                asyncio.create_task(telegram_bot_service.start_polling())
                print("✓ Telegram bot polling запущен")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации Telegram бота: {e}", exc_info=True)
            print(f"⚠️  Предупреждение: не удалось инициализировать Telegram бота: {e}")
    else:
        print("ℹ️  Telegram bot отключен (TELEGRAM_BOT_TOKEN не указан)")
    
    yield
    
    # Shutdown
    print("🛑 Остановка приложения...")
    shutdown_scheduler()
    
    # Остановка Telegram бота
    if telegram_bot_service:
        try:
            await telegram_bot_service.shutdown()
            print("✓ Telegram bot остановлен")
        except Exception as e:
            logger.error(f"Ошибка остановки Telegram бота: {e}", exc_info=True)


# Создаем приложение
app = FastAPI(
    title="Consultation Middleware API",
    description="""
    Единая точка интеграции для системы консультаций.
    
    ## Аутентификация
    Все запросы требуют заголовок `X-Front-Secret` или `Authorization: Bearer <token>`.
    
    ## Rate Limiting
    - Общие endpoints: 100 запросов/минуту
    - Создание консультаций: 10 запросов/минуту
    
    ## Idempotency
    Для предотвращения дублирования операций используйте заголовок `Idempotency-Key`.
    
    ## Real-time обновления
    - **SSE**: `GET /api/consultations/{cons_id}/stream`
    - **WebSocket**: `WS /ws/consultations/{cons_id}`
    - **Polling**: `GET /api/consultations/{cons_id}/updates`
    
    Подробная документация: см. API_DOCUMENTATION.md
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    redirect_slashes=False
)

# CORS middleware
# Парсим ALLOWED_ORIGINS из env (через запятую) или используем "*" если не указано
allowed_origins = settings.ALLOWED_ORIGINS.split(",") if settings.ALLOWED_ORIGINS != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Обработчик ошибок валидации Pydantic"""
    body = await request.body()
    
    # Преобразуем ошибки в сериализуемый формат
    errors = []
    for error in exc.errors():
        error_dict = {
            "type": error.get("type"),
            "loc": error.get("loc"),
            "msg": error.get("msg"),
        }
        
        # Обрабатываем input - конвертируем bytes в строку если нужно
        input_value = error.get("input")
        if isinstance(input_value, bytes):
            try:
                error_dict["input"] = input_value.decode("utf-8")
            except UnicodeDecodeError:
                error_dict["input"] = f"<bytes object of length {len(input_value)}>"
        else:
            error_dict["input"] = input_value
        
        # Обрабатываем ctx если есть, преобразуя ValueError в строку
        if "ctx" in error:
            ctx = error["ctx"].copy()
            if "error" in ctx and isinstance(ctx["error"], Exception):
                ctx["error"] = str(ctx["error"])
            error_dict["ctx"] = ctx
        errors.append(error_dict)
    
    # Логируем ошибку (без body в логах, чтобы не засорять)
    logger.error(f"Validation error: {errors}")
    
    # Конвертируем body в строку для ответа
    body_str = None
    if body:
        try:
            body_str = body.decode("utf-8")
        except UnicodeDecodeError:
            body_str = f"<bytes object of length {len(body)}>"
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": errors, "body": body_str},
    )


@app.exception_handler(ConsultationNotFoundError)
async def consultation_not_found_handler(request: Request, exc: ConsultationNotFoundError):
    """Обработчик ошибки - консультация не найдена"""
    logger.warning(f"Consultation not found: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": exc.message, "details": exc.details},
    )


@app.exception_handler(ConsultationLimitExceededError)
async def consultation_limit_exceeded_handler(request: Request, exc: ConsultationLimitExceededError):
    """Обработчик ошибки - превышен лимит консультаций"""
    logger.warning(f"Consultation limit exceeded: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": exc.message, "details": exc.details},
    )


@app.exception_handler(ClientNotFoundError)
async def client_not_found_handler(request: Request, exc: ClientNotFoundError):
    """Обработчик ошибки - клиент не найден"""
    logger.warning(f"Client not found: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": exc.message, "details": exc.details},
    )


@app.exception_handler(SyncError)
async def sync_error_handler(request: Request, exc: SyncError):
    """Обработчик ошибок синхронизации"""
    logger.error(f"Sync error ({exc.system}): {exc.message}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={
            "detail": exc.message,
            "system": exc.system,
            "details": exc.details
        },
    )


@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    """Обработчик ошибок валидации бизнес-логики"""
    logger.warning(f"Validation error: {exc.message}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.message, "details": exc.details},
    )


@app.exception_handler(ConsultationError)
async def consultation_error_handler(request: Request, exc: ConsultationError):
    """Обработчик общих ошибок консультаций"""
    logger.error(f"Consultation error: {exc.message}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": exc.message, "details": exc.details},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Глобальный обработчик исключений"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": f"Internal server error: {str(exc)}"},
    )

# Подключаем роуты
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(clients.router, prefix="/api/clients", tags=["clients"])
app.include_router(consultations.router, prefix="/api/consultations", tags=["consultations"])
app.include_router(managers.router, prefix="/api/managers", tags=["managers"])
app.include_router(webhooks.router, prefix="/webhook", tags=["webhooks"])
app.include_router(dicts.router, prefix="/api/dicts", tags=["dicts"])
app.include_router(ws_router.router, prefix="/ws/consultations", tags=["websocket"])
app.include_router(telegram.router, prefix="/api/telegram", tags=["telegram"])


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "Consultation Middleware",
        "version": "1.0.0",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "FastAPI.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.DEBUG
    )
