"""
Главный файл FastAPI приложения.

Middleware для интеграции:
- Сайта
- Telegram Mini App
- Chatwoot
- 1C:ЦЛ
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import settings
from .init_db import init_db, check_db_connection
from .routers import auth, tickets, webhooks, health, consultations, clients


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
    
    yield
    
    # Shutdown
    print("🛑 Остановка приложения...")


# Создаем приложение
app = FastAPI(
    title="Consultation Middleware",
    description="Единая точка интеграции для системы консультаций",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роуты
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(clients.router, prefix="/api/clients", tags=["clients"])
app.include_router(consultations.router, prefix="/api/consultations", tags=["consultations"])
app.include_router(tickets.router, prefix="/api/tickets", tags=["tickets"])
app.include_router(webhooks.router, prefix="/webhook", tags=["webhooks"])


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
