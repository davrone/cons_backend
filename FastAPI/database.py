from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
    AsyncAttrs
)
from sqlalchemy.orm import DeclarativeBase
from .config import settings

# Async database URL для asyncpg
DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

# Async engine
# Настройки пула для предотвращения переполнения соединений:
# pool_size - базовый размер пула постоянных соединений
# max_overflow - дополнительные соединения, создаваемые при перегрузке
# pool_recycle - переиспользование соединений для предотвращения устаревших соединений
# pool_timeout - максимальное время ожидания свободного соединения
# pool_pre_ping - проверка работоспособности соединения перед использованием
# 
# ВАЖНО: PostgreSQL обычно имеет лимит ~100 соединений по умолчанию.
# Рекомендуемые значения: pool_size=20, max_overflow=10 (максимум 30 соединений)
engine = create_async_engine(
    DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    pool_pre_ping=True,  # Проверка соединения перед использованием
    pool_size=settings.DB_POOL_SIZE,  # Базовый размер пула (настраивается через env)
    max_overflow=settings.DB_MAX_OVERFLOW,  # Дополнительные соединения при перегрузке
    pool_recycle=settings.DB_POOL_RECYCLE,  # Переиспользование соединений
    pool_timeout=settings.DB_POOL_TIMEOUT,  # Таймаут ожидания соединения из пула
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


class Base(AsyncAttrs, DeclarativeBase):
    """Базовый класс для всех моделей"""
    pass


async def get_db() -> AsyncSession:
    """
    Dependency для получения async сессии БД.
    Использование:
        async def some_route(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
