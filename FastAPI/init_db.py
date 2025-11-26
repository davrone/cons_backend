"""
Идемпотентная инициализация базы данных.

Создает схемы, таблицы и начальные данные.
Можно запускать многократно без ошибок.
"""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from .database import Base, engine
from .config import settings

# Версия миграции для отслеживания
MIGRATION_VERSION = "v1.0.0"

# Схемы для создания
SCHEMAS = ["cons", "dict", "sys", "log"]


async def create_schemas():
    """Создает схемы БД (идемпотентно)"""
    async with engine.begin() as conn:
        # Создаем расширение для UUID
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))
        
        # Создаем схемы
        for schema in SCHEMAS:
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
        
        print(f"✓ Схемы созданы: {', '.join(SCHEMAS)}")


async def create_tables():
    """Создает все таблицы через SQLAlchemy (идемпотентно)"""
    async with engine.begin() as conn:
        # Создаем все таблицы из метаданных моделей
        await conn.run_sync(Base.metadata.create_all)
        
        # Создаем служебную таблицу для отслеживания синхронизаций
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sys.sync_state (
                entity_name TEXT PRIMARY KEY,
                last_synced_at TIMESTAMPTZ
            )
        """))
        
        print("✓ Таблицы созданы")


async def seed_initial_data():
    """Заполняет начальные справочные данные (идемпотентно)"""
    async with engine.begin() as conn:
        # Регистрируем миграцию
        await conn.execute(text("""
            INSERT INTO sys.db_migrations (version)
            SELECT :version
            WHERE NOT EXISTS (
                SELECT 1 FROM sys.db_migrations WHERE version = :version
            )
        """), {"version": MIGRATION_VERSION})
        
        print(f"✓ Миграция зарегистрирована: {MIGRATION_VERSION}")


async def init_db():
    """
    Полная инициализация БД.
    
    Выполняет:
    1. Создание схем
    2. Создание таблиц
    3. Заполнение начальных данных
    
    Идемпотентна - можно запускать многократно.
    """
    try:
        print("Начало инициализации БД...")
        await create_schemas()
        await create_tables()
        await seed_initial_data()
        print("✓ Инициализация БД завершена успешно")
    except Exception as e:
        print(f"✗ Ошибка инициализации БД: {e}")
        raise
    finally:
        await engine.dispose()


async def check_db_connection():
    """Проверяет подключение к БД"""
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1"))
            result.scalar()
        print("✓ Подключение к БД успешно")
        return True
    except Exception as e:
        print(f"✗ Ошибка подключения к БД: {e}")
        return False


if __name__ == "__main__":
    """Запуск инициализации из командной строки"""
    asyncio.run(init_db())
