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
# Импортируем все модели для регистрации в Base.metadata
from . import models  # noqa: F401

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


async def table_exists(conn, schema: str, table: str) -> bool:
    """Проверяет существование таблицы в схеме"""
    result = await conn.execute(text("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = :schema 
            AND table_name = :table
        )
    """), {"schema": schema, "table": table})
    return result.scalar()


async def create_tables():
    """Создает все таблицы через SQLAlchemy (идемпотентно)"""
    async with engine.begin() as conn:
        # Создаем все таблицы из метаданных моделей
        await conn.run_sync(Base.metadata.create_all)

        # Гарантируем наличие новых колонок в cons.clients для обратной совместимости
        # (только если таблица уже существует)
        if await table_exists(conn, "cons", "clients"):
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS name TEXT
            """))
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS contact_name TEXT
            """))
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS code_abonent TEXT
            """))
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS company_name TEXT
            """))
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS source_id TEXT
            """))
            await conn.execute(text("""
                ALTER TABLE cons.clients
                ADD COLUMN IF NOT EXISTS chatwoot_pubsub_token TEXT
            """))
        
        # Гарантируем наличие chatwoot_user_id в cons.users
        # (только если таблица уже существует)
        if await table_exists(conn, "cons", "users"):
            await conn.execute(text("""
                ALTER TABLE cons.users
                ADD COLUMN IF NOT EXISTS chatwoot_user_id INTEGER
            """))
        
        # Гарантируем наличие consultation_type в cons.cons
        # (только если таблица уже существует)
        if await table_exists(conn, "cons", "cons"):
            await conn.execute(text("""
                ALTER TABLE cons.cons
                ADD COLUMN IF NOT EXISTS consultation_type TEXT
            """))
        
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
        # Убеждаемся, что таблица sys.db_migrations существует
        # (она должна создаваться через create_all, но на всякий случай проверяем)
        if not await table_exists(conn, "sys", "db_migrations"):
            # Создаём таблицу, если её нет (на случай, если create_all её не создал)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS sys.db_migrations (
                    id SERIAL PRIMARY KEY,
                    version TEXT UNIQUE NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
        
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
    # Чтобы избежать гонок/взаимных блокировок при параллельном старте нескольких контейнеров,
    # держим advisory lock на время инициализации.
    lock_key = 987654321  # произвольный ключ блокировки
    lock_conn = None
    try:
        lock_conn = await engine.connect()
        await lock_conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": lock_key})

        print("Начало инициализации БД...")
        await create_schemas()
        await create_tables()
        await seed_initial_data()
        print("✓ Инициализация БД завершена успешно")
    except Exception as e:
        print(f"✗ Ошибка инициализации БД: {e}")
        raise
    finally:
        # Освобождаем advisory lock и закрываем соединение
        if lock_conn:
            try:
                await lock_conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
            finally:
                await lock_conn.close()
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
