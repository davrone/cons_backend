"""
Alembic environment для async миграций.

Поддерживает асинхронные миграции через asyncpg.
"""
from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from alembic import context
import os
import sys
from pathlib import Path

# Добавляем путь к проекту
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Импортируем модели и настройки
from FastAPI.database import Base
from FastAPI.config import settings
import FastAPI.models  # Импортируем все модели для метаданных

# this is the Alembic Config object
config = context.config

# Интерпретируем файл конфигурации для логирования
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Устанавливаем URL БД из настроек
database_url = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)
config.set_main_option("sqlalchemy.url", database_url)

# Метаданные для автогенерации миграций
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Запуск миграций в offline режиме."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Выполнение миграций через соединение."""
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Запуск миграций в online режиме (async)."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    import asyncio
    asyncio.run(run_migrations_online())
