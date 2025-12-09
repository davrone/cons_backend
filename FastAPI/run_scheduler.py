#!/usr/bin/env python3
"""
Отдельный сервис для запуска ETL Scheduler.
Запускается в отдельном контейнере для изоляции логов ETL процессов.
"""
import asyncio
import logging
import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from FastAPI.scheduler import setup_scheduler, start_scheduler, shutdown_scheduler
from FastAPI.config import settings
from FastAPI.init_db import check_db_connection

# Настраиваем детальное логирование
LOG_LEVEL = os.getenv("ETL_LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('scheduler_service')


async def wait_for_db(max_attempts: int = 30):
    """Ждем готовности БД"""
    logger.info("⏳ Waiting for database to be ready...")
    for attempt in range(max_attempts):
        try:
            if await check_db_connection():
                logger.info("✓ Database is ready")
                return True
        except Exception as e:
            logger.debug(f"Database check failed (attempt {attempt + 1}/{max_attempts}): {e}")
        
        if attempt < max_attempts - 1:
            await asyncio.sleep(2)
    
    logger.error("✗ Database is still unavailable after %s attempts", max_attempts)
    return False


async def main():
    """Главная функция scheduler сервиса"""
    logger.info("=" * 80)
    logger.info("🚀 ETL Scheduler Service Starting")
    logger.info("=" * 80)
    logger.info("⚠ NOTE: This container ONLY runs ETL scheduler.")
    logger.info("   Database initialization and dictionary loading happen in cons_api container.")
    logger.info("=" * 80)
    
    # Проверяем конфигурацию
    logger.info("📋 Configuration:")
    logger.info(f"  Database: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
    logger.info(f"  OData URL: {settings.ODATA_BASEURL_CL}")
    logger.info(f"  Log Level: {LOG_LEVEL}")
    
    # Ждем готовности БД (только проверка подключения, БЕЗ инициализации)
    if not await wait_for_db():
        logger.error("✗ Cannot start scheduler: database is not available")
        sys.exit(1)
    
    # Настраиваем и запускаем scheduler
    # ВАЖНО: НЕ запускаем init_db() или load_dicts() - это только для API контейнера
    try:
        logger.info("📅 Setting up scheduler...")
        setup_scheduler()
        start_scheduler()
        logger.info("✓ Scheduler started successfully")
        logger.info("=" * 80)
        logger.info("🔄 Scheduler is running. ETL tasks will execute according to schedule.")
        logger.info("=" * 80)
        
        # Держим процесс живым
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            logger.info("⚠ Received shutdown signal...")
            shutdown_scheduler()
            logger.info("✓ Scheduler stopped")
    except Exception as e:
        logger.error("✗ Failed to start scheduler: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⚠ Shutting down scheduler service...")
        shutdown_scheduler()
        sys.exit(0)
