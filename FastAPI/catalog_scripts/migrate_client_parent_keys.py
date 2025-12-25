"""
Скрипт миграции данных для обновления parent_key в таблице clients.

Этот скрипт:
1. Для всех клиентов с cl_ref_key запрашивает Parent_Key из ЦЛ
2. Сохраняет parent_key в БД
3. Сбрасывает cl_ref_key в NULL для клиентов с неправильным Parent_Key
4. Сбрасывает cl_ref_key в NULL для клиентов, не найденных в ЦЛ

Запуск:
    python FastAPI/catalog_scripts/migrate_client_parent_keys.py
"""
import asyncio
import os
import sys
from pathlib import Path

# Добавляем путь к корню проекта (выше FastAPI директории)
root_dir = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, root_dir)

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
try:
    # Основной путь: используем фабрику сессий из FastAPI.database
    from FastAPI.database import AsyncSessionLocal
except ImportError:
    try:
        # Фолбэк для окружений, где имя отличается
        from FastAPI.database import async_session_maker as AsyncSessionLocal  # type: ignore
    except Exception:
        # Последний фолбэк: создаем локальную фабрику на основе engine
        from FastAPI.database import engine
        AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

from FastAPI.models import Client
from FastAPI.services.onec_client import OneCClient
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# GUID папки CLOBUS
CLOBUS_PARENT_KEY = "7ccd31ca-887b-11eb-938b-00e04cd03b68"


async def migrate_parent_keys():
    """
    Миграция parent_key для всех клиентов.
    
    ПОДХОД:
    1. Загружаем ВСЕХ клиентов из 1C батчами ($top/$skip по 1000)
    2. Индексируем в памяти по Ref_Key, code+inn
    3. Загружаем клиентов из БД
    4. Сравниваем и обновляем в памяти
    5. Сохраняем батчами в БД
    """
    import requests
    from FastAPI.config import settings
    
    logger.info("=" * 80)
    logger.info("ШАГ 1: Загрузка ВСЕХ клиентов из 1C")
    logger.info("=" * 80)
    
    # Загружаем всех клиентов из 1C
    all_onec_clients = []
    PAGE_SIZE = 1000
    skip = 0
    
    while True:
        url = f"{settings.ODATA_BASEURL_CL}/Catalog_Контрагенты"
        params = {
            "$format": "json",
            "$select": "Ref_Key,Parent_Key,ИНН,КодАбонентаClobus,DeletionMark,Description",
            "$top": PAGE_SIZE,
            "$skip": skip,
            "$orderby": "Ref_Key"
        }
        
        try:
            logger.info(f"Загрузка из 1C: skip={skip}, top={PAGE_SIZE}")
            response = requests.get(
                url,
                params=params,
                auth=(settings.ODATA_USER, settings.ODATA_PASSWORD),
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            batch = data.get("value", [])
            
            if not batch:
                break
            
            all_onec_clients.extend(batch)
            logger.info(f"  Загружено {len(batch)} записей (всего: {len(all_onec_clients)})")
            
            if len(batch) < PAGE_SIZE:
                break
            
            skip += PAGE_SIZE
            
        except Exception as e:
            logger.error(f"Ошибка загрузки из 1C: {e}")
            break
    
    logger.info(f"\nВсего загружено из 1C: {len(all_onec_clients)} клиентов")
    
    # Индексируем клиентов из 1C
    logger.info("\nИндексация данных из 1C...")
    onec_by_ref = {}      # {ref_key: data}
    onec_by_code_inn = {} # {(code, inn): data}
    
    for item in all_onec_clients:
        ref_key = item.get("Ref_Key")
        code = item.get("КодАбонентаClobus")
        inn = item.get("ИНН")
        
        if ref_key:
            onec_by_ref[ref_key] = item
        
        if code and code != "0" and inn:
            key = (str(code), str(inn))
            onec_by_code_inn[key] = item
    
    logger.info(f"  Индексировано по Ref_Key: {len(onec_by_ref)}")
    logger.info(f"  Индексировано по code+inn: {len(onec_by_code_inn)}")
    
    # ШАГ 2: Обрабатываем клиентов из БД
    logger.info("\n" + "=" * 80)
    logger.info("ШАГ 2: Обработка клиентов из БД")
    logger.info("=" * 80)
    
    async with AsyncSessionLocal() as db:
        # Загружаем ВСЕХ клиентов из БД
        result = await db.execute(select(Client))
        all_db_clients = result.scalars().all()
        
        logger.info(f"Всего клиентов в БД: {len(all_db_clients)}")
        
        total_updated = 0
        total_reset = 0
        total_linked = 0
        total_not_found = 0
        
        BATCH_SIZE = 100
        
        for batch_start in range(0, len(all_db_clients), BATCH_SIZE):
            batch = all_db_clients[batch_start:batch_start + BATCH_SIZE]
            
            for client in batch:
                # СЛУЧАЙ 1: У клиента есть cl_ref_key - проверяем в 1C
                if client.cl_ref_key:
                    onec_data = onec_by_ref.get(client.cl_ref_key)
                    
                    if not onec_data:
                        # Не найден в 1C - сбрасываем cl_ref_key
                        client.cl_ref_key = None
                        total_not_found += 1
                        continue
                    
                    parent_key = onec_data.get("Parent_Key")
                    
                    # Обновляем parent_key
                    if client.parent_key != parent_key:
                        client.parent_key = parent_key
                        total_updated += 1
                    
                    # Если Parent_Key НЕ CLOBUS - сбрасываем cl_ref_key
                    if parent_key != CLOBUS_PARENT_KEY:
                        client.cl_ref_key = None
                        total_reset += 1
                
                # СЛУЧАЙ 2: Нет cl_ref_key, но есть code+inn - ищем в индексе
                elif client.code_abonent and client.org_inn:
                    key = (str(client.code_abonent), str(client.org_inn))
                    onec_data = onec_by_code_inn.get(key)
                    
                    if onec_data:
                        parent_key = onec_data.get("Parent_Key")
                        ref_key = onec_data.get("Ref_Key")
                        
                        # Связываем ТОЛЬКО если клиент из папки CLOBUS
                        if parent_key == CLOBUS_PARENT_KEY:
                            client.cl_ref_key = ref_key
                            client.parent_key = parent_key
                            total_linked += 1
            
            # Коммитим батч
            await db.commit()
            
            if (batch_start + len(batch)) % 1000 == 0 or (batch_start + len(batch)) == len(all_db_clients):
                logger.info(f"Обработано {batch_start + len(batch)} / {len(all_db_clients)} клиентов")
        
        logger.info("\n" + "=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"  Всего клиентов в БД: {len(all_db_clients)}")
        logger.info(f"  Обновлено parent_key: {total_updated}")
        logger.info(f"  Сброшено cl_ref_key (не в CLOBUS): {total_reset}")
        logger.info(f"  Сброшено cl_ref_key (не найдено в 1C): {total_not_found}")
        logger.info(f"  Связано по code+inn: {total_linked}")
        logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(migrate_parent_keys())
