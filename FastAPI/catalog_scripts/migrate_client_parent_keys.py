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
    """Миграция parent_key для всех клиентов (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ с batch обработкой)"""
    onec_client = OneCClient()
    
    async with AsyncSessionLocal() as db:
        # Получаем всех клиентов с cl_ref_key
        result = await db.execute(
            select(Client).where(Client.cl_ref_key.isnot(None))
        )
        clients_with_ref = result.scalars().all()
        
        logger.info(f"Найдено {len(clients_with_ref)} клиентов с cl_ref_key")
        
        total_updated = 0
        total_reset = 0
        total_not_found = 0
        total_errors = 0
        
        # ОПТИМИЗАЦИЯ: обрабатываем батчами по 100 штук
        BATCH_SIZE = 100
        
        for batch_start in range(0, len(clients_with_ref), BATCH_SIZE):
            batch = clients_with_ref[batch_start:batch_start + BATCH_SIZE]
            logger.info(f"\n=== Обработка батча {batch_start//BATCH_SIZE + 1} ({len(batch)} клиентов) ===")
            
            # 1. Собираем все ref_keys из батча
            ref_keys = [c.cl_ref_key for c in batch]
            
            # 2. Запрашиваем ВСЕ клиенты из батча ОДНИМ запросом к 1C
            # Используем filter по Ref_Key с оператором in (через несколько запросов, т.к. OData не поддерживает большие in)
            clients_data = {}
            
            # Делим на подбатчи по 50 (ограничение OData $filter длины)
            for sub_batch_start in range(0, len(ref_keys), 50):
                sub_batch_keys = ref_keys[sub_batch_start:sub_batch_start + 50]
                
                # Формируем фильтр: Ref_Key eq guid'...' or Ref_Key eq guid'...'
                filter_parts = [f"Ref_Key eq guid'{key}'" for key in sub_batch_keys]
                filter_str = " or ".join(filter_parts)
                
                try:
                    import requests
                    from FastAPI.config import settings
                    
                    url = f"{settings.ODATA_BASEURL_CL}/Catalog_Контрагенты"
                    params = {
                        "$format": "json",
                        "$select": "Ref_Key,Parent_Key,ИНН,КодАбонентаClobus,DeletionMark",
                        "$filter": filter_str
                    }
                    
                    response = requests.get(
                        url,
                        params=params,
                        auth=(settings.ODATA_USER, settings.ODATA_PASSWORD),
                        timeout=60
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    # Индексируем результаты по Ref_Key
                    for item in data.get("value", []):
                        ref_key = item.get("Ref_Key")
                        if ref_key:
                            clients_data[ref_key] = item
                    
                    logger.info(f"  Загружено {len(data.get('value', []))} записей из 1C (подбатч {sub_batch_start//50 + 1})")
                    
                except Exception as e:
                    logger.error(f"  Ошибка загрузки подбатча из 1C: {e}")
                    continue
            
            # 3. Обрабатываем каждого клиента из батча В ПАМЯТИ (без await)
            for i, client in enumerate(batch, 1):
                try:
                    client_data = clients_data.get(client.cl_ref_key)
                    
                    if not client_data:
                        logger.warning(
                            f"  [{batch_start + i}] Клиент {client.client_id} не найден в ЦЛ "
                            f"(cl_ref_key={client.cl_ref_key[:20]}). Сбрасываем cl_ref_key."
                        )
                        client.cl_ref_key = None
                        total_not_found += 1
                        continue
                    
                    parent_key = client_data.get("Parent_Key")
                    inn = client_data.get("ИНН")
                    code = client_data.get("КодАбонентаClobus")
                    
                    if (batch_start + i) % 20 == 0:  # Логируем каждого 20-го
                        logger.info(
                            f"  [{batch_start + i}] Клиент {client.client_id}: "
                            f"Parent_Key={parent_key[:20] if parent_key else None}, код={code}"
                        )
                    
                    # Сохраняем parent_key
                    client.parent_key = parent_key
                    
                    # Проверяем, что Parent_Key правильный
                    if parent_key != CLOBUS_PARENT_KEY:
                        logger.warning(
                            f"  [{batch_start + i}] Parent_Key неправильный "
                            f"(expected: {CLOBUS_PARENT_KEY[:20]}, got: {parent_key[:20] if parent_key else None}). "
                            f"Сбрасываем cl_ref_key."
                        )
                        client.cl_ref_key = None
                        total_reset += 1
                    else:
                        total_updated += 1
                        
                except Exception as e:
                    logger.error(f"  Ошибка обработки клиента {client.client_id}: {e}")
                    total_errors += 1
                    continue
            
            # 4. Коммитим батч
            await db.commit()
            logger.info(f"Батч сохранен ({batch_start + len(batch)} / {len(clients_with_ref)})")
        
        logger.info("=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА (клиенты с cl_ref_key):")
        logger.info(f"  Обработано клиентов: {len(clients_with_ref)}")
        logger.info(f"  Обновлено parent_key (правильный Parent_Key): {total_updated}")
        logger.info(f"  Сброшено cl_ref_key (неправильный Parent_Key): {total_reset}")
        logger.info(f"  Сброшено cl_ref_key (не найдено в ЦЛ): {total_not_found}")
        logger.info(f"  Ошибок: {total_errors}")
        logger.info("=" * 80)
        
        # Теперь обработаем клиентов БЕЗ cl_ref_key, но с code_abonent и org_inn
        logger.info("\nОбработка клиентов БЕЗ cl_ref_key...")
        result = await db.execute(
            select(Client).where(
                Client.cl_ref_key.is_(None),
                Client.code_abonent.isnot(None),
                Client.org_inn.isnot(None)
            )
        )
        clients_without_ref = result.scalars().all()
        
        logger.info(f"Найдено {len(clients_without_ref)} клиентов без cl_ref_key (но с code+inn)")
        
        total_found_and_linked = 0
        total_search_errors = 0
        
        # ОПТИМИЗАЦИЯ: обрабатываем батчами
        for batch_start in range(0, len(clients_without_ref), BATCH_SIZE):
            batch = clients_without_ref[batch_start:batch_start + BATCH_SIZE]
            logger.info(f"\n=== Поиск батча {batch_start//BATCH_SIZE + 1} ({len(batch)} клиентов) ===")
            
            # Для каждого клиента в батче делаем поиск (здесь сложно оптимизировать, т.к. поиск по двум полям)
            # Но хотя бы коммитим батчами, а не по одному
            for i, client in enumerate(batch, 1):
                try:
                    # Ищем клиента в ЦЛ по code+inn в папке CLOBUS
                    client_data = await onec_client.find_client_by_code_and_inn(
                        code_abonent=client.code_abonent,
                        org_inn=client.org_inn,
                        parent_key=CLOBUS_PARENT_KEY  # Ищем только в папке CLOBUS
                    )
                    
                    if client_data:
                        ref_key = client_data.get("Ref_Key")
                        parent_key = client_data.get("Parent_Key")
                        
                        if (batch_start + i) % 20 == 0:  # Логируем каждого 20-го
                            logger.info(
                                f"  [{batch_start + i}] Найден: Ref_Key={ref_key[:20]}, Parent_Key={parent_key[:20]}"
                            )
                        
                        # Связываем клиента
                        client.cl_ref_key = ref_key
                        client.parent_key = parent_key
                        total_found_and_linked += 1
                        
                except Exception as e:
                    logger.error(f"  Ошибка поиска клиента {client.client_id}: {e}")
                    total_search_errors += 1
                    continue
            
            # Коммитим батч
            await db.commit()
            logger.info(f"Батч сохранен ({batch_start + len(batch)} / {len(clients_without_ref)}, найдено: {total_found_and_linked})")
        
        logger.info("=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА (клиенты без cl_ref_key):")
        logger.info(f"  Обработано клиентов: {len(clients_without_ref)}")
        logger.info(f"  Найдено и связано с ЦЛ: {total_found_and_linked}")
        logger.info(f"  Ошибок поиска: {total_search_errors}")
        logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(migrate_parent_keys())
