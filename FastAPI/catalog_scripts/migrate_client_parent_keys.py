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
    """Миграция parent_key для всех клиентов"""
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
        
        for i, client in enumerate(clients_with_ref, 1):
            try:
                logger.info(f"[{i}/{len(clients_with_ref)}] Обработка клиента {client.client_id}")
                
                # Запрашиваем клиента из ЦЛ по Ref_Key
                client_data = await onec_client.get_client_by_ref_key(client.cl_ref_key)
                
                if not client_data:
                    logger.warning(
                        f"  Клиент не найден в ЦЛ (cl_ref_key={client.cl_ref_key[:20]}). "
                        f"Сбрасываем cl_ref_key."
                    )
                    client.cl_ref_key = None
                    total_not_found += 1
                    continue
                
                parent_key = client_data.get("Parent_Key")
                ref_key = client_data.get("Ref_Key")
                inn = client_data.get("ИНН") or client_data.get("ИННФизЛица")
                code = client_data.get("КодАбонентаClobus")
                
                logger.info(
                    f"  Найден в ЦЛ: Parent_Key={parent_key}, "
                    f"ИНН={inn}, код={code}"
                )
                
                # Сохраняем parent_key
                client.parent_key = parent_key
                
                # Проверяем, что Parent_Key правильный
                if parent_key != CLOBUS_PARENT_KEY:
                    logger.warning(
                        f"  Parent_Key неправильный (expected: {CLOBUS_PARENT_KEY}, got: {parent_key}). "
                        f"Сбрасываем cl_ref_key."
                    )
                    client.cl_ref_key = None
                    total_reset += 1
                else:
                    logger.info(f"  Parent_Key правильный. Клиент в папке CLOBUS.")
                    total_updated += 1
                
                # Коммитим каждые 100 записей
                if i % 100 == 0:
                    await db.commit()
                    logger.info(f"Сохранено {i} записей...")
                    
            except Exception as e:
                logger.error(f"  Ошибка обработки клиента {client.client_id}: {e}", exc_info=True)
                total_errors += 1
                continue
        
        # Финальный коммит
        await db.commit()
        
        logger.info("=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА:")
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
        
        for i, client in enumerate(clients_without_ref, 1):
            try:
                logger.info(f"[{i}/{len(clients_without_ref)}] Поиск клиента {client.client_id} в ЦЛ")
                
                # Ищем клиента в ЦЛ по code+inn в папке CLOBUS
                client_data = await onec_client.find_client_by_code_and_inn(
                    code_abonent=client.code_abonent,
                    org_inn=client.org_inn,
                    parent_key=CLOBUS_PARENT_KEY  # Ищем только в папке CLOBUS
                )
                
                if client_data:
                    ref_key = client_data.get("Ref_Key")
                    parent_key = client_data.get("Parent_Key")
                    
                    logger.info(
                        f"  Найден в папке CLOBUS: Ref_Key={ref_key[:20]}, "
                        f"Parent_Key={parent_key}"
                    )
                    
                    # Связываем клиента
                    client.cl_ref_key = ref_key
                    client.parent_key = parent_key
                    total_found_and_linked += 1
                else:
                    logger.info(f"  Не найден в папке CLOBUS")
                
                # Коммитим каждые 100 записей
                if i % 100 == 0:
                    await db.commit()
                    logger.info(f"Обработано {i} записей...")
                    
            except Exception as e:
                logger.error(f"  Ошибка поиска клиента {client.client_id}: {e}", exc_info=True)
                continue
        
        # Финальный коммит
        await db.commit()
        
        logger.info("=" * 80)
        logger.info("ИТОГОВАЯ СТАТИСТИКА (клиенты без cl_ref_key):")
        logger.info(f"  Обработано клиентов: {len(clients_without_ref)}")
        logger.info(f"  Найдено и связано с ЦЛ: {total_found_and_linked}")
        logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(migrate_parent_keys())
