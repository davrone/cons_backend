"""
Скрипт для очистки и восстановления базы данных.

Варианты использования:
1. Полная очистка (удаление всех таблиц и пересоздание):
   python -m FastAPI.reset_db --full

2. Только очистка данных (TRUNCATE, сохранение структуры):
   python -m FastAPI.reset_db --data-only

3. Восстановление структуры без очистки:
   python -m FastAPI.reset_db --recreate-only
"""
import asyncio
import argparse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from .database import Base, engine
from .config import settings
from .init_db import init_db, SCHEMAS

# Схемы для очистки
CLEANUP_SCHEMAS = ["cons", "dict", "sys", "log"]


async def get_all_tables(conn, schema: str):
    """Получает список всех таблиц в схеме"""
    result = await conn.execute(text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = :schema
        AND table_type = 'BASE TABLE'
    """), {"schema": schema})
    return [row[0] for row in result.fetchall()]


async def drop_all_tables():
    """Удаляет все таблицы из всех схем (CASCADE)"""
    async with engine.begin() as conn:
        print("🗑️  Удаление всех таблиц...")
        
        # Отключаем проверку внешних ключей для безопасного удаления
        await conn.execute(text("SET session_replication_role = 'replica';"))
        
        # Удаляем таблицы из каждой схемы
        for schema in CLEANUP_SCHEMAS:
            tables = await get_all_tables(conn, schema)
            if tables:
                print(f"  Схема {schema}: найдено {len(tables)} таблиц")
                for table in tables:
                    try:
                        await conn.execute(
                            text(f'DROP TABLE IF EXISTS {schema}."{table}" CASCADE;')
                        )
                        print(f"    ✓ Удалена таблица {schema}.{table}")
                    except Exception as e:
                        print(f"    ✗ Ошибка при удалении {schema}.{table}: {e}")
            else:
                print(f"  Схема {schema}: таблицы не найдены")
        
        # Включаем обратно проверку внешних ключей
        await conn.execute(text("SET session_replication_role = 'origin';"))
        
        print("✓ Все таблицы удалены")


async def truncate_all_tables():
    """Очищает данные из всех таблиц (TRUNCATE)"""
    async with engine.begin() as conn:
        print("🧹 Очистка данных из всех таблиц...")
        
        # Отключаем проверку внешних ключей для безопасной очистки
        await conn.execute(text("SET session_replication_role = 'replica';"))
        
        # Получаем все таблицы из всех схем
        all_tables = []
        for schema in CLEANUP_SCHEMAS:
            tables = await get_all_tables(conn, schema)
            for table in tables:
                all_tables.append((schema, table))
        
        if not all_tables:
            print("  ⚠️  Таблицы не найдены")
            await conn.execute(text("SET session_replication_role = 'origin';"))
            return
        
        print(f"  Найдено {len(all_tables)} таблиц для очистки")
        
        # Очищаем все таблицы (CASCADE автоматически обработает зависимости)
        for schema, table in all_tables:
            try:
                await conn.execute(
                    text(f'TRUNCATE TABLE {schema}."{table}" CASCADE;')
                )
                print(f"  ✓ Очищена таблица {schema}.{table}")
            except Exception as e:
                print(f"  ✗ Ошибка при очистке {schema}.{table}: {e}")
        
        # Включаем обратно проверку внешних ключей
        await conn.execute(text("SET session_replication_role = 'origin';"))
        
        print(f"✓ Очищено {len(all_tables)} таблиц")


async def recreate_structure():
    """Восстанавливает структуру БД через init_db"""
    print("🔨 Восстановление структуры БД...")
    await init_db()


async def reset_full(confirm: bool = False):
    """Полная очистка: удаление таблиц + восстановление структуры"""
    print("=" * 60)
    print("🔄 ПОЛНАЯ ОЧИСТКА И ВОССТАНОВЛЕНИЕ БД")
    print("=" * 60)
    print(f"База данных: {settings.DB_NAME} на {settings.DB_HOST}:{settings.DB_PORT}")
    
    if not confirm:
        print("\n⚠️  ВНИМАНИЕ: Это удалит ВСЕ таблицы и данные!")
        print("Для подтверждения используйте флаг --confirm")
        print("=" * 60)
        return
    
    try:
        await drop_all_tables()
        await recreate_structure()
        print("\n" + "=" * 60)
        print("✅ Полная очистка и восстановление завершены успешно!")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Ошибка при полной очистке: {e}")
        raise
    finally:
        await engine.dispose()


async def reset_data_only(confirm: bool = False):
    """Только очистка данных (структура сохраняется)"""
    print("=" * 60)
    print("🧹 ОЧИСТКА ДАННЫХ (структура сохраняется)")
    print("=" * 60)
    print(f"База данных: {settings.DB_NAME} на {settings.DB_HOST}:{settings.DB_PORT}")
    
    if not confirm:
        print("\n⚠️  ВНИМАНИЕ: Это удалит ВСЕ данные из таблиц!")
        print("Для подтверждения используйте флаг --confirm")
        print("=" * 60)
        return
    
    try:
        await truncate_all_tables()
        print("\n" + "=" * 60)
        print("✅ Очистка данных завершена успешно!")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Ошибка при очистке данных: {e}")
        raise
    finally:
        await engine.dispose()


async def recreate_only():
    """Только восстановление структуры (без очистки)"""
    print("=" * 60)
    print("🔨 ВОССТАНОВЛЕНИЕ СТРУКТУРЫ БД")
    print("=" * 60)
    
    try:
        await recreate_structure()
        print("\n" + "=" * 60)
        print("✅ Восстановление структуры завершено успешно!")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Ошибка при восстановлении структуры: {e}")
        raise
    finally:
        await engine.dispose()


async def main():
    """Главная функция с парсингом аргументов"""
    parser = argparse.ArgumentParser(
        description="Очистка и восстановление базы данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python -m FastAPI.reset_db --full --confirm          # Полная очистка (удаление таблиц + восстановление)
  python -m FastAPI.reset_db --data-only --confirm     # Только очистка данных (TRUNCATE)
  python -m FastAPI.reset_db --recreate-only           # Только восстановление структуры
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--full",
        action="store_true",
        help="Полная очистка: удаление всех таблиц и восстановление структуры"
    )
    group.add_argument(
        "--data-only",
        action="store_true",
        help="Только очистка данных (TRUNCATE), структура сохраняется"
    )
    group.add_argument(
        "--recreate-only",
        action="store_true",
        help="Только восстановление структуры (без очистки)"
    )
    
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Подтверждение для операций очистки (обязательно для --full и --data-only)"
    )
    
    args = parser.parse_args()
    
    if args.full:
        await reset_full(confirm=args.confirm)
    elif args.data_only:
        await reset_data_only(confirm=args.confirm)
    elif args.recreate_only:
        await recreate_only()


if __name__ == "__main__":
    asyncio.run(main())

