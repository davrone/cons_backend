# Docker Setup и Автоматизация

## Автоматическая инициализация при запуске контейнера

При каждом запуске контейнера автоматически выполняются следующие шаги:

1. **Ожидание доступности БД** - скрипт ждет, пока PostgreSQL станет доступен
2. **Применение миграций Alembic** - автоматически применяются все миграции БД
3. **Инициализация БД** - создание схем, таблиц и начальных данных
4. **Загрузка справочников** (опционально) - загрузка данных из 1C:CL
5. **Синхронизация пользователей** (опционально) - синхронизация с Chatwoot
6. **Запуск API сервера** - запуск FastAPI приложения

## Переменные окружения для управления автоматизацией

Добавьте в ваш `.env` файл следующие переменные для управления автоматизацией:

```env
# Загружать справочники при старте (по умолчанию: true)
LOAD_DICTS_ON_START=true

# Синхронизировать пользователей с Chatwoot при старте (по умолчанию: false)
SYNC_USERS_ON_START=false
```

### Рекомендации:

- **LOAD_DICTS_ON_START=true** - рекомендуется для первого запуска и после обновления справочников в 1C:CL
- **LOAD_DICTS_ON_START=false** - если справочники уже загружены и не изменились
- **SYNC_USERS_ON_START=true** - только при первом запуске или после добавления новых пользователей
- **SYNC_USERS_ON_START=false** - для обычных запусков (синхронизацию можно запускать вручную)

## Ручной запуск скриптов

Если нужно запустить скрипты вручную:

```bash
# Применить миграции
docker-compose exec cons_api alembic -c alembic.ini upgrade head

# Загрузить справочники
docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts

# Синхронизировать пользователей с Chatwoot
docker-compose exec cons_api python -m FastAPI.catalog_scripts.sync_users_to_chatwoot

# Инициализировать БД
docker-compose exec cons_api python -m FastAPI.init_db
```

## Пересборка контейнера

После изменений в коде:

```bash
# Пересобрать и перезапустить
docker-compose up -d --build cons_api

# Или только пересобрать
docker-compose build cons_api
docker-compose up -d cons_api
```

Все автоматические шаги выполнятся при запуске контейнера.

## Логи инициализации

Чтобы посмотреть логи инициализации:

```bash
docker-compose logs -f cons_api
```

Вы увидите процесс инициализации:
```
=== Starting initialization ===
Waiting for database to be ready...
✓ Database is ready
Applying database migrations...
✓ Migrations applied
Initializing database...
✓ Database initialized
Loading dictionaries from 1C:CL...
✓ Dictionaries loaded
=== Initialization complete ===
```

