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

## Полная пересборка с нуля

Если нужно полностью пересобрать инстанс с нуля (например, после удаления всех таблиц в БД):

### Вариант 1: Через Docker (рекомендуется)

1. **Остановите контейнеры:**
```bash
docker-compose down
```

2. **Удалите контейнеры и volumes (если нужно):**
```bash
# Удалить только контейнеры
docker-compose rm -f

# Или удалить контейнеры и volumes (удалит все данные в БД, если БД в Docker)
docker-compose down -v
```

3. **Если БД находится вне Docker, очистите БД вручную:**
```sql
-- Подключитесь к PostgreSQL и выполните:
DROP SCHEMA IF EXISTS cons CASCADE;
DROP SCHEMA IF EXISTS dict CASCADE;
DROP SCHEMA IF EXISTS sys CASCADE;
DROP SCHEMA IF EXISTS log CASCADE;
```

4. **Пересоберите и запустите контейнер:**
```bash
docker-compose up -d --build cons_api
```

При запуске автоматически выполнится:
- Создание схем (`cons`, `dict`, `sys`, `log`)
- Применение миграций Alembic
- Создание всех таблиц
- Загрузка справочников (если `LOAD_DICTS_ON_START=true`)
- Синхронизация пользователей (если `SYNC_USERS_ON_START=true`)

### Вариант 2: Ручная пересборка БД

Если нужно пересобрать БД вручную без пересборки контейнера:

1. **Остановите контейнер:**
```bash
docker-compose stop cons_api
```

2. **Очистите БД:**
```sql
-- Подключитесь к PostgreSQL и выполните:
DROP SCHEMA IF EXISTS cons CASCADE;
DROP SCHEMA IF EXISTS dict CASCADE;
DROP SCHEMA IF EXISTS sys CASCADE;
DROP SCHEMA IF EXISTS log CASCADE;
```

3. **Запустите контейнер:**
```bash
docker-compose up -d cons_api
```

При запуске автоматически выполнится инициализация БД.

### Вариант 3: Использование reset_db.py

Для полной очистки и пересоздания БД можно использовать скрипт `reset_db.py`:

```bash
# Полная очистка (удаление всех таблиц + восстановление структуры)
docker-compose exec cons_api python -m FastAPI.reset_db --full --confirm

# Только очистка данных (TRUNCATE, структура сохраняется)
docker-compose exec cons_api python -m FastAPI.reset_db --data-only --confirm

# Только восстановление структуры (без очистки)
docker-compose exec cons_api python -m FastAPI.reset_db --recreate-only
```

**Внимание:** 
- Скрипт `reset_db.py` удаляет все данные и таблицы. Используйте только в dev-окружении!
- Для операций `--full` и `--data-only` обязательно требуется флаг `--confirm`

### Проверка после пересборки

После пересборки проверьте:

1. **Логи инициализации:**
```bash
docker-compose logs cons_api | grep -E "(✓|Error|Failed)"
```

2. **Health check:**
```bash
curl http://localhost:7070/api/health
curl http://localhost:7070/api/health/db
```

3. **Проверка таблиц в БД:**
```sql
-- Подключитесь к PostgreSQL и проверьте схемы:
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('cons', 'dict', 'sys', 'log');

-- Проверьте основные таблицы:
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'cons' 
ORDER BY table_name;
```

### Рекомендации для первого запуска

При первом запуске с пустой БД рекомендуется установить в `.env`:

```env
# Загружать справочники при старте (обязательно для первого запуска)
LOAD_DICTS_ON_START=true

# Синхронизировать пользователей с Chatwoot при старте (рекомендуется для первого запуска)
SYNC_USERS_ON_START=true
```

После первого запуска можно установить:
```env
LOAD_DICTS_ON_START=false  # Справочники уже загружены
SYNC_USERS_ON_START=false  # Пользователи уже синхронизированы
```

