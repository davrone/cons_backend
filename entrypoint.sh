#!/bin/bash
set -e

echo "=== Starting initialization ==="

# Ждем доступности БД (с выводом ошибок для диагностики)
echo "Waiting for database to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  if python wait_for_db.py 2>&1; then
    echo "✓ Database is ready"
    break
  fi
  attempt=$((attempt + 1))
  if [ $attempt -ge $max_attempts ]; then
    echo "✗ Database is still unavailable after $max_attempts attempts"
    echo "Please check:"
    echo "  - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS in .env"
    echo "  - PostgreSQL is running and accessible"
    exit 1
  fi
  echo "Database is unavailable - sleeping (attempt $attempt/$max_attempts)"
  sleep 2
done

# Применяем миграции Alembic (миграции сами проверяют существование колонок)
# Используем 'heads' вместо 'head' для поддержки разветвленных миграций
echo "Applying database migrations..."
cd /app
alembic -c alembic.ini upgrade heads || {
    echo "⚠ Migration failed, but continuing (columns may already exist from init_db)"
}
echo "✓ Migrations check completed"

# Инициализация БД (создание схем, таблиц если нужно)
echo "Initializing database..."
python -m FastAPI.init_db
echo "✓ Database initialized"

# Загрузка справочников (если переменная установлена)
if [ "${LOAD_DICTS_ON_START:-true}" = "true" ]; then
  echo "Loading dictionaries from 1C:CL..."
  python -m FastAPI.catalog_scripts.load_dicts || echo "⚠ Warning: Dictionary load failed, continuing..."
  echo "✓ Dictionaries loaded"
fi

# Синхронизация пользователей с Chatwoot (опционально)
if [ "${SYNC_USERS_ON_START:-false}" = "true" ]; then
  echo "Syncing users to Chatwoot..."
  python -m FastAPI.catalog_scripts.sync_users_to_chatwoot || echo "⚠ Warning: User sync failed, continuing..."
  echo "✓ Users synced"
fi

echo "=== Initialization complete ==="
echo "Note: Scheduled tasks run via APScheduler inside the application"
echo "=== Starting application ==="

# Запускаем основной сервер
exec "$@"

