#!/bin/bash
set -e

echo "🚀 Starting ETL Scheduler Service..."

# Ждем доступности БД (используем тот же скрипт, что и в entrypoint.sh)
echo "⏳ Waiting for database to be ready..."
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
    exit 1
  fi
  echo "Database is unavailable - sleeping (attempt $attempt/$max_attempts)"
  sleep 2
done

# Запускаем scheduler через отдельный Python скрипт
# ВАЖНО: НЕ запускаем инициализацию БД и загрузку справочников - это только для API контейнера
echo "📅 Starting scheduler..."
exec python -m FastAPI.run_scheduler
