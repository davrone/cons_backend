FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Устанавливаем зависимости
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get update && \
    rm -rf /var/lib/apt/lists/*

# Копируем код приложения
COPY FastAPI ./FastAPI
COPY alembic.ini ./alembic.ini
COPY entrypoint.sh ./entrypoint.sh
COPY scheduler_entrypoint.sh ./scheduler_entrypoint.sh
COPY wait_for_db.py ./wait_for_db.py

# Делаем скрипты исполняемыми и исправляем формат (LF вместо CRLF)
RUN chmod +x ./entrypoint.sh ./scheduler_entrypoint.sh ./wait_for_db.py && \
    sed -i 's/\r$//' ./entrypoint.sh ./scheduler_entrypoint.sh 2>/dev/null || true && \
    ls -la /app/scheduler_entrypoint.sh

ENV PYTHONPATH=/app

# Используем entrypoint для автоматической инициализации
ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "FastAPI.main:app", "--host", "0.0.0.0", "--port", "7070"]
