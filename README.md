# Consultation Middleware

Единая точка интеграции для системы консультаций с тикетингом.

## Архитектура

Middleware интегрирует:
- **Сайт** - точка входа для создания заявок
- **Telegram Mini App** - мобильный интерфейс
- **Chatwoot** - система тикетинга (ticket backend)
- **1C:ЦЛ** - колл-центр
- **PostgreSQL 16** - единая БД (источник истины)

## Технологии

- **FastAPI** - асинхронный веб-фреймворк
- **SQLAlchemy 2.0** - async ORM
- **Alembic** - миграции БД
- **PostgreSQL 16** - база данных
- **asyncpg** - async драйвер для PostgreSQL

## Структура проекта

```
cons_backend/
├── FastAPI/
│   ├── __init__.py
│   ├── main.py              # Точка входа приложения
│   ├── config.py            # Конфигурация из .env
│   ├── database.py          # Подключение к БД (async)
│   ├── models.py            # SQLAlchemy модели
│   ├── init_db.py           # Инициализация БД
│   ├── routers/             # API роуты
│   │   ├── auth.py          # Аутентификация (OpenID)
│   │   ├── tickets.py       # Работа с тикетами
│   │   ├── webhooks.py      # Вебхуки от Chatwoot и 1C
│   │   └── health.py        # Health checks
│   ├── schemas/             # Pydantic схемы
│   │   ├── auth.py
│   │   ├── tickets.py
│   │   └── webhooks.py
│   ├── services/            # Клиенты для внешних API
│   │   ├── chatwoot_client.py
│   │   └── onec_client.py
│   └── alembic/             # Миграции
│       ├── env.py
│       └── versions/
├── alembic.ini              # Конфигурация Alembic
├── requirements.txt         # Зависимости
└── dockerfile               # Docker образ

```

## Схемы БД

- **cons** - бизнес-данные (клиенты, консультации, пользователи)
- **dict** - справочники
- **sys** - служебные таблицы (миграции)
- **log** - логирование (вебхуки)

## Установка и запуск

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

Создайте файл `.env` на основе `.env.example`:

```bash
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=cons_backend
DB_USER=postgres
DB_PASS=qwerty123
# ... остальные переменные
```

### 3. Инициализация БД

БД инициализируется автоматически при первом запуске приложения.

Или вручную:

```bash
python -m FastAPI.init_db
```

### 4. Запуск приложения

```bash
uvicorn FastAPI.main:app --host 0.0.0.0 --port 7070 --reload
```

Или через Docker:

```bash
docker compose up --build
```

## API Endpoints

### Аутентификация

- `POST /api/auth/login` - Вход через OpenID токен

### Тикеты

- `POST /api/tickets/create` - Создание нового тикета
- `GET /api/tickets/{cons_id}` - Получение тикета по ID
- `GET /api/tickets/clients/{client_id}/tickets` - Список тикетов клиента

### Вебхуки

- `POST /webhook/chatwoot` - Вебхук от Chatwoot
- `POST /webhook/1c_cl` - Вебхук от 1C:ЦЛ

### Health

- `GET /api/health` - Базовая проверка
- `GET /api/health/db` - Проверка подключения к БД

## Миграции

Создание новой миграции:

```bash
alembic revision --autogenerate -m "Описание изменений"
```

Применение миграций:

```bash
alembic upgrade head
```

## Бизнес-процесс

### Создание заявки

1. Сайт/Telegram вызывает `POST /api/tickets/create`
2. Middleware сохраняет в БД
3. Middleware отправляет в Chatwoot
4. Middleware отправляет в 1C:ЦЛ
5. Middleware обновляет запись с полученными ID

### Обновление статусов

- **Из 1C:ЦЛ**: вебхук → обновление БД → синхронизация с Chatwoot
- **Из Chatwoot**: вебхук → обновление БД → синхронизация с 1C:ЦЛ

## Особенности

- ✅ Асинхронная архитектура
- ✅ Идемпотентная инициализация БД
- ✅ Все CREATE через SQLAlchemy (без ручного SQL)
- ✅ Миграции через Alembic
- ✅ Конфигурация через .env (без hardcode)
- ✅ Единый источник истины (БД)
- ✅ Резервирование данных
- ✅ История изменений

## Разработка

Проект использует:
- Python 3.10+
- FastAPI 0.109+
- SQLAlchemy 2.0+ (async)
- Pydantic 2.5+

