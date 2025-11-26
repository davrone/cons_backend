# Чеклист проверки системы

## 1. Проверка доступности API

### Health Check
```bash
# PowerShell
Invoke-WebRequest -Uri http://localhost:7070/api/health

# Или откройте в браузере:
http://localhost:7070/api/health
```

**Ожидаемый ответ:**
```json
{"status": "ok"}
```

### Swagger UI
Откройте в браузере:
```
http://localhost:7070/docs
```

Если не открывается, проверьте:
- Контейнер запущен: `docker-compose ps`
- Логи: `docker-compose logs cons_api`
- Порт проброшен: `0.0.0.0:7070->7070/tcp`

### Проверка БД
```bash
# PowerShell
Invoke-WebRequest -Uri http://localhost:7070/api/health/db -UseBasicParsing
```

**Ожидаемый ответ:**
```json
{"status": "ok", "database": "connected"}
```

---

## 2. Проверка структуры БД

### Подключитесь к PostgreSQL и проверьте:

```sql
-- Проверка схем
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('cons', 'dict', 'sys', 'log');

-- Проверка таблиц в схеме cons
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'cons';

-- Проверка таблиц в схеме dict
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'dict';

-- Проверка колонок в cons.cons
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'cons';

-- Проверка колонок в cons.clients
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'clients';
```

**Ожидаемые таблицы:**
- `cons.clients`
- `cons.cons`
- `cons.q_and_a`
- `cons.users`
- `cons.users_skill`
- `cons.cons_redate`
- `cons.calls`
- `dict.online_question_cat`
- `dict.online_question`
- `dict.knowledge_base`
- `dict.po_sections`
- `dict.po_types`
- `sys.db_migrations`
- `sys.sync_state`
- `log.webhook_log`

---

## 3. Загрузка справочников из ЦЛ

### Настройка .env

Убедитесь, что в `.env` есть:
```env
ODATA_BASE_URL=https://your-1c-host/odata/standard.odata
ODATA_USER=your_username
ODATA_PASSWORD=your_password
```

### Запуск скрипта загрузки справочников

**Вариант 1: Из контейнера**
```bash
docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts
```

**Вариант 2: Локально (если Python установлен)**
```bash
# Установите зависимости
pip install -r requirements.txt

# Запустите скрипт
python -m FastAPI.catalog_scripts.load_dicts
```

**Проверка загруженных данных:**
```sql
-- Проверка справочников
SELECT COUNT(*) FROM dict.online_question_cat;
SELECT COUNT(*) FROM dict.online_question;
SELECT COUNT(*) FROM dict.knowledge_base;
SELECT COUNT(*) FROM dict.po_types;
SELECT COUNT(*) FROM dict.po_sections;
```

---

## 4. Загрузка консультаций из ЦЛ

### Запуск скрипта синхронизации

**Из контейнера:**
```bash
docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_cons_cl
```

**Локально:**
```bash
python -m FastAPI.catalog_scripts.pull_cons_cl
```

**Проверка загруженных данных:**
```sql
-- Проверка консультаций
SELECT COUNT(*) FROM cons.cons;
SELECT cons_id, cl_ref_key, number, status, create_date 
FROM cons.cons 
ORDER BY create_date DESC 
LIMIT 10;

-- Проверка Q&A
SELECT COUNT(*) FROM cons.q_and_a;
```

---

## 5. Тестирование API endpoints

### Создание клиента

```bash
# PowerShell
$body = @{
    email = "test@example.com"
    phone_number = "+998901234567"
    org_inn = "123456789"
} | ConvertTo-Json

Invoke-WebRequest -Uri http://localhost:7070/api/clients `
    -Method POST `
    -Headers @{"Content-Type"="application/json"} `
    -Body $body
```

### Создание консультации

```bash
$body = @{
    client = @{
        email = "test@example.com"
        phone_number = "+998901234567"
        org_inn = "123456789"
    }
    consultation = @{
        comment = "Тестовая консультация"
        lang = "ru"
    }
    source = "SITE"
} | ConvertTo-Json -Depth 10

Invoke-WebRequest -Uri http://localhost:7070/api/consultations/create `
    -Method POST `
    -Headers @{"Content-Type"="application/json"} `
    -Body $body
```

### Получение тикетов клиента

```bash
# Замените {client_id} на реальный UUID
Invoke-WebRequest -Uri http://localhost:7070/api/tickets/clients/{client_id}/tickets
```

---

## 6. Проверка интеграций

### Chatwoot
- Проверьте настройки в `.env`:
  ```
  CHATWOOT_API_URL=https://...
  CHATWOOT_API_TOKEN=...
  CHATWOOT_ACCOUNT_ID=...
  ```

### 1C:ЦЛ OData
- Проверьте настройки в `.env`:
  ```
  ODATA_BASE_URL=https://...
  ODATA_USER=...
  ODATA_PASSWORD=...
  ```

---

## 7. Проверка логов

### Логи API
```bash
docker-compose logs cons_api -f
```

### Логи Redis
```bash
docker-compose logs cons_redis -f
```

---

## 8. Типичные проблемы

### Swagger не открывается
1. Проверьте, что контейнер запущен: `docker-compose ps`
2. Проверьте логи: `docker-compose logs cons_api`
3. Проверьте порт: `netstat -an | findstr 7070` (Windows)
4. Попробуйте перезапустить: `docker-compose restart cons_api`

### Ошибки подключения к БД
1. Проверьте `.env`:
   ```
   DB_HOST=host.docker.internal
   DB_PORT=5432
   DB_NAME=cons_backend
   DB_USER=postgres
   DB_PASS=qwerty123
   ```
2. Убедитесь, что PostgreSQL запущен и доступен
3. Проверьте подключение: `docker-compose exec cons_api python -c "from FastAPI.init_db import check_db_connection; import asyncio; asyncio.run(check_db_connection())"`

### Данные не загружаются из ЦЛ
1. Проверьте настройки OData в `.env`
2. Проверьте доступность ЦЛ: `curl -u user:pass https://your-1c-host/odata/standard.odata`
3. Запустите скрипты вручную и проверьте логи
4. Проверьте таблицу `sys.sync_state`:
   ```sql
   SELECT * FROM sys.sync_state;
   ```

### Отсутствующие колонки в таблицах
Если в таблицах не хватает колонок:
1. Создайте миграцию Alembic:
   ```bash
   alembic revision --autogenerate -m "Add missing columns"
   ```
2. Примените миграцию:
   ```bash
   alembic upgrade head
   ```
3. Или добавьте колонки вручную через SQL (не рекомендуется)

---

## 9. Быстрая проверка всего

```bash
# 1. Проверка контейнеров
docker-compose ps

# 2. Проверка health
Invoke-WebRequest -Uri http://localhost:7070/api/health

# 3. Проверка БД
Invoke-WebRequest -Uri http://localhost:7070/api/health/db

# 4. Проверка Swagger
# Откройте http://localhost:7070/docs в браузере

# 5. Проверка справочников в БД
# Подключитесь к PostgreSQL и выполните:
# SELECT COUNT(*) FROM dict.online_question_cat;

# 6. Загрузка справочников (если пусто)
docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts

# 7. Загрузка консультаций (если пусто)
docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_cons_cl
```

---

## 10. Следующие шаги

После проверки:
1. ✅ Загрузите справочники из ЦЛ
2. ✅ Загрузите консультации из ЦЛ
3. ✅ Протестируйте создание консультации через API
4. ✅ Проверьте синхронизацию с Chatwoot и ЦЛ
5. ⚠️ Добавьте недостающие колонки в таблицы (через миграции)
6. ⚠️ Настройте валидацию токенов (когда будет готово)

