# Быстрая проверка системы

## ✅ Что уже работает

1. **API запущен** - контейнер `cons_api` работает на порту 7070
2. **БД подключена** - таблицы созданы
3. **Health check работает** - `/api/health` возвращает `{"status":"ok"}`

## 🔍 Что нужно проверить сейчас

### 1. Swagger UI (/docs)

**Проблема:** Если не открывается в браузере, попробуйте:

1. **Проверьте в браузере:**
   ```
   http://localhost:7070/docs
   http://127.0.0.1:7070/docs
   ```

2. **Альтернатива - ReDoc:**
   ```
   http://localhost:7070/redoc
   ```

3. **Проверьте OpenAPI JSON:**
   ```
   http://localhost:7070/openapi.json
   ```

4. **Если не работает, проверьте логи:**
   ```bash
   docker-compose logs cons_api --tail 50
   ```

### 2. Загрузка справочников из ЦЛ

**Данные НЕ загружаются автоматически!** Нужно запустить скрипт вручную:

```bash
# Вариант 1: Из контейнера (рекомендуется)
docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts

# Вариант 2: Локально (если Python установлен)
python -m FastAPI.catalog_scripts.load_dicts
```

**Перед запуском проверьте `.env`:**
```env
ODATA_BASE_URL=https://your-1c-host/odata/standard.odata
ODATA_USER=your_username
ODATA_PASSWORD=your_password
```

**После загрузки проверьте в БД:**
```sql
SELECT COUNT(*) FROM dict.online_question_cat;
SELECT COUNT(*) FROM dict.online_question;
SELECT COUNT(*) FROM dict.knowledge_base;
SELECT COUNT(*) FROM dict.po_types;
SELECT COUNT(*) FROM dict.po_sections;
```

### 3. Загрузка консультаций из ЦЛ

**Также нужно запустить вручную:**

```bash
# Из контейнера
docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_cons_cl

# Локально
python -m FastAPI.catalog_scripts.pull_cons_cl
```

**Проверка:**
```sql
SELECT COUNT(*) FROM cons.cons;
SELECT cons_id, cl_ref_key, number, status, create_date 
FROM cons.cons 
ORDER BY create_date DESC 
LIMIT 10;
```

### 4. Проверка структуры таблиц

Если в таблицах не хватает колонок, это нормально - нужно будет создать миграции.

**Проверьте текущую структуру:**
```sql
-- Колонки в cons.cons
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'cons'
ORDER BY ordinal_position;

-- Колонки в cons.clients
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'clients'
ORDER BY ordinal_position;
```

**Для добавления недостающих колонок:**
1. Создайте миграцию: `alembic revision --autogenerate -m "Add missing columns"`
2. Примените: `alembic upgrade head`

### 5. Тестирование API

**Создание тестовой консультации:**

```powershell
# PowerShell
$body = @{
    client = @{
        email = "test@example.com"
        phone_number = "+998901234567"
        org_inn = "123456789"
    }
    consultation = @{
        comment = "Тестовая консультация через API"
        lang = "ru"
    }
    source = "SITE"
} | ConvertTo-Json -Depth 10

Invoke-WebRequest -Uri http://localhost:7070/api/consultations/create `
    -Method POST `
    -Headers @{"Content-Type"="application/json"} `
    -Body $body `
    -UseBasicParsing
```

**Или через curl (если установлен):**
```bash
curl -X POST http://localhost:7070/api/consultations/create \
  -H "Content-Type: application/json" \
  -d '{
    "client": {
      "email": "test@example.com",
      "phone_number": "+998901234567"
    },
    "consultation": {
      "comment": "Тестовая консультация",
      "lang": "ru"
    },
    "source": "SITE"
  }'
```

## 📋 Чеклист проверки

- [ ] API отвечает: `http://localhost:7070/api/health` → `{"status":"ok"}`
- [ ] БД подключена: `http://localhost:7070/api/health/db` → `{"status":"ok","database":"connected"}`
- [ ] Swagger UI открывается: `http://localhost:7070/docs` (или `/redoc`)
- [ ] Таблицы созданы в БД (проверьте через pgAdmin или psql)
- [ ] Справочники загружены из ЦЛ (запустите `load_dicts.py`)
- [ ] Консультации загружены из ЦЛ (запустите `pull_cons_cl.py`)
- [ ] API создает консультации (протестируйте через Swagger или curl)
- [ ] Проверены логи: `docker-compose logs cons_api`

## 🚨 Если что-то не работает

### Swagger не открывается
1. Проверьте, что контейнер запущен: `docker-compose ps`
2. Проверьте логи: `docker-compose logs cons_api --tail 100`
3. Попробуйте перезапустить: `docker-compose restart cons_api`
4. Проверьте порт: `netstat -an | findstr 7070`

### Данные не загружаются
1. Проверьте `.env` - все ли настройки OData заполнены
2. Проверьте доступность ЦЛ из контейнера:
   ```bash
   docker-compose exec cons_api python -c "import requests; print(requests.get('https://your-1c-host/odata/standard.odata', auth=('user','pass')).status_code)"
   ```
3. Запустите скрипты вручную и смотрите логи
4. Проверьте таблицу `sys.sync_state`:
   ```sql
   SELECT * FROM sys.sync_state;
   ```

### Ошибки в логах
```bash
# Смотреть логи в реальном времени
docker-compose logs cons_api -f

# Последние 100 строк
docker-compose logs cons_api --tail 100
```

## 📝 Следующие шаги

1. **Загрузите справочники:**
   ```bash
   docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts
   ```

2. **Загрузите консультации:**
   ```bash
   docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_cons_cl
   ```

3. **Протестируйте API через Swagger** (если открывается) или через curl/PowerShell

4. **Добавьте недостающие колонки** через миграции Alembic

5. **Настройте автоматическую синхронизацию** (cron job или scheduler)

