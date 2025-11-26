# Руководство по проверке и тестированию

## ✅ Что уже работает

1. **API запущен** - контейнер работает на порту 7070
2. **БД подключена** - таблицы созданы
3. **requests установлен** - можно запускать скрипты

## 🔧 Что нужно проверить

### 1. Загрузка справочников из ЦЛ

```bash
docker-compose exec cons_api python -m FastAPI.catalog_scripts.load_dicts
```

**Проверка:**
```sql
SELECT COUNT(*) FROM dict.online_question_cat;
SELECT COUNT(*) FROM dict.online_question;
SELECT COUNT(*) FROM dict.knowledge_base;
SELECT COUNT(*) FROM dict.po_types;
SELECT COUNT(*) FROM dict.po_sections;
```

### 2. Загрузка консультаций из ЦЛ

```bash
docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_cons_cl
```

**Проверка:**
```sql
SELECT COUNT(*) FROM cons.cons;
SELECT cons_id, cl_ref_key, number, status, create_date 
FROM cons.cons 
ORDER BY create_date DESC 
LIMIT 10;
```

### 3. Загрузка дозвонов из ЦЛ

```bash
docker-compose exec cons_api python -m FastAPI.catalog_scripts.pull_calls_cl
```

**Проверка:**
```sql
SELECT COUNT(*) FROM cons.calls;
SELECT period, cons_key, cons_id, manager 
FROM cons.calls 
ORDER BY period DESC 
LIMIT 10;
```

### 4. Тестирование API

**Создание консультации:**
```powershell
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

## ⚠️ Исправление структуры таблиц

Если в таблицах есть лишние колонки (например, `created_at`, `updated_at` в `cons.cons`), нужно создать миграцию:

```bash
# Создать миграцию
docker-compose exec cons_api alembic revision --autogenerate -m "Fix table structure"

# Применить миграцию
docker-compose exec cons_api alembic upgrade head
```

Или вручную через SQL (см. `FIX_TABLES.md`).

## 📋 Чеклист

- [ ] API работает: `http://localhost:7070/api/health` → `{"status":"ok"}`
- [ ] Загружены справочники: `SELECT COUNT(*) FROM dict.online_question_cat;` > 0
- [ ] Загружены консультации: `SELECT COUNT(*) FROM cons.cons;` > 0
- [ ] Загружены дозвоны: `SELECT COUNT(*) FROM cons.calls;` > 0
- [ ] Структура таблиц соответствует требованиям
- [ ] API создает консультации (протестировано)

## 🚨 Типичные ошибки

### "ModuleNotFoundError: No module named 'requests'"
**Решение:** Образ пересобран, перезапустите контейнер:
```bash
docker-compose restart cons_api
```

### "Request URL is missing an 'http://'"
**Решение:** В `.env` не заполнены URL для Chatwoot/ЦЛ. Это нормально для тестирования, API все равно работает.

### Таблицы имеют лишние колонки
**Решение:** Создайте миграцию Alembic или исправьте вручную через SQL.

