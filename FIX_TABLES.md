# Исправление структуры таблиц

## Проблема

В таблицах есть лишние колонки или не хватает нужных. Нужно привести структуру в соответствие с требованиями.

## Решение

### Вариант 1: Через миграции Alembic (рекомендуется)

1. **Создайте миграцию:**
   ```bash
   docker-compose exec cons_api alembic revision --autogenerate -m "Fix table structure"
   ```

2. **Проверьте созданную миграцию** в `FastAPI/alembic/versions/`

3. **Примените миграцию:**
   ```bash
   docker-compose exec cons_api alembic upgrade head
   ```

### Вариант 2: Вручную через SQL (если миграции не работают)

Выполните SQL команды для исправления структуры:

```sql
-- Удалить лишние колонки из cons.cons (если есть)
-- ALTER TABLE cons.cons DROP COLUMN IF EXISTS created_at;
-- ALTER TABLE cons.cons DROP COLUMN IF EXISTS updated_at;

-- Удалить лишние колонки из cons.calls (если есть)
-- ALTER TABLE cons.calls DROP COLUMN IF EXISTS id;
-- ALTER TABLE cons.calls DROP COLUMN IF EXISTS created_at;

-- Удалить лишние колонки из cons.cons_redate (если есть)
-- ALTER TABLE cons.cons_redate DROP COLUMN IF EXISTS id;
-- ALTER TABLE cons.cons_redate DROP COLUMN IF EXISTS created_at;

-- Удалить лишние колонки из cons.users_skill (если есть)
-- ALTER TABLE cons.users_skill DROP COLUMN IF EXISTS id;

-- Добавить составной PK для calls (если нужно)
-- ALTER TABLE cons.calls ADD PRIMARY KEY (period, cons_key, manager);

-- Добавить составной PK для users_skill (если нужно)
-- ALTER TABLE cons.users_skill ADD PRIMARY KEY (user_key, category_key);
```

## Текущая структура (требования)

### cons.clients
- client_id (PK, UUID)
- client_id_hash (TEXT, UNIQUE)
- cl_ref_key (TEXT)
- email, phone_number, country, region, city (TEXT)
- subs_id, subs_start, subs_end (TEXT, TIMESTAMP)
- tariff_id, tariffperiod_id (TEXT)
- org_id, org_inn (TEXT)
- source_id (TEXT)
- is_parent (BOOLEAN)
- parent_id (FK -> clients.client_id)

### cons.cons
- cons_id (PK, TEXT) - из Chatwoot
- cl_ref_key (TEXT) - из ЦЛ
- client_id (FK -> clients.client_id)
- client_key (TEXT)
- number (TEXT) - из ЦЛ
- status (TEXT)
- org_inn (TEXT)
- importance (INTEGER)
- create_date, start_date, end_date (TIMESTAMP)
- redate_time (TIME)
- redate (DATE)
- lang (TEXT)
- denied (BOOLEAN)
- manager, author (TEXT)
- comment (TEXT)
- online_question_cat, online_question (TEXT)
- con_blocks (TEXT)
- con_rates, con_calls (JSONB)

### cons.calls
- period (PK, TIMESTAMP)
- cons_key (PK, TEXT)
- cons_id (TEXT)
- client_key (TEXT)
- client_id (FK -> clients.client_id)
- manager (PK, TEXT)
- **БЕЗ id, БЕЗ created_at**

### cons.users_skill
- user_key (PK, TEXT)
- category_key (PK, TEXT)
- **БЕЗ id**

### cons.cons_redate
- cons_key (TEXT)
- clients_key (TEXT)
- manager_key (TEXT)
- period (TEXT)
- old_date, new_date (TIMESTAMP)
- **БЕЗ id, БЕЗ created_at**

## Проверка структуры

```sql
-- Проверить колонки в cons.cons
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'cons'
ORDER BY ordinal_position;

-- Проверить колонки в cons.calls
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'cons' AND table_name = 'calls'
ORDER BY ordinal_position;

-- Проверить ключи
SELECT 
    tc.constraint_name, 
    tc.constraint_type,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'cons' AND tc.table_name = 'calls';
```

