# Архитектура бэкенда системы консультаций

## Общее описание

Бэкенд представляет собой **middleware** (промежуточный слой) для интеграции следующих систем:
- **Frontend** (веб-сайт и Telegram Mini App)
- **Chatwoot** (система чата и управления обращениями)
- **1C:ЦЛ** (клиентская лицензия, учетная система через OData)

Основная задача: обеспечить синхронизацию данных о консультациях между всеми системами, сохраняя единый источник истины в PostgreSQL.

---

## Терминология

### Основные понятия

- **Консультация** (Consultation) — заявка клиента на получение консультации. Это основная бизнес-сущность системы. В коде используется термин "consultation", а не "ticket".

- **Клиент** (Client) — физическое или юридическое лицо, обращающееся за консультацией. Поддерживает иерархию владельцев и пользователей.

- **Консультант/Менеджер** (User/Manager) — сотрудник, который проводит консультацию. Может быть из Chatwoot или из 1C:ЦЛ.

- **Перенос консультации** (ConsRedate) — изменение даты/времени консультации. Может быть несколько переносов для одной консультации.

- **Оценка консультации** (ConsRatingAnswer) — оценка клиентом качества консультации. Может быть несколько оценок, вычисляется среднее.

- **Попытка дозвона** (Call) — попытка менеджера дозвониться до клиента. Отображается в чате.

---

## Архитектура системы

### Компоненты

```
┌─────────────┐
│  Frontend   │ (Веб-сайт / Telegram Mini App)
└──────┬──────┘
       │ HTTP/REST API
       ▼
┌─────────────────────────────────────┐
│      Middleware (FastAPI)            │
│  ┌──────────────────────────────┐  │
│  │  PostgreSQL (источник истины) │  │
│  └──────────────────────────────┘  │
│           │              │          │
│           │              │          │
│  ┌────────▼────┐  ┌──────▼──────┐  │
│  │  Chatwoot   │  │  1C:ЦЛ      │  │
│  │  (Webhook)  │  │  (OData)    │  │
│  └─────────────┘  └─────────────┘  │
└─────────────────────────────────────┘
```

### База данных (PostgreSQL)

**Схемы:**
- **`cons`** — основные бизнес-таблицы (клиенты, консультации, пользователи)
- **`dict`** — справочники (типы ПО, разделы, вопросы, база знаний)
- **`sys`** — служебные таблицы (синхронизация, миграции, маппинг пользователей)
- **`log`** — логирование (webhook логи)

**Основные таблицы (`cons`):**
- `clients` — клиенты (PK: `client_id` UUID)
  - `source_id` — source_id из Chatwoot для идентификации контакта
  - `chatwoot_pubsub_token` — pubsub_token из Chatwoot для WebSocket подключения виджета
  - `is_parent` — флаг владельца (`true` для владельца, `false` для пользователя)
  - `parent_id` — ссылка на владельца (FK к `cons.clients.client_id`)
  - `company_name` — название компании клиента для формирования имени в 1C (приоритет над `name`)
- `cons` — консультации (PK: `cons_id` Text, ID из Chatwoot)
  - `chatwoot_source_id` — source_id из Chatwoot для подключения виджета
  - `consultation_type` — вид обращения: "Техническая поддержка" или "Консультация по ведению учёта"
- `users` — пользователи (консультанты) (PK: `account_id` UUID)
  - `chatwoot_user_id` — ID пользователя в Chatwoot для маппинга
- `users_skill` — навыки пользователей (PK: `(user_key, category_key)`)
- `cons_redate` — история переносов консультаций (PK: `id` Integer)
- `cons_rating_answers` — оценки консультаций (PK: `id` Integer)
  - `rating_date` — дата оценки (ДатаОценки из 1C)
- `calls` — попытки дозвона (PK: `(period, cons_key, manager)`)
- `q_and_a` — вопросы и ответы по консультациям (PK: `id` Integer)
- `queue_closing` — регистр закрытия очереди для консультантов (PK: `(period, manager_key)`)
  - `period` — дата закрытия очереди (Date)
  - `manager_key` — ключ менеджера из ЦЛ (Text)

**Справочники (`dict`):**
- `po_types` — типы ПО
- `po_sections` — разделы ПО
- `online_question_cat` — категории вопросов
- `online_question` — вопросы
- `knowledge_base` — база знаний
- `consultation_interference` — помехи для консультаций

**Служебные таблицы (`sys`):**
- `db_migrations` — история миграций БД
- `sync_state` — состояние синхронизации ETL (последняя дата синхронизации)
- `user_mapping` — маппинг менеджеров между Chatwoot и 1C:ЦЛ
  - `chatwoot_user_id` — ID пользователя в Chatwoot
  - `cl_manager_key` — ключ менеджера в ЦЛ (GUID)

**Логирование (`log`):**
- `webhook_log` — логи webhook'ов (JSONB payload)
- `notification_log` — лог отправленных уведомлений для предотвращения дублирования
  - Колонки: `id` (Integer PK), `notification_type` (Text), `entity_id` (Text), `unique_hash` (Text unique), `created_at` (DateTime)
  - Используется для предотвращения повторной отправки одинаковых уведомлений (переносы, оценки, дозвоны)
  - Хеш генерируется на основе типа уведомления, ID сущности и данных уведомления

---

## Потоки данных

### 1. Создание консультации (Frontend → Middleware → Chatwoot + 1C:ЦЛ)

**Endpoint:** `POST /api/consultations/create`

**Процесс:**

1. **Frontend отправляет запрос** с данными клиента и консультации
2. **Middleware обрабатывает:**
   - Находит или создает клиента в БД (`cons.clients`)
   - Определяет владельца (owner) клиента (если клиент — пользователь, берется родитель)
   - Убеждается, что владелец синхронизирован с 1C:ЦЛ (создает контрагента при необходимости)
   - **Автоматически выбирает менеджера** из БД (`cons.users`) с учетом навыков, загрузки и рабочего времени
   - Создает запись консультации в БД с временным `cons_id = "temp_{uuid}"`
   - **Параллельно** отправляет запросы:
     - В **Chatwoot** через Public API:
       - Создает или находит contact (используя `identifier`, `email`, `phone_number`)
       - Извлекает `source_id` и `pubsub_token` из ответа создания contact
       - Создает conversation через Public API с `source_id` контакта
       - Получает `cons_id` (ID conversation) и `chatwoot_source_id`
     - В **1C:ЦЛ** через OData → создает `Document_ТелефонныйЗвонок` с:
       - **ВАЖНО:** Отправляет в ЦЛ только консультации с типом "Консультация по ведению учёта"
       - `ДатаСоздания` = текущее время (UTC)
       - `Менеджер_Key` = автоматически выбранный менеджер
       - `КатегорияВопроса_Key` и `ВопросНаКонсультацию_Key` = всегда передаются (даже если пустые UUID)
       - `АбонентПредставление` = `Clobus + {company_name или name} + {code_abonent} + ({ИНН})`
       - Получает `Ref_Key` и `Number`
   - Обновляет запись в БД с полученными ID
   - Отправляет информационное сообщение в Chatwoot с номером заявки и информацией об очереди
   - Проверяет ограничение на создание заявок на будущее (максимум `MAX_FUTURE_CONSULTATION_DAYS` дней, по умолчанию 30)
3. **Возвращает** `cons_id`, `cl_ref_key`, `chatwoot_source_id`, `chatwoot_pubsub_token` и другие данные для подключения виджета

**Важно:**
- Middleware — единый источник истины для создания консультации
- Если Chatwoot или ЦЛ недоступны, консультация все равно создается в БД
- Фронт получает `cons_id` и далее работает напрямую с Chatwoot для чата
- Менеджер выбирается автоматически из БД с учетом навыков, загрузки и рабочего времени
- Название клиента в 1C формируется по правилу: `Clobus + {company_name или name} + {code_abonent} + ({ИНН})`
- Используется **Public API Chatwoot** для создания contacts и conversations (не Platform API)

**Схема:**

```
Frontend → POST /api/consultations/create
           ↓
    [Middleware]
           ├─→ Найти/создать клиента в БД
           ├─→ Определить владельца
           ├─→ Синхронизировать владельца с 1C:ЦЛ
           ├─→ Автоматически выбрать менеджера
           ├─→ Создать в БД (cons.cons)
           ├─→ Chatwoot Public API → contact → conversation_id, source_id, pubsub_token
           └─→ 1C:ЦЛ OData → Ref_Key, Number
           ↓
    Обновить БД с ID
           ↓
    Отправить информационное сообщение в Chatwoot
           ↓
    Вернуть cons_id, cl_ref_key, chatwoot_source_id, pubsub_token
```

### 2. Чат (Frontend ↔ Chatwoot)

**Важно:** Чат идет **напрямую** между Frontend и Chatwoot, **не через middleware**.

- Frontend использует Chatwoot UI/WebSocket для обмена сообщениями
- Middleware **НЕ проксирует** сообщения (это было бы медленно)
- Middleware получает события через webhook от Chatwoot

### 3. Синхронизация статусов (Chatwoot → Middleware)

**Webhook:** `POST /webhook/chatwoot`

**События:**
- `conversation.created` — новая консультация создана в Chatwoot
- `conversation.updated` — изменен статус или назначен консультант
- `conversation.resolved` — консультация закрыта
- `message.created` — новое сообщение в консультации

**Процесс:**

1. **Chatwoot отправляет webhook** при изменении статуса/assignee
2. **Middleware обрабатывает:**
   - **ТОЛЬКО обновляет запись в БД** (`cons.cons`) - Middleware это мастер-база
   - При изменении менеджера использует маппинг из `sys.user_mapping` для преобразования `chatwoot_user_id` в `cl_manager_key`
   - Отправляет уведомления менеджерам о переназначении
   - **НЕ отправляет данные в 1C:ЦЛ** через webhook (1C не имеет webhooks)
3. **1C:ЦЛ обновляется:**
   - Через **ETL** (pull из 1C) - синхронизация данных из 1C в Middleware
   - Когда **Middleware инициирует изменение** (создание консультации, перенос, оценка) - отправка данных из Middleware в 1C через OData API

**ВАЖНО:**
- Middleware - это **мастер-база**, все изменения хранятся здесь
- Chatwoot webhooks **только обновляют БД** в Middleware
- 1C:ЦЛ **НЕ имеет webhooks**, поэтому обновляется только через ETL (pull) или когда Middleware инициирует изменение

**Схема:**

```
Chatwoot → Webhook (conversation.updated)
           ↓
    [Middleware]
           ├─→ Обновить БД (cons.cons.status, manager)
           ├─→ Маппинг chatwoot_user_id → cl_manager_key через sys.user_mapping
           └─→ Отправить уведомления менеджерам
               
1C:ЦЛ обновляется:
   - ETL (pull) → Middleware БД
   - Middleware API → 1C:ЦЛ OData (при создании/переносе/оценке)
```

### 4. ETL из 1C:ЦЛ (1C:ЦЛ → Middleware)

**Важно:** У 1C:ЦЛ **нет webhook'ов**, поэтому используется **ETL** (Extract, Transform, Load).

**Скрипты:**
- `pull_cons_cl.py` — загрузка консультаций из `Document_ТелефонныйЗвонок`
  - Фильтрация: на уровне OData запроса (`$filter=Абонент/Parent_Key eq guid'7ccd31ca-887b-11eb-938b-00e04cd03b68'`)
  - Загружаются все консультации из OData (они уже отфильтрованы на стороне сервера)
  - Консультации загружаются даже если клиент еще не создан в БД (позволяет отображать заявки, созданные вне сайта)
  - **Инкрементальная загрузка по полю `ДатаИзменения`** (не `ДатаСоздания`) для эффективного обновления только измененных документов
  - **Два режима работы:** `incremental` (по умолчанию) — загрузка по дате изменения, `open_update` — обновление открытых консультаций по Ref_Key
  - **Не меняет терминальные статусы** (`closed`, `resolved`, `cancelled`) из ЦЛ — они остаются неизменными
  - **Автоматически синхронизирует статусы с Chatwoot** при изменении (открытие/закрытие беседы)
  - **Обновляет custom_attributes в Chatwoot** (номер, даты, переносы, тип консультации)
  - **Ограничивает last_sync текущей датой** (не использует будущие даты из запланированных консультаций)
  - При переназначении менеджера через ETL отправляет уведомления клиентам
- `pull_cons_redate_cl.py` — загрузка переносов из `InformationRegister_РегистрацияПереносаКонсультации`
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых переносов
  - **Предотвращает дублирование уведомлений** через `NotificationLog` (проверка хеша уведомления)
  - **Использует отдельную транзакцию** для сохранения NotificationLog (предотвращает потерю при rollback)
  - Обновляет дату консультации в ЦЛ через OData при обнаружении нового переноса
- `pull_cons_rates_cl.py` — загрузка оценок из `InformationRegister_ОценкаКонсультацийПоЗаявкам`
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых оценок из ЦЛ
  - **Предотвращает дублирование уведомлений** через `NotificationLog`
- `pull_calls_cl.py` — загрузка попыток дозвона из `InformationRegister_РегистрацияДозвона`
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых дозвонов
  - **Предотвращает дублирование уведомлений** через `NotificationLog`
  - **Использует отдельную транзакцию** для сохранения NotificationLog
  - Всегда обновляет заявки на будущее (минимум 7 дней назад)
- `pull_users_cl.py` — загрузка пользователей и навыков
  - Фильтрация: не загружает пользователей с `DeletionMark=true`, `Недействителен=true` или `Служебный=true`
- `pull_queue_closing_cl.py` — загрузка закрытия очереди из `InformationRegister_ЗакрытиеОчередиНаКонсультанта`
  - **Обрабатывает только записи для текущего дня** (поле `Дата`)
  - Учитывает поле `Закрыт`: если `true` — очередь закрыта, если `false` — удаляет запись о закрытии
  - Одна запись в регистре действует ровно на один день
  - При обнаружении закрытия очереди отправляет уведомления клиентам о скором переназначении менеджера
- `pull_all_cons_cl.py` — загрузка **ВСЕХ** консультаций (без фильтра по Parent_Key)
  - Используется для расчета очереди консультантов (консультанты обслуживают не только клиентов нашего сервиса)
  - Использует отдельную сущность `Document_ТелефонныйЗвонок_ALL` для отслеживания синхронизации
  - Консультации создаются с префиксом `cl_all_` для идентификации
- `load_dicts.py` — загрузка справочников
- `sync_users_to_chatwoot.py` — синхронизация пользователей с Chatwoot (создание пользователей и сохранение `chatwoot_user_id`)
  - Фильтрация: синхронизирует только пользователей с `deletion_mark=false` и `invalid=false`

**Логика синхронизации:**
- Используется таблица `sys.sync_state` для хранения последней даты синхронизации
- Инкрементальная загрузка (только новые/измененные записи)
- UPSERT через `ON CONFLICT DO UPDATE` или `ON CONFLICT DO NOTHING`
- **Сохранение sync_state после каждого батча** для устойчивости при прерывании ETL
- При обнаружении новых переносов, оценок и дозвонов автоматически отправляются сообщения в Chatwoot через `send_message()` (не `send_note()`, так как note-сообщения не видны клиенту)
- **Предотвращение дублирования уведомлений** через таблицу `log.notification_log` (хеш уведомления)
- **Использование отдельной транзакции** для сохранения NotificationLog (предотвращает потерю при rollback основной транзакции)
- При обнаружении закрытия очереди менеджера отправляются уведомления клиентам о скором переназначении

**Пример для консультаций (инкрементальная загрузка):**

```python
# pull_cons_cl.py
last_sync = get_last_sync_date("cons")
from_date = last_sync - timedelta(days=7)  # Буфер 7 дней

# ВАЖНО: Используем поле ДатаИзменения для инкрементального обновления
# Это позволяет загружать только измененные документы, что более эффективно
filter_part = (
    f"ДатаИзменения ge datetime'{from_date}' "
    f"and Абонент/Parent_Key eq guid'7ccd31ca-887b-11eb-938b-00e04cd03b68'"
)
url = f"{ODATA_BASEURL}Document_ТелефонныйЗвонок?$filter={quote(filter_part)}"

# Обработка и сохранение в БД
# Загружаются все консультации из OData (они уже отфильтрованы)
for item in fetched_items:
    processed_at = upsert_consultation(item)
    # ВАЖНО: Ограничиваем last_sync текущей датой (не используем будущие даты)
    if processed_at and processed_at <= current_time:
        last_processed_at = max(last_processed_at, processed_at)

# Сохранение даты синхронизации после каждого батча
save_sync_date("cons", last_processed_at)
```

---

## API Endpoints

### Консультации

- `POST /api/consultations/create` — создание консультации (основной endpoint)
  - Принимает `ConsultationWithClient` (данные клиента + консультации)
  - Возвращает `ConsultationResponse` с `cons_id`, `cl_ref_key`, `chatwoot_source_id`, `chatwoot_pubsub_token`, данными для Chatwoot виджета
- `POST /api/consultations/simple` — упрощенное создание (если клиент уже есть)
  - Принимает `ConsultationCreate` (только данные консультации с `client_id`)
- `GET /api/consultations/{cons_id}` — получение консультации
  - Возвращает `ConsultationRead`
- `PUT /api/consultations/{cons_id}` — обновление консультации
  - Поддерживает обновление: `status`, `start_date`, `end_date`, `comment`, `importance`
  - Синхронизирует с 1C:ЦЛ если есть `cl_ref_key`
- `POST /api/consultations/{cons_id}/cancel` — отмена консультации пользователем
  - Доступна только в течение 1.5 часа с момента создания
  - Удаляет документ в 1C:ЦЛ (освобождает лимит)
  - Закрывает беседу в Chatwoot со статусом "resolved" и флагом "закрыто без консультации"
- `GET /api/consultations/{cons_id}/calls` — список попыток дозвона
  - Возвращает `List[CallRead]` отсортированные по дате (новые первыми)
  - Поддерживает пагинацию: `skip`, `limit` (по умолчанию 100)
- `GET /api/consultations/{cons_id}/redates` — список переносов
  - Возвращает `List[ConsultationRedateRead]` отсортированные по дате (новые первыми)
- `POST /api/consultations/{cons_id}/redates` — создание переноса
  - Принимает `ConsultationRedateCreate` (new_date, manager_key, comment)
  - Автоматически отправляет в 1C:ЦЛ и Chatwoot (note-сообщение)
  - Валидирует `manager_key` (должен быть валидный GUID, не "FRONT")
- `GET /api/consultations/{cons_id}/ratings` — получение оценок
  - Возвращает `ConsultationRatingResponse` с средней оценкой и списком ответов
- `POST /api/consultations/{cons_id}/ratings` — отправка оценок
  - Принимает `ConsultationRatingRequest` (массив ответов)
  - Автоматически отправляет в 1C:ЦЛ с полем `ДатаОценки` и Chatwoot (note-сообщение)
  - Валидирует `manager_key` перед отправкой в 1C
- `GET /api/consultations/clients/{client_id}/consultations` — список консультаций клиента
  - Поддерживает пагинацию: `skip`, `limit` (по умолчанию 100, максимум 1000)
  - Возвращает `ConsultationListResponse` с `total`

### Клиенты

- `POST /api/clients` — создание/обновление клиента
  - Принимает `ClientCreate`
  - Ищет существующего по `client_id`, `client_id_hash` или `email+phone+inn`
  - Поддерживает владельцев (`is_parent=true`) и пользователей (`parent_id`)
  - Автоматически синхронизирует контакт в Chatwoot
- `GET /api/clients/{client_id}` — получение клиента
  - Возвращает `ClientRead`
  - Для пользователей (`is_parent=false`) возвращает данные с полями `company_name`, `org_inn`, `region`, `city` из владельца
- `GET /api/clients/by-hash/{client_hash}` — получение клиента по хешу
  - Возвращает `ClientRead`
- `GET /api/clients/by-subscriber/{code_abonent}` — поиск владельца по code_abonent
  - Ищет клиента-владельца (`is_parent=true`) по полю `code_abonent`
  - Если найден только пользователь, возвращает данные его владельца

### Менеджеры

- `GET /api/managers/load` — получить загрузку всех менеджеров
- `GET /api/managers/{manager_key}/load` — получить загрузку конкретного менеджера
- `GET /api/managers/{manager_key}/wait-time` — рассчитать примерное время ожидания
- `GET /api/managers/available` — получить список доступных менеджеров
  - Поддерживает фильтры: `po_section_key`, `po_type_key`, `category_key`
- `GET /api/managers/consultations/{cons_id}/queue-info` — информация об очереди для консультации

### Справочники

Все справочники кэшируются в памяти на 30 минут (TTL) для повышения производительности.

- `GET /api/dicts/po-types` — типы ПО
  - Возвращает `List[POTypeReadSimple]` (упрощенная схема для фронтенда: только `ref_key` и `description`)
- `GET /api/dicts/po-sections` — разделы ПО
  - Query параметр: `owner_key` (опционально) — фильтр по типу ПО
  - Возвращает `List[POSectionRead]`
- `GET /api/dicts/online-question/categories` — категории вопросов
  - Query параметр: `language` (опционально, ru/uz)
  - Возвращает `List[OnlineQuestionCategoryRead]`
- `GET /api/dicts/online-questions` — вопросы
  - Query параметры: `language` (опционально), `category_key` (опционально)
  - Возвращает `List[OnlineQuestionRead]`
- `GET /api/dicts/knowledge-base` — база знаний
  - Query параметры: `po_type_key` (опционально), `po_section_key` (опционально)
  - Возвращает `List[KnowledgeBaseEntry]`
- `GET /api/dicts/interference` — помехи для консультаций
  - Возвращает `List[ConsultationInterferenceRead]`

### Health Check

- `GET /api/health` — базовая проверка здоровья сервиса
- `GET /api/health/db` — проверка подключения к БД

### Webhooks

- `POST /webhook/chatwoot` — webhook от Chatwoot
  - Проверка подписи через `x-chatwoot-signature` (HMAC SHA256)
  - События: `conversation.created`, `conversation.updated`, `conversation.resolved`, `message.created`
  - Сохраняет все webhook'и в `log.webhook_log`
- `POST /webhook/1c_cl` — webhook от 1C:ЦЛ (заглушка, т.к. у ЦЛ нет webhook'ов)
  - Обрабатывает события: `consultation.created`, `consultation.updated`, `consultation.closed`

---

## Интеграции

### Chatwoot

**Назначение:** Система чата и управления обращениями.

**Использование:**
- **Создание контактов** — контакт создается через Public API перед созданием conversation
- Создание conversations для консультаций с custom_attributes (человеко-читаемые данные)
- Чат между клиентом и консультантом (напрямую, не через middleware)
- Управление статусами и назначение консультантов
- Отправка note-сообщений для отображения системных событий (переносы, оценки, дозвоны)
- Синхронизация пользователей (создание менеджеров в Chatwoot для маппинга с ЦЛ)

**API клиент:** `ChatwootClient` (`FastAPI/services/chatwoot_client.py`)

**API типы:**
- **Public API** — используется для создания contacts и conversations (возвращает `pubsub_token`)
- **Platform API** — используется для обновления conversations, добавления labels, назначения менеджеров

**Важно:** 
- Токен передается в заголовке `api_access_token` (не `Authorization: Bearer`). Это требование Chatwoot Platform API.
- Public API использует `inbox_identifier` для создания contacts и conversations
- `pubsub_token` возвращается только в ответе создания contact через Public API
- `source_id` создается автоматически Chatwoot при создании contact через Public API

**Методы:**
- `create_contact_via_public_api()` — создание контакта через Public API (возвращает `pubsub_token`)
- `get_contact_via_public_api()` — получение контакта через Public API (для извлечения `pubsub_token`)
- `create_conversation_via_public_api()` — создание conversation через Public API
- `find_contact_by_identifier()` — поиск контакта по identifier (UUID `client_id`)
- `find_contact_by_email()` — поиск контакта по email
- `find_contact_by_phone()` — поиск контакта по телефону
- `create_contact()` — создание контакта через Platform API (устаревший метод)
- `create_conversation()` — создание conversation через Platform API (устаревший метод)
- `update_conversation()` — обновление статуса/assignee/custom_attributes
- `send_note()` — отправка служебного note-сообщения в conversation (используется только для внутренних сообщений, не видимых клиенту)
- `send_message()` — отправка сообщения клиенту (используется вместо `send_note()` для уведомлений клиентов)
- `create_user()` — создание пользователя в Chatwoot (для синхронизации менеджеров)
- `find_user_by_custom_attribute()` — поиск пользователя по кастомному атрибуту
- `get_conversation()` — получение conversation
- `add_conversation_labels()` — добавление labels к conversation
- `send_message()` — отправка сообщения (используется редко)

**Идентификация контактов:**
- **Обязательно:** При создании conversation через Public API передается `source_id` контакта
- `source_id` — локальный ID клиента в рамках одного Inbox (создается автоматически Chatwoot)
- `identifier` — глобальный внешний ID (UUID `client_id` из нашей системы)
- `email` / `phone_number` — глобальные идентификаторы контакта (обеспечивают склейку контактов)

**Custom Attributes:**
- **Contact custom_attributes** — атрибуты контакта (клиента): `code_abonent`, `inn_pinfl`, `client_type`, `region`, `country`
- **Conversation custom_attributes** — атрибуты беседы (тикета): `code_abonent`, `topic_name`, `category_name`, `question_name`, `number_con`, `date_con`, и др.

**Уведомления клиентам:**
- **ВАЖНО:** Используется `send_message()` вместо `send_note()`, так как note-сообщения не видны клиенту в Chatwoot
- Автоматически отправляются при создании переносов через API (`POST /api/consultations/{cons_id}/redates`)
- Автоматически отправляются при отправке оценок через API (`POST /api/consultations/{cons_id}/ratings`)
- Автоматически отправляются ETL при обнаружении новых переносов, оценок и дозвонов из ЦЛ
- Автоматически отправляются при переназначении менеджера через webhook от Chatwoot
- Автоматически отправляются при переназначении менеджера через ETL из ЦЛ
- Автоматически отправляются при закрытии очереди менеджера (сообщение о скором переназначении)
- Автоматически отправляются при изменении очереди (информация о позиции и времени ожидания)
- Содержат человеко-читаемую информацию о событии (даты, оценки, комментарии, позиция в очереди)

**Webhook события:**
- `conversation.created`
- `conversation.updated`
- `conversation.resolved`
- `message.created`

**Настройки (.env):**
```env
CHATWOOT_API_URL=https://suppdev.clobus.uz
CHATWOOT_API_TOKEN=<platform_api_token>
CHATWOOT_ACCOUNT_ID=<account_id>
CHATWOOT_INBOX_ID=5  # ID inbox для Platform API
CHATWOOT_INBOX_IDENTIFIER=<inbox_identifier>  # Identifier inbox для Public API
```

**Важно:**
- Используем **Public API** для создания contacts и conversations (возвращает `pubsub_token`)
- Используем **Platform API** для обновления conversations, добавления labels, назначения менеджеров
- Токен (`CHATWOOT_API_TOKEN`) передается в заголовке `api_access_token` (не `Authorization`)
- Токен должен быть Platform API ключом (не bot token — у него ограниченный доступ)
- `pubsub_token` возвращается только в ответе создания contact через Public API
- `source_id` создается автоматически Chatwoot при создании contact через Public API

### 1C:ЦЛ

**Назначение:** Учетная система, источник данных о клиентах, консультациях, пользователях.

**Использование:**
- Создание консультаций через OData
- Обновление статусов консультаций
- Загрузка данных через ETL (справочники, консультации, переносы, оценки, попытки дозвона)

**API клиент:** `OneCClient` (`FastAPI/services/onec_client.py`)

**Методы:**
- `create_consultation_odata()` — создание `Document_ТелефонныйЗвонок`
- `update_consultation_odata()` — обновление через PATCH
- `delete_consultation_odata()` — удаление документа (для отмены консультации)
- `get_consultation_odata()` — получение консультации
- `create_rating_odata()` — создание записи в `InformationRegister_ОценкаКонсультацийПоЗаявкам`
- `create_redate_odata()` — создание записи в `InformationRegister_РегистрацияПереносаКонсультации`
- `find_client_by_inn()` — поиск клиента по ИНН в `Catalog_Контрагенты`
- `create_client_odata()` — создание клиента в `Catalog_Контрагенты`
  - Автоматически добавляет `Parent_Key = "7ccd31ca-887b-11eb-938b-00e04cd03b68"`
  - Определяет `ЮридическоеФизическоеЛицо` по ИНН: 9 знаков → "Юридическое лицо", 14 знаков → "Физическое лицо"
- `_odata_request()` — базовый метод с retry и backoff (обработка 429, 5xx ошибок)

**OData сущности:**
- `Document_ТелефонныйЗвонок` — консультации
- `Catalog_Пользователи` — пользователи
- `InformationRegister_РегистрацияПереносаКонсультации` — переносы
- `InformationRegister_ОценкаКонсультацийПоЗаявкам` — оценки
- `InformationRegister_РегистрацияДозвона` — попытки дозвона
- `Catalog_ВидыПОДляКонсультаций` — справочники
- И другие справочники

**Настройки (.env):**
```env
ODATA_BASEURL_CL=http://your-1c-odata-url
ODATA_USER=your-user
ODATA_PASSWORD=your-password
```

**Важно:** У 1C:ЦЛ нет webhook'ов, поэтому используется ETL для синхронизации.

**Обработка ошибок:**
- При превышении лимита консультаций (3 документа в день) возвращается `ConsultationLimitExceeded` (HTTP 429)
- Retry с экспоненциальной задержкой для 429, 5xx ошибок

---

## База данных

### Схема `cons`

**Таблицы:**

1. **`clients`** — клиенты
   - PK: `client_id` (UUID)
   - Связь с ЦЛ: `cl_ref_key` (Ref_Key из ЦЛ)
   - Поля: `name`, `contact_name`, `company_name` (название компании клиента для формирования имени в 1C)
   - `is_parent` (Boolean) — `true` для владельца, `false` для пользователя
   - `parent_id` (UUID FK) — ссылка на `client_id` владельца (для пользователей)
   - `company_name` имеет приоритет над `name` при формировании названия в 1C
   - `source_id` — source_id из Chatwoot для идентификации контакта
   - `chatwoot_pubsub_token` — pubsub_token из Chatwoot для WebSocket подключения виджета

2. **`cons`** — консультации
   - PK: `cons_id` (Text, ID из Chatwoot)
   - Связь с ЦЛ: `cl_ref_key` (Ref_Key из ЦЛ), `number` (номер из ЦЛ)
   - Связь с клиентом: `client_id` (FK к `clients.client_id`)
   - Статусы: `new`, `open`, `pending`, `resolved`, `closed`, `cancelled`
   - `chatwoot_source_id` — source_id из Chatwoot (для подключения виджета)
   - `consultation_type` — вид обращения: "Техническая поддержка" или "Консультация по ведению учёта"

3. **`users`** — пользователи (консультанты)
   - PK: `account_id` (UUID)
   - Связь с ЦЛ: `cl_ref_key` (Ref_Key из ЦЛ)
   - Связь с Chatwoot: `chatwoot_user_id` (ID из Chatwoot)

4. **`users_skill`** — навыки пользователей
   - PK: `(user_key, category_key)` (composite)

5. **`cons_redate`** — переносы консультаций
   - PK: `id` (Integer)
   - Unique: `(cons_key, clients_key, manager_key, period)`

6. **`cons_rating_answers`** — оценки консультаций
   - PK: `id` (Integer)
   - Unique: `(cons_key, manager_key, question_number)`
   - `rating_date` — дата оценки (ДатаОценки из 1C)

7. **`calls`** — попытки дозвона
   - PK: `(period, cons_key, manager)` (composite)

8. **`q_and_a`** — вопросы и ответы
   - PK: `id` (Integer)

9. **`queue_closing`** — регистр закрытия очереди для консультантов
   - PK: `(period, manager_key)` (composite)
   - `period` — дата закрытия очереди (Date, начало дня)
   - `manager_key` — ключ менеджера из ЦЛ (Text)
   - Одна запись действует ровно на один день
   - Если для менеджера есть запись с `period` = текущий день, его очередь закрыта

### Схема `dict`

**Таблицы справочников:**
- `po_types` — типы ПО
- `po_sections` — разделы ПО
- `online_question_cat` — категории вопросов
- `online_question` — вопросы
- `knowledge_base` — база знаний
- `consultation_interference` — помехи для консультаций

### Схема `sys`

**Служебные таблицы:**
- `db_migrations` — история миграций БД
  - Колонки: `id` (Integer PK), `version` (Text unique), `applied_at` (DateTime)
  - Используется для отслеживания примененных миграций Alembic
- `sync_state` — состояние синхронизации ETL
  - Колонки: `entity_name` (Text PK), `last_synced_at` (DateTime)
- `user_mapping` — маппинг менеджеров между Chatwoot и 1C:ЦЛ
  - Колонки: `id` (Integer PK), `chatwoot_user_id` (Integer unique), `cl_manager_key` (Text unique), `created_at`, `updated_at`

### Схема `log`

**Таблицы логирования:**
- `webhook_log` — логи webhook'ов
  - Колонки: `id` (Integer PK), `source` (Text), `payload` (JSONB), `created_at` (DateTime)
  - Сохраняет все входящие webhook'и для отладки

---

## ETL процессы

### Загрузка справочников

**Скрипт:** `load_dicts.py`

**Загружает:**
- Типы ПО (`Catalog_ВидыПОДляКонсультаций`)
- Разделы ПО (`Catalog_РазделыПОДляКонсультаций`)
- Категории вопросов (`Catalog_КатегорииВопросов`)
- Вопросы (`Catalog_ВопросыНаКонсультацию`)
- База знаний (`Catalog_БазаЗнанийДляКонсультаций`)
- Помехи (`Catalog_ПомехиДляКонсультаций`)

**Особенности:**
- Полная загрузка (не инкрементальная)
- UPSERT через `ON CONFLICT DO UPDATE`

### Загрузка оперативных данных

**Скрипты:**
- `pull_cons_cl.py` — консультации
  - Фильтрация: на уровне OData запроса (`$filter=Абонент/Parent_Key eq guid'7ccd31ca-887b-11eb-938b-00e04cd03b68'`)
  - Загружаются все консультации из OData (они уже отфильтрованы на стороне сервера)
  - Консультации загружаются даже если клиент еще не создан в БД (позволяет отображать заявки, созданные вне сайта)
  - **Инкрементальная загрузка по полю `ДатаИзменения`** (не `ДатаСоздания`) для эффективного обновления только измененных документов
  - **Два режима работы:** `incremental` (по умолчанию) — загрузка по дате изменения, `open_update` — обновление открытых консультаций по Ref_Key
  - **Не меняет терминальные статусы** (`closed`, `resolved`, `cancelled`) из ЦЛ — они остаются неизменными
  - **Автоматически синхронизирует статусы с Chatwoot** при изменении (открытие/закрытие беседы)
  - **Обновляет custom_attributes в Chatwoot** (номер, даты, переносы, тип консультации)
  - **Ограничивает last_sync текущей датой** (не использует будущие даты из запланированных консультаций)
  - При переназначении менеджера через ETL отправляет уведомления клиентам
- `pull_cons_redate_cl.py` — переносы
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых переносов
  - **Предотвращает дублирование уведомлений** через `NotificationLog` (проверка хеша уведомления)
  - **Использует отдельную транзакцию** для сохранения NotificationLog (предотвращает потерю при rollback)
  - Обновляет дату консультации в ЦЛ через OData при обнаружении нового переноса
- `pull_cons_rates_cl.py` — оценки
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых оценок из ЦЛ
  - **Предотвращает дублирование уведомлений** через `NotificationLog`
- `pull_calls_cl.py` — попытки дозвона
  - Автоматически отправляет сообщения клиентам через `send_message()` при обнаружении новых дозвонов
  - **Предотвращает дублирование уведомлений** через `NotificationLog`
  - **Использует отдельную транзакцию** для сохранения NotificationLog
  - Всегда обновляет заявки на будущее (минимум 7 дней назад)
- `pull_users_cl.py` — пользователи и навыки
  - Фильтрация: не загружает пользователей с `DeletionMark=true`, `Недействителен=true` или `Служебный=true`
- `pull_queue_closing_cl.py` — закрытие очереди для консультантов
  - Загружает из `InformationRegister_ЗакрытиеОчередиНаКонсультанта`
  - **Обрабатывает только записи для текущего дня** (поле `Дата`)
  - Учитывает поле `Закрыт`: если `true` — очередь закрыта, если `false` — удаляет запись о закрытии
  - Одна запись в регистре действует ровно на один день
  - При обнаружении закрытия очереди отправляет уведомления клиентам о скором переназначении менеджера
- `pull_all_cons_cl.py` — загрузка **ВСЕХ** консультаций (без фильтра по Parent_Key)
  - Используется для расчета очереди консультантов (консультанты обслуживают не только клиентов нашего сервиса)
  - Использует отдельную сущность `Document_ТелефонныйЗвонок_ALL` для отслеживания синхронизации
  - Консультации создаются с префиксом `cl_all_` для идентификации
- `sync_users_to_chatwoot.py` — синхронизация пользователей с Chatwoot (создание пользователей и сохранение `chatwoot_user_id`)
  - Фильтрация: синхронизирует только пользователей с `deletion_mark=false` и `invalid=false`

**Особенности:**
- Инкрементальная загрузка через `sys.sync_state`
- Буфер времени (7 дней для консультаций, 6 часов для переносов, 12 часов для дозвонов, 1 день для закрытия очереди)
- **Сохранение sync_state после каждого батча** для устойчивости при прерывании ETL
- UPSERT через `ON CONFLICT DO NOTHING` или `ON CONFLICT DO UPDATE`
- Retry и backoff для OData запросов (обработка 429, 5xx ошибок)
- Кэширование справочников в памяти на 30 минут (автоматическая инвалидация по TTL)
- Автоматическая отправка сообщений в Chatwoot через `send_message()` при обнаружении новых переносов, оценок и дозвонов (note-сообщения не видны клиенту)
- **Предотвращение дублирования уведомлений** через `log.notification_log` (проверка уникального хеша)
- **Использование отдельной транзакции** для сохранения NotificationLog (предотвращает потерю при rollback)
- Автоматическая отправка уведомлений при закрытии очереди менеджера и переназначении менеджера
- **Не меняет терминальные статусы** (`closed`, `resolved`, `cancelled`) из ЦЛ
- **Автоматическая синхронизация статусов с Chatwoot** при изменении статуса консультации

**Автоматический запуск через APScheduler:**
- Настроен APScheduler для автоматического запуска ETL процессов внутри приложения
- Расписание:
  - `pull_cons_cl.py` — каждые 15 минут (режим `incremental` по умолчанию)
  - `pull_cons_redate_cl.py` — каждые 15 минут
  - `pull_cons_rates_cl.py` — каждые 15 минут
  - `pull_calls_cl.py` — каждые 15 минут
  - `pull_queue_closing_cl.py` — каждые 15 минут
  - `pull_all_cons_cl.py` — каждые 15 минут (для расчета очереди)
  - `pull_users_cl.py` — ежедневно в 3:00 UTC
- Логирование выполнения в логах приложения
- Настройка расписания: `FastAPI/scheduler.py`

**Запуск:**
```bash
docker-compose exec -T cons_api python -m FastAPI.catalog_scripts.pull_cons_cl
docker-compose exec -T cons_api python -m FastAPI.catalog_scripts.pull_cons_redate_cl
docker-compose exec -T cons_api python -m FastAPI.catalog_scripts.pull_cons_rates_cl
docker-compose exec -T cons_api python -m FastAPI.catalog_scripts.pull_calls_cl
docker-compose exec -T cons_api python -m FastAPI.catalog_scripts.pull_users_cl
```

---

## Обработка ошибок

### При создании консультации

- Если Chatwoot недоступен → консультация создается в БД с `temp_` ID или UUID, можно повторить позже
- Если 1C:ЦЛ недоступен → консультация создается в БД и Chatwoot, `cl_ref_key` будет пустым, можно синхронизировать через ETL
- При превышении лимита консультаций в 1C:ЦЛ (3 документа в день) возвращается HTTP 429 с понятным сообщением

### При синхронизации через webhook

- Ошибки синхронизации с ЦЛ логируются, но не прерывают обработку webhook
- Все webhook'и сохраняются в `log.webhook_log` для отладки

### При ETL

- Ошибки логируются, но процесс продолжается
- Используется retry с экспоненциальной задержкой для OData запросов

---

## Безопасность

### Webhook'и

- Chatwoot: проверка подписи через `x-chatwoot-signature` (HMAC SHA256)
- 1C:ЦЛ: проверка не реализована (т.к. webhook'ов от ЦЛ нет)

### API

- **Статическая авторизация для фронтенда:** 
  - Заголовок `X-Front-Secret` со значением из `FRONT_SECRET` в `.env`
  - Или заголовок `Authorization: Bearer <token>` со значением из `FRONT_BEARER_TOKEN` (или `FRONT_SECRET` если `FRONT_BEARER_TOKEN` не задан)
  - При пустом `FRONT_SECRET` и `FRONT_BEARER_TOKEN` проверка отключается (для dev-режима)
  - Реализовано в `FastAPI/dependencies/security.py` через `verify_front_secret()`
  - Использует `secrets.compare_digest()` для безопасного сравнения токенов
- **Авторизация через OpenID:** Структура готова в `config.py` (`OPENID_ISSUER`, `OPENID_CLIENT_ID`, `OPENID_CLIENT_SECRET`), валидация в разработке (ожидает настройки 1C:Фреш)
- **CORS:** Настроен для всех источников (`allow_origins=["*"]`) в `main.py` (в продакшене нужно ограничить)

---

## Развертывание

### Docker Compose

```yaml
services:
  cons_api:
    build:
      context: .
      dockerfile: dockerfile
    container_name: cons_api
    env_file: .env
    ports:
      - "7070:7070"
    restart: unless-stopped
    environment:
      - LOAD_DICTS_ON_START=${LOAD_DICTS_ON_START:-true}
      - SYNC_USERS_ON_START=${SYNC_USERS_ON_START:-false}
```

### Автоматическая инициализация

При запуске контейнера автоматически выполняются следующие шаги (через `entrypoint.sh`):

1. **Ожидание доступности БД** — скрипт ждет, пока PostgreSQL станет доступен (до 30 попыток)
2. **Применение миграций Alembic** — автоматически применяются все миграции БД (с проверкой существования колонок)
3. **Инициализация БД** — создание схем, таблиц и начальных данных (`init_db.py`)
4. **Загрузка справочников** (опционально) — загрузка данных из 1C:CL (`LOAD_DICTS_ON_START=true`)
5. **Синхронизация пользователей** (опционально) — синхронизация с Chatwoot (`SYNC_USERS_ON_START=false`)
6. **Запуск API сервера** — запуск FastAPI приложения

**Переменные окружения для управления автоматизацией:**
- `LOAD_DICTS_ON_START` (по умолчанию: `true`) — загружать справочники при старте
- `SYNC_USERS_ON_START` (по умолчанию: `false`) — синхронизировать пользователей с Chatwoot при старте
- `MAX_FUTURE_CONSULTATION_DAYS` (по умолчанию: `30`) — максимальное количество дней вперед для создания консультации

**APScheduler для автоматического запуска ETL:**
- APScheduler интегрирован в FastAPI приложение и запускается автоматически
- Расписание ETL процессов настраивается в файле `FastAPI/scheduler.py`
- Логи выполнения сохраняются в логах приложения

Подробнее см. `DOCKER_SETUP.md` и `TROUBLESHOOTING.md`.

### Переменные окружения (.env)

См. разделы "Интеграции" выше для настроек Chatwoot и 1C:ЦЛ.

Также:
```env
# Database
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=cons_backend
DB_USER=postgres
DB_PASS=qwerty123

# Application
APP_HOST=0.0.0.0
APP_PORT=7070
ENV=dev
DEBUG=False

# Frontend Authorization
FRONT_SECRET=your-static-secret-for-frontend
FRONT_BEARER_TOKEN=your-bearer-token-for-frontend

# Consultation Limits
MAX_FUTURE_CONSULTATION_DAYS=30  # Максимальное количество дней вперед для создания консультации
```

---

## Логирование

- Все операции логируются через стандартный Python `logging`
- Webhook'и сохраняются в `log.webhook_log`
- Ошибки синхронизации логируются с `exc_info=True`

---

## Автоматический выбор менеджера

Система автоматически выбирает менеджера при создании консультации на основе:

1. **Навыков менеджера** — соответствие категории вопроса (`online_question_cat`)
2. **Загрузки менеджера** — количество активных консультаций относительно лимита (`con_limit`)
3. **Рабочего времени** — текущее время должно быть в диапазоне `start_hour` - `end_hour`
4. **Языка** — соответствие языку консультации (`ru`/`uz`)
5. **Закрытия очереди** — менеджер исключается, если его очередь закрыта на текущий день (проверка через `cons.queue_closing`)

**Алгоритм распределения:**
- Выбирает из менеджеров с примерно одинаковой загрузкой (разница приоритета < 0.1)
- Если есть несколько кандидатов с одинаковой загрузкой, выбирает случайно для равномерного распределения
- Использует статистику среднего времени закрытия заявок из БД (за последние 30 дней)

**Расчет времени ожидания:**
- Использует статистику среднего времени закрытия заявок менеджера
- Если статистика < 15 минут, показывает диапазон: от (статистика × очередь) до (15 минут × очередь)
- Если статистика ≥ 15 минут, показывает одно значение
- Пример: очередь 4 заявки, статистика 5 минут → "от 20 до 60 минут"

**Компоненты:**

### ManagerSelector (`FastAPI/services/manager_selector.py`)

Основной сервис для выбора менеджеров.

**Основные методы:**
- `get_available_managers()` — получить список доступных менеджеров
- `select_manager_for_consultation()` — выбрать менеджера для консультации
- `get_manager_queue_count()` — получить количество консультаций в очереди
- `get_manager_current_load()` — получить текущую загрузку менеджера
- `calculate_wait_time()` — рассчитать примерное время ожидания
- `get_all_managers_load()` — получить загрузку всех менеджеров

**Логика выбора:**
1. Фильтрация по лимитам (`con_limit > 0`)
2. Фильтрация по времени работы (`start_hour`, `end_hour`)
3. Фильтрация по навыкам (`users_skill.category_key`)
4. Выбор менеджера с наименьшей загрузкой (очередью)

### ManagerNotifications (`FastAPI/services/manager_notifications.py`)

Сервис для отправки уведомлений.

**Основные функции:**
- `send_manager_reassignment_notification()` — уведомление о переназначении менеджера
- `send_queue_update_notification()` — уведомление об изменении очереди

**Интеграция при создании консультации:**

1. Автоматически выбирается менеджер на основе:
   - Категории вопроса (`online_question_cat`)
   - Текущей загрузки менеджеров
   - Лимитов и времени работы

2. Менеджер назначается в:
   - БД (`cons.cons.manager`)
   - Chatwoot (`assignee_id`)
   - ЦЛ (`Менеджер_Key`)

3. Клиенту отправляется сообщение с информацией:
   - О принятии заявки
   - О позиции в очереди
   - О примерном времени ожидания

**Синхронизация с Chatwoot:**

### Маппинг менеджеров

Используется таблица `sys.user_mapping`:
- `chatwoot_user_id` — ID пользователя в Chatwoot
- `cl_manager_key` — ключ менеджера в ЦЛ (GUID)

При назначении менеджера:
1. Ищем `chatwoot_user_id` по `cl_manager_key` в `user_mapping`
2. Если не найдено, ищем в `cons.users` по `cl_ref_key`
3. Используем `chatwoot_user_id` для назначения в Chatwoot

### Переназначение менеджера

**Из ЦЛ (через ETL):**
- `pull_cons_cl.py` обнаруживает изменение `Менеджер_Key`
- Обновляет `cons.cons.manager` в БД
- Отправляет уведомление в Chatwoot

**Из Chatwoot (через webhook):**
- `webhooks.py` обрабатывает событие `conversation.updated`
- Обновляет `cons.cons.manager` в БД
- Синхронизирует с ЦЛ через OData
- Отправляет уведомление клиенту

**Расчет очереди:**

Очередь считается как количество консультаций со статусом:
- `pending` или `open`
- `denied = False`
- `manager` = ключ менеджера

**Настройка:**

### Лимиты менеджеров

Устанавливаются в таблице `cons.users`:
- `con_limit` — максимальное количество консультаций в очереди

### Время работы

Устанавливается в таблице `cons.users`:
- `start_hour` — время начала работы (TIME)
- `end_hour` — время окончания работы (TIME)

Если не установлено, менеджер считается работающим всегда.

### Навыки менеджеров

Хранятся в таблице `cons.users_skill`:
- `user_key` — ключ менеджера (cl_ref_key)
- `category_key` — ключ категории вопроса (КатегорияВопроса_Key)

Если у менеджера нет навыков, он считается универсальным (знает все разделы).

**API Endpoints:**

- `GET /api/managers/load` — загрузка всех менеджеров
- `GET /api/managers/{manager_key}/load` — загрузка конкретного менеджера
- `GET /api/managers/{manager_key}/wait-time` — время ожидания для менеджера
- `GET /api/managers/available` — список доступных менеджеров
- `GET /api/managers/consultations/{cons_id}/queue-info` — информация об очереди для консультации

Реализовано в `FastAPI/services/manager_selector.py` через класс `ManagerSelector`.

---

## Владельцы и пользователи абонента

- **Владелец абонента** (`is_parent=true`, `parent_id=null`): 
  - Главный аккаунт организации, имеет уникальный код абонента (`code_abonent`)
  - Имеет уникальный ИНН (`org_inn`) среди записей без `parent_id`
  - Создаётся в ЦЛ и является основным субъектом синхронизации
  - Имеет собственные `company_name`, `org_inn`, `region`, `city`
  
- **Пользователь абонента** (`is_parent=false`, `parent_id` указывает на `client_id` владельца):
  - Дочерний аккаунт, ссылается на владельца через `parent_id` (FK к `cons.clients.client_id`)
  - Наследует `org_inn` и `code_abonent` от владельца (нельзя изменить)
  - В ЦЛ не создаётся, использует реквизиты владельца
  - При получении данных пользователя поля `company_name`, `org_inn`, `region`, `city` берутся из владельца

---

## Будущие улучшения

1. **Авторизация:** Реализовать валидацию OpenID токенов (ожидает настройки 1C:Фреш)
2. **Retry логика:** Добавить retry для создания консультаций в Chatwoot/ЦЛ
3. ✅ **Маппинг пользователей:** Реализовано через таблицу `sys.user_mapping` и обновлен вебхук обработчик
4. **Мониторинг:** Добавить метрики и алерты
5. **Тестирование:** Добавить unit и integration тесты
6. ✅ **Отображение дозвонов в Chatwoot:** Реализовано - ETL автоматически отправляет сообщения при обнаружении новых дозвонов
7. ✅ **API для просмотра дозвонов:** Реализовано - Endpoint `GET /api/consultations/{cons_id}/calls` с пагинацией
8. ✅ **Отображение переносов/оценок в Chatwoot при ETL:** Реализовано - ETL автоматически отправляет сообщения через `send_message()`
9. ✅ **Обработка `ДатаОценки`:** Реализовано - добавлено поле `rating_date`, ETL обрабатывает `ДатаОценки` из OData
10. ✅ **Валидация `manager_key`:** Реализовано - добавлена валидация формата GUID, улучшено логирование ошибок
11. ✅ **Автоматический выбор менеджера:** Реализовано - система автоматически выбирает менеджера при создании консультации с учетом навыков, загрузки, рабочего времени, языка и закрытия очереди
12. ✅ **Отмена консультации:** Реализовано - endpoint `POST /api/consultations/{cons_id}/cancel` с проверкой времени
13. ✅ **Public API Chatwoot:** Реализовано - использование Public API для создания contacts и conversations (возвращает `pubsub_token`)
14. ✅ **Регистр закрытия очереди:** Реализовано - модель `QueueClosing`, ETL скрипт `pull_queue_closing_cl.py`, проверка в `ManagerSelector`, уведомления клиентам
15. ✅ **Улучшенный выбор менеджера:** Реализовано - статистика времени закрытия заявок, распределение между менеджерами, диапазон времени ожидания
16. ✅ **Уведомления клиентам:** Реализовано - уведомления при переназначении менеджера, изменении очереди, закрытии очереди (используется `send_message()` вместо `send_note()`)
17. ✅ **Поле consultation_type:** Реализовано - добавлено поле `consultation_type`, фильтрация отправки в ЦЛ только консультаций типа "Консультация по ведению учёта"
18. ✅ **Создание клиентов с Parent_Key:** Реализовано - автоматическое добавление `Parent_Key` и определение `ЮридическоеФизическоеЛицо` по ИНН
19. ✅ **Фильтрация пользователей в ETL:** Реализовано - фильтрация по `DeletionMark`, `Недействителен`, `Служебный`
20. ✅ **Планировщик задач для ETL:** Реализовано - автоматический запуск ETL процессов через APScheduler
21. ✅ **Ограничения на создание заявок:** Реализовано - ограничение на создание заявок на будущее (максимум `MAX_FUTURE_CONSULTATION_DAYS` дней)
22. ✅ **Упрощенная схема POTypeReadSimple:** Реализовано - упрощенная схема для фронтенда без поля `details`

---

## Контакты и поддержка

При возникновении вопросов или необходимости внесения изменений, обращайтесь к разработчику с указанием раздела документации.
