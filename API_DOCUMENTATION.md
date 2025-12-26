# API Документация для Frontend

## Базовый URL
```
http://localhost:7070/api  (development)
https://your-domain.com/api  (production)
```

## Аутентификация

Все запросы (кроме `/health`, `/health/*` и `/webhook/*`) требуют аутентификации через заголовок:

```
X-Front-Secret: <FRONT_SECRET>
```

Или через Bearer токен:

```
Authorization: Bearer <FRONT_BEARER_TOKEN>
```

## Rate Limiting

- Общие endpoints: **100 запросов/минуту**
- Создание консультаций: **10 запросов/минуту**

При превышении лимита возвращается `429 Too Many Requests` с заголовком `Retry-After`.

---

## Endpoints

### Health Check

#### GET `/health`
Проверка работоспособности сервиса.

**Ответ:**
```json
{
  "status": "ok"
}
```

#### GET `/health/db`
Проверка подключения к БД.

**Ответ:**
```json
{
  "status": "ok",
  "database": "connected"
}
```

**Ошибки:**
```json
{
  "status": "error",
  "database": "disconnected",
  "error": "описание ошибки"
}
```

#### GET `/health/scheduler`
Проверка статуса планировщика задач.

**Ответ:**
```json
{
  "scheduler_running": true,
  "jobs_count": 2,
  "jobs": [
    {
      "id": "sync_consultations",
      "next_run": "2025-01-27T12:00:00Z",
      "trigger": "interval[0:05:00]"
    }
  ]
}
```

---

### Аутентификация

#### POST `/auth/login`
Вход через OpenID токен.

**Request Body:**
```json
{
  "token": "openid-token"
}
```

**Response (200):**
```json
{
  "access_token": "access-token",
  "user_id": "user-id"
}
```

**Ошибки:**
- `401 Unauthorized` - Неверный токен
- `500 Internal Server Error` - Ошибка сервера

---

### Консультации

#### POST `/consultations/create`
Создание новой консультации с данными клиента.

**Headers:**
- `Idempotency-Key` (опционально): Уникальный ключ для предотвращения дублирования. При повторном запросе с тем же ключом возвращается кэшированный ответ.

**Request Body:**
```json
{
  "client": {
    "name": "Иван Иванов",
    "email": "ivan@example.com",
    "phone_number": "+79991234567",
    "code_abonent": "12345",
    "org_inn": "1234567890",
    "subs_id": "sub-123",
    "subs_start": "2025-01-01T00:00:00Z",
    "subs_end": "2025-12-31T23:59:59Z",
    "tariff_id": "tariff-1",
    "company_name": "ООО Компания",
    "partner": "ООО Обслуживающая организация",
    "region": "Москва",
    "city": "Москва",
    "country": "Россия"
  },
  "consultation": {
    "scheduled_at": "2025-02-01T10:00:00Z",
    "lang": "ru",
    "consultation_type": "Техническая поддержка",
    "selected_software": "бух",
    "comment": "Вопрос о работе системы",
    "online_question_cat": "uuid-категории",
    "online_question": "uuid-вопроса",
    "importance": 1,
    "topic": "Тема консультации"
  },
  "source": "SITE",
  "telegram_user_id": 123456789,
  "telegram_phone_number": "+79991234567"
}
```

**Поля Request Body:**
- `client` (опционально): Данные клиента. Если не указан, используется `client_id` из `consultation`.
- `consultation`: Данные консультации (обязательно).
  - `selected_software` (опционально): выбор ПО клиента для проставления label в Chatwoot. Допустимые значения:
    - `"бух"` — 1С:Бухгалтерия
    - `"рт"` — 1С:Розница
    - `"ук"` — 1С:Управление компанией
    При указании этого поля к созданной беседе в Chatwoot автоматически добавится соответствующий label (если метка отсутствует в Chatwoot, бэкенд попытается её создать).
- `source` (опционально): Источник создания. Возможные значения:
  - `"SITE"` - создано через сайт (по умолчанию)
  - `"TELEGRAM"` - создано через Telegram Web App
  - `"CALL_CENTER"` - создано через колл-центр
- `telegram_user_id` (опционально): ID пользователя Telegram. Передается только если создается через Telegram Web App.
- `telegram_phone_number` (опционально): Номер телефона из контакта Telegram. Передается только если создается через Telegram Web App и пользователь разрешил доступ к контакту.

**ВАЖНО:** Если передан `telegram_user_id`, бэкенд автоматически:
- Устанавливает `source="TELEGRAM"`
- Связывает Telegram пользователя с клиентом в таблице `telegram_users`
- Позволяет использовать чат в Telegram боте для этой консультации

**Response (200):**
```json
{
  "consultation": {
    "cons_id": "12345",
    "cl_ref_key": "uuid-из-1c",
    "client_id": "uuid-клиента",
    "client_key": "uuid-клиента-из-1c",
    "number": "CONS-001",
    "status": "open",
    "org_inn": "1234567890",
    "importance": 1,
    "create_date": "2025-01-27T12:00:00Z",
    "start_date": "2025-02-01T10:00:00Z",
    "end_date": null,
    "redate_time": null,
    "redate": null,
    "lang": "ru",
    "consultation_type": "Техническая поддержка",
    "denied": false,
    "manager": "uuid-менеджера",
    "manager_name": "Иванов Иван Иванович",
    "author": null,
    "comment": "Вопрос о работе системы",
    "online_question_cat": "uuid-категории",
    "online_question": "uuid-вопроса",
    "con_blocks": null,
    "con_rates": null,
    "con_calls": null,
    "chatwoot_source_id": "source-id-для-виджета",
    "source": "BACKEND",
    "created_at": "2025-01-27T12:00:00Z",
    "updated_at": "2025-01-27T12:00:00Z"
  },
  "client_id": "uuid-клиента",
  "message": "Консультация создана успешно",
  "chatwoot_conversation_id": "12345",
  "chatwoot_source_id": "source-id-для-виджета",
  "chatwoot_account_id": "1",
  "chatwoot_inbox_id": "1",
  "chatwoot_pubsub_token": "pubsub-token-для-websocket"
}
```

**Ошибки:**
- `400 Bad Request` - Невалидные данные
- `401 Unauthorized` - Неверный токен
- `409 Conflict` - Уже есть открытая консультация типа "Техническая поддержка"
- `429 Too Many Requests` - Превышен лимит консультаций или rate limit
- `500 Internal Server Error` - Ошибка сервера

**Ограничения:**
- **Консультации по ведению учета**: максимум 3 консультации на один день (по дате консультации)
- **Техническая поддержка**: максимум 1 открытая консультация одновременно для одного клиента

---

#### POST `/consultations/simple`
Упрощенное создание консультации (только данные консультации).

Используется когда клиент уже существует и известен `client_id`.

**Request Body:**
```json
{
  "client_id": "uuid-клиента",
  "scheduled_at": "2025-02-01T10:00:00Z",
  "lang": "ru",
  "consultation_type": "Техническая поддержка",
  "comment": "Вопрос о работе системы",
  "importance": 1,
  "topic": "Тема консультации"
}
```

**Response:** Аналогично `/consultations/create`

---

#### GET `/consultations/{cons_id}`
Получение консультации по ID.

**Response (200):**
```json
{
  "cons_id": "12345",
  "cl_ref_key": "uuid-из-1c",
  "client_id": "uuid-клиента",
  "client_key": "uuid-клиента-из-1c",
  "number": "CONS-001",
  "status": "open",
  "org_inn": "1234567890",
  "importance": 1,
  "create_date": "2025-01-27T12:00:00Z",
  "start_date": "2025-02-01T10:00:00Z",
  "end_date": null,
  "redate_time": null,
  "redate": null,
  "lang": "ru",
  "consultation_type": "Техническая поддержка",
  "denied": false,
  "manager": "uuid-менеджера",
  "manager_name": "Иванов Иван Иванович",
  "author": null,
  "comment": "Вопрос о работе системы",
  "online_question_cat": "uuid-категории",
  "online_question": "uuid-вопроса",
  "con_blocks": null,
  "con_rates": null,
  "con_calls": null,
  "chatwoot_source_id": "source-id-для-виджета",
  "source": "BACKEND",
  "created_at": "2025-01-27T12:00:00Z",
  "updated_at": "2025-01-27T12:00:00Z"
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена

---

#### PUT `/consultations/{cons_id}`
Обновление консультации.

**Request Body:**
```json
{
  "status": "pending",
  "start_date": "2025-02-01T11:00:00Z",
  "end_date": "2025-02-01T12:00:00Z",
  "comment": "Обновленный комментарий",
  "topic": "Обновленная тема",
  "importance": 2
}
```

**Response (200):**
```json
{
  "cons_id": "12345",
  "cl_ref_key": "uuid-из-1c",
  "client_id": "uuid-клиента",
  "client_key": "uuid-клиента-из-1c",
  "number": "CONS-001",
  "status": "pending",
  "org_inn": "1234567890",
  "importance": 2,
  "create_date": "2025-01-27T12:00:00Z",
  "start_date": "2025-02-01T11:00:00Z",
  "end_date": "2025-02-01T12:00:00Z",
  "redate_time": null,
  "redate": null,
  "lang": "ru",
  "consultation_type": "Техническая поддержка",
  "denied": false,
  "manager": "uuid-менеджера",
  "manager_name": "Иванов Иван Иванович",
  "author": null,
  "comment": "Обновленный комментарий",
  "online_question_cat": "uuid-категории",
  "online_question": "uuid-вопроса",
  "con_blocks": null,
  "con_rates": null,
  "con_calls": null,
  "chatwoot_source_id": "source-id-для-виджета",
  "source": "BACKEND",
  "created_at": "2025-01-27T12:00:00Z",
  "updated_at": "2025-01-27T12:05:00Z"
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена
- `400 Bad Request` - Невалидные данные

---

#### POST `/consultations/{cons_id}/cancel`
Аннулирование консультации пользователем.

Аннулирование доступно только если:
- Прошло не более `CANCEL_CONSULTATION_TIMEOUT_MINUTES` минут с момента создания
- Консультация еще не завершена (`end_date` не установлен)
- Статус консультации позволяет аннулирование (`open` или `pending`)

**Response (200):**
```json
{
  "cons_id": "12345",
  "cl_ref_key": "uuid-из-1c",
  "client_id": "uuid-клиента",
  "client_key": "uuid-клиента-из-1c",
  "number": "CONS-001",
  "status": "cancelled",
  "org_inn": "1234567890",
  "importance": 1,
  "create_date": "2025-01-27T12:00:00Z",
  "start_date": "2025-02-01T10:00:00Z",
  "end_date": "2025-01-27T12:05:00Z",
  "redate_time": null,
  "redate": null,
  "lang": "ru",
  "consultation_type": "Техническая поддержка",
  "denied": true,
  "manager": "uuid-менеджера",
  "manager_name": "Иванов Иван Иванович",
  "author": null,
  "comment": "Вопрос о работе системы",
  "online_question_cat": "uuid-категории",
  "online_question": "uuid-вопроса",
  "con_blocks": null,
  "con_rates": null,
  "con_calls": null,
  "chatwoot_source_id": "source-id-для-виджета",
  "source": "BACKEND",
  "created_at": "2025-01-27T12:00:00Z",
  "updated_at": "2025-01-27T12:05:00Z"
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена
- `400 Bad Request` - Консультация не может быть аннулирована (истекло время или уже завершена)

---

#### GET `/consultations/{cons_id}/updates`
Polling endpoint для получения обновлений консультации.

**Query Parameters:**
- `last_updated` (опционально): ISO timestamp последнего обновления

**Response (200):**
```json
{
  "has_updates": true,
  "consultation": {
    "cons_id": "12345",
    "cl_ref_key": "uuid-из-1c",
    "client_id": "uuid-клиента",
    "client_key": "uuid-клиента-из-1c",
    "number": "CONS-001",
    "status": "pending",
    "org_inn": "1234567890",
    "importance": 1,
    "create_date": "2025-01-27T12:00:00Z",
    "start_date": "2025-02-01T10:00:00Z",
    "end_date": null,
    "redate_time": null,
    "redate": null,
    "lang": "ru",
    "consultation_type": "Техническая поддержка",
    "denied": false,
    "manager": "uuid-менеджера",
    "manager_name": "Иванов Иван Иванович",
    "author": null,
    "comment": "Вопрос о работе системы",
    "online_question_cat": "uuid-категории",
    "online_question": "uuid-вопроса",
    "con_blocks": null,
    "con_rates": null,
    "con_calls": null,
    "chatwoot_source_id": "source-id-для-виджета",
    "source": "BACKEND",
    "created_at": "2025-01-27T12:00:00Z",
    "updated_at": "2025-01-27T12:05:00Z"
  },
  "updated_at": "2025-01-27T12:05:00Z"
}
```

Если обновлений нет:
```json
{
  "has_updates": false,
  "updated_at": "2025-01-27T12:00:00Z"
}
```

---

#### GET `/consultations/{cons_id}/stream`
Server-Sent Events (SSE) endpoint для real-time обновлений.

**Использование:**
```javascript
const eventSource = new EventSource('/api/consultations/12345/stream');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.has_updates) {
    // Обновить UI с данными data.consultation
  }
};
```

**Формат сообщений:**
- `{"has_updates": true, "consultation": {...}, "updated_at": "..."}` - Обновление консультации
- `: heartbeat` - Keep-alive сообщения (каждые 3 секунды)

---

#### WebSocket `/ws/consultations/{cons_id}`
WebSocket endpoint для real-time обновлений.

**Использование:**
```javascript
const ws = new WebSocket('ws://localhost:7070/ws/consultations/12345');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === 'update') {
    // Обновить UI
  }
};

// Keep-alive
setInterval(() => ws.send('ping'), 30000);
```

**Формат сообщений:**
- `{"type": "initial", "data": {...}}` - Начальное состояние
- `{"type": "update", "data": {...}}` - Обновление консультации
- `{"type": "error", "message": "..."}` - Ошибка
- `"pong"` - Ответ на ping

---

#### POST `/consultations/{cons_id}/sync`
Принудительная синхронизация консультации с Chatwoot и 1C:ЦЛ.

Получает актуальные данные из обеих систем, обновляет БД и возвращает актуальное состояние.

**Response (200):**
```json
{
  "cons_id": "12345",
  "cl_ref_key": "uuid-из-1c",
  "client_id": "uuid-клиента",
  "client_key": "uuid-клиента-из-1c",
  "number": "CONS-001",
  "status": "pending",
  "org_inn": "1234567890",
  "importance": 1,
  "create_date": "2025-01-27T12:00:00Z",
  "start_date": "2025-02-01T10:00:00Z",
  "end_date": null,
  "redate_time": null,
  "redate": null,
  "lang": "ru",
  "consultation_type": "Техническая поддержка",
  "denied": false,
  "manager": "uuid-менеджера",
  "manager_name": "Иванов Иван Иванович",
  "author": null,
  "comment": "Вопрос о работе системы",
  "online_question_cat": "uuid-категории",
  "online_question": "uuid-вопроса",
  "con_blocks": null,
  "con_rates": null,
  "con_calls": null,
  "chatwoot_source_id": "source-id-для-виджета",
  "source": "BACKEND",
  "created_at": "2025-01-27T12:00:00Z",
  "updated_at": "2025-01-27T12:05:00Z"
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена
- `500 Internal Server Error` - Ошибка синхронизации

---

#### GET `/consultations/{cons_id}/redates`
Получение истории переносов консультации.

**Response (200):**
```json
[
  {
    "id": 1,
    "cons_key": "uuid",
    "period": "2025-01-27T10:00:00Z",
    "old_date": "2025-01-26T10:00:00Z",
    "new_date": "2025-01-27T10:00:00Z",
    "comment": "Перенос по просьбе клиента"
  }
]
```

---

#### POST `/consultations/{cons_id}/redates`
Создание записи о переносе консультации.

**Request Body:**
```json
{
  "old_date": "2025-01-26T10:00:00Z",
  "new_date": "2025-01-27T10:00:00Z",
  "comment": "Перенос по просьбе клиента"
}
```

**Response (200):**
```json
{
  "id": 1,
  "cons_key": "uuid",
  "period": "2025-01-27T10:00:00Z",
  "old_date": "2025-01-26T10:00:00Z",
  "new_date": "2025-01-27T10:00:00Z",
  "comment": "Перенос по просьбе клиента"
}
```

---

#### GET `/consultations/{cons_id}/calls`
Получение истории дозвонов по консультации.

**Query Parameters:**
- `skip` (опционально): Количество записей для пропуска (по умолчанию: 0)
- `limit` (опционально): Максимальное количество записей (по умолчанию: 100)

**Response (200):**
```json
[
  {
    "period": "2025-01-27T10:00:00Z",
    "cons_key": "uuid",
    "cons_id": "12345",
    "client_key": "uuid-клиента",
    "client_id": "uuid-клиента",
    "manager": "uuid-менеджера"
  }
]
```

---

#### GET `/consultations/{cons_id}/ratings`
Получение оценок консультации.

**Response (200):**
```json
{
  "cons_id": "12345",
  "cons_key": "uuid",
  "ratings": [
    {
      "question_number": 1,
      "rating": 5,
      "question_text": "Оцените качество консультации",
      "comment": "Отличная консультация"
    }
  ],
  "average_rating": 4.5,
  "total_ratings": 2
}
```

---

#### POST `/consultations/{cons_id}/ratings`
Создание оценки консультации.

**Request Body:**
```json
{
  "answers": [
    {
      "question_number": 1,
      "rating": 5,
      "comment": "Отличная консультация"
    },
    {
      "question_number": 2,
      "rating": 4,
      "comment": "Можно улучшить"
    }
  ]
}
```

**Response (200):**
```json
{
  "cons_id": "12345",
  "ratings": [
    {
      "question_number": 1,
      "rating": 5,
      "question_text": "Оцените качество консультации",
      "comment": "Отличная консультация"
    }
  ],
  "average_rating": 4.5,
  "total_ratings": 1
}
```

---

#### GET `/consultations/clients/{client_id}/consultations`
Получение всех консультаций клиента.

**Query Parameters:**
- `skip` (опционально): Количество записей для пропуска (по умолчанию: 0)
- `limit` (опционально): Максимальное количество записей (по умолчанию: 100)

**Response (200):**
```json
{
  "consultations": [
    {
      "cons_id": "12345",
      "cl_ref_key": "uuid-из-1c",
      "client_id": "uuid-клиента",
      "client_key": "uuid-клиента-из-1c",
      "number": "CONS-001",
      "status": "open",
      "org_inn": "1234567890",
      "importance": 1,
      "create_date": "2025-01-27T12:00:00Z",
      "start_date": "2025-02-01T10:00:00Z",
      "end_date": null,
      "redate_time": null,
      "redate": null,
      "lang": "ru",
      "consultation_type": "Техническая поддержка",
      "denied": false,
      "manager": "uuid-менеджера",
      "manager_name": "Иванов Иван Иванович",
      "author": null,
      "comment": "Вопрос о работе системы",
      "online_question_cat": "uuid-категории",
      "online_question": "uuid-вопроса",
      "con_blocks": null,
      "con_rates": null,
      "con_calls": null,
      "chatwoot_source_id": "source-id-для-виджета",
      "source": "BACKEND",
      "created_at": "2025-01-27T12:00:00Z",
      "updated_at": "2025-01-27T12:00:00Z"
    }
  ],
  "total": 10
}
```

**Ошибки:**
- `404 Not Found` - Клиент не найден

---

### Клиенты

#### POST `/clients`
Создание или обновление клиента.

**ВАЖНО: Ограничения прав доступа:**
- **Пользователи абонента** (`is_parent=false`) **НЕ МОГУТ** изменять:
  - `org_inn` (ИНН)
  - `company_name` (название организации)
- Только **владельцы абонента** (`is_parent=true`) могут изменять эти поля.
- При попытке изменения этих полей пользователем возвращается ошибка `403 Forbidden`.

**ВАЖНО: Обновление роли клиента:**
- Если в запросе указан `is_parent=true` для существующего клиента с `is_parent=false`, роль будет обновлена до владельца (повышение роли).
- Понижение роли с владельца до пользователя запрещено (ошибка `403 Forbidden`).
- Если `is_parent` не указан в запросе, роль определяется автоматически по наличию `parent_id`:
  - Если `parent_id` указан → `is_parent=false`
  - Если `parent_id` не указан → `is_parent=true`

**Логика поиска существующего клиента:**
1. По `client_id` (если указан)
2. По `client_id_hash` (если указан)
3. По вычисленному hash (email + phone + org_inn)
4. По `code_abonent` (если указан)
5. По `org_inn` (только для владельцев)
6. Создание нового клиента

**Request Body:**
```json
{
  "client_id": "uuid-клиента",
  "name": "Иван Иванов",
  "email": "ivan@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "region": "Москва",
  "city": "Москва",
  "country": "Россия",
  "subs_id": "sub-123",
  "subs_start": "2025-01-01T00:00:00Z",
  "subs_end": "2025-12-31T23:59:59Z",
  "tariff_id": "tariff-1",
  "parent_id": "uuid-родителя",
  "is_parent": true
}
```

**Поля Request Body:**
- `partner` (опционально): Обслуживающая организация. Используется для передачи в Chatwoot как кастомный атрибут контакта. Может быть заполнен как владельцем, так и пользователем.

**Response (200):**
```json
{
  "client_id": "uuid-клиента",
  "name": "Иван Иванов",
  "email": "ivan@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "region": "Москва",
  "city": "Москва",
  "country": "Россия",
  "source_id": "chatwoot-source-id",
  "chatwoot_pubsub_token": "pubsub-token",
  "is_parent": true,
  "parent_id": null
}
```

**Ошибки:**
- `400 Bad Request` - Невалидные данные (например, `is_parent=true` и указан `parent_id`, или `is_parent=false` без `parent_id`)
- `403 Forbidden` - Попытка изменения ИНН или названия организации пользователем, или попытка понижения роли с владельца до пользователя
- `409 Conflict` - Конфликт данных (например, ИНН уже существует с другим кодом абонента)

---

#### GET `/clients/{client_id}`
Получение клиента по ID.

Если клиент является пользователем (`is_parent=false`), возвращает данные с полями `company_name`, `org_inn`, `country`, `region`, `city`, `partner` из владельца.

**ВАЖНО:** В ответе всегда присутствуют поля `is_parent` и `parent_id`, которые определяют права доступа:
- `is_parent=true` и `parent_id=null` → владелец (может изменять ИНН и название организации)
- `is_parent=false` и `parent_id="uuid-владельца"` → пользователь (не может изменять ИНН и название организации)

**Response (200) - для владельца:**
```json
{
  "client_id": "uuid-клиента",
  "name": "Иван Иванов",
  "email": "ivan@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "source_id": "chatwoot-source-id",
  "chatwoot_pubsub_token": "pubsub-token",
  "is_parent": true,
  "parent_id": null
}
```

**Response (200) - для пользователя:**
```json
{
  "client_id": "uuid-пользователя",
  "name": "Петр Петров",
  "email": "petr@example.com",
  "phone_number": "+79991234568",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "source_id": "chatwoot-source-id",
  "chatwoot_pubsub_token": "pubsub-token",
  "is_parent": false,
  "parent_id": "uuid-владельца"
}
```

**Ошибки:**
- `404 Not Found` - Клиент не найден

---

#### GET `/clients/by-hash/{hash}`
Получение клиента по хешу.

Если клиент является пользователем (`is_parent=false`), возвращает данные с полями `company_name`, `org_inn`, `country`, `region`, `city`, `partner` из владельца.

**Response (200) - для владельца:**
```json
{
  "client_id": "uuid-клиента",
  "name": "Иван Иванов",
  "email": "ivan@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "is_parent": true,
  "parent_id": null
}
```

**Response (200) - для пользователя:**
```json
{
  "client_id": "uuid-пользователя",
  "name": "Петр Петров",
  "email": "petr@example.com",
  "phone_number": "+79991234568",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "is_parent": false,
  "parent_id": "uuid-владельца"
}
```

**Ошибки:**
- `404 Not Found` - Клиент не найден

---

#### GET `/clients/by-subscriber/{code_abonent}`
Получение клиента по subscriber_id (`code_abonent`).

**Логика поиска:**
1. Сначала ищет владельца (`is_parent=true`) с таким `code_abonent`
2. Если владелец не найден, ищет пользователя (`is_parent=false`) с таким `code_abonent`
3. Если найден пользователь с `parent_id` → возвращает данные его владельца
4. Если найден пользователь без `parent_id` → возвращает самого пользователя (возможно, он должен быть владельцем)

Используется для восстановления клиента после очистки localStorage на фронте, когда известен только `subscriberId`.

**ВАЖНО:** В ответе всегда присутствуют поля `is_parent` и `parent_id`, которые определяют права доступа.

**Response (200) - если найден владелец:**
```json
{
  "client_id": "uuid-владельца",
  "name": "Иван Иванов",
  "email": "ivan@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "is_parent": true,
  "parent_id": null
}
```

**Response (200) - если найден только пользователь с parent_id:**
```json
{
  "client_id": "uuid-владельца",
  "name": "Владелец компании",
  "email": "owner@example.com",
  "phone_number": "+79991234567",
  "code_abonent": "12345",
  "org_inn": "1234567890",
  "company_name": "ООО Компания",
  "partner": "ООО Обслуживающая организация",
  "country": "Россия",
  "region": "Москва",
  "city": "Москва",
  "is_parent": true,
  "parent_id": null
}
```

**Response (200) - если найден пользователь без parent_id:**
```json
{
  "client_id": "uuid-пользователя",
  "name": "Петр Петров",
  "email": "petr@example.com",
  "phone_number": "+79991234568",
  "code_abonent": "12345",
  "partner": "ООО Обслуживающая организация",
  "is_parent": false,
  "parent_id": null
}
```

**Ошибки:**
- `400 Bad Request` - `code_abonent` не указан или пустой
- `404 Not Found` - Клиент с таким `code_abonent` не найден

---

### Менеджеры

#### GET `/managers/load`
Получение информации о загрузке всех менеджеров.

**Query Parameters:**
- `current_time` (опционально): Текущее время для расчета загрузки (по умолчанию: now())

**Response (200):**
```json
[
  {
    "manager_key": "uuid-менеджера",
    "manager_id": "uuid-account-id",
    "chatwoot_user_id": 123,
    "name": "Иванов Иван Иванович",
    "queue_count": 5,
    "limit": 10,
    "load_percent": 50.0,
    "available_slots": 5,
    "start_hour": "09:00:00",
    "end_hour": "18:00:00"
  }
]
```

---

#### GET `/managers/{manager_key}/load`
Получение информации о загрузке конкретного менеджера.

**Response (200):**
```json
{
  "manager_key": "uuid-менеджера",
  "queue_count": 5,
  "limit": 10,
  "load_percent": 50.0,
  "available_slots": 5
}
```

**Ошибки:**
- `500 Internal Server Error` - Ошибка получения данных

---

#### GET `/managers/{manager_key}/wait-time`
Расчет времени ожидания в очереди менеджера.

**Query Parameters:**
- `average_duration_minutes` (опционально): Средняя длительность консультации в минутах (по умолчанию: 60)

**Response (200):**
```json
{
  "queue_position": 3,
  "estimated_wait_minutes": 45,
  "estimated_wait_hours": 0,
  "show_range": false,
  "estimated_wait_minutes_min": 30,
  "estimated_wait_minutes_max": 60
}
```

---

#### GET `/managers/available`
Получение списка доступных менеджеров.

**Query Parameters:**
- `po_section_key` (опционально): Ключ раздела ПО
- `po_type_key` (опционально): Ключ типа ПО
- `category_key` (опционально): Ключ категории вопроса
- `current_time` (опционально): Текущее время

**Response (200):**
```json
[
  {
    "manager_key": "uuid-менеджера",
    "manager_id": "uuid-account-id",
    "chatwoot_user_id": 123,
    "name": "Иванов Иван Иванович",
    "queue_count": 5,
    "limit": 10,
    "load_percent": 50.0,
    "available_slots": 5
  }
]
```

---

#### GET `/managers/consultations/{cons_id}/queue-info`
Получение информации об очереди для конкретной консультации.

**Response (200):**
```json
{
  "queue_position": 3,
  "estimated_wait_minutes": 45,
  "estimated_wait_hours": 0,
  "manager_key": "uuid-менеджера"
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена

---

### Справочники

#### GET `/dicts/po-types`
Получение списка типов ПО.

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "description": "Тип ПО 1"
  }
]
```

---

#### GET `/dicts/po-sections`
Получение списка разделов ПО.

**Query Parameters:**
- `owner_key` (опционально): Фильтр по владельцу

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "owner_key": "uuid-владельца",
    "description": "Раздел ПО 1",
    "details": "Дополнительная информация"
  }
]
```

---

#### GET `/dicts/online-question/categories`
Получение списка категорий онлайн-вопросов.

**Query Parameters:**
- `language` (опционально): Фильтр по языку (`ru` или `uz`)

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "code": "CAT-001",
    "description": "Категория 1",
    "language": "ru"
  }
]
```

---

#### GET `/dicts/online-questions`
Получение списка онлайн-вопросов.

**Query Parameters:**
- `language` (опционально): Фильтр по языку
- `category_key` (опционально): Фильтр по категории

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "code": "Q-001",
    "description": "Вопрос 1",
    "language": "ru",
    "category_key": "uuid-категории",
    "useful_info": "Полезная информация",
    "question": "Текст вопроса"
  }
]
```

---

#### GET `/dicts/knowledge-base`
Получение списка записей базы знаний.

**Query Parameters:**
- `po_type_key` (опционально): Фильтр по типу ПО
- `po_section_key` (опционально): Фильтр по разделу ПО

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "description": "Запись базы знаний",
    "po_type_key": "uuid-типа",
    "po_section_key": "uuid-раздела",
    "author_key": "uuid-автора",
    "question": "Вопрос",
    "answer": "Ответ"
  }
]
```

---

#### GET `/dicts/interference`
Получение списка помех для консультаций.

**Response (200):**
```json
[
  {
    "ref_key": "uuid",
    "code": "INT-001",
    "description": "Помеха 1"
  }
]
```

---

### Telegram

#### POST `/telegram/webhook`
Webhook от Telegram для получения обновлений бота.

**ВАЖНО:** Этот endpoint используется только бэкендом для получения обновлений от Telegram. Фронт не должен вызывать его напрямую.

**Headers:**
- `X-Telegram-Bot-Api-Secret-Token` (опционально): Секрет для проверки webhook (если настроен `TELEGRAM_WEBHOOK_SECRET`)

**Response (200):**
```json
{
  "ok": true
}
```

---

#### POST `/telegram/webhook/chatwoot`
Webhook от Chatwoot для отправки новых сообщений в Telegram.

**ВАЖНО:** Этот endpoint используется только бэкендом для синхронизации сообщений из Chatwoot в Telegram. Фронт не должен вызывать его напрямую.

**Response (200):**
```json
{
  "ok": true
}
```

---

#### GET `/telegram/consultations/{cons_id}/messages`
Получение истории сообщений из Chatwoot для консультации.

Используется для загрузки истории при открытии чата в Telegram боте.

**Query Parameters:**
- `page` (опционально): Номер страницы (по умолчанию: 1)
- `per_page` (опционально): Количество сообщений на странице (по умолчанию: 50)

**Response (200):**
```json
{
  "messages": [
    {
      "id": "123",
      "content": "Текст сообщения",
      "message_type": "incoming",
      "created_at": "2025-01-27T12:00:00Z",
      "sender_name": "Иванов Иван",
      "sender_type": "user"
    }
  ],
  "total": 25,
  "page": 1,
  "per_page": 50
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена
- `500 Internal Server Error` - Ошибка получения сообщений

---

#### GET `/telegram/consultations/{cons_id}`
Получение информации о консультации (статус, можно ли отправлять сообщения).

**Response (200):**
```json
{
  "cons_id": "12345",
  "status": "open",
  "is_open": true,
  "message": null
}
```

Если консультация закрыта:
```json
{
  "cons_id": "12345",
  "status": "closed",
  "is_open": false,
  "message": "Консультация закрыта. Новые сообщения не принимаются."
}
```

**Ошибки:**
- `404 Not Found` - Консультация не найдена

---

#### POST `/telegram/link-user`
Связывание Telegram пользователя с клиентом.

**ВАЖНО:** Обычно вызывается автоматически при создании консультации через Telegram Web App. Может использоваться для ручной привязки.

**Request Body:**
```json
{
  "telegram_user_id": 123456789,
  "client_id": "uuid-клиента",
  "phone_number": "+79991234567",
  "username": "username",
  "first_name": "Иван",
  "last_name": "Иванов"
}
```

**Response (200):**
```json
{
  "success": true,
  "message": "Telegram user linked successfully",
  "telegram_user_id": 123456789,
  "client_id": "uuid-клиента"
}
```

**Ошибки:**
- `404 Not Found` - Клиент не найден (если указан `client_id`)
- `400 Bad Request` - Невалидный формат `client_id`
- `500 Internal Server Error` - Ошибка связывания

---

## Webhooks (для внешних систем)

### POST `/webhook/chatwoot`
Вебхук от Chatwoot для получения обновлений.

**Headers:**
- `X-Chatwoot-Signature`: Подпись вебхука (HMAC SHA256)

**События:**
- `conversation.created` - Создана новая беседа
- `conversation.updated` - Обновлена беседа
- `conversation.status_changed` - Изменен статус беседы
- `conversation.resolved` - Беседа закрыта
- `message.created` - Создано новое сообщение

**Response (200):**
```json
{
  "status": "ok",
  "message": "Processed conversation.updated"
}
```

**Ошибки:**
- `401 Unauthorized` - Неверная подпись вебхука
- `500 Internal Server Error` - Ошибка обработки

**Особенности:**
- Для консультаций типа "Консультация по ведению учёта" запрещено закрытие беседы клиентом. При попытке закрытия статус откатывается обратно.

---

### POST `/webhook/1c_cl`
Вебхук от 1C:ЦЛ для получения обновлений.

**События:**
- `consultation.created` - Создана новая консультация
- `consultation.updated` - Обновлена консультация
- `consultation.closed` - Консультация закрыта
- `consultation.rescheduled` - Консультация перенесена

**Response (200):**
```json
{
  "status": "ok",
  "message": "Processed consultation.updated"
}
```

**Ошибки:**
- `500 Internal Server Error` - Ошибка обработки

---

## Коды ошибок

| Код | Описание |
|-----|----------|
| 400 | Bad Request - Невалидные данные запроса |
| 401 | Unauthorized - Неверный токен аутентификации |
| 403 | Forbidden - Недостаточно прав доступа |
| 404 | Not Found - Ресурс не найден |
| 409 | Conflict - Конфликт данных (например, уже существует открытая консультация) |
| 422 | Unprocessable Entity - Ошибка валидации данных |
| 429 | Too Many Requests - Превышен rate limit или лимит консультаций |
| 500 | Internal Server Error - Внутренняя ошибка сервера |
| 502 | Bad Gateway - Ошибка синхронизации с внешними системами |

---

## Примеры использования

### Создание консультации с idempotency key

```javascript
const idempotencyKey = crypto.randomUUID();

const response = await fetch('/api/consultations/create', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'X-Front-Secret': 'your-secret',
    'Idempotency-Key': idempotencyKey
  },
  body: JSON.stringify({
    client: {
      name: 'Иван Иванов',
      email: 'ivan@example.com',
      phone_number: '+79991234567',
      code_abonent: '12345',
      org_inn: '1234567890'
    },
    consultation: {
      scheduled_at: '2025-02-01T10:00:00Z',
      lang: 'ru',
      consultation_type: 'Техническая поддержка',
      comment: 'Вопрос о работе системы'
    },
    source: 'SITE',
    telegram_user_id: 123456789,
    telegram_phone_number: '+79991234567'
  })
});

const data = await response.json();
```

### Polling для обновлений

```javascript
let lastUpdated = null;

async function pollUpdates(consId) {
  const url = lastUpdated 
    ? `/api/consultations/${consId}/updates?last_updated=${lastUpdated.toISOString()}`
    : `/api/consultations/${consId}/updates`;
  
  const response = await fetch(url, {
    headers: {
      'X-Front-Secret': 'your-secret'
    }
  });
  
  const data = await response.json();
  
  if (data.has_updates) {
    // Обновить UI
    updateUI(data.consultation);
    lastUpdated = new Date(data.updated_at);
  }
  
  // Повторить через 3 секунды
  setTimeout(() => pollUpdates(consId), 3000);
}
```

### SSE для real-time обновлений

```javascript
const eventSource = new EventSource('/api/consultations/12345/stream', {
  headers: {
    'X-Front-Secret': 'your-secret'
  }
});

eventSource.onmessage = (event) => {
  if (event.data.startsWith(':')) {
    // Heartbeat, игнорируем
    return;
  }
  
  const data = JSON.parse(event.data);
  
  if (data.has_updates) {
    updateUI(data.consultation);
  }
};

eventSource.onerror = (error) => {
  console.error('SSE connection error:', error);
  // Переподключиться через некоторое время
  setTimeout(() => {
    eventSource.close();
    // Переподключение
  }, 5000);
};
```

### WebSocket для real-time обновлений

```javascript
const ws = new WebSocket('ws://localhost:7070/ws/consultations/12345');

ws.onopen = () => {
  console.log('WebSocket connected');
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  switch (data.type) {
    case 'initial':
      updateUI(data.data);
      break;
    case 'update':
      updateUI(data.data);
      break;
    case 'error':
      console.error('WebSocket error:', data.message);
      break;
  }
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket disconnected');
  // Переподключиться через некоторое время
  setTimeout(() => {
    // Переподключение
  }, 5000);
};

// Keep-alive
setInterval(() => {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send('ping');
  }
}, 30000);
```

### Получение загрузки менеджеров

```javascript
const response = await fetch('/api/managers/load', {
  headers: {
    'X-Front-Secret': 'your-secret'
  }
});

const managers = await response.json();
// managers - массив менеджеров с информацией о загрузке
```

### Расчет времени ожидания

```javascript
const response = await fetch('/api/managers/uuid-менеджера/wait-time?average_duration_minutes=60', {
  headers: {
    'X-Front-Secret': 'your-secret'
  }
});

const waitInfo = await response.json();
// waitInfo.estimated_wait_minutes - примерное время ожидания в минутах
```

---

## Важные замечания

1. **Idempotency Keys**: Используйте уникальные ключи для предотвращения дублирования операций. Ключи действительны 24 часа.

2. **Rate Limiting**: Следите за заголовками ответа `Retry-After` при получении 429 ошибок.

3. **Real-time обновления**: Используйте SSE или WebSocket для получения обновлений в реальном времени. Polling endpoint предназначен для fallback.

4. **Обработка ошибок**: Всегда обрабатывайте ошибки и проверяйте статус ответа перед использованием данных.

5. **Валидация данных**: Проверяйте данные на клиенте перед отправкой, но не полагайтесь только на клиентскую валидацию.

6. **Токены**: Никогда не храните токены в коде или публичных репозиториях. Используйте переменные окружения или secure storage.

7. **Ограничения консультаций**:
   - Консультации по ведению учета: максимум 3 консультации на один день
   - Техническая поддержка: максимум 1 открытая консультация одновременно

8. **Права доступа**: Пользователи абонента (`is_parent=false`) не могут изменять ИНН и название организации. Эти поля могут изменять только владельцы (`is_parent=true`).

9. **Кэширование справочников**: Справочники кэшируются на 30 минут для повышения производительности.

10. **ETL синхронизация**: 
    - ETL использует инкрементальную загрузку по полю `ДатаИзменения` для эффективного обновления только измененных консультаций
    - Терминальные статусы (`closed`, `resolved`, `cancelled`) не меняются из ЦЛ
    - Статусы автоматически синхронизируются с Chatwoot при изменении
    - Уведомления о переносах, оценках и дозвонах автоматически отправляются клиентам через ETL
    - Предотвращение дублирования уведомлений через систему логирования `NotificationLog`

---

## Swagger UI

Интерактивная документация API доступна по адресу:
- `/docs` - Swagger UI
- `/redoc` - ReDoc
