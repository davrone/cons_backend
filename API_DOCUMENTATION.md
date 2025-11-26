# API Документация для фронтенда

## Базовый URL

```
http://localhost:7070/api
```

В продакшене: `https://api.clobus.uz/api` (или ваш домен)

## Аутентификация

Все запросы должны включать заголовок `Authorization`:

```
Authorization: Bearer <token>
```

Токен получается после авторизации на `dev.clobus.uz` через OpenID.

**Примечание**: В текущей версии токен принимается, но валидация еще не реализована. Структура готова для будущей интеграции.

---

## Endpoints

### 1. Создание консультации (основной endpoint)

**POST** `/api/consultations/create`

Создает консультацию с данными клиента. Если клиента нет, создает его автоматически.

#### Request Body

```json
{
  "client": {
    "email": "user@example.com",
    "phone_number": "+998901234567",
    "country": "UZ",
    "region": "Tashkent",
    "city": "Tashkent",
    "org_inn": "123456789",
    "org_id": "org-123",
    "subs_id": "SUB-123",
    "subs_start": "2025-01-01T00:00:00Z",
    "subs_end": "2025-12-31T23:59:59Z",
    "tariff_id": "PRO",
    "tariffperiod_id": "MONTHLY",
    "cl_ref_key": "uuid-from-1c",
    "client_id_hash": "optional-hash-for-existing-client",
    "client_id": "optional-uuid-of-existing-client"
  },
  "consultation": {
    "client_id": "optional-if-client-already-exists",
    "cl_ref_key": "optional-ref-key-from-1c",
    "org_inn": "123456789",
    "lang": "ru",
    "comment": "Нужна консультация по НДС",
    "topic": "Бухгалтерия",
    "online_question_cat": "uuid-category-key",
    "online_question": "uuid-question-key",
    "importance": 2,
    "scheduled_at": "2025-12-01T10:00:00Z"
  },
  "source": "SITE"
}
```

#### Поля клиента (все опциональны, но рекомендуется указать хотя бы email или phone):

- `email` - Email клиента
- `phone_number` - Телефон
- `country` - Страна
- `region` - Регион
- `city` - Город
- `org_inn` - ИНН организации
- `org_id` - ID организации
- `subs_id` - ID подписки
- `subs_start` - Начало подписки (ISO datetime)
- `subs_end` - Конец подписки (ISO datetime)
- `tariff_id` - ID тарифа
- `tariffperiod_id` - ID периода тарифа
- `cl_ref_key` - Ref_Key клиента из 1C:ЦЛ (если известен)
- `client_id_hash` - Хеш для поиска существующего клиента
- `client_id` - UUID существующего клиента (если известен)

#### Поля консультации:

- `client_id` - UUID клиента (если клиент уже существует)
- `cl_ref_key` - Ref_Key из ЦЛ (если известен)
- `org_inn` - ИНН организации
- `lang` - Язык (`ru` или `uz`, по умолчанию `ru`)
- `comment` - **Обязательно**: Вопрос/описание консультации
- `topic` - Тема консультации
- `online_question_cat` - UUID категории вопроса (из справочника)
- `online_question` - UUID вопроса (из справочника)
- `importance` - Важность (число, опционально)
- `scheduled_at` - Желаемая дата/время консультации (ISO datetime)

#### Response (200 OK)

```json
{
  "consultation": {
    "cons_id": "chatwoot-conversation-id",
    "cl_ref_key": "1c-ref-key",
    "client_id": "client-uuid",
    "number": "00000522222",
    "status": "new",
    "org_inn": "123456789",
    "create_date": "2025-11-25T10:00:00Z",
    "start_date": "2025-12-01T10:00:00Z",
    "lang": "ru",
    "comment": "Нужна консультация по НДС",
    "online_question_cat": "uuid-category-key",
    "online_question": "uuid-question-key",
    "created_at": "2025-11-25T10:00:00Z",
    "updated_at": "2025-11-25T10:00:00Z"
  },
  "client_id": "client-uuid",
  "message": "Consultation created successfully"
}
```

#### Пример запроса (cURL)

```bash
curl -X POST "http://localhost:7070/api/consultations/create" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "client": {
      "email": "user@example.com",
      "phone_number": "+998901234567",
      "org_inn": "123456789"
    },
    "consultation": {
      "comment": "Нужна консультация по НДС",
      "lang": "ru"
    },
    "source": "SITE"
  }'
```

---

### 2. Упрощенное создание консультации

**POST** `/api/consultations/simple`

Используется когда клиент уже существует и известен `client_id`.

#### Request Body

```json
{
  "client_id": "client-uuid",
  "org_inn": "123456789",
  "lang": "ru",
  "comment": "Нужна консультация по НДС",
  "topic": "Бухгалтерия",
  "online_question_cat": "uuid-category-key",
  "online_question": "uuid-question-key",
  "importance": 2,
  "scheduled_at": "2025-12-01T10:00:00Z"
}
```

#### Response

Аналогично `/api/consultations/create`

---

### 3. Создание/обновление клиента

**POST** `/api/clients`

Создает нового клиента или обновляет существующего (по `client_id`, `client_id_hash` или `email+phone+inn`).

#### Request Body

```json
{
  "email": "user@example.com",
  "phone_number": "+998901234567",
  "country": "UZ",
  "region": "Tashkent",
  "city": "Tashkent",
  "org_inn": "123456789",
  "org_id": "org-123",
  "subs_id": "SUB-123",
  "subs_start": "2025-01-01T00:00:00Z",
  "subs_end": "2025-12-31T23:59:59Z",
  "tariff_id": "PRO",
  "tariffperiod_id": "MONTHLY",
  "cl_ref_key": "uuid-from-1c",
  "client_id_hash": "optional-hash",
  "client_id": "optional-uuid"
}
```

#### Response (200 OK)

```json
{
  "client_id": "client-uuid",
  "email": "user@example.com",
  "phone_number": "+998901234567",
  "country": "UZ",
  "region": "Tashkent",
  "city": "Tashkent",
  "org_inn": "123456789",
  "org_id": "org-123",
  "subs_id": "SUB-123",
  "subs_start": "2025-01-01T00:00:00Z",
  "subs_end": "2025-12-31T23:59:59Z",
  "tariff_id": "PRO",
  "tariffperiod_id": "MONTHLY",
  "cl_ref_key": "uuid-from-1c",
  "created_at": "2025-11-25T10:00:00Z",
  "updated_at": "2025-11-25T10:00:00Z"
}
```

---

### 4. Получение клиента

**GET** `/api/clients/{client_id}`

Получает данные клиента по UUID.

#### Response (200 OK)

Аналогично ответу создания клиента.

#### Response (404 Not Found)

```json
{
  "detail": "Client not found"
}
```

---

### 5. Получение клиента по хешу

**GET** `/api/clients/by-hash/{client_hash}`

Получает клиента по `client_id_hash`.

---

### 6. Получение тикета

**GET** `/api/tickets/{cons_id}`

Получает консультацию по `cons_id` (ID из Chatwoot).

#### Response (200 OK)

```json
{
  "cons_id": "chatwoot-id",
  "cl_ref_key": "1c-ref-key",
  "client_id": "client-uuid",
  "number": "00000522222",
  "status": "pending",
  "org_inn": "123456789",
  "create_date": "2025-11-25T10:00:00Z",
  "start_date": "2025-12-01T10:00:00Z",
  "end_date": null,
  "lang": "ru",
  "comment": "Нужна консультация по НДС",
  "created_at": "2025-11-25T10:00:00Z",
  "updated_at": "2025-11-25T10:00:00Z"
}
```

---

### 7. Получение тикетов клиента

**GET** `/api/tickets/clients/{client_id}/tickets?skip=0&limit=100`

Получает список тикетов клиента с пагинацией.

#### Query Parameters

- `skip` - Смещение (по умолчанию 0)
- `limit` - Лимит (по умолчанию 100, максимум 1000)

#### Response (200 OK)

```json
{
  "tickets": [
    {
      "cons_id": "chatwoot-id-1",
      "number": "00000522222",
      "status": "closed",
      "comment": "Консультация завершена",
      "created_at": "2025-11-25T10:00:00Z"
    },
    {
      "cons_id": "chatwoot-id-2",
      "number": "00000522223",
      "status": "pending",
      "comment": "Ожидает обработки",
      "created_at": "2025-11-26T10:00:00Z"
    }
  ],
  "total": 2
}
```

---

## Статусы консультаций

- `new` - Новая заявка
- `pending` - В очереди на консультацию
- `closed` - Завершена (КонсультацияИТС)
- `other` - Другое обращение

---

## Обработка ошибок

### 400 Bad Request

```json
{
  "detail": "Client data or client_id is required"
}
```

### 404 Not Found

```json
{
  "detail": "Client not found"
}
```

### 500 Internal Server Error

```json
{
  "detail": "Internal server error"
}
```

---

## Примеры использования

### JavaScript (Fetch API)

```javascript
// Создание консультации
async function createConsultation(clientData, consultationData) {
  const response = await fetch('http://localhost:7070/api/consultations/create', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      client: clientData,
      consultation: consultationData,
      source: 'SITE'
    })
  });
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail);
  }
  
  return await response.json();
}

// Использование
const result = await createConsultation(
  {
    email: 'user@example.com',
    phone_number: '+998901234567',
    org_inn: '123456789'
  },
  {
    comment: 'Нужна консультация по НДС',
    lang: 'ru'
  }
);

console.log('Consultation ID:', result.consultation.cons_id);
console.log('Client ID:', result.client_id);
```

### TypeScript

```typescript
interface ClientData {
  email?: string;
  phone_number?: string;
  org_inn?: string;
  // ... остальные поля
}

interface ConsultationData {
  comment: string;
  lang?: string;
  scheduled_at?: string;
  // ... остальные поля
}

interface ConsultationResponse {
  consultation: {
    cons_id: string;
    client_id: string;
    status: string;
    // ... остальные поля
  };
  client_id: string;
  message: string;
}

async function createConsultation(
  client: ClientData,
  consultation: ConsultationData
): Promise<ConsultationResponse> {
  const response = await fetch('http://localhost:7070/api/consultations/create', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      client,
      consultation,
      source: 'SITE'
    })
  });
  
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  
  return await response.json();
}
```

---

## Swagger UI

После запуска приложения доступна интерактивная документация:

```
http://localhost:7070/docs
```

Там можно:
- Просмотреть все endpoints
- Протестировать запросы
- Увидеть схемы данных

---

## Важные замечания

1. **Идемпотентность**: Повторные запросы с теми же данными не создадут дубликатов. Клиент ищется по `email+phone+inn` или `client_id`.

2. **Автоматическая синхронизация**: При создании консультации она автоматически отправляется в:
   - Chatwoot (получает `cons_id`)
   - 1C:ЦЛ (получает `cl_ref_key` и `number`)

3. **Обработка ошибок**: Если Chatwoot или ЦЛ недоступны, консультация все равно создается в БД. Синхронизация произойдет позже.

4. **Минимальные данные**: Для создания консультации достаточно указать `comment` в `consultation` и хотя бы `email` или `phone_number` в `client`.

---

## Поддержка

При возникновении проблем проверьте:
1. Правильность формата JSON
2. Наличие заголовка `Authorization`
3. Доступность сервиса (health check: `/api/health`)
4. Логи сервера

