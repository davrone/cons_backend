# Резюме изменений - Решение 3 проблем

Дата: 26 декабря 2025

## Проблема 1: Labels "ук", "рт" и "бух" не прикрепляются в conversation в chatwoot

### Решение:
1. **Добавлен новый field в schema** (`FastAPI/schemas/tickets.py`):
   - Добавлено поле `selected_software: Optional[str] = None` в класс `ConsultationCreate`
   - Это поле содержит выбор ПО клиентом: "бух" (бухгалтерия), "рт" (розница), "ук" (управление компанией)

2. **Обновлена функция формирования labels** (`FastAPI/routers/consultations.py`):
   - Функция `_build_chatwoot_labels()` теперь принимает параметр `selected_software`
   - Добавлено маппирование выбранного ПО на соответствующие labels:
     - "бух" → "бух"
     - "рт" → "рт"
     - "ук" → "ук"

3. **Обновлена логика создания консультации** (`FastAPI/routers/consultations.py`):
   - При создании консультации извлекается значение `selected_software` из payload
   - Это значение передаётся в функцию `_build_chatwoot_labels()` для формирования labels
   - Labels автоматически добавляются к conversation в Chatwoot через `add_conversation_labels()`

### Использование:
Фронт должен отправлять в API поле `selected_software` при создании консультации:
```json
{
  "client": {...},
  "consultation": {
    "lang": "ru",
    "selected_software": "бух",
    ...
  }
}
```

---

## Проблема 2: Дублирование названия компании при синхронизации с 1C

### Причина:
Когда фронт отправляет обновление client с полным названием вида "Clobus OOO TOP AGRO TRADE 10240 (303045154)" (содержащем маску), оно сохранялось в БД как есть. При следующей синхронизации с 1C, функция `_build_client_display_name()` добавляла маску снова, результирующее имя имело дублирование.

### Решение:
1. **Создана функция для очистки company_name** (`FastAPI/routers/clients.py`):
   - Функция `_clean_company_name()` рекурсивно удаляет все части маски:
     - Удаляет префикс "Clobus" (может быть несколько раз)
     - Удаляет ИНН в скобках в конце `(ИНН)` (может быть несколько раз)
     - Удаляет числовой код абонента в конце (может быть несколько раз)

2. **Обновлены все места обновления company_name** (`FastAPI/routers/clients.py`):
   - В функции `create_or_update_client()` добавлена очистка company_name через `_clean_company_name()` перед сохранением в БД
   - Это применяется в 4 местах обновления client (по client_id, по client_id_hash, по code_abonent)

### Результат:
- Когда фронт отправляет "Clobus OOO TOP AGRO TRADE 10240 (303045154)", в БД сохраняется "OOO TOP AGRO TRADE"
- При синхронизации с 1C функция `_build_client_display_name()` добавляет маску один раз: "Clobus OOO TOP AGRO TRADE 10240 (303045154)"
- Дублирование маски полностью исключено

---

## Проблема 3: Управление отправкой сообщения об "Примерном времени ожидания"

### Решение:
1. **Добавлена новая env переменная** (`FastAPI/config.py`):
   ```python
   SEND_QUEUE_WAIT_TIME_MESSAGE: bool = Field(
       default=True, 
       description="Отправлять ли сообщение об примерном времени ожидания в очереди"
   )
   ```
   - По умолчанию `True` (отправлять сообщение об ожидании, как раньше)
   - Если `False` - отправляется только номер очереди без времени ожидания

2. **Обновлена логика в consultations.py** (`FastAPI/routers/consultations.py`):
   - При формировании сообщения об очереди проверяется значение `settings.SEND_QUEUE_WAIT_TIME_MESSAGE`
   - Если `True`: отправляется "Вы в очереди #6. Примерное время ожидания: 78 часов."
   - Если `False`: отправляется "Вы в очереди #6. (Подробнее время ожидания вы узнаете в чате)"

3. **Обновлена логика в manager_notifications.py** (`FastAPI/services/manager_notifications.py`):
   - Функция `send_queue_update_notification()` теперь также проверяет `settings.SEND_QUEUE_WAIT_TIME_MESSAGE`
   - Применяется та же логика условной отправки информации о времени ожидания

### Использование:
В файле `.env` добавить/изменить переменную:
```bash
# Отправлять сообщение об времени ожидания (true/false)
SEND_QUEUE_WAIT_TIME_MESSAGE=true
```

---

## Технические детали

### Файлы, измененные в этом обновлении:

1. **FastAPI/config.py**
   - Добавлена переменная `SEND_QUEUE_WAIT_TIME_MESSAGE`

2. **FastAPI/schemas/tickets.py**
   - Добавлено поле `selected_software` в `ConsultationCreate`

3. **FastAPI/routers/consultations.py**
   - Обновлена функция `_build_chatwoot_labels()` (теперь принимает `selected_software`)
   - Обновлена логика создания консультации для передачи `selected_software`
   - Обновлена логика отправки сообщения об очереди с проверкой `SEND_QUEUE_WAIT_TIME_MESSAGE`

4. **FastAPI/routers/clients.py**
   - Добавлена функция `_clean_company_name()` для очистки названия компании от маски
   - Обновлены 4 места обновления `client.company_name` для применения очистки
   - Добавлен импорт модуля `re` для regex операций

5. **FastAPI/services/manager_notifications.py**
   - Добавлен импорт `settings` из config
   - Обновлена функция `send_queue_update_notification()` для проверки `SEND_QUEUE_WAIT_TIME_MESSAGE`

---

## Тестирование

### Проблема 1 - Labels:
1. Создать консультацию с `selected_software: "бух"`
2. Проверить в Chatwoot, что conversation имеет label "бух"

### Проблема 2 - Company name:
1. Обновить client с company_name содержащим маску
2. Проверить в БД, что сохранено чистое имя без маски
3. Синхронизировать с 1C и убедиться, что нет дублирования

### Проблема 3 - Queue message:
1. Установить `SEND_QUEUE_WAIT_TIME_MESSAGE=true` в .env
2. Создать консультацию - должно приходить сообщение с временем ожидания
3. Установить `SEND_QUEUE_WAIT_TIME_MESSAGE=false` в .env
4. Создать консультацию - должно приходить сообщение без времени ожидания
