# Переменные окружения (.env файл)

## Обязательные переменные

### База данных
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cons_backend
DB_USER=postgres
DB_PASS=your_password
```

### Приложение
```env
APP_HOST=0.0.0.0
APP_PORT=7070
ENV=dev
DEBUG=True
FRONT_SECRET=your_frontend_secret_key
FRONT_BEARER_TOKEN=your_bearer_token_optional
```

### Chatwoot API
```env
CHATWOOT_API_URL=https://your-chatwoot-instance.com
CHATWOOT_API_TOKEN=your_chatwoot_api_token
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1
CHATWOOT_INBOX_IDENTIFIER=your_inbox_identifier
CHATWOOT_BOT_ID=optional_bot_id  # Опционально, будет определен автоматически если не указан
```

### 1C:ЦЛ API
```env
ONEC_API_URL=https://your-1c-instance.com/api
ONEC_API_TOKEN=your_1c_api_token
```

### OData (1C:CL)
```env
ODATA_BASEURL_CL=https://your-1c-instance.com/odata/standard.odata
ODATA_USER=your_odata_username
ODATA_PASSWORD=your_odata_password
ODATA_PAGE_SIZE=1000
```

### OpenID (опционально, для будущей аутентификации)
```env
OPENID_ISSUER=https://your-openid-provider.com
OPENID_CLIENT_ID=your_client_id
OPENID_CLIENT_SECRET=your_client_secret
```

---

## Новые переменные (добавлены в последних обновлениях)

### CORS
```env
# Разрешенные источники для CORS (через запятую)
# Пример: http://localhost:3000,https://yourdomain.com
# Если не указано, используется "*" (разрешает все источники)
ALLOWED_ORIGINS=http://localhost:3000,https://yourdomain.com
```

### Rate Limiting
```env
# Общий лимит запросов в минуту (по умолчанию 100)
RATE_LIMIT_PER_MINUTE=100

# Лимит запросов на создание консультаций в минуту (по умолчанию 10)
RATE_LIMIT_CREATE_PER_MINUTE=10
```

### Chatwoot Bot ID (опционально)
```env
# ID бота в Chatwoot (опционально)
# Если не указано, будет определен автоматически через API при первом использовании
CHATWOOT_BOT_ID=123
```

### Telegram Bot (НОВОЕ)
```env
# Токен Telegram бота от BotFather (ОБЯЗАТЕЛЬНО для работы бота)
# Получить можно у @BotFather в Telegram: /newbot -> создать бота -> скопировать токен
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz

# URL для webhook от Telegram (ОПЦИОНАЛЬНО, для production)
# Используется для получения обновлений от Telegram через webhook
# Формат: https://your-domain.com/api/telegram/webhook
# Если не указан, используется polling (для разработки)
TELEGRAM_WEBHOOK_URL=https://your-domain.com/api/telegram/webhook

# Секрет для проверки webhook от Telegram (ОПЦИОНАЛЬНО, для production)
# Используется для проверки подлинности webhook запросов
# Рекомендуется указать для безопасности
TELEGRAM_WEBHOOK_SECRET=your_webhook_secret_token
```

**Важно:**
- `TELEGRAM_BOT_TOKEN` - **обязательная** переменная. Без неё бот не запустится
- `TELEGRAM_WEBHOOK_URL` - для production рекомендуется использовать webhook (быстрее и надежнее)
- `TELEGRAM_WEBHOOK_SECRET` - рекомендуется для production для защиты от поддельных запросов
- Для разработки можно использовать только `TELEGRAM_BOT_TOKEN` (будет использоваться polling)

---

## Полный пример .env файла

```env
# ============================================
# БАЗА ДАННЫХ
# ============================================
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cons_backend
DB_USER=postgres
DB_PASS=your_password

# ============================================
# ПРИЛОЖЕНИЕ
# ============================================
APP_HOST=0.0.0.0
APP_PORT=7070
ENV=dev
DEBUG=True
FRONT_SECRET=your_very_secret_key_here
FRONT_BEARER_TOKEN=optional_bearer_token

# ============================================
# CORS (НОВОЕ)
# ============================================
# Разрешенные источники через запятую
# Для разработки: http://localhost:3000,http://localhost:8080
# Для продакшена: https://yourdomain.com,https://www.yourdomain.com
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:8080

# ============================================
# RATE LIMITING (НОВОЕ)
# ============================================
# Общий лимит запросов в минуту
RATE_LIMIT_PER_MINUTE=100

# Лимит на создание консультаций в минуту
RATE_LIMIT_CREATE_PER_MINUTE=10

# ============================================
# CHATWOOT API
# ============================================
CHATWOOT_API_URL=https://your-chatwoot-instance.com
CHATWOOT_API_TOKEN=your_chatwoot_api_token
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1
CHATWOOT_INBOX_IDENTIFIER=your_inbox_identifier

# ID бота в Chatwoot (опционально, будет определен автоматически)
CHATWOOT_BOT_ID=

# ============================================
# 1C:ЦЛ API
# ============================================
ONEC_API_URL=https://your-1c-instance.com/api
ONEC_API_TOKEN=your_1c_api_token

# ============================================
# ODATA (1C:CL)
# ============================================
ODATA_BASEURL_CL=https://your-1c-instance.com/odata/standard.odata
ODATA_USER=your_odata_username
ODATA_PASSWORD=your_odata_password
ODATA_PAGE_SIZE=1000

# ============================================
# OPENID (опционально)
# ============================================
OPENID_ISSUER=
OPENID_CLIENT_ID=
OPENID_CLIENT_SECRET=

# ============================================
# ОГРАНИЧЕНИЯ (опционально, есть значения по умолчанию)
# ============================================
# Максимальное количество дней вперед для создания консультации (по умолчанию 30)
MAX_FUTURE_CONSULTATION_DAYS=30

# Время в минутах с момента создания, в течение которого можно аннулировать консультацию (по умолчанию 30)
CANCEL_CONSULTATION_TIMEOUT_MINUTES=30

# Рабочее время для технической поддержки (формат: HH:MM)
# Время начала рабочего дня (по умолчанию 09:00)
TECH_SUPPORT_WORKING_HOURS_START=09:00

# Время окончания рабочего дня (по умолчанию 18:00)
TECH_SUPPORT_WORKING_HOURS_END=18:00

# Рабочее время для технической поддержки (формат: HH:MM)
# Время начала рабочего дня (по умолчанию 09:00)
TECH_SUPPORT_WORKING_HOURS_START=09:00

# Время окончания рабочего дня (по умолчанию 18:00)
TECH_SUPPORT_WORKING_HOURS_END=18:00

# ============================================
# TELEGRAM BOT (НОВОЕ)
# ============================================
# Токен Telegram бота от BotFather (ОБЯЗАТЕЛЬНО)
# Получить: https://t.me/BotFather -> /newbot -> создать бота -> скопировать токен
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz

# URL для webhook от Telegram (ОПЦИОНАЛЬНО, для production)
# Формат: https://your-domain.com/api/telegram/webhook
# Если не указан, используется polling (для разработки)
TELEGRAM_WEBHOOK_URL=https://your-domain.com/api/telegram/webhook

# Секрет для проверки webhook (ОПЦИОНАЛЬНО, для production)
# Рекомендуется указать для безопасности
TELEGRAM_WEBHOOK_SECRET=your_webhook_secret_token
```

---

## Важные замечания

1. **ALLOWED_ORIGINS**: 
   - В разработке можно использовать `*` (через код) или указать конкретные домены
   - В продакшене **обязательно** указать конкретные домены через запятую
   - Формат: `http://localhost:3000,https://yourdomain.com`

2. **RATE_LIMIT_PER_MINUTE**:
   - Общий лимит для всех endpoints (кроме создания консультаций)
   - По умолчанию: 100 запросов/минуту
   - Можно увеличить для высоконагруженных систем

3. **RATE_LIMIT_CREATE_PER_MINUTE**:
   - Специальный лимит для создания консультаций
   - По умолчанию: 10 запросов/минуту
   - Защищает от злоупотреблений

4. **CHATWOOT_BOT_ID**:
   - Опциональная переменная
   - Если не указана, будет определена автоматически через API при первом использовании
   - Кэшируется в памяти процесса
   - Рекомендуется указать для стабильности

5. **TELEGRAM_BOT_TOKEN**:
   - **ОБЯЗАТЕЛЬНАЯ** переменная для работы Telegram бота
   - Получить можно у [@BotFather](https://t.me/BotFather) в Telegram:
     1. Отправьте команду `/newbot`
     2. Следуйте инструкциям для создания бота
     3. Скопируйте токен (формат: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`)
   - Без этой переменной бот не запустится (будет предупреждение в логах)

6. **TELEGRAM_WEBHOOK_URL**:
   - Опциональная переменная для production
   - Если указана, бот будет использовать webhook (рекомендуется для production)
   - Если не указана, используется polling (подходит для разработки)
   - Формат: `https://your-domain.com/api/telegram/webhook`
   - URL должен быть доступен из интернета (HTTPS обязателен для production)

7. **TELEGRAM_WEBHOOK_SECRET**:
   - Опциональная переменная для production
   - Используется для проверки подлинности webhook запросов от Telegram
   - Рекомендуется указать случайную строку для безопасности
   - Telegram будет отправлять этот секрет в заголовке `X-Telegram-Bot-Api-Secret-Token`

8. **TECH_SUPPORT_WORKING_HOURS_START и TECH_SUPPORT_WORKING_HOURS_END**:
   - Переменные для настройки рабочего времени технической поддержки
   - Используются только для консультаций типа "Техническая поддержка"
   - Формат: `HH:MM` (например, `09:00`, `18:00`)
   - По умолчанию: `09:00` - `18:00`
   - Если клиент выбирает время вне рабочего времени, оно автоматически корректируется на ближайшее рабочее время
   - Можно изменить динамически через `.env` файл без перезапуска приложения (если используется hot-reload)

9. **Безопасность**:
   - Никогда не коммитьте `.env` файл в git
   - Используйте разные значения для dev/staging/production
   - Регулярно меняйте секретные ключи

---

## Проверка переменных

После добавления переменных в `.env`, перезапустите приложение:

```bash
# Проверка что переменные загружаются
python -c "from FastAPI.config import settings; print(settings.ALLOWED_ORIGINS)"

# Проверка Telegram бота
python -c "from FastAPI.config import settings; print('Bot token set:', bool(settings.TELEGRAM_BOT_TOKEN))"
```

Если переменные не загружаются, убедитесь что:
1. Файл `.env` находится в корне проекта (рядом с `main.py`)
2. Переменные указаны в правильном формате (без пробелов вокруг `=`)
3. Приложение перезапущено после изменения `.env`

