# ТЗ: Интеграция Telegram Web App для фронтенда

## Цель

Интегрировать поддержку Telegram Web App в существующий фронтенд для работы с консультациями через Telegram бота. Фронт должен определять, что сеанс открыт из Telegram, передавать данные Telegram пользователя в бэкенд и предоставлять возможность перехода в чат Telegram бота.

## Обзор процесса

1. Пользователь запускает Telegram бота
2. Бот приветствует и предлагает авторизоваться через кнопку запуска Web App
3. Открывается браузер Telegram с сайтом на странице subscriptions
4. Пользователь проходит стандартную авторизацию сайта
5. Пользователь заполняет данные и создает заявку
6. После создания заявки появляется кнопка "Продолжить в Telegram"
7. При нажатии пользователь возвращается в Telegram бот с историей сообщений
8. Пользователь может общаться в чате Telegram, сообщения синхронизируются с Chatwoot

## Определение Telegram сеанса

### Проверка наличия Telegram Web App

Фронт должен проверять наличие Telegram Web App API:

```javascript
// Проверка, что сеанс открыт из Telegram
const isTelegram = window.Telegram && window.Telegram.WebApp;

if (isTelegram) {
  // Сеанс из Telegram
  const tg = window.Telegram.WebApp;
  
  // Инициализация Web App
  tg.ready();
  tg.expand(); // Развернуть на весь экран
  
  // Получение данных пользователя
  const user = tg.initDataUnsafe?.user;
  const telegramUserId = user?.id; // ID пользователя Telegram
  const phoneNumber = user?.phone_number; // Телефон (если разрешен)
}
```

### Получение данных Telegram пользователя

```javascript
function getTelegramUserData() {
  if (!window.Telegram?.WebApp) {
    return null;
  }
  
  const tg = window.Telegram.WebApp;
  const user = tg.initDataUnsafe?.user;
  
  if (!user) {
    return null;
  }
  
  return {
    telegram_user_id: user.id,
    phone_number: user.phone_number || null,
    username: user.username || null,
    first_name: user.first_name || null,
    last_name: user.last_name || null
  };
}
```

## Создание консультации через Telegram

### Обновление запроса создания консультации

При создании консультации через Telegram Web App нужно передавать дополнительные поля:

```javascript
async function createConsultation(consultationData) {
  const telegramData = getTelegramUserData();
  
  const payload = {
    client: consultationData.client,
    consultation: consultationData.consultation,
    source: telegramData ? "TELEGRAM" : "SITE", // Определяем источник
    // Добавляем данные Telegram если сеанс из Telegram
    ...(telegramData && {
      telegram_user_id: telegramData.telegram_user_id,
      telegram_phone_number: telegramData.phone_number
    })
  };
  
  const response = await fetch('/api/consultations/create', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Front-Secret': 'your-secret'
    },
    body: JSON.stringify(payload)
  });
  
  return await response.json();
}
```

### Определение источника

```javascript
function getSource() {
  const isTelegram = window.Telegram && window.Telegram.WebApp;
  return isTelegram ? "TELEGRAM" : "SITE";
}
```

## Кнопка "Продолжить в Telegram"

### Отображение кнопки

Кнопка должна отображаться только если:
1. Консультация создана через Telegram Web App (`source === "TELEGRAM"`)
2. Консультация успешно создана

### Реализация кнопки

```javascript
function renderContinueInTelegramButton(consId) {
  const isTelegram = window.Telegram && window.Telegram.WebApp;
  
  if (!isTelegram) {
    return null; // Не показываем кнопку если не из Telegram
  }
  
  const botUsername = 'your_bot_username'; // Имя вашего бота без @
  const deepLink = `https://t.me/${botUsername}?start=cons_${consId}`;
  
  return (
    <button 
      onClick={() => {
        // Открываем deep link для перехода в бот
        window.open(deepLink, '_blank');
        
        // Или используем Telegram Web App API для закрытия
        if (window.Telegram?.WebApp) {
          window.Telegram.WebApp.close();
        }
      }}
    >
      Продолжить в Telegram
    </button>
  );
}
```

### Альтернативный способ (через Telegram Web App API)

```javascript
function openTelegramChat(consId) {
  const tg = window.Telegram?.WebApp;
  
  if (!tg) {
    // Fallback: обычная ссылка
    const botUsername = 'your_bot_username';
    window.open(`https://t.me/${botUsername}?start=cons_${consId}`, '_blank');
    return;
  }
  
  // Используем Telegram Web App API для открытия бота
  const botUsername = 'your_bot_username';
  tg.openTelegramLink(`https://t.me/${botUsername}?start=cons_${consId}`);
  
  // Закрываем Web App после открытия бота
  setTimeout(() => {
    tg.close();
  }, 500);
}
```

## Работа со списком консультаций

### Отображение кнопки для существующих консультаций

На странице со списком консультаций для каждой консультации, созданной через Telegram (`source === "TELEGRAM"`), должна быть кнопка "Продолжить в Telegram" вместо кнопки "Открыть чат на сайте".

```javascript
function renderConsultationActions(consultation) {
  const isTelegram = window.Telegram && window.Telegram.WebApp;
  const isTelegramConsultation = consultation.source === "TELEGRAM";
  
  if (isTelegram && isTelegramConsultation) {
    return (
      <button onClick={() => openTelegramChat(consultation.cons_id)}>
        Продолжить в Telegram
      </button>
    );
  } else {
    return (
      <button onClick={() => openChatOnSite(consultation.cons_id)}>
        Открыть чат
      </button>
    );
  }
}
```

## Мобильная версия страницы subscriptions

### Адаптация для Telegram Web App

При открытии из Telegram Web App страница должна:
1. Определять, что сеанс из Telegram
2. Адаптировать UI для мобильного экрана
3. Использовать стили, подходящие для Telegram Web App

### Рекомендации по UI

```javascript
// Определение Telegram сеанса и применение стилей
useEffect(() => {
  const tg = window.Telegram?.WebApp;
  
  if (tg) {
    // Применяем стили для Telegram Web App
    document.body.classList.add('telegram-webapp');
    
    // Используем тему Telegram
    const theme = tg.colorScheme; // 'light' или 'dark'
    document.body.setAttribute('data-theme', theme);
    
    // Используем цвета Telegram
    const bgColor = tg.backgroundColor;
    const textColor = tg.headerColor;
    
    // Применяем цвета к странице
    document.documentElement.style.setProperty('--tg-theme-bg-color', bgColor);
    document.documentElement.style.setProperty('--tg-theme-text-color', textColor);
  }
}, []);
```

### CSS для Telegram Web App

```css
/* Стили для Telegram Web App */
.telegram-webapp {
  /* Адаптация под мобильный экран */
  padding-bottom: env(safe-area-inset-bottom);
}

.telegram-webapp .consultation-form {
  /* Упрощенная форма для мобильных */
  max-width: 100%;
  padding: 1rem;
}

.telegram-webapp .button-continue-telegram {
  /* Выделенная кнопка для Telegram */
  background-color: var(--tg-theme-button-color, #3390ec);
  color: var(--tg-theme-button-text-color, #ffffff);
  width: 100%;
  padding: 1rem;
  border-radius: 8px;
  font-size: 1.1rem;
}
```

## Обработка ошибок

### Обработка отсутствия Telegram Web App

```javascript
function getTelegramUserData() {
  try {
    if (!window.Telegram?.WebApp) {
      return null;
    }
    
    const tg = window.Telegram.WebApp;
    const user = tg.initDataUnsafe?.user;
    
    return user ? {
      telegram_user_id: user.id,
      phone_number: user.phone_number || null,
      username: user.username || null,
      first_name: user.first_name || null,
      last_name: user.last_name || null
    } : null;
  } catch (error) {
    console.error('Error getting Telegram user data:', error);
    return null;
  }
}
```

## Пример полной реализации

### Компонент создания консультации

```javascript
import { useState, useEffect } from 'react';

function ConsultationForm() {
  const [isTelegram, setIsTelegram] = useState(false);
  const [telegramData, setTelegramData] = useState(null);
  
  useEffect(() => {
    // Проверяем наличие Telegram Web App
    const tg = window.Telegram?.WebApp;
    
    if (tg) {
      setIsTelegram(true);
      tg.ready();
      tg.expand();
      
      // Получаем данные пользователя
      const user = tg.initDataUnsafe?.user;
      if (user) {
        setTelegramData({
          telegram_user_id: user.id,
          phone_number: user.phone_number || null,
          username: user.username || null,
          first_name: user.first_name || null,
          last_name: user.last_name || null
        });
      }
    }
  }, []);
  
  const handleSubmit = async (formData) => {
    const payload = {
      client: formData.client,
      consultation: formData.consultation,
      source: isTelegram ? "TELEGRAM" : "SITE",
      ...(telegramData && {
        telegram_user_id: telegramData.telegram_user_id,
        telegram_phone_number: telegramData.phone_number
      })
    };
    
    const response = await fetch('/api/consultations/create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Front-Secret': process.env.REACT_APP_FRONT_SECRET
      },
      body: JSON.stringify(payload)
    });
    
    const result = await response.json();
    
    if (result.consultation && isTelegram) {
      // Показываем кнопку "Продолжить в Telegram"
      showContinueButton(result.consultation.cons_id);
    }
  };
  
  const openTelegramChat = (consId) => {
    const tg = window.Telegram?.WebApp;
    const botUsername = 'your_bot_username';
    
    if (tg) {
      tg.openTelegramLink(`https://t.me/${botUsername}?start=cons_${consId}`);
      setTimeout(() => tg.close(), 500);
    } else {
      window.open(`https://t.me/${botUsername}?start=cons_${consId}`, '_blank');
    }
  };
  
  return (
    <form onSubmit={handleSubmit}>
      {/* Форма создания консультации */}
      {/* ... */}
      
      {isTelegram && (
        <div className="telegram-notice">
          Вы создаете заявку через Telegram. После создания вы сможете продолжить общение в чате бота.
        </div>
      )}
    </form>
  );
}
```

## Рекомендации

### 1. Определение источника

- Всегда проверяйте наличие `window.Telegram.WebApp` перед использованием
- Используйте флаг `isTelegram` для условного рендеринга
- Передавайте `source: "TELEGRAM"` только если сеанс действительно из Telegram

### 2. Передача данных Telegram

- Передавайте `telegram_user_id` и `telegram_phone_number` только если они доступны
- Не передавайте `null` значения, если данные недоступны
- Обрабатывайте случаи, когда пользователь не разрешил доступ к контакту

### 3. Deep Links

- Используйте формат: `https://t.me/{bot_username}?start=cons_{cons_id}`
- Бот обработает параметр `cons_{cons_id}` и откроет чат с историей
- Убедитесь, что имя бота указано правильно (без @)

### 4. UI/UX

- Адаптируйте интерфейс для мобильных устройств при открытии из Telegram
- Используйте цвета и стили Telegram Web App для лучшей интеграции
- Показывайте понятные сообщения о переходе в Telegram

### 5. Обработка ошибок

- Всегда обрабатывайте случаи, когда Telegram Web App недоступен
- Предоставляйте fallback для обычных браузеров
- Логируйте ошибки для отладки

### 6. Безопасность

- Не полагайтесь только на данные из `initDataUnsafe` для критических операций
- Валидируйте данные на сервере
- Используйте `initData` и проверку подписи для production (если требуется)

## Тестирование

### Тестирование в Telegram

1. Создайте бота через BotFather
2. Настройте Web App URL в настройках бота
3. Откройте бота в Telegram
4. Нажмите кнопку запуска Web App
5. Проверьте определение Telegram сеанса
6. Создайте консультацию
7. Проверьте переход в чат бота

### Тестирование в обычном браузере

1. Откройте сайт в обычном браузере
2. Убедитесь, что `source: "SITE"` передается
3. Убедитесь, что кнопка "Продолжить в Telegram" не отображается
4. Проверьте, что все функции работают как обычно

## Чеклист реализации

- [ ] Добавить проверку наличия Telegram Web App
- [ ] Реализовать получение данных Telegram пользователя
- [ ] Обновить запрос создания консультации (добавить `telegram_user_id`, `telegram_phone_number`, `source`)
- [ ] Добавить кнопку "Продолжить в Telegram" после создания консультации
- [ ] Реализовать deep link для перехода в бот
- [ ] Обновить список консультаций (кнопка для Telegram консультаций)
- [ ] Адаптировать UI для мобильных устройств (Telegram Web App)
- [ ] Добавить обработку ошибок
- [ ] Протестировать в Telegram
- [ ] Протестировать в обычном браузере

## Дополнительные ресурсы

- [Telegram Web App Documentation](https://core.telegram.org/bots/webapps)
- [Telegram Bot API](https://core.telegram.org/bots/api)
- [Deep Linking](https://core.telegram.org/bots/features#deep-linking)

