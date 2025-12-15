import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from functools import lru_cache


class Settings(BaseSettings):
    """Настройки приложения из переменных окружения"""
    
    # Database
    DB_HOST: str = "host.docker.internal"
    DB_PORT: int = 5432
    DB_NAME: str = "cons_backend"
    DB_USER: str = "postgres"
    DB_PASS: str = "qwerty123"
    
    # Database Connection Pool
    DB_POOL_SIZE: int = Field(default=20, description="Базовый размер пула соединений к БД")
    DB_MAX_OVERFLOW: int = Field(default=10, description="Максимальное количество дополнительных соединений при перегрузке")
    DB_POOL_TIMEOUT: int = Field(default=30, description="Таймаут ожидания свободного соединения из пула (секунды)")
    DB_POOL_RECYCLE: int = Field(default=3600, description="Время переиспользования соединений (секунды)")
    
    # Application
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 7070
    ENV: str = "dev"
    DEBUG: bool = False
    FRONT_SECRET: str = ""
    FRONT_BEARER_TOKEN: str = ""
    
    # CORS
    ALLOWED_ORIGINS: str = "*"  # Разрешенные источники через запятую, или "*" для всех
    
    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = Field(default=100, description="Общий лимит запросов в минуту")
    RATE_LIMIT_CREATE_PER_MINUTE: int = Field(default=10, description="Лимит создания консультаций в минуту")
    
    # Chatwoot Bot ID (опционально, будет определен автоматически если не указан)
    CHATWOOT_BOT_ID: Optional[int] = None
    
    # Chatwoot API
    CHATWOOT_API_URL: str = ""
    CHATWOOT_API_TOKEN: str = ""
    CHATWOOT_ACCOUNT_ID: str = ""
    CHATWOOT_INBOX_ID: Optional[int] = None  # ID inbox для создания conversations (Application API)
    CHATWOOT_INBOX_IDENTIFIER: Optional[str] = None  # Identifier inbox для Public API (используется для создания contacts и conversations)
    
    # 1C:ЦЛ API
    ONEC_API_URL: str = ""
    ONEC_API_TOKEN: str = ""
    
    # OData (1C:CL)
    # Поддержка ODATA_BASEURL (без подчеркивания) для совместимости
    ODATA_BASE_URL: str = ""
    ODATA_BASEURL_CL: str = ""  # Для ЦЛ (1C:CL)
    ODATA_USER: str = ""
    ODATA_PASSWORD: str = ""
    ODATA_PAGE_SIZE: int = 1000
    
    def __init__(self, **kwargs):
        """Инициализация с поддержкой ODATA_BASEURL"""
        super().__init__(**kwargs)
        # Если ODATA_BASEURL_CL пустой, пробуем прочитать из env
        if not self.ODATA_BASEURL_CL:
            self.ODATA_BASEURL_CL = os.getenv("ODATA_BASEURL_CL", "")

    # OpenID для аутентификации
    OPENID_ISSUER: str = ""
    OPENID_CLIENT_ID: str = ""
    OPENID_CLIENT_SECRET: str = ""
    
    # Ограничения на создание консультаций
    MAX_FUTURE_CONSULTATION_DAYS: int = Field(default=30, description="Максимальное количество дней вперед для создания консультации")
    
    # Время для аннулирования консультации
    CANCEL_CONSULTATION_TIMEOUT_MINUTES: int = Field(default=30, description="Время в минутах с момента создания, в течение которого можно аннулировать консультацию")
    
    # Автор по умолчанию для создания ТелефонныйЗвонок в ЦЛ
    ONEC_DEFAULT_AUTHOR_NAME: str = Field(default="<не определено>", description="Название менеджера (description) из справочника users для использования как Автор_Key при создании консультаций в ЦЛ")
    
    # Telegram Bot
    TELEGRAM_BOT_TOKEN: str = Field(default="", description="Токен Telegram бота от BotFather")
    TELEGRAM_WEBHOOK_URL: Optional[str] = Field(default=None, description="URL для webhook от Telegram (опционально, для production)")
    TELEGRAM_WEBHOOK_SECRET: Optional[str] = Field(default=None, description="Секрет для проверки webhook от Telegram (опционально)")
    TELEGRAM_WEBAPP_URL: Optional[str] = Field(default=None, description="URL для Telegram Web App (фронтенд, опционально, если не указан, используется базовый URL из TELEGRAM_WEBHOOK_URL)")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Получить настройки (кэшируется)"""
    return Settings()


settings = get_settings()
