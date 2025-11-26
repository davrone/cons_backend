import os
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
    
    # Application
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 7070
    ENV: str = "dev"
    DEBUG: bool = False
    
    # Chatwoot API
    CHATWOOT_API_URL: str = ""
    CHATWOOT_API_TOKEN: str = ""
    CHATWOOT_ACCOUNT_ID: str = ""
    
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
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Получить настройки (кэшируется)"""
    return Settings()


settings = get_settings()
