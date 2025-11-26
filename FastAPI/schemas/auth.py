"""Схемы для аутентификации"""
from pydantic import BaseModel
from typing import Optional


class LoginRequest(BaseModel):
    """Запрос на вход через OpenID"""
    token: str  # OpenID токен для проверки


class LoginResponse(BaseModel):
    """Ответ на вход"""
    access_token: str
    token_type: str = "bearer"
    user_id: Optional[str] = None

