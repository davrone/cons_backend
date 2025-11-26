"""Схемы для вебхуков"""
from pydantic import BaseModel
from typing import Optional, Dict, Any


class WebhookResponse(BaseModel):
    """Ответ на вебхук"""
    status: str = "ok"
    message: Optional[str] = None

