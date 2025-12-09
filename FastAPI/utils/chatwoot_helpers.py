"""
Общие вспомогательные функции для работы с Chatwoot.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from ..services.chatwoot_client import ChatwootClient


def build_conversation_message(
    consultation_type: Optional[str] = None,
    comment: Optional[str] = None,
    scheduled_at: Optional[datetime] = None
) -> str:
    """
    Формирует сообщение для создания conversation в Chatwoot.
    
    Args:
        consultation_type: Тип консультации
        comment: Комментарий/описание
        scheduled_at: Запланированная дата
    
    Returns:
        Текст сообщения для conversation
    """
    parts = []
    
    if consultation_type:
        parts.append(f"Тип: {consultation_type}")
    
    if scheduled_at:
        # Форматируем дату в читаемый формат
        if scheduled_at.tzinfo:
            scheduled_at = scheduled_at.astimezone(timezone.utc)
        date_str = scheduled_at.strftime("%d.%m.%Y %H:%M")
        parts.append(f"Запланировано на: {date_str}")
    
    if comment:
        parts.append(f"\n{comment}")
    
    return "\n".join(parts) if parts else "Консультация"


def normalize_chatwoot_status(status: Optional[str]) -> str:
    """
    Нормализует статус консультации для Chatwoot.
    
    Args:
        status: Статус из нашей системы
    
    Returns:
        Статус для Chatwoot (open, pending, resolved, closed)
    """
    if not status:
        return "open"
    
    status_lower = status.lower()
    
    # Маппинг наших статусов в статусы Chatwoot
    status_mapping = {
        "new": "open",
        "open": "open",
        "pending": "pending",
        "in_progress": "pending",
        "resolved": "resolved",
        "closed": "resolved",
        "cancelled": "resolved",
    }
    
    return status_mapping.get(status_lower, "open")


def build_custom_attributes_summary(custom_attrs: Dict[str, Any]) -> str:
    """
    Формирует краткое описание custom_attributes для логирования.
    
    Args:
        custom_attrs: Словарь custom_attributes
    
    Returns:
        Строка с кратким описанием
    """
    if not custom_attrs:
        return "empty"
    
    keys = list(custom_attrs.keys())
    if len(keys) <= 3:
        return f"{len(keys)} fields: {', '.join(keys)}"
    else:
        return f"{len(keys)} fields: {', '.join(keys[:3])}, ..."


async def ensure_labels_exist(labels: List[str]) -> None:
    """
    Убеждается, что все labels существуют в Chatwoot.
    
    Args:
        labels: Список labels для проверки
    """
    if not labels:
        return
    
    chatwoot_client = ChatwootClient()
    for label in labels:
        if label:
            await chatwoot_client.ensure_label_exists(label)


def format_chatwoot_error(error: Exception) -> str:
    """
    Форматирует ошибку Chatwoot в читаемый формат.
    
    Args:
        error: Исключение от Chatwoot API
    
    Returns:
        Отформатированное сообщение об ошибке
    """
    error_str = str(error)
    
    # Извлекаем полезную информацию из ошибки
    if hasattr(error, "response"):
        try:
            import httpx
            if isinstance(error, httpx.HTTPStatusError):
                status_code = error.response.status_code
                try:
                    error_body = error.response.json()
                    detail = error_body.get("error", error_body.get("message", error_str))
                    return f"Chatwoot API error {status_code}: {detail}"
                except:
                    return f"Chatwoot API error {status_code}: {error_str}"
        except:
            pass
    
    return f"Chatwoot error: {error_str}"

