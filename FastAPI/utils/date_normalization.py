"""
Утилиты для нормализации дат и времени к единому формату с timezone.
"""
from datetime import datetime, timezone
from typing import Optional, Union
from dateutil import parser as date_parser


def normalize_datetime(
    dt: Optional[Union[str, datetime]],
    default_timezone: timezone = timezone.utc
) -> Optional[datetime]:
    """
    Нормализует дату/время к datetime с timezone (UTC по умолчанию).
    
    Args:
        dt: Дата/время (строка или datetime объект)
        default_timezone: Timezone по умолчанию если timezone отсутствует
    
    Returns:
        datetime объект с timezone или None
    """
    if dt is None:
        return None
    
    if isinstance(dt, datetime):
        # Если уже datetime, нормализуем timezone
        if dt.tzinfo is None:
            return dt.replace(tzinfo=default_timezone)
        return dt.astimezone(default_timezone)
    
    if isinstance(dt, str):
        try:
            # Парсим строку
            parsed = date_parser.parse(dt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=default_timezone)
            return parsed.astimezone(default_timezone)
        except (ValueError, TypeError) as e:
            # Если не удалось распарсить, возвращаем None
            return None
    
    return None


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Убеждается, что datetime имеет timezone и конвертирует в UTC.
    
    Args:
        dt: datetime объект
    
    Returns:
        datetime в UTC или None
    """
    if dt is None:
        return None
    
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(timezone.utc)

