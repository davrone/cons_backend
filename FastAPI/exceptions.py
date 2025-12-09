"""
Кастомные исключения для приложения.
"""
from typing import Optional, Dict, Any


class ConsultationError(Exception):
    """Базовое исключение для ошибок консультаций"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ConsultationNotFoundError(ConsultationError):
    """Консультация не найдена"""
    pass


class ConsultationLimitExceededError(ConsultationError):
    """Превышен лимит консультаций"""
    pass


class ClientNotFoundError(ConsultationError):
    """Клиент не найден"""
    pass


class SyncError(ConsultationError):
    """Ошибка синхронизации с внешними системами"""
    def __init__(self, message: str, system: str, details: Optional[Dict[str, Any]] = None):
        self.system = system
        super().__init__(message, details)


class ChatwootError(SyncError):
    """Ошибка взаимодействия с Chatwoot"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "CHATWOOT", details)


class OneCError(SyncError):
    """Ошибка взаимодействия с 1C:ЦЛ"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "1C_CL", details)


class ValidationError(ConsultationError):
    """Ошибка валидации данных"""
    pass


class NotificationError(ConsultationError):
    """Ошибка отправки уведомления"""
    pass

