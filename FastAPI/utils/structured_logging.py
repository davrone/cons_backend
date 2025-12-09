"""
Утилиты для структурированного логирования.
"""
import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    """
    Форматтер для структурированного логирования в JSON формате.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Форматирует запись лога в JSON формат.
        """
        log_data: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Добавляем контекстные данные если есть
        if hasattr(record, "context"):
            log_data["context"] = record.context
        
        # Добавляем исключение если есть
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Добавляем дополнительные поля
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in (
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info"
            )
        }
        if extra_fields:
            log_data["extra"] = extra_fields
        
        return json.dumps(log_data, ensure_ascii=False, default=str)


def get_structured_logger(name: str) -> logging.Logger:
    """
    Получить логгер с структурированным форматированием.
    
    Args:
        name: Имя логгера
    
    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    
    # Добавляем структурированный форматтер если его еще нет
    if not any(isinstance(h.formatter, StructuredFormatter) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
    
    return logger


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    context: Optional[Dict[str, Any]] = None,
    **kwargs
) -> None:
    """
    Логирует сообщение с контекстными данными.
    
    Args:
        logger: Логгер
        level: Уровень логирования
        message: Сообщение
        context: Контекстные данные
        **kwargs: Дополнительные поля для логирования
    """
    extra = kwargs.copy()
    if context:
        extra["context"] = context
    
    logger.log(level, message, extra=extra)

