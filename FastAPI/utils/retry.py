"""
Утилиты для retry механизмов.
"""
import asyncio
import logging
from typing import Callable, TypeVar, Optional, List, Type
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')


async def retry_async(
    func: Callable[..., T],
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> T:
    """
    Асинхронный retry механизм с экспоненциальной задержкой.
    
    Args:
        func: Асинхронная функция для выполнения
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка в секундах
        backoff: Множитель для экспоненциальной задержки
        exceptions: Кортеж исключений, при которых нужно повторять попытку
        on_retry: Callback функция, вызываемая при каждой попытке (attempt_number, exception)
    
    Returns:
        Результат выполнения функции
    
    Raises:
        Последнее исключение если все попытки исчерпаны
    """
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            
            if attempt < max_attempts:
                wait_time = delay * (backoff ** (attempt - 1))
                logger.warning(
                    f"Attempt {attempt}/{max_attempts} failed: {e}. "
                    f"Retrying in {wait_time:.2f}s..."
                )
                
                if on_retry:
                    try:
                        on_retry(attempt, e)
                    except Exception as callback_error:
                        logger.warning(f"Error in retry callback: {callback_error}")
                
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    f"All {max_attempts} attempts failed. Last error: {e}",
                    exc_info=True
                )
    
    raise last_exception


def retry_sync(
    func: Callable[..., T],
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> T:
    """
    Синхронный retry механизм с экспоненциальной задержкой.
    
    Args:
        func: Синхронная функция для выполнения
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка в секундах
        backoff: Множитель для экспоненциальной задержки
        exceptions: Кортеж исключений, при которых нужно повторять попытку
        on_retry: Callback функция, вызываемая при каждой попытке (attempt_number, exception)
    
    Returns:
        Результат выполнения функции
    
    Raises:
        Последнее исключение если все попытки исчерпаны
    """
    import time
    
    last_exception = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            
            if attempt < max_attempts:
                wait_time = delay * (backoff ** (attempt - 1))
                logger.warning(
                    f"Attempt {attempt}/{max_attempts} failed: {e}. "
                    f"Retrying in {wait_time:.2f}s..."
                )
                
                if on_retry:
                    try:
                        on_retry(attempt, e)
                    except Exception as callback_error:
                        logger.warning(f"Error in retry callback: {callback_error}")
                
                time.sleep(wait_time)
            else:
                logger.error(
                    f"All {max_attempts} attempts failed. Last error: {e}",
                    exc_info=True
                )
    
    raise last_exception


def retry_decorator(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Декоратор для автоматического retry асинхронных функций.
    
    Usage:
        @retry_decorator(max_attempts=3, delay=1.0)
        async def my_function():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async def attempt():
                return await func(*args, **kwargs)
            return await retry_async(
                attempt,
                max_attempts=max_attempts,
                delay=delay,
                backoff=backoff,
                exceptions=exceptions
            )
        return wrapper
    return decorator

