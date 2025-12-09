"""
Утилиты для структурированного логирования в ETL скриптах.

Обеспечивает единый формат логов для всех ETL процессов:
- Четкие маркеры начала/конца процесса
- Структурированное логирование прогресса
- Единый формат ошибок
- Прогресс-индикаторы
"""
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class ETLLogger:
    """Класс для структурированного логирования ETL процессов"""
    
    def __init__(self, script_name: str, entity_name: str):
        """
        Инициализация логгера для ETL скрипта.
        
        Args:
            script_name: Имя скрипта (например, "pull_cons_cl")
            entity_name: Имя сущности (например, "Document_ТелефонныйЗвонок")
        """
        self.script_name = script_name
        self.entity_name = entity_name
        self.logger = logging.getLogger(script_name)
        self.start_time: Optional[datetime] = None
        self.total_processed = 0
        self.total_created = 0
        self.total_updated = 0
        self.total_errors = 0
    
    def start(self, config: Optional[Dict[str, Any]] = None):
        """Логирует начало ETL процесса"""
        self.start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info(f"[{self.script_name}] 🚀 Starting ETL process")
        self.logger.info(f"[{self.script_name}] Entity: {self.entity_name}")
        self.logger.info("=" * 80)
        
        if config:
            self.logger.info(f"[{self.script_name}] Configuration:")
            for key, value in config.items():
                self.logger.info(f"[{self.script_name}]   {key}: {value}")
    
    def sync_info(self, last_sync: Optional[datetime], from_date: str, buffer_days: Optional[int] = None):
        """Логирует информацию о синхронизации"""
        if last_sync:
            buffer_info = f" (buffer: {buffer_days} days)" if buffer_days else ""
            self.logger.info(
                f"[{self.script_name}] 📅 Incremental sync from {from_date} "
                f"(last sync: {last_sync}{buffer_info})"
            )
        else:
            self.logger.info(f"[{self.script_name}] 📅 First run — loading from {from_date}")
    
    def batch_start(self, batch_num: int, skip: int, batch_size: int):
        """Логирует начало обработки батча"""
        self.logger.debug(
            f"[{self.script_name}] 📦 Batch {batch_num}: fetching (skip={skip}, size={batch_size})"
        )
    
    def batch_progress(self, batch_num: int, batch_size: int, created: int = 0, updated: int = 0, errors: int = 0):
        """Логирует прогресс обработки батча"""
        self.total_processed += batch_size
        self.total_created += created
        self.total_updated += updated
        self.total_errors += errors
        
        if errors > 0:
            self.logger.warning(
                f"[{self.script_name}] ⚠️  Batch {batch_num}: {batch_size} items "
                f"(created={created}, updated={updated}, errors={errors})"
            )
        else:
            self.logger.info(
                f"[{self.script_name}] ✓ Batch {batch_num}: {batch_size} items "
                f"(created={created}, updated={updated})"
            )
    
    def batch_error(self, batch_num: int, error: Exception, skip: int = 0):
        """Логирует ошибку при обработке батча"""
        self.total_errors += 1
        self.logger.error(
            f"[{self.script_name}] ✗ Batch {batch_num} failed (skip={skip}): {error}",
            exc_info=True
        )
    
    def http_error(self, status_code: int, url: str, attempt: int, max_attempts: int, retry: bool = True):
        """Логирует HTTP ошибку"""
        if retry:
            self.logger.warning(
                f"[{self.script_name}] ⚠️  HTTP {status_code} — retry in {min(2 ** attempt, 60)}s "
                f"(attempt {attempt + 1}/{max_attempts + 1})"
            )
        else:
            self.logger.error(
                f"[{self.script_name}] ✗ HTTP {status_code} Client Error (no retry)"
            )
            self.logger.error(f"[{self.script_name}]   URL: {url[:200]}")
    
    def item_error(self, item_id: str, error: Exception, item_type: str = "item", full_traceback: bool = True):
        """Логирует ошибку при обработке отдельного элемента"""
        self.total_errors += 1
        if full_traceback:
            self.logger.error(
                f"[{self.script_name}] ✗ Error processing {item_type} {item_id[:20]}: {error}",
                exc_info=True
            )
        else:
            # Без полного traceback для уменьшения шума при множественных ошибках
            self.logger.error(
                f"[{self.script_name}] ✗ Error processing {item_type} {item_id[:20]}: {error}"
            )
    
    def sync_state_saved(self, sync_date: datetime, batch_num: Optional[int] = None):
        """Логирует сохранение sync_state"""
        if batch_num:
            self.logger.debug(
                f"[{self.script_name}] 💾 Sync state saved after batch {batch_num}: {sync_date}"
            )
        else:
            self.logger.info(
                f"[{self.script_name}] 💾 Final sync state saved: {sync_date}"
            )
    
    def finish(self, success: bool = True, error: Optional[Exception] = None):
        """Логирует завершение ETL процесса"""
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        self.logger.info("=" * 80)
        if success:
            self.logger.info(
                f"[{self.script_name}] ✅ Completed successfully "
                f"(processed={self.total_processed}, created={self.total_created}, "
                f"updated={self.total_updated}, errors={self.total_errors}, duration={duration:.1f}s)"
            )
        else:
            self.logger.error(
                f"[{self.script_name}] ❌ Failed "
                f"(processed={self.total_processed}, created={self.total_created}, "
                f"updated={self.total_updated}, errors={self.total_errors}, duration={duration:.1f}s)"
            )
            if error:
                self.logger.error(f"[{self.script_name}] Error: {error}", exc_info=True)
        self.logger.info("=" * 80)
    
    def critical_error(self, message: str, error: Optional[Exception] = None):
        """Логирует критическую ошибку, требующую остановки"""
        self.logger.error("=" * 80)
        self.logger.error(f"[{self.script_name}] 🚨 CRITICAL ERROR: {message}")
        if error:
            self.logger.error(f"[{self.script_name}] {error}", exc_info=True)
        self.logger.error("=" * 80)
