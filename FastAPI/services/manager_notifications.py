"""
Сервис для отправки уведомлений о переназначении менеджеров и изменении очереди.
"""
import logging
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.chatwoot_client import ChatwootClient
from ..services.manager_selector import ManagerSelector
from ..models import Consultation, User, UserMapping
from ..utils.notification_helpers import check_and_log_notification
from ..config import settings
from sqlalchemy import select

logger = logging.getLogger(__name__)


def is_valid_chatwoot_conversation_id(cons_id: Optional[str]) -> bool:
    """
    Проверяет, является ли cons_id валидным числовым ID Chatwoot.
    
    Chatwoot использует числовые ID для conversations (например, 12345),
    а не UUID. Если cons_id это UUID или временный ID (temp_, cl_), 
    то это невалидный ID для запросов к Chatwoot.
    
    Args:
        cons_id: ID консультации из БД
        
    Returns:
        True если cons_id валидный числовой ID Chatwoot, False иначе
    """
    if not cons_id:
        return False
    
    # Пропускаем временные ID
    if cons_id.startswith(("temp_", "cl_")):
        return False
    
    # Проверяем, что это числовой ID (не UUID)
    # UUID имеет формат: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 символов с дефисами)
    # Числовой ID: только цифры
    if len(cons_id) > 10:  # UUID обычно длиннее 10 символов
        # Проверяем, не UUID ли это (содержит дефисы)
        if '-' in cons_id:
            return False
    
    # Проверяем, что это число (или строка из цифр)
    return cons_id.isdigit()


async def send_manager_reassignment_notification(
    db: AsyncSession,
    consultation: Consultation,
    old_manager_key: Optional[str],
    new_manager_key: Optional[str],
    reason: Optional[str] = None,
) -> None:
    """
    Отправить уведомление о переназначении менеджера в Chatwoot.
    
    Args:
        db: Сессия БД
        consultation: Консультация
        old_manager_key: Старый менеджер (cl_ref_key)
        new_manager_key: Новый менеджер (cl_ref_key)
        reason: Причина переназначения (опционально)
    """
    if not is_valid_chatwoot_conversation_id(consultation.cons_id):
        logger.debug(f"Skipping notification for consultation {consultation.cons_id} (invalid Chatwoot ID - UUID or temporary)")
        return
    
    chatwoot_client = ChatwootClient()
    
    # Получаем имена менеджеров
    old_manager_name = "Не назначен"
    new_manager_name = "Не назначен"
    
    if old_manager_key:
        from sqlalchemy import select
        old_manager_result = await db.execute(
            select(User).where(User.cl_ref_key == old_manager_key).limit(1)
        )
        old_manager = old_manager_result.scalar_one_or_none()
        if old_manager:
            old_manager_name = old_manager.description or old_manager.user_id or old_manager_key[:8]
    
    if new_manager_key:
        from sqlalchemy import select
        new_manager_result = await db.execute(
            select(User).where(User.cl_ref_key == new_manager_key).limit(1)
        )
        new_manager = new_manager_result.scalar_one_or_none()
        if new_manager:
            new_manager_name = new_manager.description or new_manager.user_id or new_manager_key[:8]
    
    # Формируем сообщение
    message_parts = ["👤 Менеджер переназначен"]
    if old_manager_key:
        message_parts.append(f"\nБыло: {old_manager_name}")
    message_parts.append(f"\nСтало: {new_manager_name}")
    
    if reason:
        message_parts.append(f"\nПричина: {reason}")
    
    message = "".join(message_parts)
    
    # Проверяем, не было ли уже отправлено такое уведомление
    # ВАЖНО: Нормализуем ключи для стабильного хеша (None -> "")
    # Это предотвращает разные хеши для одного и того же переназначения
    normalized_old_manager = old_manager_key if old_manager_key else ""
    normalized_new_manager = new_manager_key if new_manager_key else ""
    normalized_reason = reason if reason else ""
    
    notification_data = {
        "old_manager_key": normalized_old_manager,
        "new_manager_key": normalized_new_manager,
        "reason": normalized_reason
    }
    # ВАЖНО: Используем отдельную транзакцию для сохранения NotificationLog,
    # чтобы запись не потерялась при rollback основной транзакции
    already_sent = await check_and_log_notification(
        db=db,
        notification_type="manager_reassignment",
        entity_id=consultation.cons_id,
        data=notification_data,
        use_separate_transaction=True  # Используем отдельную транзакцию для надежности
    )
    if already_sent:
        logger.debug(f"Manager reassignment notification already sent for consultation {consultation.cons_id}, skipping")
        return
    
    try:
        # Используем send_message вместо send_note, так как note сообщения не видны клиенту
        await chatwoot_client.send_message(
            conversation_id=consultation.cons_id,
            content=message,
            message_type="outgoing"
        )
        
        logger.info(f"Sent manager reassignment notification to Chatwoot for consultation {consultation.cons_id}")
        
        # ВАЖНО: Обновляем агента в conversation через Chatwoot API
        # Это синхронизирует назначение менеджера в Chatwoot с данными из ЦЛ
        if new_manager_key:
            assignee_id = None
            
            # Сначала пытаемся найти маппинг через таблицу user_mapping
            mapping_result = await db.execute(
                select(UserMapping).where(UserMapping.cl_manager_key == new_manager_key).limit(1)
            )
            mapping = mapping_result.scalar_one_or_none()
            if mapping:
                assignee_id = mapping.chatwoot_user_id
                logger.info(f"Mapped manager {new_manager_key} to Chatwoot user {assignee_id} via UserMapping")
            else:
                # Если маппинга нет, пробуем найти через таблицу users
                user_result = await db.execute(
                    select(User).where(
                        User.cl_ref_key == new_manager_key,
                        User.deletion_mark == False,
                        User.invalid == False
                    ).limit(1)
                )
                user = user_result.scalar_one_or_none()
                if user and user.chatwoot_user_id:
                    assignee_id = user.chatwoot_user_id
                    logger.info(f"Found Chatwoot user {assignee_id} for manager {new_manager_key} via User table")
            
            # Обновляем агента в Chatwoot conversation через отдельный endpoint /assignments
            # ВАЖНО: Используем assign_conversation_agent, а не update_conversation
            # Это правильный способ назначения агента в Chatwoot
            if assignee_id:
                try:
                    await chatwoot_client.assign_conversation_agent(
                        conversation_id=consultation.cons_id,
                        assignee_id=assignee_id
                    )
                    logger.info(f"Assigned agent {assignee_id} to conversation {consultation.cons_id} (manager {new_manager_key})")
                except Exception as assign_error:
                    logger.warning(
                        f"Failed to assign agent to conversation in Chatwoot for consultation {consultation.cons_id}: {assign_error}",
                        exc_info=True
                    )
            else:
                logger.warning(
                    f"Manager {new_manager_key} not found in Chatwoot (no mapping or chatwoot_user_id). "
                    f"Conversation {consultation.cons_id} assignee not updated. "
                    f"Please run sync_users_to_chatwoot.py to sync this user."
                )
        else:
            # Если new_manager_key None, снимаем назначение агента
            try:
                await chatwoot_client.update_conversation(
                    conversation_id=consultation.cons_id,
                    assignee_id=None  # Снимаем назначение
                )
                logger.info(f"Removed assignee from conversation {consultation.cons_id} (manager was unassigned)")
            except Exception as unassign_error:
                logger.warning(
                    f"Failed to remove conversation assignee in Chatwoot for consultation {consultation.cons_id}: {unassign_error}"
                )
                
    except Exception as e:
        logger.error(f"Failed to send manager reassignment notification: {e}", exc_info=True)


async def send_queue_update_notification(
    db: AsyncSession,
    consultation: Consultation,
    manager_key: Optional[str] = None,
) -> None:
    """
    Отправить уведомление об изменении очереди клиенту.
    
    ВАЖНО: Для "Техническая поддержка" уведомления об очереди не отправляются,
    так как там нет очередей - консультанты сами забирают заявки.
    
    Args:
        db: Сессия БД
        consultation: Консультация
        manager_key: Ключ менеджера (если не указан, берется из consultation.manager)
    """
    if not is_valid_chatwoot_conversation_id(consultation.cons_id):
        logger.debug(f"Skipping queue notification for consultation {consultation.cons_id} (invalid Chatwoot ID - UUID or temporary)")
        return
    
    # ВАЖНО: Для "Техническая поддержка" не отправляем уведомления об очереди
    if consultation.consultation_type == "Техническая поддержка":
        logger.debug(f"Skipping queue notification for consultation {consultation.cons_id} (Техническая поддержка - no queue)")
        return
    
    manager_key = manager_key or consultation.manager
    if not manager_key:
        logger.debug(f"No manager key for consultation {consultation.cons_id}, skipping queue notification")
        return
    
    chatwoot_client = ChatwootClient()
    manager_selector = ManagerSelector(db)
    
    try:
        # Рассчитываем позицию в очереди и время ожидания
        wait_info = await manager_selector.calculate_wait_time(manager_key)
        queue_position = wait_info["queue_position"]
        show_range = wait_info.get("show_range", False)
        
        # Формируем сообщение с учетом диапазона времени ожидания
        if show_range:
            # Показываем диапазон: от (статистика * очередь) до (15 минут * очередь)
            wait_min_minutes = wait_info["estimated_wait_minutes_min"]
            wait_max_minutes = wait_info["estimated_wait_minutes_max"]
            
            wait_min_hours = round(wait_min_minutes / 60)
            wait_max_hours = round(wait_max_minutes / 60)
            
            # Формируем сообщение в зависимости от settings.SEND_QUEUE_WAIT_TIME_MESSAGE
            queue_message = f"📊 Вы в очереди #{queue_position}."
            
            if settings.SEND_QUEUE_WAIT_TIME_MESSAGE:
                # Добавляем информацию о времени ожидания
                if wait_min_hours == 0 and wait_max_hours == 0:
                    queue_message += f" Примерное время ожидания: от {wait_min_minutes} до {wait_max_minutes} минут."
                elif wait_min_hours == 0:
                    hours_text_max = "час" if wait_max_hours == 1 else "часа" if wait_max_hours < 5 else "часов"
                    queue_message += f" Примерное время ожидания: от {wait_min_minutes} минут до {wait_max_hours} {hours_text_max}."
                else:
                    hours_text_min = "час" if wait_min_hours == 1 else "часа" if wait_min_hours < 5 else "часов"
                    hours_text_max = "час" if wait_max_hours == 1 else "часа" if wait_max_hours < 5 else "часов"
                    queue_message += f" Примерное время ожидания: от {wait_min_hours} {hours_text_min} до {wait_max_hours} {hours_text_max}."
            else:
                queue_message += " (Подробнее время ожидания вы узнаете в чате)"
            
            message = queue_message
        else:
            # Показываем одно значение
            wait_hours = wait_info["estimated_wait_hours"]
            queue_message = f"📊 Вы в очереди #{queue_position}."
            
            if settings.SEND_QUEUE_WAIT_TIME_MESSAGE:
                # Добавляем информацию о времени ожидания
                if wait_hours == 0:
                    wait_minutes = wait_info["estimated_wait_minutes"]
                    queue_message = f"✅ Вы в очереди #{queue_position}. Примерное время ожидания: {wait_minutes} минут."
                else:
                    hours_text = "час" if wait_hours == 1 else "часа" if wait_hours < 5 else "часов"
                    queue_message = f"📊 Вы в очереди #{queue_position}. Примерное время ожидания: {wait_hours} {hours_text}."
            else:
                queue_message += " (Подробнее время ожидания вы узнаете в чате)"
            
            message = queue_message
        
        # Генерируем уникальный ключ уведомления
        # ВАЖНО: Нормализуем manager_key для стабильного хеша (None -> "")
        # НЕ включаем wait_info в хеш, так как он может меняться каждый раз
        # Используем только manager_key и queue_position для идентификации уведомления
        normalized_manager_key = manager_key if manager_key else ""
        notification_data = {
            "manager_key": normalized_manager_key,
            "queue_position": queue_position
            # НЕ включаем wait_info - он может меняться, но уведомление должно быть одно для одной позиции в очереди
        }
        
        # Проверяем, не было ли уже отправлено такое уведомление
        # ВАЖНО: Используем отдельную транзакцию для сохранения NotificationLog,
        # чтобы запись не потерялась при rollback основной транзакции
        already_sent = await check_and_log_notification(
            db=db,
            notification_type="queue_update",
            entity_id=consultation.cons_id,
            data=notification_data,
            use_separate_transaction=True  # Используем отдельную транзакцию для надежности
        )
        if already_sent:
            logger.debug(f"Queue update notification already sent for consultation {consultation.cons_id}, skipping")
            return
        
        # Используем send_message вместо send_note, так как note сообщения не видны клиенту
        await chatwoot_client.send_message(
            conversation_id=consultation.cons_id,
            content=message,
            message_type="outgoing"
        )
        
        wait_info_str = f"position={queue_position}"
        if show_range:
            wait_info_str += f", wait_range={wait_info['estimated_wait_minutes_min']}-{wait_info['estimated_wait_minutes_max']}min"
        else:
            wait_info_str += f", wait={wait_info['estimated_wait_minutes']}min"
        
        logger.info(
            f"Sent queue update notification to Chatwoot for consultation {consultation.cons_id}: {wait_info_str}"
        )
    except Exception as e:
        logger.error(f"Failed to send queue update notification: {e}", exc_info=True)

