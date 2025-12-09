"""Роуты для обработки вебхуков от внешних систем"""
from fastapi import APIRouter, Request, HTTPException, Header, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
import hmac
import hashlib
import json
import logging
import asyncio
from datetime import datetime, timezone, time, date
from dateutil import parser as date_parser

from ..database import get_db, AsyncSessionLocal
from ..models import WebhookLog, Consultation, UserMapping
from ..services.onec_client import OneCClient
from ..schemas.webhooks import WebhookResponse
from ..schemas.tickets import parse_datetime_flexible
from ..config import settings
from ..services.chatwoot_client import ChatwootClient
from ..utils.change_log import log_consultation_change, mark_change_synced

logger = logging.getLogger(__name__)
router = APIRouter()


async def _sync_status_to_1c_background(cons_id: str, cl_ref_key: str, onec_status: str):
    """
    Фоновая задача для синхронизации статуса консультации с 1C:ЦЛ.
    Выполняется асинхронно, чтобы не блокировать webhook обработчик.
    
    ВАЖНО: Использует отдельную сессию БД, которая автоматически закрывается через async with.
    """
    db = None
    try:
        async with AsyncSessionLocal() as db:
            onec_client = OneCClient()
            await onec_client.update_consultation_odata(
                ref_key=cl_ref_key,
                status=onec_status,
            )
            await mark_change_synced(
                db=db,
                cons_id=cons_id,
                field_name="status",
                synced_to_1c=True
            )
            await db.commit()
            logger.info(f"Synced status change to 1C for consultation {cons_id}")
    except Exception as e:
        logger.warning(f"Failed to sync status change to 1C in background task: {e}", exc_info=True)
    finally:
        # Явно закрываем сессию для гарантии освобождения соединения
        if db:
            try:
                await db.close()
            except Exception:
                pass


async def _sync_manager_to_1c_background(cons_id: str, cl_ref_key: str, manager_key: str):
    """
    Фоновая задача для синхронизации менеджера консультации с 1C:ЦЛ.
    Выполняется асинхронно, чтобы не блокировать webhook обработчик.
    
    ВАЖНО: Использует отдельную сессию БД, которая автоматически закрывается через async with.
    """
    db = None
    try:
        async with AsyncSessionLocal() as db:
            onec_client = OneCClient()
            await onec_client.update_consultation_odata(
                ref_key=cl_ref_key,
                manager_key=manager_key,
            )
            await db.commit()
            logger.info(f"Synced manager reassignment to 1C for consultation {cons_id}")
    except Exception as e:
        logger.warning(f"Failed to sync manager reassignment to 1C in background task: {e}", exc_info=True)
    finally:
        # Явно закрываем сессию для гарантии освобождения соединения
        if db:
            try:
                await db.close()
            except Exception:
                pass


def verify_chatwoot_signature(payload: bytes, signature: str) -> bool:
    """Проверка подписи вебхука от Chatwoot"""
    if not settings.CHATWOOT_API_TOKEN:
        return True  # В dev режиме пропускаем
    
    expected_signature = hmac.new(
        settings.CHATWOOT_API_TOKEN.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected_signature, signature)


@router.post("/chatwoot", response_model=WebhookResponse)
async def chatwoot_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_chatwoot_signature: Optional[str] = Header(None)
):
    """
    Обработка вебхука от Chatwoot.
    
    События:
    - conversation.created
    - conversation.updated
    - message.created
    - message.updated
    """
    # Получаем payload
    body = await request.body()
    payload = json.loads(body)
    
    # Сохраняем в лог
    webhook_log = WebhookLog(
        source="CHATWOOT",
        payload=payload
    )
    db.add(webhook_log)
    await db.flush()
    
    # Проверяем подпись
    if x_chatwoot_signature:
        if not verify_chatwoot_signature(body, x_chatwoot_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Обрабатываем событие
    event_type = payload.get("event")
    event_data = payload.get("data", {})
    
    try:
        if event_type == "conversation.created":
            # Новая консультация создана в Chatwoot
            conversation = event_data.get("conversation", {})
            cons_id = str(conversation.get("id"))
            
            # Проверяем, есть ли уже в БД
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation:
                # Создаем новую запись
                consultation = Consultation(
                    cons_id=cons_id,
                    status=conversation.get("status"),
                    create_date=conversation.get("created_at"),
                )
                db.add(consultation)
        
        elif event_type == "conversation.updated":
            # Обновление консультации
            conversation = event_data.get("conversation", {})
            cons_id = str(conversation.get("id"))
            
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if consultation:
                status_changed = False
                manager_changed = False
                custom_attrs_changed = False
                
                # Обновляем поля в БД (Middleware - мастер-база)
                # НЕ отправляем данные в 1C через webhook - 1C обновляется только через ETL или когда middleware инициирует изменение
                if "status" in conversation:
                    old_status = consultation.status
                    new_status = conversation["status"]
                    if old_status != new_status:
                        # ВАЖНО: Запрещаем закрытие беседы клиентом для консультаций по ведению учета
                        # Закрытие должно происходить только через ЦЛ или Chatwoot (менеджером)
                        # Если клиент пытается закрыть беседу, откатываем статус обратно
                        if consultation.consultation_type == "Консультация по ведению учёта":
                            if new_status in ("resolved", "closed"):
                                # Откатываем статус обратно в Chatwoot
                                logger.warning(
                                    f"Attempt to close consultation {cons_id} by client denied "
                                    f"(consultation_type='Консультация по ведению учёта'). "
                                    f"Reverting status from {new_status} back to {old_status}"
                                )
                                try:
                                    chatwoot_client = ChatwootClient()
                                    # Откатываем статус обратно на предыдущий
                                    await chatwoot_client.update_conversation(
                                        conversation_id=cons_id,
                                        status=old_status or "open"  # Если old_status None, используем "open"
                                    )
                                    logger.info(f"Reverted conversation {cons_id} status back to {old_status or 'open'}")
                                except Exception as revert_error:
                                    logger.error(f"Failed to revert conversation {cons_id} status: {revert_error}", exc_info=True)
                                
                                # НЕ обновляем статус в БД и НЕ синхронизируем в ЦЛ
                                await db.commit()
                                return WebhookResponse(
                                    status="ok",
                                    message=f"Status change denied for consultation type 'Консультация по ведению учёта'"
                                )
                        
                        consultation.status = new_status
                        status_changed = True
                        # Логируем изменение статуса
                        await log_consultation_change(
                            db=db,
                            cons_id=cons_id,
                            field_name="status",
                            old_value=old_status,
                            new_value=new_status,
                            source="CHATWOOT"
                        )
                        logger.info(f"Status changed for consultation {cons_id}: {old_status} -> {new_status}")
                        
                        # Синхронизируем статус обратно в 1C:ЦЛ в фоновой задаче
                        # ВАЖНО: Выполняем в фоне, чтобы не блокировать webhook и не удерживать соединение с БД
                        if consultation.cl_ref_key:
                            # Маппим статус Chatwoot в статус 1C
                            status_mapping = {
                                "open": "new",
                                "resolved": "closed",
                                "pending": "in_progress",
                            }
                            onec_status = status_mapping.get(new_status, new_status)
                            # Запускаем фоновую задачу (не ждем её завершения)
                            asyncio.create_task(_sync_status_to_1c_background(
                                cons_id=cons_id,
                                cl_ref_key=consultation.cl_ref_key,
                                onec_status=onec_status
                            ))
                
                if "assignee" in conversation:
                    old_manager = consultation.manager
                    chatwoot_user_id = conversation["assignee"].get("id") if conversation["assignee"] else None
                    if chatwoot_user_id:
                        # Пытаемся найти маппинг менеджера из таблицы user_mapping
                        mapping_result = await db.execute(
                            select(UserMapping).where(UserMapping.chatwoot_user_id == chatwoot_user_id).limit(1)
                        )
                        mapping = mapping_result.scalar_one_or_none()
                        if mapping:
                            # Используем cl_manager_key из маппинга
                            consultation.manager = mapping.cl_manager_key
                            logger.info(f"Mapped Chatwoot user {chatwoot_user_id} to CL manager {mapping.cl_manager_key}")
                        else:
                            # Если маппинга нет, сохраняем chatwoot_user_id как есть
                            consultation.manager = str(chatwoot_user_id)
                            logger.warning(f"No mapping found for Chatwoot user {chatwoot_user_id}, using chatwoot_user_id as manager")
                    else:
                        consultation.manager = None
                    
                    manager_changed = (old_manager != consultation.manager)
                    
                    # Логируем изменение менеджера
                    if manager_changed:
                        await log_consultation_change(
                            db=db,
                            cons_id=cons_id,
                            field_name="manager",
                            old_value=old_manager,
                            new_value=consultation.manager,
                            source="CHATWOOT"
                        )
                    
                    # Если менеджер изменился, отправляем уведомление и синхронизируем с ЦЛ
                    if manager_changed:
                        try:
                            from ..services.manager_notifications import send_manager_reassignment_notification
                            await send_manager_reassignment_notification(
                                db=db,
                                consultation=consultation,
                                old_manager_key=old_manager,
                                new_manager_key=consultation.manager,
                                reason="Переназначено в Chatwoot"
                            )
                            
                            # Отправляем информацию об изменении очереди
                            if consultation.manager:
                                from ..services.manager_notifications import send_queue_update_notification
                                await send_queue_update_notification(
                                    db=db,
                                    consultation=consultation,
                                    manager_key=consultation.manager,
                                )
                            
                            # Синхронизируем с ЦЛ в фоновой задаче
                            # ВАЖНО: Выполняем в фоне, чтобы не блокировать webhook и не удерживать соединение с БД
                            if consultation.cl_ref_key and consultation.manager:
                                # Запускаем фоновую задачу (не ждем её завершения)
                                asyncio.create_task(_sync_manager_to_1c_background(
                                    cons_id=cons_id,
                                    cl_ref_key=consultation.cl_ref_key,
                                    manager_key=consultation.manager
                                ))
                        except Exception as e:
                            logger.warning(f"Failed to send manager reassignment notification: {e}")
                
                # Обрабатываем custom_attributes из Chatwoot
                # ВАЖНО: Chatwoot может не всегда отправлять custom_attributes в webhook'е
                # Это зависит от версии Chatwoot и настроек webhook'а
                custom_attributes = conversation.get("custom_attributes", {})
                
                # Логируем для отладки - что именно пришло в webhook'е
                logger.debug(f"Webhook conversation.updated for {cons_id}: "
                           f"has custom_attributes={bool(custom_attributes)}, "
                           f"conversation keys={list(conversation.keys())}")
                
                if custom_attributes:
                    try:
                        # date_con -> start_date
                        if "date_con" in custom_attributes and custom_attributes["date_con"]:
                            try:
                                # Парсим дату из формата YYYY-MM-DDTHH:MM:SS или ISO формата
                                date_str = str(custom_attributes["date_con"])
                                parsed_date = date_parser.parse(date_str)
                                # Если дата без timezone, считаем её UTC
                                if parsed_date.tzinfo is None:
                                    parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                                if consultation.start_date != parsed_date:
                                    consultation.start_date = parsed_date
                                    custom_attrs_changed = True
                                    logger.info(f"Updated start_date from custom_attributes.date_con: {parsed_date}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Failed to parse date_con from custom_attributes: {e}")
                        
                        # con_end -> end_date
                        if "con_end" in custom_attributes and custom_attributes["con_end"]:
                            try:
                                date_str = str(custom_attributes["con_end"])
                                parsed_date = date_parser.parse(date_str)
                                if parsed_date.tzinfo is None:
                                    parsed_date = parsed_date.replace(tzinfo=timezone.utc)
                                if consultation.end_date != parsed_date:
                                    consultation.end_date = parsed_date
                                    custom_attrs_changed = True
                                    logger.info(f"Updated end_date from custom_attributes.con_end: {parsed_date}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Failed to parse con_end from custom_attributes: {e}")
                        
                        # redate_con -> redate
                        if "redate_con" in custom_attributes and custom_attributes["redate_con"]:
                            try:
                                date_str = str(custom_attributes["redate_con"])
                                parsed_date = date_parser.parse(date_str)
                                # redate - это Date, без времени
                                redate_date = parsed_date.date()
                                if consultation.redate != redate_date:
                                    consultation.redate = redate_date
                                    custom_attrs_changed = True
                                    logger.info(f"Updated redate from custom_attributes.redate_con: {redate_date}")
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Failed to parse redate_con from custom_attributes: {e}")
                        
                        # retime_con -> redate_time
                        if "retime_con" in custom_attributes and custom_attributes["retime_con"]:
                            try:
                                time_str = str(custom_attributes["retime_con"])
                                # Формат HH:MM или HH:MM:SS
                                time_parts = time_str.split(":")
                                if len(time_parts) >= 2:
                                    hour = int(time_parts[0])
                                    minute = int(time_parts[1])
                                    parsed_time = time(hour, minute)
                                    if consultation.redate_time != parsed_time:
                                        consultation.redate_time = parsed_time
                                        custom_attrs_changed = True
                                        logger.info(f"Updated redate_time from custom_attributes.retime_con: {parsed_time}")
                            except (ValueError, TypeError, IndexError) as e:
                                logger.warning(f"Failed to parse retime_con from custom_attributes: {e}")
                        
                        # closed_without_con -> denied
                        if "closed_without_con" in custom_attributes:
                            closed_value = custom_attributes["closed_without_con"]
                            # Может быть bool, строка "true"/"false", или число 0/1
                            if isinstance(closed_value, bool):
                                denied_value = closed_value
                            elif isinstance(closed_value, str):
                                denied_value = closed_value.lower() in ("true", "1", "yes")
                            elif isinstance(closed_value, (int, float)):
                                denied_value = bool(closed_value)
                            else:
                                denied_value = False
                            
                            if consultation.denied != denied_value:
                                consultation.denied = denied_value
                                custom_attrs_changed = True
                                logger.info(f"Updated denied from custom_attributes.closed_without_con: {denied_value}")
                        
                        if custom_attrs_changed:
                            logger.info(f"Custom attributes synced from Chatwoot to DB for consultation {cons_id}")
                    except Exception as e:
                        logger.error(f"Error processing custom_attributes from Chatwoot webhook: {e}", exc_info=True)
                
                await db.flush()
                changes = []
                if status_changed:
                    changes.append("status")
                if manager_changed:
                    changes.append("manager")
                if custom_attrs_changed:
                    changes.append("custom_attributes")
                logger.info(f"Updated consultation {cons_id} in DB from Chatwoot webhook. Changes: {', '.join(changes) if changes else 'none'}")
        
        elif event_type == "conversation.status_changed" or event_type == "conversation.resolved":
            # Изменение статуса консультации в Chatwoot
            conversation = event_data.get("conversation", {})
            cons_id = str(conversation.get("id"))
            new_status = conversation.get("status", "resolved" if event_type == "conversation.resolved" else None)
            
            if not new_status:
                logger.warning(f"No status in conversation.status_changed event for {cons_id}")
                await db.commit()
                return WebhookResponse(status="ok", message=f"Processed {event_type} (no status)")
            
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if consultation:
                old_status = consultation.status
                if old_status != new_status:
                    # ВАЖНО: Запрещаем закрытие беседы клиентом для консультаций по ведению учета
                    # Закрытие должно происходить только через ЦЛ или Chatwoot (менеджером)
                    # Если клиент пытается закрыть беседу, откатываем статус обратно
                    if consultation.consultation_type == "Консультация по ведению учёта":
                        if new_status in ("resolved", "closed"):
                            # Откатываем статус обратно в Chatwoot
                            logger.warning(
                                f"Attempt to close consultation {cons_id} by client denied "
                                f"(consultation_type='Консультация по ведению учёта'). "
                                f"Reverting status from {new_status} back to {old_status}"
                            )
                            try:
                                chatwoot_client = ChatwootClient()
                                # Откатываем статус обратно на предыдущий
                                await chatwoot_client.update_conversation(
                                    conversation_id=cons_id,
                                    status=old_status or "open"  # Если old_status None, используем "open"
                                )
                                logger.info(f"Reverted conversation {cons_id} status back to {old_status or 'open'}")
                            except Exception as revert_error:
                                logger.error(f"Failed to revert conversation {cons_id} status: {revert_error}", exc_info=True)
                            
                            # НЕ обновляем статус в БД и НЕ синхронизируем в ЦЛ
                            await db.commit()
                            return WebhookResponse(
                                status="ok",
                                message=f"Status change denied for consultation type 'Консультация по ведению учёта'"
                            )
                    
                    consultation.status = new_status
                    
                    # Если статус изменился на "resolved" или "closed", обновляем end_date
                    if new_status in ("resolved", "closed") and not consultation.end_date:
                        from datetime import datetime, timezone
                        consultation.end_date = datetime.now(timezone.utc)
                        logger.info(f"Set end_date for consultation {cons_id} (status changed to {new_status})")
                    
                    # Логируем изменение статуса
                    await log_consultation_change(
                        db=db,
                        cons_id=cons_id,
                        field_name="status",
                        old_value=old_status,
                        new_value=new_status,
                        source="CHATWOOT"
                    )
                    
                    # Синхронизируем статус обратно в 1C:ЦЛ
                    # Синхронизируем статус обратно в 1C:ЦЛ в фоновой задаче
                    # ВАЖНО: Выполняем в фоне, чтобы не блокировать webhook и не удерживать соединение с БД
                    if consultation.cl_ref_key:
                        status_mapping = {
                            "open": "new",
                            "resolved": "closed",
                            "pending": "in_progress",
                        }
                        onec_status = status_mapping.get(new_status, new_status)
                        # Запускаем фоновую задачу (не ждем её завершения)
                        asyncio.create_task(_sync_status_to_1c_background(
                            cons_id=cons_id,
                            cl_ref_key=consultation.cl_ref_key,
                            onec_status=onec_status
                        ))
                    
                    await db.flush()
                    logger.info(f"Updated consultation {cons_id} status to '{new_status}' in DB from Chatwoot webhook")
                    
                    # Уведомляем WebSocket клиентов об обновлении
                    try:
                        from ..routers.websocket import notify_consultation_update
                        await notify_consultation_update(cons_id, consultation)
                    except Exception as ws_error:
                        logger.debug(f"Failed to notify WebSocket clients: {ws_error}")
                else:
                    logger.debug(f"Status unchanged for consultation {cons_id}: {new_status}")
        
        elif event_type == "message.created":
            # Новое сообщение в консультации
            message = event_data.get("message", {})
            conversation_id = str(message.get("conversation_id"))
            
            # Можно обновить last_message_at или сохранить в q_and_a
            # В зависимости от бизнес-логики
        
        await db.commit()
        webhook_log.processed = True
        await db.commit()
        
        return WebhookResponse(status="ok", message=f"Processed {event_type}")
    
    except Exception as e:
        await db.rollback()
        webhook_log.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")


@router.post("/1c_cl", response_model=WebhookResponse)
async def onec_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    Обработка вебхука от 1C:ЦЛ.
    
    События:
    - consultation.created
    - consultation.updated
    - consultation.closed
    - consultation.rescheduled
    """
    payload = await request.json()
    
    # Сохраняем в лог
    webhook_log = WebhookLog(
        source="1C_CL",
        payload=payload
    )
    db.add(webhook_log)
    await db.flush()
    
    event_type = payload.get("event")
    event_data = payload.get("data", {})
    
    try:
        if event_type == "consultation.created":
            # Новая консультация из ЦЛ
            cl_ref_key = event_data.get("cl_ref_key")
            number = event_data.get("number")
            
            # Ищем по cl_ref_key или создаем новую
            result = await db.execute(
                select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
            )
            consultation = result.scalar_one_or_none()
            
            if not consultation:
                # Создаем новую (cons_id будет обновлен после синхронизации с Chatwoot)
                consultation = Consultation(
                    cons_id=f"cl_{cl_ref_key}",  # Временный ID
                    cl_ref_key=cl_ref_key,
                    number=number,
                    status=event_data.get("status", "new"),
                    org_inn=event_data.get("org_inn"),
                )
                db.add(consultation)
            
            # Отправляем в Chatwoot
            chatwoot_client = ChatwootClient()
            try:
                chatwoot_response = await chatwoot_client.create_conversation(
                    source_id=None,
                    inbox_id=None,
                    message=event_data.get("description", ""),
                )
                consultation.cons_id = str(chatwoot_response.get("id"))
            except Exception:
                pass  # Chatwoot недоступен
        
        elif event_type == "consultation.updated":
            # Обновление из ЦЛ
            cl_ref_key = event_data.get("cl_ref_key")
            cons_id = event_data.get("cons_id")
            
            # Ищем консультацию
            if cons_id:
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
            else:
                result = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
                )
            
            consultation = result.scalar_one_or_none()
            
            if consultation:
                # Обновляем поля
                if "status" in event_data:
                    consultation.status = event_data["status"]
                if "manager" in event_data:
                    consultation.manager = event_data["manager"]
                if "start_date" in event_data:
                    start_date_value = event_data["start_date"]
                    if isinstance(start_date_value, str):
                        consultation.start_date = parse_datetime_flexible(start_date_value)
                    else:
                        consultation.start_date = start_date_value
                if "end_date" in event_data:
                    end_date_value = event_data["end_date"]
                    if isinstance(end_date_value, str):
                        consultation.end_date = parse_datetime_flexible(end_date_value)
                    else:
                        consultation.end_date = end_date_value
                
                # Синхронизируем с Chatwoot
                if consultation.cons_id and consultation.cons_id.startswith("cl_"):
                    # Если еще нет Chatwoot ID, создаем
                    pass
                else:
                    # Обновляем в Chatwoot
                    chatwoot_client = ChatwootClient()
                    try:
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status=consultation.status,
                        )
                    except Exception:
                        pass
        
        elif event_type == "consultation.closed":
            # Закрытие консультации
            cl_ref_key = event_data.get("cl_ref_key")
            cons_id = event_data.get("cons_id")
            
            if cons_id:
                result = await db.execute(
                    select(Consultation).where(Consultation.cons_id == cons_id)
                )
            else:
                result = await db.execute(
                    select(Consultation).where(Consultation.cl_ref_key == cl_ref_key)
                )
            
            consultation = result.scalar_one_or_none()
            
            if consultation:
                consultation.status = "closed"
                end_date_value = event_data.get("end_date")
                if end_date_value:
                    if isinstance(end_date_value, str):
                        consultation.end_date = parse_datetime_flexible(end_date_value)
                    else:
                        consultation.end_date = end_date_value
                else:
                    # Если end_date не пришел в событии, устанавливаем текущее время
                    if not consultation.end_date:
                        from datetime import datetime, timezone
                        consultation.end_date = datetime.now(timezone.utc)
                        logger.info(f"Set end_date for consultation {cons_id or cl_ref_key} (closed from 1C)")
                
                # Логируем изменение статуса
                await log_consultation_change(
                    db=db,
                    cons_id=cons_id or consultation.cons_id,
                    field_name="status",
                    old_value=consultation.status,
                    new_value="closed",
                    source="1C_CL"
                )
                
                # Обновляем в Chatwoot
                if consultation.cons_id and not consultation.cons_id.startswith("cl_"):
                    chatwoot_client = ChatwootClient()
                    try:
                        await chatwoot_client.update_conversation(
                            conversation_id=consultation.cons_id,
                            status="resolved",
                        )
                    except Exception:
                        pass
        
        await db.commit()
        webhook_log.processed = True
        await db.commit()
        
        return WebhookResponse(status="ok", message=f"Processed {event_type}")
    
    except Exception as e:
        await db.rollback()
        webhook_log.error_message = str(e)
        await db.commit()
        raise HTTPException(status_code=500, detail=f"Error processing webhook: {str(e)}")
