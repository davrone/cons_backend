"""
API endpoints для работы с менеджерами и их загрузкой.
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..dependencies.security import verify_front_secret
from ..services.manager_selector import ManagerSelector
from ..models import Consultation

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(verify_front_secret)])


@router.get("/load")
async def get_managers_load(
    db: AsyncSession = Depends(get_db),
    current_time: Optional[datetime] = Query(None, description="Текущее время (по умолчанию now())"),
) -> List[dict]:
    """
    Получить загрузку всех менеджеров.
    
    Returns:
        Список менеджеров с информацией о загрузке:
        - manager_key: cl_ref_key менеджера
        - manager_id: account_id менеджера
        - chatwoot_user_id: ID в Chatwoot
        - name: Имя менеджера
        - queue_count: Количество консультаций в очереди
        - limit: Лимит менеджера
        - load_percent: Процент загрузки (0-100)
        - available_slots: Свободные слоты
        - start_hour: Время начала работы
        - end_hour: Время окончания работы
    """
    manager_selector = ManagerSelector(db)
    
    if current_time is None:
        current_time = datetime.now(timezone.utc)
    
    try:
        managers_load = await manager_selector.get_all_managers_load(current_time=current_time)
        return managers_load
    except Exception as e:
        logger.error(f"Failed to get managers load: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get managers load: {str(e)}")


@router.get("/{manager_key}/load")
async def get_manager_load(
    manager_key: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Получить загрузку конкретного менеджера.
    
    Args:
        manager_key: cl_ref_key менеджера
    
    Returns:
        Информация о загрузке менеджера
    """
    manager_selector = ManagerSelector(db)
    
    try:
        load_info = await manager_selector.get_manager_current_load(manager_key)
        return load_info
    except Exception as e:
        logger.error(f"Failed to get manager load: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get manager load: {str(e)}")


@router.get("/{manager_key}/wait-time")
async def get_manager_wait_time(
    manager_key: str,
    db: AsyncSession = Depends(get_db),
    average_duration_minutes: int = Query(60, description="Средняя длительность консультации в минутах"),
) -> dict:
    """
    Рассчитать примерное время ожидания для менеджера.
    
    Args:
        manager_key: cl_ref_key менеджера
        average_duration_minutes: Средняя длительность консультации в минутах
    
    Returns:
        Информация о времени ожидания:
        - queue_position: Позиция в очереди
        - estimated_wait_minutes: Примерное время ожидания в минутах
        - estimated_wait_hours: Примерное время ожидания в часах
    """
    manager_selector = ManagerSelector(db)
    
    try:
        wait_info = await manager_selector.calculate_wait_time(
            manager_key=manager_key,
            average_consultation_duration_minutes=average_duration_minutes,
        )
        return wait_info
    except Exception as e:
        logger.error(f"Failed to calculate wait time: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to calculate wait time: {str(e)}")


@router.get("/available")
async def get_available_managers(
    db: AsyncSession = Depends(get_db),
    po_section_key: Optional[str] = Query(None, description="Ключ раздела ПО"),
    po_type_key: Optional[str] = Query(None, description="Ключ типа ПО"),
    category_key: Optional[str] = Query(None, description="Ключ категории вопроса"),
    current_time: Optional[datetime] = Query(None, description="Текущее время"),
) -> List[dict]:
    """
    Получить список доступных менеджеров.
    
    Args:
        po_section_key: Ключ раздела ПО
        po_type_key: Ключ типа ПО
        category_key: Ключ категории вопроса
        current_time: Текущее время
    
    Returns:
        Список доступных менеджеров с информацией о загрузке
    """
    manager_selector = ManagerSelector(db)
    
    if current_time is None:
        current_time = datetime.now(timezone.utc)
    
    try:
        managers = await manager_selector.get_available_managers(
            current_time=current_time,
            po_section_key=po_section_key,
            po_type_key=po_type_key,
            category_key=category_key,
        )
        
        # Добавляем информацию о загрузке для каждого менеджера
        result = []
        for manager in managers:
            if not manager.cl_ref_key:
                continue
            
            load_info = await manager_selector.get_manager_current_load(manager.cl_ref_key)
            
            result.append({
                "manager_key": manager.cl_ref_key,
                "manager_id": str(manager.account_id),
                "chatwoot_user_id": manager.chatwoot_user_id,
                "name": manager.description or manager.user_id or "Unknown",
                "queue_count": load_info["queue_count"],
                "limit": load_info["limit"],
                "load_percent": load_info["load_percent"],
                "available_slots": load_info["available_slots"],
            })
        
        return result
    except Exception as e:
        logger.error(f"Failed to get available managers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get available managers: {str(e)}")


@router.get("/consultations/{cons_id}/queue-info")
async def get_consultation_queue_info(
    cons_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Получить информацию об очереди для конкретной консультации.
    
    Args:
        cons_id: ID консультации
    
    Returns:
        Информация об очереди:
        - queue_position: Позиция в очереди
        - estimated_wait_minutes: Примерное время ожидания в минутах
        - estimated_wait_hours: Примерное время ожидания в часах
        - manager_key: Ключ менеджера
    """
    from sqlalchemy import select
    
    # Получаем консультацию
    result = await db.execute(
        select(Consultation).where(Consultation.cons_id == cons_id)
    )
    consultation = result.scalar_one_or_none()
    
    if not consultation:
        raise HTTPException(status_code=404, detail="Consultation not found")
    
    if not consultation.manager:
        return {
            "queue_position": None,
            "estimated_wait_minutes": None,
            "estimated_wait_hours": None,
            "manager_key": None,
        }
    
    manager_selector = ManagerSelector(db)
    
    try:
        wait_info = await manager_selector.calculate_wait_time(consultation.manager)
        wait_info["manager_key"] = consultation.manager
        return wait_info
    except Exception as e:
        logger.error(f"Failed to get queue info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get queue info: {str(e)}")

