"""
WebSocket endpoints для real-time обновлений.
"""
import json
import logging
from typing import Dict, Any
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_db, AsyncSessionLocal
from ..models import Consultation, User
from ..schemas.tickets import ConsultationRead
from sqlalchemy import select

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectionManager:
    """Менеджер WebSocket соединений"""
    
    def __init__(self):
        # Храним активные соединения по cons_id
        self.active_connections: Dict[str, list[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, cons_id: str):
        """Подключить клиента к WebSocket"""
        await websocket.accept()
        if cons_id not in self.active_connections:
            self.active_connections[cons_id] = []
        self.active_connections[cons_id].append(websocket)
        logger.info(f"WebSocket connected for consultation {cons_id}. Total connections: {len(self.active_connections[cons_id])}")
    
    def disconnect(self, websocket: WebSocket, cons_id: str):
        """Отключить клиента от WebSocket"""
        if cons_id in self.active_connections:
            if websocket in self.active_connections[cons_id]:
                self.active_connections[cons_id].remove(websocket)
            if not self.active_connections[cons_id]:
                del self.active_connections[cons_id]
        logger.info(f"WebSocket disconnected for consultation {cons_id}")
    
    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket):
        """Отправить сообщение конкретному клиенту"""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
    
    async def broadcast_to_consultation(self, cons_id: str, message: Dict[str, Any]):
        """Отправить сообщение всем клиентам, подписанным на консультацию"""
        if cons_id not in self.active_connections:
            return
        
        disconnected = []
        for websocket in self.active_connections[cons_id]:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket client: {e}")
                disconnected.append(websocket)
        
        # Удаляем отключенные соединения
        for websocket in disconnected:
            self.disconnect(websocket, cons_id)


# Глобальный менеджер соединений
manager = ConnectionManager()


@router.websocket("/{cons_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    cons_id: str = ...
):
    """
    WebSocket endpoint для real-time обновлений консультации.
    
    **Использование:**
    ```javascript
    const ws = new WebSocket('ws://localhost:7070/ws/consultations/12345');
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'update') {
        // Обновить UI
      }
    };
    // Keep-alive
    setInterval(() => ws.send('ping'), 30000);
    ```
    
    **Формат сообщений:**
    - `{"type": "initial", "data": {...}}` - Начальное состояние консультации
    - `{"type": "update", "data": {...}}` - Обновление консультации
    - `{"type": "error", "message": "..."}` - Ошибка
    
    **Keep-alive:**
    Отправляйте `"ping"` каждые 30 секунд для поддержания соединения.
    Сервер отвечает `"pong"`.
    
    **Альтернативы:**
    - SSE: `GET /api/consultations/{cons_id}/stream` - односторонняя связь
    - Polling: `GET /api/consultations/{cons_id}/updates` - простой fallback
    """
    await manager.connect(websocket, cons_id)
    
    try:
        # Отправляем начальное состояние консультации
        # ВАЖНО: get_db() - это dependency, нужно использовать AsyncSessionLocal напрямую
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Consultation).where(Consultation.cons_id == cons_id)
            )
            consultation = result.scalar_one_or_none()
            
            if consultation:
                # Получаем ФИО менеджера
                manager_name = None
                if consultation.manager:
                    manager_result = await db.execute(
                        select(User.description)
                        .where(User.cl_ref_key == consultation.manager)
                        .where(User.deletion_mark == False)
                        .limit(1)
                    )
                    manager_name = manager_result.scalar_one_or_none()
                
                initial_data = {
                    "type": "initial",
                    "data": ConsultationRead.from_model(consultation, manager_name=manager_name).dict()
                }
                await manager.send_personal_message(initial_data, websocket)
            else:
                await manager.send_personal_message(
                    {"type": "error", "message": f"Consultation {cons_id} not found"},
                    websocket
                )
        
        # Ожидаем сообщения от клиента (ping/pong для keep-alive)
        while True:
            try:
                data = await websocket.receive_text()
                # Обрабатываем ping/pong
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                break
    
    except WebSocketDisconnect:
        manager.disconnect(websocket, cons_id)
    except Exception as e:
        logger.error(f"WebSocket error for consultation {cons_id}: {e}", exc_info=True)
        manager.disconnect(websocket, cons_id)


# Функция для отправки обновлений (можно вызывать из других модулей)
async def notify_consultation_update(cons_id: str, consultation: Consultation):
    """
    Уведомить всех подключенных клиентов об обновлении консультации.
    
    Args:
        cons_id: ID консультации
        consultation: Обновленная консультация
    """
    # Получаем ФИО менеджера
    manager_name = None
    if consultation.manager:
        async with AsyncSessionLocal() as db:
            manager_result = await db.execute(
                select(User.description)
                .where(User.cl_ref_key == consultation.manager)
                .where(User.deletion_mark == False)
                .limit(1)
            )
            manager_name = manager_result.scalar_one_or_none()
    
    update_data = {
        "type": "update",
        "data": ConsultationRead.from_model(consultation, manager_name=manager_name).dict()
    }
    await manager.broadcast_to_consultation(cons_id, update_data)

