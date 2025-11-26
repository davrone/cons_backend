"""Клиент для работы с Chatwoot API"""
import httpx
from typing import Optional, Dict, Any
from ..config import settings


class ChatwootClient:
    """Асинхронный клиент для Chatwoot API"""
    
    def __init__(self):
        self.base_url = settings.CHATWOOT_API_URL.rstrip("/")
        self.api_token = settings.CHATWOOT_API_TOKEN
        self.account_id = settings.CHATWOOT_ACCOUNT_ID
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Базовый метод для запросов к API"""
        url = f"{self.base_url}{endpoint}"
        headers = {
            "api_access_token": self.api_token,
            "Content-Type": "application/json"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params
            )
            response.raise_for_status()
            return response.json()
    
    async def create_conversation(
        self,
        source_id: Optional[str] = None,
        inbox_id: Optional[int] = None,
        message: str = "",
        contact_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Создание новой консультации в Chatwoot.
        
        Returns:
            Dict с данными созданной консультации (включая id)
        """
        payload = {
            "source_id": source_id or "web",
            "inbox_id": inbox_id,
            "message": {
                "content": message
            }
        }
        
        if contact_id:
            payload["contact_id"] = contact_id
        
        return await self._request("POST", "/api/v1/conversations", data=payload)
    
    async def update_conversation(
        self,
        conversation_id: str,
        status: Optional[str] = None,
        assignee_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Обновление консультации в Chatwoot"""
        payload = {}
        if status:
            payload["status"] = status
        if assignee_id:
            payload["assignee_id"] = assignee_id
        
        return await self._request(
            "PUT",
            f"/api/v1/conversations/{conversation_id}",
            data=payload
        )
    
    async def get_conversation(self, conversation_id: str) -> Dict[str, Any]:
        """Получение консультации из Chatwoot"""
        return await self._request("GET", f"/api/v1/conversations/{conversation_id}")
    
    async def send_message(
        self,
        conversation_id: str,
        content: str,
        message_type: str = "outgoing"
    ) -> Dict[str, Any]:
        """Отправка сообщения в консультацию"""
        payload = {
            "content": content,
            "message_type": message_type
        }
        return await self._request(
            "POST",
            f"/api/v1/conversations/{conversation_id}/messages",
            data=payload
        )
