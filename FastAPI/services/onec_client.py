"""Клиент для работы с 1C:ЦЛ через OData"""
import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
from ..config import settings


class OneCClient:
    """Асинхронный клиент для 1C:ЦЛ OData API"""
    
    def __init__(self):
        self.odata_base_url = settings.ODATA_BASE_URL.rstrip("/")
        self.odata_user = settings.ODATA_USER
        self.odata_password = settings.ODATA_PASSWORD
        self.entity = "Document_ТелефонныйЗвонок"
    
    async def _odata_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Базовый метод для OData запросов"""
        url = f"{self.odata_base_url}{endpoint}"
        auth = (self.odata_user, self.odata_password)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=method,
                url=url,
                auth=auth,
                headers=headers,
                json=data,
                params=params
            )
            response.raise_for_status()
            if response.content:
                return response.json()
            return {}
    
    def _map_status_to_vid_obrascheniya(self, status: str) -> str:
        """Маппинг нашего статуса в ВидОбращения ЦЛ"""
        status_map = {
            "closed": "КонсультацияИТС",
            "pending": "ВОчередьНаКонсультацию",
            "other": "Другое",
            "new": "ВОчередьНаКонсультацию"
        }
        return status_map.get(status, "ВОчередьНаКонсультацию")
    
    async def create_consultation_odata(
        self,
        client_key: Optional[str] = None,
        manager_key: Optional[str] = None,
        author_key: Optional[str] = None,
        description: str = "",
        topic: Optional[str] = None,
        scheduled_at: Optional[datetime] = None,
        question_category_key: Optional[str] = None,
        question_key: Optional[str] = None,
        consultations_its: Optional[List[Dict[str, Any]]] = None,
        questions_answers: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Создание новой консультации в 1C:ЦЛ через OData.
        
        Args:
            client_key: Абонент_Key (UUID клиента в ЦЛ)
            manager_key: Менеджер_Key
            author_key: Автор_Key
            description: Описание/Вопрос
            topic: Тема
            scheduled_at: ДатаКонсультации
            question_category_key: КатегорияВопроса_Key
            question_key: ВопросНаКонсультацию_Key
            consultations_its: Массив КонсультацииИТС
            questions_answers: Массив ВопросыИОтветы
        
        Returns:
            Dict с данными созданной консультации (Ref_Key, Number и т.д.)
        """
        payload = {
            "Описание": description,
            "ВидОбращения": "ВОчередьНаКонсультацию",  # По умолчанию
            "Входящий": True,
        }
        
        if client_key:
            payload["Абонент_Key"] = client_key
        if manager_key:
            payload["Менеджер_Key"] = manager_key
        if author_key:
            payload["Автор_Key"] = author_key
        if topic:
            payload["Тема"] = topic
        if scheduled_at:
            payload["ДатаКонсультации"] = scheduled_at.strftime("%Y-%m-%dT%H:%M:%S")
        if question_category_key:
            payload["КатегорияВопроса_Key"] = question_category_key
        if question_key:
            payload["ВопросНаКонсультацию_Key"] = question_key
        
        # КонсультацииИТС
        if consultations_its:
            payload["КонсультацииИТС"] = consultations_its
        
        # ВопросыИОтветы
        if questions_answers:
            payload["ВопросыИОтветы"] = questions_answers
        
        # НЕ отправляем Ref_Key и Number - они создаются автоматически
        
        endpoint = f"{self.entity}"
        return await self._odata_request("POST", endpoint, data=payload)
    
    async def update_consultation_odata(
        self,
        ref_key: str,
        number: Optional[str] = None,
        status: Optional[str] = None,
        manager_key: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        description: Optional[str] = None,
        consultations_its: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Обновление консультации в 1C:ЦЛ через OData (PATCH).
        
        Args:
            ref_key: Ref_Key документа (обязательно для поиска)
            number: Number документа (опционально, для проверки)
            status: Статус (маппится в ВидОбращения)
            manager_key: Менеджер_Key
            start_date: ДатаКонсультации
            end_date: Конец
            description: Описание
            consultations_its: Обновление КонсультацииИТС
        
        Returns:
            Обновленный документ
        """
        # Формируем endpoint для PATCH
        # OData: Document_ТелефонныйЗвонок(guid'ref_key')
        endpoint = f"{self.entity}(guid'{ref_key}')"
        
        payload = {}
        
        if status:
            payload["ВидОбращения"] = self._map_status_to_vid_obrascheniya(status)
        if manager_key:
            payload["Менеджер_Key"] = manager_key
        if start_date:
            payload["ДатаКонсультации"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        if end_date:
            payload["Конец"] = end_date.strftime("%Y-%m-%dT%H:%M:%S")
        if description is not None:
            payload["Описание"] = description
        if consultations_its is not None:
            payload["КонсультацииИТС"] = consultations_its
        
        # Используем PATCH для частичного обновления
        return await self._odata_request("PATCH", endpoint, data=payload)
    
    async def get_consultation_odata(self, ref_key: str) -> Dict[str, Any]:
        """Получение консультации из 1C:ЦЛ через OData"""
        endpoint = f"{self.entity}(guid'{ref_key}')"
        return await self._odata_request("GET", endpoint)
    
    # Старые методы для обратной совместимости (если используется REST API)
    async def create_consultation(
        self,
        client_ref_key: Optional[str] = None,
        org_inn: Optional[str] = None,
        description: str = "",
        scheduled_at: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Создание новой консультации (REST API, устаревший метод).
        Используйте create_consultation_odata для OData.
        """
        # Если есть REST API URL, используем его
        if hasattr(settings, 'ONEC_API_URL') and settings.ONEC_API_URL:
            url = f"{settings.ONEC_API_URL.rstrip('/')}/api/consultations"
            headers = {
                "Authorization": f"Bearer {settings.ONEC_API_TOKEN}",
                "Content-Type": "application/json"
            }
            payload = {"description": description}
            if client_ref_key:
                payload["client_ref_key"] = client_ref_key
            if org_inn:
                payload["org_inn"] = org_inn
            if scheduled_at:
                payload["scheduled_at"] = scheduled_at.isoformat()
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                return response.json()
        else:
            # Fallback на OData
            return await self.create_consultation_odata(
                client_key=client_ref_key,
                description=description,
                scheduled_at=scheduled_at
            )
    
    async def update_consultation(
        self,
        cl_ref_key: str,
        status: Optional[str] = None,
        manager: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Обновление консультации (REST API, устаревший метод)"""
        return await self.update_consultation_odata(
            ref_key=cl_ref_key,
            status=status,
            manager_key=manager,
            start_date=start_date,
            end_date=end_date
        )
    
    async def get_consultation(self, cl_ref_key: str) -> Dict[str, Any]:
        """Получение консультации (REST API, устаревший метод)"""
        return await self.get_consultation_odata(cl_ref_key)
    
    async def close_consultation(
        self,
        cl_ref_key: str,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Закрытие консультации"""
        return await self.update_consultation_odata(
            ref_key=cl_ref_key,
            status="closed",
            end_date=end_date
        )
