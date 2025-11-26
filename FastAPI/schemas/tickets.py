"""Схемы для работы с тикетами (консультациями)"""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, date, time

from .clients import ClientCreate


class ConsultationCreate(BaseModel):
    """Создание консультации - данные только консультации"""
    client_id: Optional[str] = None  # UUID клиента (если уже есть)
    cl_ref_key: Optional[str] = None  # Ref_Key из ЦЛ
    org_inn: Optional[str] = None
    lang: Optional[str] = "ru"
    comment: Optional[str] = None  # Вопрос/описание
    topic: Optional[str] = None  # Тема
    online_question_cat: Optional[str] = None  # КатегорияВопроса_Key
    online_question: Optional[str] = None  # ВопросНаКонсультацию_Key
    importance: Optional[int] = None
    scheduled_at: Optional[datetime] = None  # Желаемая дата/время консультации


class ConsultationWithClient(BaseModel):
    """Создание консультации с данными клиента (комплексный запрос от фронта)"""
    # Данные клиента (если клиента еще нет)
    client: Optional[ClientCreate] = None
    
    # Данные консультации
    consultation: ConsultationCreate
    
    # Метаданные
    source: Optional[str] = "SITE"  # SITE, TELEGRAM, CALL_CENTER


class TicketCreate(BaseModel):
    """Создание нового тикета (упрощенная версия для обратной совместимости)"""
    client_id: Optional[str] = None
    cl_ref_key: Optional[str] = None
    org_inn: Optional[str] = None
    lang: Optional[str] = "ru"
    comment: Optional[str] = None
    online_question_cat: Optional[str] = None
    online_question: Optional[str] = None
    importance: Optional[int] = None
    scheduled_at: Optional[datetime] = None


class TicketRead(BaseModel):
    """Чтение тикета"""
    cons_id: str
    cl_ref_key: Optional[str] = None
    client_id: Optional[str] = None
    client_key: Optional[str] = None
    number: Optional[str] = None
    status: Optional[str] = None
    org_inn: Optional[str] = None
    importance: Optional[int] = None
    create_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    redate_time: Optional[time] = None
    redate: Optional[date] = None
    lang: Optional[str] = None
    denied: Optional[bool] = None
    manager: Optional[str] = None
    author: Optional[str] = None
    comment: Optional[str] = None
    online_question_cat: Optional[str] = None
    online_question: Optional[str] = None
    con_rates: Optional[Dict[str, Any]] = None
    con_calls: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_model(cls, model):
        """Конвертация модели SQLAlchemy в схему Pydantic"""
        return cls(
            cons_id=model.cons_id,
            cl_ref_key=model.cl_ref_key,
            client_id=str(model.client_id) if model.client_id else None,
            client_key=model.client_key,
            number=model.number,
            status=model.status,
            org_inn=model.org_inn,
            importance=model.importance,
            create_date=model.create_date,
            start_date=model.start_date,
            end_date=model.end_date,
            redate_time=model.redate_time,
            redate=model.redate,
            lang=model.lang,
            denied=model.denied,
            manager=model.manager,
            author=model.author,
            comment=model.comment,
            online_question_cat=model.online_question_cat,
            online_question=model.online_question,
            con_rates=model.con_rates,
            con_calls=model.con_calls,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    class Config:
        from_attributes = True


class TicketListResponse(BaseModel):
    """Список тикетов клиента"""
    tickets: List[TicketRead]
    total: int


class ConsultationResponse(BaseModel):
    """Ответ при создании консультации"""
    consultation: TicketRead
    client_id: str
    message: str = "Consultation created successfully"
