"""Схемы для переноса и оценок консультаций."""
from datetime import datetime
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, field_validator


class ConsultationRedateCreate(BaseModel):
    new_date: datetime
    comment: Optional[str] = None
    manager_key: Optional[str] = None


class ConsultationRedateRead(BaseModel):
    id: int
    cons_key: Optional[str] = None
    clients_key: Optional[str] = None
    manager_key: Optional[str] = None
    period: datetime
    old_date: Optional[datetime] = None
    new_date: Optional[datetime] = None
    comment: Optional[str] = None

    class Config:
        from_attributes = True


class ConsultationRatingAnswerPayload(BaseModel):
    question_number: int
    rating: Optional[int] = None
    question: Optional[str] = None
    comment: Optional[str] = None
    manager_key: Optional[str] = None


class ConsultationRatingRequest(BaseModel):
    answers: List[ConsultationRatingAnswerPayload]


class ConsultationRatingResponse(BaseModel):
    average: Optional[float] = None
    count: int = 0
    answers: List[ConsultationRatingAnswerPayload] = []


class CallRead(BaseModel):
    """Схема для чтения попытки дозвона"""
    period: datetime
    cons_key: Optional[str] = None
    cons_id: Optional[str] = None
    client_key: Optional[str] = None
    client_id: Optional[str] = None
    manager: Optional[str] = None

    @field_validator('client_id', mode='before')
    @classmethod
    def convert_uuid_to_str(cls, v):
        """Конвертирует UUID в строку, если это необходимо"""
        if isinstance(v, UUID):
            return str(v)
        return v

    class Config:
        from_attributes = True
