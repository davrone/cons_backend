"""Схемы для переноса и оценок консультаций."""
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel


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

