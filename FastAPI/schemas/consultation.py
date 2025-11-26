from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ConsultationCreate(BaseModel):
    client_id: Optional[str]
    ticket_type: Optional[str]  # 'TECH' or 'PROD'
    lang: Optional[str] = "ru"
    title: Optional[str]
    description: Optional[str]
    scheduled_at: Optional[datetime]

class ConsultationRead(ConsultationCreate):
    id: str
    status: Optional[str]
    created_at: Optional[datetime]
