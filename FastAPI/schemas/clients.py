"""Схемы для работы с клиентами"""
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


class ClientCreate(BaseModel):
    """Создание/обновление клиента от фронта"""
    # Основные данные
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None
    
    # География
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    
    # Организация
    org_inn: Optional[str] = None
    org_id: Optional[str] = None
    
    # Подписка
    subs_id: Optional[str] = None
    subs_start: Optional[datetime] = None
    subs_end: Optional[datetime] = None
    
    # Тариф
    tariff_id: Optional[str] = None
    tariffperiod_id: Optional[str] = None
    
    # Связи с ЦЛ
    cl_ref_key: Optional[str] = None  # Ref_Key клиента из 1C:ЦЛ
    
    # Для идентификации существующего клиента
    client_id_hash: Optional[str] = None  # Хеш для поиска существующего клиента
    client_id: Optional[str] = None  # UUID существующего клиента


class ClientRead(BaseModel):
    """Чтение данных клиента"""
    client_id: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    org_inn: Optional[str] = None
    org_id: Optional[str] = None
    subs_id: Optional[str] = None
    subs_start: Optional[datetime] = None
    subs_end: Optional[datetime] = None
    tariff_id: Optional[str] = None
    tariffperiod_id: Optional[str] = None
    cl_ref_key: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
