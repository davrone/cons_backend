"""Схемы для работы с клиентами"""
from pydantic import BaseModel, Field
from pydantic import ConfigDict
from typing import Optional
from datetime import datetime
from uuid import UUID


class ClientCreate(BaseModel):
    """Создание/обновление клиента от фронта"""
    # Основные данные
    email: Optional[str] = None  # Используем str вместо EmailStr для гибкости (валидация на уровне бизнес-логики)
    phone_number: Optional[str] = None
    name: Optional[str] = None
    contact_name: Optional[str] = None
    company_name: Optional[str] = None  # Название компании клиента (для формирования имени в 1C, НЕ обслуживающая организация)
    partner: Optional[str] = None  # Обслуживающая организация (для Chatwoot и витрин)
    subscriber_id: Optional[str] = Field(default=None, alias="subscriberId")
    
    # География
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    
    # Организация
    org_inn: Optional[str] = None
    code_abonent: Optional[str] = None
    
    # Подписка
    subs_id: Optional[str] = None
    subs_start: Optional[datetime] = None
    subs_end: Optional[datetime] = None
    
    # Тариф
    tariff_id: Optional[str] = None
    tariffperiod_id: Optional[str] = None
    
    # Связи с ЦЛ
    cl_ref_key: Optional[str] = None  # Ref_Key клиента из 1C:ЦЛ
    parent_id: Optional[str] = None
    is_parent: Optional[bool] = None
    
    # Для идентификации существующего клиента
    client_id_hash: Optional[str] = None  # Хеш для поиска существующего клиента
    client_id: Optional[str] = None  # UUID существующего клиента

    model_config = ConfigDict(populate_by_name=True)


class ClientRead(BaseModel):
    """Чтение данных клиента"""
    client_id: UUID
    email: Optional[str] = None
    phone_number: Optional[str] = None
    name: Optional[str] = None
    contact_name: Optional[str] = None
    company_name: Optional[str] = None
    partner: Optional[str] = None
    subscriber_id: Optional[str] = Field(default=None, alias="subscriberId")
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    org_inn: Optional[str] = None
    code_abonent: Optional[str] = None
    subs_id: Optional[str] = None
    subs_start: Optional[datetime] = None
    subs_end: Optional[datetime] = None
    tariff_id: Optional[str] = None
    tariffperiod_id: Optional[str] = None
    cl_ref_key: Optional[str] = None
    parent_id: Optional[str] = None
    is_parent: Optional[bool] = None
    source_id: Optional[str] = None  # source_id из Chatwoot (для идентификации контакта)
    chatwoot_pubsub_token: Optional[str] = None  # pubsub_token из Chatwoot (для WebSocket подключения, принадлежит контакту)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
