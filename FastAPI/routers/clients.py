"""Роуты для работы с клиентами"""
import logging
import hashlib
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
import uuid

from ..database import get_db
from ..models import Client
from ..schemas.clients import ClientCreate, ClientRead

logger = logging.getLogger(__name__)
router = APIRouter()


def compute_client_hash(email: Optional[str], phone: Optional[str], org_inn: Optional[str]) -> str:
    """Вычисляет хеш клиента для идентификации"""
    parts = []
    if email:
        parts.append(f"email:{email.lower().strip()}")
    if phone:
        parts.append(f"phone:{phone.strip()}")
    if org_inn:
        parts.append(f"inn:{org_inn.strip()}")
    
    if not parts:
        return hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()
    
    s = "|".join(sorted(parts))
    return hashlib.md5(s.encode("utf-8")).hexdigest()


async def find_or_create_client(
    db: AsyncSession,
    client_data: ClientCreate
) -> Client:
    """
    Находит существующего клиента или создает нового.
    
    Логика поиска:
    1. По client_id (если указан)
    2. По client_id_hash (если указан)
    3. По email + phone + org_inn (вычисляем hash)
    4. Создаем нового
    """
    # 1. По client_id
    if client_data.client_id:
        try:
            client_uuid = uuid.UUID(client_data.client_id)
            result = await db.execute(
                select(Client).where(Client.client_id == client_uuid)
            )
            client = result.scalar_one_or_none()
            if client:
                # Обновляем данные если нужно
                if client_data.email:
                    client.email = client_data.email
                if client_data.phone_number:
                    client.phone_number = client_data.phone_number
                if client_data.org_inn:
                    client.org_inn = client_data.org_inn
                if client_data.cl_ref_key:
                    client.cl_ref_key = client_data.cl_ref_key
                # Обновляем подписку и тариф если указаны
                if client_data.subs_id:
                    client.subs_id = client_data.subs_id
                if client_data.subs_start:
                    client.subs_start = client_data.subs_start
                if client_data.subs_end:
                    client.subs_end = client_data.subs_end
                if client_data.tariff_id:
                    client.tariff_id = client_data.tariff_id
                if client_data.tariffperiod_id:
                    client.tariffperiod_id = client_data.tariffperiod_id
                return client
        except ValueError:
            pass  # Невалидный UUID
    
    # 2. По client_id_hash
    if client_data.client_id_hash:
        result = await db.execute(
            select(Client).where(Client.client_id_hash == client_data.client_id_hash)
        )
        client = result.scalar_one_or_none()
        if client:
            # Обновляем данные
            if client_data.email:
                client.email = client_data.email
            if client_data.phone_number:
                client.phone_number = client_data.phone_number
            return client
    
    # 3. По вычисленному hash
    client_hash = compute_client_hash(
        client_data.email,
        client_data.phone_number,
        client_data.org_inn
    )
    
    result = await db.execute(
        select(Client).where(Client.client_id_hash == client_hash)
    )
    client = result.scalar_one_or_none()
    if client:
        # Обновляем данные
        if client_data.email:
            client.email = client_data.email
        if client_data.phone_number:
            client.phone_number = client_data.phone_number
        if client_data.org_inn:
            client.org_inn = client_data.org_inn
        if client_data.cl_ref_key:
            client.cl_ref_key = client_data.cl_ref_key
        if client_data.subs_id:
            client.subs_id = client_data.subs_id
        if client_data.subs_start:
            client.subs_start = client_data.subs_start
        if client_data.subs_end:
            client.subs_end = client_data.subs_end
        if client_data.tariff_id:
            client.tariff_id = client_data.tariff_id
        if client_data.tariffperiod_id:
            client.tariffperiod_id = client_data.tariffperiod_id
        return client
    
    # 4. Создаем нового
    client = Client(
        client_id_hash=client_hash,
        email=client_data.email,
        phone_number=client_data.phone_number,
        country=client_data.country,
        region=client_data.region,
        city=client_data.city,
        org_inn=client_data.org_inn,
        org_id=client_data.org_id,
        subs_id=client_data.subs_id,
        subs_start=client_data.subs_start,
        subs_end=client_data.subs_end,
        tariff_id=client_data.tariff_id,
        tariffperiod_id=client_data.tariffperiod_id,
        cl_ref_key=client_data.cl_ref_key,
    )
    db.add(client)
    await db.flush()
    return client


@router.post("/", response_model=ClientRead)
async def create_or_update_client(
    payload: ClientCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Создание или обновление клиента.
    
    Если клиент существует (по client_id, client_id_hash или email+phone+inn),
    обновляет его данные. Иначе создает нового.
    """
    client = await find_or_create_client(db, payload)
    await db.commit()
    await db.refresh(client)
    
    return ClientRead.model_validate(client)


@router.get("/{client_id}", response_model=ClientRead)
async def get_client(
    client_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Получение клиента по ID"""
    try:
        client_uuid = uuid.UUID(client_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid client_id format")
    
    result = await db.execute(
        select(Client).where(Client.client_id == client_uuid)
    )
    client = result.scalar_one_or_none()
    
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    return ClientRead.model_validate(client)


@router.get("/by-hash/{client_hash}", response_model=ClientRead)
async def get_client_by_hash(
    client_hash: str,
    db: AsyncSession = Depends(get_db)
):
    """Получение клиента по хешу"""
    result = await db.execute(
        select(Client).where(Client.client_id_hash == client_hash)
    )
    client = result.scalar_one_or_none()
    
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    return ClientRead.model_validate(client)
