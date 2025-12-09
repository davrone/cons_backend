#!/usr/bin/env python3
"""
Скрипт для синхронизации пользователей из ЦЛ в Chatwoot.

Создает пользователей в Chatwoot только для операторов, у которых есть:
- con_limit (лимит консультаций)
- start_hour или end_hour (время работы)

Перед созданием проверяет существование пользователя в Chatwoot по email.
Сохраняет маппинг chatwoot_user_id в БД.

Логирует только ошибки, успешные операции не логируются.
"""
import os
import sys
import asyncio
import logging
from typing import Optional
import re

# Добавляем путь к проекту
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import select, or_, and_, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert

from FastAPI.config import settings
from FastAPI.models import User, UserMapping
from FastAPI.services.chatwoot_client import ChatwootClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sync_users_to_chatwoot")

DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


def _build_chatwoot_email(user: User) -> str:
    """
    Chatwoot Application API требует уникальный email.
    Если в данных пользователя его нет, генерируем техничекй email.
    """
    candidates = [
        user.user_id if user.user_id and "@" in user.user_id else None,
        user.description if user.description and "@" in user.description else None,
    ]
    for candidate in candidates:
        if candidate:
            return candidate.lower()

    base = user.description or user.user_id or user.cl_ref_key or str(user.account_id)
    sanitized = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
    if not sanitized:
        sanitized = str(user.account_id)
    return f"{sanitized}@cons.local"


async def sync_user_to_chatwoot(
    db: AsyncSession,
    user: User,
    chatwoot_client: ChatwootClient,
) -> bool:
    """
    Синхронизирует одного пользователя в Chatwoot.
    
    Returns:
        True если пользователь был создан/найден, False если ошибка
    """
    # Если уже есть chatwoot_user_id, пропускаем
    if user.chatwoot_user_id:
        return True
    
    email = _build_chatwoot_email(user)
    
    # Проверяем существование пользователя в Chatwoot по email
    try:
        existing = await chatwoot_client.find_user_by_email(email)
        if existing:
            user.chatwoot_user_id = existing.get("id")
            await db.flush()
            
            # Сохраняем маппинг в user_mapping если есть cl_ref_key
            if user.cl_ref_key and user.chatwoot_user_id:
                try:
                    stmt = insert(UserMapping).values(
                        chatwoot_user_id=user.chatwoot_user_id,
                        cl_manager_key=user.cl_ref_key
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["chatwoot_user_id"],
                        set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                    )
                    await db.execute(stmt)
                    await db.flush()
                except Exception as mapping_error:
                    logger.warning(f"Failed to save user mapping: {mapping_error}")
            
            return True
    except Exception as e:
        logger.error(f"Failed to check existing Chatwoot user by email for {user.description}: {e}", exc_info=True)
    
    # Ищем по cl_ref_key в кастомных атрибутах (fallback)
    if user.cl_ref_key:
        try:
            existing = await chatwoot_client.find_user_by_custom_attribute("cl_ref_key", user.cl_ref_key)
            if existing:
                user.chatwoot_user_id = existing.get("id")
                await db.flush()
                
                # Сохраняем маппинг в user_mapping если есть cl_ref_key
                if user.cl_ref_key and user.chatwoot_user_id:
                    try:
                        stmt = insert(UserMapping).values(
                            chatwoot_user_id=user.chatwoot_user_id,
                            cl_manager_key=user.cl_ref_key
                        )
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["chatwoot_user_id"],
                            set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                        )
                        await db.execute(stmt)
                        await db.flush()
                    except Exception as mapping_error:
                        logger.warning(f"Failed to save user mapping: {mapping_error}")
                
                return True
        except Exception as e:
            logger.error(f"Failed to check existing Chatwoot user by cl_ref_key for {user.description}: {e}", exc_info=True)
    
    # Создаем нового пользователя в Chatwoot
    name = user.description or f"User {user.cl_ref_key or user.account_id}"
    custom_attrs = {}
    if user.cl_ref_key:
        custom_attrs["cl_ref_key"] = user.cl_ref_key

    try:
        created = await chatwoot_client.create_user(
            name=name,
            email=email,
            custom_attributes=custom_attrs if custom_attrs else None,
        )
        
        user.chatwoot_user_id = created.get("id")
        await db.flush()
        
        # Сохраняем маппинг в user_mapping если есть cl_ref_key
        if user.cl_ref_key and user.chatwoot_user_id:
            try:
                stmt = insert(UserMapping).values(
                    chatwoot_user_id=user.chatwoot_user_id,
                    cl_manager_key=user.cl_ref_key
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["chatwoot_user_id"],
                    set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                )
                await db.execute(stmt)
                await db.flush()
                logger.debug(f"Saved user mapping: chatwoot_user_id={user.chatwoot_user_id}, cl_manager_key={user.cl_ref_key}")
            except Exception as mapping_error:
                logger.warning(f"Failed to save user mapping: {mapping_error}")
        
        return True
    except Exception as e:
        # Если ошибка 422 - пользователь уже существует, пытаемся найти его
        import httpx
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 422:
            # Пытаемся найти пользователя по имени (точное совпадение)
            try:
                existing = await chatwoot_client.find_user_by_name(name)
                if existing:
                    user.chatwoot_user_id = existing.get("id")
                    await db.flush()
                    
                    # Сохраняем маппинг в user_mapping если есть cl_ref_key
                    if user.cl_ref_key and user.chatwoot_user_id:
                        try:
                            stmt = insert(UserMapping).values(
                                chatwoot_user_id=user.chatwoot_user_id,
                                cl_manager_key=user.cl_ref_key
                            )
                            stmt = stmt.on_conflict_do_update(
                                index_elements=["chatwoot_user_id"],
                                set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                            )
                            await db.execute(stmt)
                            await db.flush()
                        except Exception as mapping_error:
                            logger.warning(f"Failed to save user mapping: {mapping_error}")
                    
                    return True
            except Exception as find_error:
                logger.error(f"Failed to find existing Chatwoot user by name for {user.description}: {find_error}", exc_info=True)
            
            # Если не нашли по имени, пытаемся найти по cl_ref_key в списке всех агентов
            if user.cl_ref_key:
                try:
                    all_agents = await chatwoot_client.list_all_agents()
                    for agent in all_agents:
                        # Проверяем custom_attributes если доступны
                        custom_attrs_agent = agent.get("custom_attributes", {})
                        if custom_attrs_agent and custom_attrs_agent.get("cl_ref_key") == user.cl_ref_key:
                            user.chatwoot_user_id = agent.get("id")
                            await db.flush()
                            
                            # Сохраняем маппинг в user_mapping если есть cl_ref_key
                            if user.cl_ref_key and user.chatwoot_user_id:
                                try:
                                    stmt = insert(UserMapping).values(
                                        chatwoot_user_id=user.chatwoot_user_id,
                                        cl_manager_key=user.cl_ref_key
                                    )
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=["chatwoot_user_id"],
                                        set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                                    )
                                    await db.execute(stmt)
                                    await db.flush()
                                except Exception as mapping_error:
                                    logger.warning(f"Failed to save user mapping: {mapping_error}")
                            
                            return True
                except Exception as find_error:
                    logger.error(f"Failed to find existing Chatwoot user by cl_ref_key in all agents for {user.description}: {find_error}", exc_info=True)
            
            # Если все еще не нашли, пытаемся найти по части имени (fallback)
            # Это может помочь, если имя немного отличается
            try:
                all_agents = await chatwoot_client.list_all_agents()
                name_parts = name.lower().strip().split()
                if len(name_parts) >= 2:
                    # Пытаемся найти по первым двум словам имени
                    search_name = " ".join(name_parts[:2])
                    for agent in all_agents:
                        agent_name = agent.get("name", "").lower().strip()
                        agent_available_name = agent.get("available_name", "").lower().strip()
                        if (agent_name and search_name in agent_name) or (agent_available_name and search_name in agent_available_name):
                            user.chatwoot_user_id = agent.get("id")
                            await db.flush()
                            
                            # Сохраняем маппинг в user_mapping если есть cl_ref_key
                            if user.cl_ref_key and user.chatwoot_user_id:
                                try:
                                    stmt = insert(UserMapping).values(
                                        chatwoot_user_id=user.chatwoot_user_id,
                                        cl_manager_key=user.cl_ref_key
                                    )
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=["chatwoot_user_id"],
                                        set_={"cl_manager_key": stmt.excluded.cl_manager_key, "updated_at": func.now()}
                                    )
                                    await db.execute(stmt)
                                    await db.flush()
                                except Exception as mapping_error:
                                    logger.warning(f"Failed to save user mapping: {mapping_error}")
                            
                            return True
            except Exception as find_error:
                logger.error(f"Failed to find existing Chatwoot user by partial name for {user.description}: {find_error}", exc_info=True)
        
        logger.error(f"Failed to create Chatwoot user for {user.description}: {e}", exc_info=True)
        return False


async def sync_all_users():
    """
    Синхронизирует пользователей из БД в Chatwoot.
    
    Синхронизируются только операторы, у которых есть:
    - con_limit (лимит консультаций)
    - НЕ deletion_mark (не помечен на удаление)
    - НЕ invalid (не недействителен)
    - НЕ service (не служебный)
    - start_hour или end_hour (время работы)
    """
    if not (settings.CHATWOOT_API_URL and settings.CHATWOOT_API_TOKEN):
        logger.error("Chatwoot config missing. Check CHATWOOT_API_URL, CHATWOOT_API_TOKEN")
        sys.exit(1)
    
    # ВАЖНО: Настраиваем пул соединений для ETL скрипта
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        pool_size=2,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30
    )
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
    chatwoot_client = ChatwootClient()
    
    try:
        async with AsyncSessionLocal() as db:
            # Получаем только нужных операторов:
            # - без chatwoot_user_id (еще не синхронизированы)
            # - с con_limit (есть лимит консультаций)
            # - с start_hour или end_hour (есть время работы)
            # - не удаленные и не недействительные
            # Синхронизируем всех пользователей с chatwoot_user_id=None (новые)
            # И также обновляем маппинг для существующих пользователей с chatwoot_user_id
            result_new = await db.execute(
                select(User).where(
                    and_(
                        User.chatwoot_user_id.is_(None),
                        User.con_limit.isnot(None),
                        or_(User.start_hour.isnot(None), User.end_hour.isnot(None)),
                        User.deletion_mark == False,
                        User.invalid == False,
                    )
                )
            )
            users_new = result_new.scalars().all()
            
            # Также синхронизируем пользователей, у которых есть chatwoot_user_id но нет маппинга
            result_existing = await db.execute(
                select(User).where(
                    and_(
                        User.chatwoot_user_id.isnot(None),
                        User.cl_ref_key.isnot(None),
                        User.con_limit.isnot(None),
                        or_(User.start_hour.isnot(None), User.end_hour.isnot(None)),
                        User.deletion_mark == False,
                        User.invalid == False,
                    )
                )
            )
            users_existing = result_existing.scalars().all()
            
            # Проверяем какие из существующих пользователей не имеют маппинга
            users_without_mapping = []
            for user in users_existing:
                if user.chatwoot_user_id and user.cl_ref_key:
                    mapping_check = await db.execute(
                        select(UserMapping).where(
                            UserMapping.chatwoot_user_id == user.chatwoot_user_id
                        ).limit(1)
                    )
                    if not mapping_check.scalar_one_or_none():
                        users_without_mapping.append(user)
            
            users = list(users_new) + users_without_mapping
            
            if not users:
                logger.info("No users to sync with Chatwoot")
                return
            
            logger.info(f"Found {len(users_new)} new users and {len(users_without_mapping)} users without mapping to sync")
            
            synced = 0
            failed = 0
            
            for user in users:
                if await sync_user_to_chatwoot(db, user, chatwoot_client):
                    synced += 1
                else:
                    failed += 1
            
            await db.commit()
            
            if failed > 0:
                logger.error(f"Sync completed with errors. Synced: {synced}, Failed: {failed}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(sync_all_users())

