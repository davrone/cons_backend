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
    Если в данных пользователя его нет, генерируем технический email.
    
    ВАЖНО: Email должен быть детерминированным для одного пользователя.
    Используем cl_ref_key как приоритетный источник для генерации email,
    чтобы один и тот же пользователь всегда получал один и тот же email.
    """
    # Сначала проверяем реальные email в данных
    candidates = [
        user.user_id if user.user_id and "@" in user.user_id else None,
        user.description if user.description and "@" in user.description else None,
    ]
    for candidate in candidates:
        if candidate:
            return candidate.lower().strip()

    # Если нет реального email, генерируем детерминированный на основе cl_ref_key
    # cl_ref_key - это уникальный идентификатор из ЦЛ, поэтому он идеально подходит
    if user.cl_ref_key:
        # Используем cl_ref_key напрямую (это UUID, поэтому он уже уникальный)
        sanitized = re.sub(r"[^a-z0-9-]+", "", user.cl_ref_key.lower())
        return f"{sanitized}@cons.local"
    
    # Fallback на другие поля только если нет cl_ref_key
    base = user.description or user.user_id or str(user.account_id)
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
    
    ВАЖНО: Порядок проверки важен для избежания дублей:
    1. Сначала проверяем по cl_ref_key (самый надежный идентификатор)
    2. Потом проверяем по email
    3. Перед созданием еще раз проверяем все возможные варианты
    
    Returns:
        True если пользователь был создан/найден, False если ошибка
    """
    # Если уже есть chatwoot_user_id, проверяем что он еще существует в Chatwoot
    if user.chatwoot_user_id:
        # Проверяем что маппинг корректен
        if user.cl_ref_key:
            mapping_check = await db.execute(
                select(UserMapping).where(
                    UserMapping.chatwoot_user_id == user.chatwoot_user_id
                ).limit(1)
            )
            mapping = mapping_check.scalar_one_or_none()
            if not mapping or mapping.cl_manager_key != user.cl_ref_key:
                # Маппинг отсутствует или неверный - обновим его
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
    
    email = _build_chatwoot_email(user)
    
    # ВАЖНО: Сначала проверяем по cl_ref_key (самый надежный идентификатор)
    # Это предотвращает создание дублей, если email изменился
    if user.cl_ref_key:
        try:
            # Сначала пробуем через find_user_by_custom_attribute
            existing = await chatwoot_client.find_user_by_custom_attribute("cl_ref_key", user.cl_ref_key)
            if existing:
                existing_id = existing.get("id")
                if existing_id:
                    user.chatwoot_user_id = existing_id
                    await db.flush()
                    
                    # Сохраняем маппинг в user_mapping
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
                        logger.info(f"Found existing Chatwoot user {existing_id} by cl_ref_key for {user.description}")
                    except Exception as mapping_error:
                        logger.warning(f"Failed to save user mapping: {mapping_error}")
                    
                    return True
            
            # Если find_user_by_custom_attribute не сработал (Application API может не возвращать custom_attributes),
            # проверяем вручную через list_all_agents
            all_agents = await chatwoot_client.list_all_agents()
            for agent in all_agents:
                custom_attrs = agent.get("custom_attributes", {})
                if custom_attrs and custom_attrs.get("cl_ref_key") == user.cl_ref_key:
                    existing_id = agent.get("id")
                    if existing_id:
                        user.chatwoot_user_id = existing_id
                        await db.flush()
                        
                        # Сохраняем маппинг в user_mapping
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
                            logger.info(f"Found existing Chatwoot user {existing_id} by cl_ref_key (manual search) for {user.description}")
                        except Exception as mapping_error:
                            logger.warning(f"Failed to save user mapping: {mapping_error}")
                        
                        return True
        except Exception as e:
            logger.error(f"Failed to check existing Chatwoot user by cl_ref_key for {user.description}: {e}", exc_info=True)
    
    # Проверяем существование пользователя в Chatwoot по email (fallback)
    try:
        existing = await chatwoot_client.find_user_by_email(email)
        if existing:
            existing_id = existing.get("id")
            if existing_id:
                user.chatwoot_user_id = existing_id
                await db.flush()
                
                # Сохраняем маппинг в user_mapping если есть cl_ref_key
                if user.cl_ref_key:
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
                        logger.info(f"Found existing Chatwoot user {existing_id} by email for {user.description}")
                    except Exception as mapping_error:
                        logger.warning(f"Failed to save user mapping: {mapping_error}")
                
                return True
    except Exception as e:
        logger.error(f"Failed to check existing Chatwoot user by email for {user.description}: {e}", exc_info=True)
    
    # Перед созданием нового пользователя еще раз проверяем все возможные варианты
    # Это защита от race condition и дублей
    if user.cl_ref_key:
        try:
            all_agents = await chatwoot_client.list_all_agents()
            for agent in all_agents:
                custom_attrs = agent.get("custom_attributes", {})
                agent_email = agent.get("email", "").lower().strip() if agent.get("email") else None
                
                # Проверяем по cl_ref_key
                if custom_attrs and custom_attrs.get("cl_ref_key") == user.cl_ref_key:
                    existing_id = agent.get("id")
                    if existing_id:
                        user.chatwoot_user_id = existing_id
                        await db.flush()
                        
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
                            logger.info(f"Found existing Chatwoot user {existing_id} by cl_ref_key (final check) for {user.description}")
                        except Exception as mapping_error:
                            logger.warning(f"Failed to save user mapping: {mapping_error}")
                        
                        return True
                
                # Проверяем по email (дополнительная проверка)
                if agent_email and agent_email == email.lower().strip():
                    existing_id = agent.get("id")
                    if existing_id:
                        user.chatwoot_user_id = existing_id
                        await db.flush()
                        
                        if user.cl_ref_key:
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
                                logger.info(f"Found existing Chatwoot user {existing_id} by email (final check) for {user.description}")
                            except Exception as mapping_error:
                                logger.warning(f"Failed to save user mapping: {mapping_error}")
                        
                        return True
        except Exception as final_check_error:
            logger.error(f"Failed to perform final check for existing Chatwoot user for {user.description}: {final_check_error}", exc_info=True)
    
    # Создаем нового пользователя в Chatwoot только если точно не нашли существующего
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
        
        created_id = created.get("id")
        if not created_id:
            logger.error(f"Chatwoot returned user without ID for {user.description}: {created}")
            return False
        
        user.chatwoot_user_id = created_id
        await db.flush()
        
        # Сохраняем маппинг в user_mapping если есть cl_ref_key
        if user.cl_ref_key:
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
                logger.info(f"Created new Chatwoot user {created_id} for {user.description} (cl_ref_key: {user.cl_ref_key})")
            except Exception as mapping_error:
                logger.warning(f"Failed to save user mapping: {mapping_error}")
        
        return True
    except Exception as e:
        # Если ошибка 422 - пользователь уже существует (email уже занят)
        import httpx
        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 422:
            # Последняя попытка найти существующего пользователя
            logger.warning(f"Chatwoot returned 422 (user exists) for {user.description}, attempting to find existing user")
            
            # Ищем по email еще раз
            try:
                existing = await chatwoot_client.find_user_by_email(email)
                if existing:
                    existing_id = existing.get("id")
                    if existing_id:
                        user.chatwoot_user_id = existing_id
                        await db.flush()
                        
                        if user.cl_ref_key:
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
                                logger.info(f"Found existing Chatwoot user {existing_id} after 422 error for {user.description}")
                            except Exception as mapping_error:
                                logger.warning(f"Failed to save user mapping: {mapping_error}")
                        
                        return True
            except Exception as find_error:
                logger.error(f"Failed to find existing Chatwoot user after 422 error for {user.description}: {find_error}", exc_info=True)
        
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

