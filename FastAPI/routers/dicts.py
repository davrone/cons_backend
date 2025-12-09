from __future__ import annotations

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from functools import lru_cache

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..dependencies.security import verify_front_secret
from ..models import (
    POType,
    POSection,
    OnlineQuestionCat,
    OnlineQuestion,
    KnowledgeBase,
    ConsultationInterference,
)
from ..schemas.dicts import (
    POTypeRead,
    POTypeReadSimple,
    POSectionRead,
    OnlineQuestionCategoryRead,
    OnlineQuestionRead,
    KnowledgeBaseEntry,
    ConsultationInterferenceRead,
)

router = APIRouter(dependencies=[Depends(verify_front_secret)])

# Простой in-memory кэш с TTL 30 минут
_cache: Dict[str, tuple] = {}  # key -> (data, expiry_time)
CACHE_TTL = timedelta(minutes=30)


def _get_cache_key(endpoint: str, **params) -> str:
    """Генерирует ключ кэша из endpoint и параметров"""
    if params:
        sorted_params = sorted(params.items())
        param_str = "&".join(f"{k}={v}" for k, v in sorted_params if v is not None)
        return f"{endpoint}?{param_str}"
    return endpoint


def _get_cached(key: str) -> Optional[Any]:
    """Получает данные из кэша если они не устарели"""
    if key in _cache:
        data, expiry = _cache[key]
        if datetime.now() < expiry:
            return data
        del _cache[key]
    return None


def _set_cache(key: str, data: Any):
    """Сохраняет данные в кэш с TTL"""
    expiry = datetime.now() + CACHE_TTL
    _cache[key] = (data, expiry)


@router.get("/po-types", response_model=List[POTypeReadSimple])
async def list_po_types(db: AsyncSession = Depends(get_db)):
    """Получить список типов ПО (упрощенная версия для фронтенда)"""
    cache_key = _get_cache_key("po-types")
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    result = await db.execute(select(POType).order_by(POType.description))
    data = [POTypeReadSimple.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data


@router.get("/po-sections", response_model=List[POSectionRead])
async def list_po_sections(
    owner_key: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    cache_key = _get_cache_key("po-sections", owner_key=owner_key)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    stmt = select(POSection).order_by(POSection.description)
    if owner_key:
        stmt = stmt.where(POSection.owner_key == owner_key)
    result = await db.execute(stmt)
    data = [POSectionRead.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data


@router.get("/online-question/categories", response_model=List[OnlineQuestionCategoryRead])
async def list_online_question_categories(
    language: Optional[str] = Query(default=None, description="Filter by language code (ru/uz)"),
    db: AsyncSession = Depends(get_db),
):
    cache_key = _get_cache_key("online-question/categories", language=language)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    stmt = select(OnlineQuestionCat).order_by(OnlineQuestionCat.description)
    if language:
        stmt = stmt.where(OnlineQuestionCat.language == language)
    result = await db.execute(stmt)
    data = [OnlineQuestionCategoryRead.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data


@router.get("/online-questions", response_model=List[OnlineQuestionRead])
async def list_online_questions(
    language: Optional[str] = Query(default=None),
    category_key: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    cache_key = _get_cache_key("online-questions", language=language, category_key=category_key)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    stmt = select(OnlineQuestion).order_by(OnlineQuestion.description)
    if language:
        stmt = stmt.where(OnlineQuestion.language == language)
    if category_key:
        stmt = stmt.where(OnlineQuestion.category_key == category_key)
    result = await db.execute(stmt)
    data = [OnlineQuestionRead.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data


@router.get("/knowledge-base", response_model=List[KnowledgeBaseEntry])
async def list_knowledge_base(
    po_type_key: Optional[str] = Query(default=None),
    po_section_key: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    cache_key = _get_cache_key("knowledge-base", po_type_key=po_type_key, po_section_key=po_section_key)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    stmt = select(KnowledgeBase).order_by(KnowledgeBase.description)
    if po_type_key:
        stmt = stmt.where(KnowledgeBase.po_type_key == po_type_key)
    if po_section_key:
        stmt = stmt.where(KnowledgeBase.po_section_key == po_section_key)
    result = await db.execute(stmt)
    data = [KnowledgeBaseEntry.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data


@router.get("/interference", response_model=List[ConsultationInterferenceRead])
async def list_consultation_interference(db: AsyncSession = Depends(get_db)):
    cache_key = _get_cache_key("interference")
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    
    result = await db.execute(
        select(ConsultationInterference).order_by(ConsultationInterference.description)
    )
    data = [ConsultationInterferenceRead.model_validate(row) for row in result.scalars().all()]
    _set_cache(cache_key, data)
    return data

