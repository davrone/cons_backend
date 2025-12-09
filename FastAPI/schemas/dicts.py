from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict


class POTypeRead(BaseModel):
    ref_key: str
    description: Optional[str] = None
    details: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class POTypeReadSimple(BaseModel):
    """Упрощенная схема для фронтенда - только необходимые поля"""
    ref_key: str
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class POSectionRead(BaseModel):
    ref_key: str
    owner_key: Optional[str] = None
    description: Optional[str] = None
    details: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class OnlineQuestionCategoryRead(BaseModel):
    ref_key: str
    code: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class OnlineQuestionRead(BaseModel):
    ref_key: str
    code: Optional[str] = None
    description: Optional[str] = None
    language: Optional[str] = None
    category_key: Optional[str] = None
    useful_info: Optional[str] = None
    question: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class KnowledgeBaseEntry(BaseModel):
    ref_key: str
    description: Optional[str] = None
    po_type_key: Optional[str] = None
    po_section_key: Optional[str] = None
    author_key: Optional[str] = None
    question: Optional[str] = None
    answer: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class ConsultationInterferenceRead(BaseModel):
    ref_key: str
    code: Optional[str] = None
    description: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

