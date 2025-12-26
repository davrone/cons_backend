"""Схемы для работы с консультациями"""
from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, time

from .clients import ClientCreate


def parse_datetime_flexible(value: Optional[str]) -> Optional[datetime]:
    """Гибкий парсинг datetime с поддержкой различных форматов"""
    if not value:
        return None
    
    if isinstance(value, datetime):
        return value
    
    if isinstance(value, str):
        # Исправляем распространенные ошибки формата
        # "2025-12-04T18:01:58:00Z" -> "2025-12-04T18:01:58Z"
        # Убираем лишние :00 перед Z или +
        import re
        # Сначала исправляем :SS:00Z -> :SSZ (где SS - любые две цифры секунд)
        value = re.sub(r':(\d{2}):00([Z\+])', r':\1\2', value)
        # Затем исправляем :00Z -> Z (на случай если формат еще более неправильный)
        value = re.sub(r':00([Z\+])', r'\1', value)
        
        # Пробуем разные форматы парсинга
        try:
            # Стандартный ISO формат с Z
            if value.endswith('Z'):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            # Стандартный ISO формат с timezone
            if '+' in value or (value.count('-') > 2 and not value.endswith('Z')):
                return datetime.fromisoformat(value)
            # Формат без timezone (считаем UTC)
            dt = datetime.fromisoformat(value)
            # Добавляем UTC timezone если его нет
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError as e:
            # Последняя попытка - парсинг через dateutil (если доступен)
            try:
                from dateutil import parser
                return parser.isoparse(value)
            except (ImportError, ValueError):
                # Возвращаем строку ошибки вместо ValueError объекта
                error_msg = str(e) if e else "Unknown error"
                raise ValueError(f"Invalid datetime format: {value}. Original error: {error_msg}")


class ConsultationCreate(BaseModel):
    """Создание консультации - данные только консультации"""
    client_id: Optional[str] = None  # UUID клиента (если уже есть)
    cl_ref_key: Optional[str] = None  # Ref_Key из ЦЛ
    org_inn: Optional[str] = None
    lang: Optional[str] = "ru"
    comment: Optional[str] = None  # Вопрос/описание
    topic: Optional[str] = None  # Тема
    online_question_cat: Optional[str] = None  # КатегорияВопроса_Key
    online_question: Optional[str] = None  # ВопросНаКонсультацию_Key
    consultation_type: Optional[str] = None  # Вид обращения: "Техническая поддержка" или "Консультация по ведению учёта"
    selected_software: Optional[str] = None  # Выбранное ПО: "бух" (бухгалтерия), "рт" (розница), "ук" (управление компанией)
    importance: Optional[int] = None
    scheduled_at: Optional[datetime] = None  # Желаемая дата/время консультации
    
    @field_validator('scheduled_at', mode='before')
    @classmethod
    def parse_scheduled_at(cls, v):
        """Валидатор для гибкого парсинга scheduled_at"""
        return parse_datetime_flexible(v)


class ConsultationWithClient(BaseModel):
    """Создание консультации с данными клиента (комплексный запрос от фронта)"""
    # Данные клиента (если клиента еще нет)
    client: Optional[ClientCreate] = None
    
    # Данные консультации
    consultation: ConsultationCreate
    
    # Метаданные
    source: Optional[str] = "SITE"  # SITE, TELEGRAM, CALL_CENTER
    
    # Telegram данные (если создается через Telegram Web App)
    telegram_user_id: Optional[int] = None  # ID пользователя Telegram
    telegram_phone_number: Optional[str] = None  # Телефон из контакта Telegram


class ConsultationCreateSimple(BaseModel):
    """Создание консультации (упрощенная версия, алиас для обратной совместимости)"""
    client_id: Optional[str] = None
    cl_ref_key: Optional[str] = None
    org_inn: Optional[str] = None
    lang: Optional[str] = "ru"
    comment: Optional[str] = None
    online_question_cat: Optional[str] = None
    online_question: Optional[str] = None
    importance: Optional[int] = None
    scheduled_at: Optional[datetime] = None


class ConsultationUpdate(BaseModel):
    """Обновление консультации"""
    status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    comment: Optional[str] = None
    topic: Optional[str] = None
    importance: Optional[int] = None
    
    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def parse_datetime_fields(cls, v):
        """Валидатор для гибкого парсинга дат"""
        return parse_datetime_flexible(v)


class ConsultationRead(BaseModel):
    """Чтение консультации"""
    cons_id: str
    cl_ref_key: Optional[str] = None
    client_id: Optional[str] = None
    client_key: Optional[str] = None
    number: Optional[str] = None
    status: Optional[str] = None
    org_inn: Optional[str] = None
    importance: Optional[int] = None
    create_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    redate_time: Optional[time] = None
    redate: Optional[date] = None
    lang: Optional[str] = None
    consultation_type: Optional[str] = None  # Вид обращения: "Техническая поддержка" или "Консультация по ведению учёта"
    denied: Optional[bool] = None
    manager: Optional[str] = None  # UUID менеджера (cl_ref_key)
    manager_name: Optional[str] = None  # ФИО менеджера (из users.description)
    author: Optional[str] = None
    comment: Optional[str] = None
    online_question_cat: Optional[str] = None
    online_question: Optional[str] = None
    con_blocks: Optional[str] = None  # Блоки консультации
    con_rates: Optional[Dict[str, Any]] = None
    con_calls: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None  # Может быть словарем или списком словарей
    chatwoot_source_id: Optional[str] = None  # source_id из Chatwoot (для подключения виджета)
    source: Optional[str] = None  # Источник создания: BACKEND, 1C_CL, CHATWOOT, ETL
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_model(cls, model, manager_name: Optional[str] = None):
        """
        Конвертация модели SQLAlchemy в схему Pydantic.
        
        Args:
            model: Модель Consultation
            manager_name: ФИО менеджера (опционально, если не передано, будет None)
        """
        return cls(
            cons_id=model.cons_id,
            cl_ref_key=model.cl_ref_key,
            client_id=str(model.client_id) if model.client_id else None,
            client_key=model.client_key,
            number=model.number,
            status=model.status,
            org_inn=model.org_inn,
            importance=model.importance,
            create_date=model.create_date,
            start_date=model.start_date,
            end_date=model.end_date,
            redate_time=model.redate_time,
            redate=model.redate,
            lang=model.lang,
            consultation_type=model.consultation_type,
            denied=model.denied,
            manager=model.manager,
            manager_name=manager_name,
            author=model.author,
            comment=model.comment,
            online_question_cat=model.online_question_cat,
            online_question=model.online_question,
            con_blocks=model.con_blocks,
            con_rates=model.con_rates,
            con_calls=model.con_calls,
            chatwoot_source_id=model.chatwoot_source_id,
            source=model.source,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    class Config:
        from_attributes = True


class ConsultationListResponse(BaseModel):
    """Список консультаций клиента"""
    consultations: List[ConsultationRead]
    total: int


class ConsultationResponse(BaseModel):
    """Ответ при создании консультации"""
    consultation: ConsultationRead
    client_id: str
    message: str = "Consultation created successfully"
    source: Optional[str] = None  # Источник создания консультации (TELEGRAM, SITE, BACKEND)
    telegram_user_id: Optional[int] = None  # ID пользователя Telegram (если создано через Telegram)
    bot_username: Optional[str] = None  # Username бота для Telegram (если создано через Telegram)
    # Поля для подключения чат-виджета Chatwoot
    chatwoot_conversation_id: Optional[str] = None  # ID conversation в Chatwoot (cons_id)
    chatwoot_source_id: Optional[str] = None  # source_id из contact (для идентификации пользователя в виджете)
    chatwoot_account_id: Optional[str] = None  # account_id для подключения виджета
    chatwoot_inbox_id: Optional[int] = None  # inbox_id для подключения виджета
    chatwoot_pubsub_token: Optional[str] = None  # pubsub_token для WebSocket подключения (из Public API)


# Алиасы для обратной совместимости (deprecated, использовать ConsultationRead и т.д.)
TicketCreate = ConsultationCreateSimple
TicketRead = ConsultationRead
TicketListResponse = ConsultationListResponse
