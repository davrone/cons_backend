"""
SQLAlchemy модели для системы консультаций.

Схемы:
- cons: бизнес-данные (клиенты, консультации, пользователи)
- dict: справочники
- sys: служебные таблицы
- log: логирование
"""
from typing import Optional

from sqlalchemy import (
    Column, String, Boolean, Integer, DateTime, ForeignKey, Text, 
    JSON, SmallInteger, Time, Date, Sequence, PrimaryKeyConstraint,
    UniqueConstraint
)
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from .database import Base


# ============================================================================
# SCHEMA: cons (основные бизнес-таблицы)
# ============================================================================

class Client(Base):
    """Клиенты"""
    __tablename__ = "clients"
    __table_args__ = {"schema": "cons"}

    client_id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.uuid_generate_v4())
    client_id_hash = Column(Text, unique=True, nullable=True)
    cl_ref_key = Column(Text, nullable=True)  # TEXT из ЦЛ
    email = Column(Text, nullable=True)
    phone_number = Column(Text, nullable=True)
    country = Column(Text, nullable=True)
    region = Column(Text, nullable=True)
    city = Column(Text, nullable=True)
    subs_id = Column(Text, nullable=True)
    subs_start = Column(DateTime(timezone=True), nullable=True)
    subs_end = Column(DateTime(timezone=True), nullable=True)
    tariff_id = Column(Text, nullable=True)
    tariffperiod_id = Column(Text, nullable=True)
    org_id = Column(Text, nullable=True)
    org_inn = Column(Text, nullable=True)
    name = Column(Text, nullable=True)
    contact_name = Column(Text, nullable=True)
    company_name = Column(Text, nullable=True)  # Название компании клиента для формирования имени в 1C (НЕ обслуживающая организация)
    code_abonent = Column(Text, nullable=True)
    source_id = Column(Text, nullable=True)  # source_id из Chatwoot (для идентификации контакта)
    chatwoot_pubsub_token = Column(Text, nullable=True)  # pubsub_token из Chatwoot (для WebSocket подключения виджета, принадлежит контакту)
    is_parent = Column(Boolean, default=False)
    parent_id = Column(UUID(as_uuid=True), ForeignKey("cons.clients.client_id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    @property
    def subscriber_id(self) -> Optional[str]:
        return self.code_abonent

    @subscriber_id.setter
    def subscriber_id(self, value: Optional[str]) -> None:
        self.code_abonent = value


class Consultation(Base):
    """Консультации (заявки)"""
    __tablename__ = "cons"
    __table_args__ = {"schema": "cons"}

    cons_id = Column(Text, primary_key=True)  # ID из Chatwoot (PK!)
    cl_ref_key = Column(Text, nullable=True)  # TEXT из ЦЛ
    client_id = Column(UUID(as_uuid=True), ForeignKey("cons.clients.client_id"), nullable=True)
    client_key = Column(Text, nullable=True)
    number = Column(Text, nullable=True)  # Номер из ЦЛ
    status = Column(Text, nullable=True)
    org_inn = Column(Text, nullable=True)
    importance = Column(Integer, nullable=True)
    create_date = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=True)
    end_date = Column(DateTime(timezone=True), nullable=True)
    redate_time = Column(Time, nullable=True)
    redate = Column(Date, nullable=True)
    lang = Column(Text, nullable=True)
    consultation_type = Column(Text, nullable=True)  # Вид обращения: "Техническая поддержка" или "Консультация по ведению учёта"
    denied = Column(Boolean, default=False)
    manager = Column(Text, nullable=True)  # TEXT (user_id из Chatwoot или ЦЛ)
    author = Column(Text, nullable=True)
    comment = Column(Text, nullable=True)
    online_question_cat = Column(Text, nullable=True)
    online_question = Column(Text, nullable=True)
    con_blocks = Column(Text, nullable=True)
    con_rates = Column(JSONB, nullable=True)
    con_calls = Column(JSONB, nullable=True)
    chatwoot_source_id = Column(Text, nullable=True)  # source_id из Chatwoot (для подключения виджета)
    source = Column(Text, nullable=True, default="BACKEND")  # Источник создания: BACKEND, 1C_CL, CHATWOOT, ETL
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class QAndA(Base):
    """Вопросы и ответы по консультациям"""
    __tablename__ = "q_and_a"
    __table_args__ = {"schema": "cons"}

    id = Column(Integer, primary_key=True, autoincrement=True)  # SERIAL
    org_inn = Column(Text, nullable=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("cons.clients.client_id"), nullable=True)
    cons_id = Column(Text, ForeignKey("cons.cons.cons_id"), nullable=True)  # FK к cons.cons_id
    cons_ref_key = Column(Text, nullable=True)
    line_number = Column(Integer, nullable=True)
    po_type_key = Column(Text, nullable=True)
    po_section_key = Column(Text, nullable=True)
    con_blocks_key = Column(Text, nullable=True)
    manager_help_key = Column(Text, nullable=True)
    is_repeat = Column(Boolean, default=False)
    question = Column(Text, nullable=True)
    answer = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class User(Base):
    """Пользователи (консультанты, операторы)"""
    __tablename__ = "users"
    __table_args__ = {"schema": "cons"}

    account_id = Column(UUID(as_uuid=True), primary_key=True, server_default=func.uuid_generate_v4())
    user_id = Column(Text, nullable=True)  # TEXT (ID из Chatwoot или ЦЛ)
    chatwoot_user_id = Column(Integer, nullable=True)  # ID пользователя в Chatwoot (для маппинга)
    chatwoot_team = Column(Text, nullable=True)
    avatar_url = Column(Text, nullable=True)
    confirmed = Column(Boolean, default=False)
    cl_ref_key = Column(Text, nullable=True)  # TEXT из ЦЛ
    deletion_mark = Column(Boolean, default=False)
    description = Column(Text, nullable=True)
    invalid = Column(Boolean, default=False)
    ru = Column(Boolean, default=True)
    uz = Column(Boolean, default=False)
    department = Column(Text, nullable=True)
    con_limit = Column(Integer, nullable=True)
    start_hour = Column(Time, nullable=True)  # TIME
    end_hour = Column(Time, nullable=True)  # TIME
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class UserSkill(Base):
    """Навыки пользователей"""
    __tablename__ = "users_skill"
    __table_args__ = (
        PrimaryKeyConstraint("user_key", "category_key"),
        {"schema": "cons"}
    )

    user_key = Column(Text, nullable=False)  # TEXT (ссылка на users)
    category_key = Column(Text, nullable=False)  # TEXT


class ConsRedate(Base):
    """История переносов консультаций"""
    __tablename__ = "cons_redate"
    __table_args__ = (
        UniqueConstraint("cons_key", "clients_key", "manager_key", "period", name="uq_cons_redate_keys"),
        {"schema": "cons"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    cons_key = Column(Text, nullable=True)
    clients_key = Column(Text, nullable=True)
    manager_key = Column(Text, nullable=True)
    period = Column(DateTime(timezone=True), nullable=False)
    old_date = Column(DateTime(timezone=True), nullable=True)
    new_date = Column(DateTime(timezone=True), nullable=True)
    comment = Column(Text, nullable=True)


class Call(Base):
    """Регистрация дозвонов менеджера до клиента"""
    __tablename__ = "calls"
    __table_args__ = (
        PrimaryKeyConstraint("period", "cons_key", "manager"),  # Составной PK для уникальности
        {"schema": "cons"}
    )

    period = Column(DateTime(timezone=True), nullable=False)  # Period из ЦЛ
    cons_key = Column(Text, nullable=False)  # ДокументОбращения_Key из ЦЛ
    cons_id = Column(Text, nullable=True)  # cons_id из Chatwoot (для связи с cons.cons)
    client_key = Column(Text, nullable=True)  # Абонент_Key из ЦЛ
    client_id = Column(UUID(as_uuid=True), ForeignKey("cons.clients.client_id"), nullable=True)
    manager = Column(Text, nullable=False)  # Менеджер_Key из ЦЛ


class QueueClosing(Base):
    """Регистр закрытия очереди для консультантов"""
    __tablename__ = "queue_closing"
    __table_args__ = (
        PrimaryKeyConstraint("period", "manager_key"),
        {"schema": "cons"},
    )

    period = Column(DateTime(timezone=True), nullable=False)  # Period из ЦЛ (дата закрытия очереди)
    manager_key = Column(Text, nullable=False)  # Менеджер_Key из ЦЛ
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ConsRatingAnswer(Base):
    """Подробные ответы на оценку консультации"""
    __tablename__ = "cons_rating_answers"
    __table_args__ = (
        UniqueConstraint("cons_key", "manager_key", "question_number", name="uq_cons_rating_answer"),
        {"schema": "cons"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    cons_key = Column(Text, nullable=False, index=True)
    cons_id = Column(Text, nullable=True, index=True)
    client_key = Column(Text, nullable=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("cons.clients.client_id"), nullable=True)
    manager_key = Column(Text, nullable=True)
    question_number = Column(Integer, nullable=False)
    rating = Column(SmallInteger, nullable=True)
    question_text = Column(Text, nullable=True)
    comment = Column(Text, nullable=True)
    sent_to_base = Column(Boolean, nullable=True)
    rating_date = Column(DateTime(timezone=True), nullable=True)  # ДатаОценки из 1C
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


# ============================================================================
# SCHEMA: dict (справочники)
# ============================================================================

class OnlineQuestionCat(Base):
    """Категории онлайн-вопросов"""
    __tablename__ = "online_question_cat"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    code = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    language = Column(Text, nullable=True)


class OnlineQuestion(Base):
    """Онлайн-вопросы"""
    __tablename__ = "online_question"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    code = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    language = Column(Text, nullable=True)
    category_key = Column(Text, nullable=True)  # TEXT FK к online_question_cat
    useful_info = Column(Text, nullable=True)
    question = Column(Text, nullable=True)


class KnowledgeBase(Base):
    """База знаний"""
    __tablename__ = "knowledge_base"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    description = Column(Text, nullable=True)
    po_type_key = Column(Text, nullable=True)
    po_section_key = Column(Text, nullable=True)
    author_key = Column(Text, nullable=True)
    question = Column(Text, nullable=True)
    answer = Column(Text, nullable=True)


class POSection(Base):
    """Разделы ПО"""
    __tablename__ = "po_sections"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    owner_key = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    details = Column(Text, nullable=True)  # Может быть JSON, но в требованиях TEXT


class POType(Base):
    """Типы ПО"""
    __tablename__ = "po_types"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    description = Column(Text, nullable=True)
    details = Column(Text, nullable=True)  # Может быть JSON, но в требованиях TEXT


class ConsultationInterference(Base):
    """Помехи для консультаций"""
    __tablename__ = "consultation_interference"
    __table_args__ = {"schema": "dict"}

    ref_key = Column(Text, primary_key=True)  # TEXT PK
    code = Column(Text, nullable=True)
    description = Column(Text, nullable=True)


# ============================================================================
# SCHEMA: sys (служебные таблицы)
# ============================================================================

class Migration(Base):
    """История миграций БД"""
    __tablename__ = "db_migrations"
    __table_args__ = {"schema": "sys"}

    id = Column(Integer, primary_key=True, autoincrement=True)  # SERIAL
    version = Column(Text, unique=True, nullable=False)
    applied_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class UserMapping(Base):
    """Маппинг менеджеров между Chatwoot и 1C:ЦЛ"""
    __tablename__ = "user_mapping"
    __table_args__ = (
        UniqueConstraint("chatwoot_user_id", name="uq_user_mapping_chatwoot_user_id"),
        UniqueConstraint("cl_manager_key", name="uq_user_mapping_cl_manager_key"),
        {"schema": "sys"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    chatwoot_user_id = Column(Integer, nullable=False, unique=True)  # ID пользователя в Chatwoot
    cl_manager_key = Column(Text, nullable=False, unique=True)  # Ключ менеджера в ЦЛ (GUID)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


# ============================================================================
# SCHEMA: log (логирование)
# ============================================================================

class WebhookLog(Base):
    """Логи вебхуков"""
    __tablename__ = "webhook_log"
    __table_args__ = {"schema": "log"}

    id = Column(Integer, primary_key=True, autoincrement=True)  # SERIAL
    source = Column(Text, nullable=True)  # Источник: CHATWOOT, 1C_CL и т.д.
    payload = Column(JSONB, nullable=True)  # Полный payload вебхука
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class NotificationLog(Base):
    """Лог отправленных уведомлений для предотвращения дублирования"""
    __tablename__ = "notification_log"
    __table_args__ = {"schema": "log"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    notification_type = Column(Text, nullable=False)  # Тип уведомления (e.g., 'redate', 'manager_reassignment', 'rating', 'call')
    entity_id = Column(Text, nullable=False)  # ID сущности, к которой относится уведомление (e.g., cons_id)
    unique_hash = Column(Text, unique=True, nullable=False)  # Хеш для предотвращения дублирования
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ConsultationChangeLog(Base):
    """Лог изменений консультаций для отслеживания синхронизации"""
    __tablename__ = "consultation_change_log"
    __table_args__ = {"schema": "log"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    cons_id = Column(Text, nullable=False, index=True)  # ID консультации
    field_name = Column(Text, nullable=False)  # Название измененного поля
    old_value = Column(Text, nullable=True)  # Старое значение (JSON строка)
    new_value = Column(Text, nullable=True)  # Новое значение (JSON строка)
    source = Column(Text, nullable=False)  # Источник изменения: CHATWOOT, 1C_CL, API, ETL
    synced_to_chatwoot = Column(Boolean, default=False, nullable=False)  # Синхронизировано в Chatwoot
    synced_to_1c = Column(Boolean, default=False, nullable=False)  # Синхронизировано в 1C:ЦЛ
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class IdempotencyKey(Base):
    """Таблица для хранения idempotency keys для предотвращения дублирования операций"""
    __tablename__ = "idempotency_keys"
    __table_args__ = (
        UniqueConstraint("key", "operation_type", name="uq_idempotency_key"),
        {"schema": "sys"}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(Text, nullable=False, index=True)  # Idempotency key от клиента
    operation_type = Column(Text, nullable=False)  # Тип операции: create_consultation, update_consultation, etc.
    resource_id = Column(Text, nullable=True)  # ID созданного/обновленного ресурса
    request_hash = Column(Text, nullable=True)  # Хеш запроса для проверки идентичности
    response_data = Column(JSONB, nullable=True)  # Кэшированный ответ (для повторных запросов)
    expires_at = Column(DateTime(timezone=True), nullable=False)  # Время истечения ключа
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)