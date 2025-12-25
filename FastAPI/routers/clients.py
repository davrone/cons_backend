"""Роуты для работы с клиентами"""
import logging
import hashlib
import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Optional, Dict, Any
import uuid

from ..database import get_db
from ..dependencies.security import verify_front_secret
from ..models import Client
from ..schemas.clients import ClientCreate, ClientRead
from ..services.chatwoot_client import ChatwootClient, is_valid_email
from ..config import settings

logger = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(verify_front_secret)])


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


def _build_chatwoot_contact_custom_attrs(
    owner: Client,
    client: Client
) -> Dict[str, Any]:
    """
    Готовим custom attributes для Contact в Chatwoot.
    
    Custom attributes для Contact:
    - code_abonent, inn_pinfl, client_type (обязательные)
    - partner (обслуживающая организация, если есть)
    
    Регион/страна теперь передаются в типовые additional_attributes Chatwoot,
    не в custom_attributes.
    """
    attrs: Dict[str, Any] = {
        "code_abonent": owner.code_abonent or "",
        "inn_pinfl": owner.org_inn or "",
        "client_type": "owner" if not client.parent_id else "user",
    }
    
    # Обслуживающая организация - берем из владельца или клиента
    partner_to_use = owner.partner if owner.partner else client.partner
    if partner_to_use:
        attrs["partner"] = str(partner_to_use)
    
    # Фильтруем пустые значения для опциональных полей
    filtered = {}
    for key, value in attrs.items():
        # Обязательные поля всегда включаем (даже если пустые)
        if key in ("code_abonent", "inn_pinfl", "client_type"):
            filtered[key] = value if value else ""
        # Опциональные поля включаем только если не пустые
        elif value not in (None, "", []):
            filtered[key] = value
    
    return filtered


def _get_country_code(country_name: Optional[str]) -> Optional[str]:
    """
    Получает код страны из названия страны.
    
    Маппинг основных стран на их коды ISO 3166-1 alpha-2.
    """
    if not country_name:
        return None
    
    country_name_lower = country_name.strip().lower()
    
    # Маппинг названий стран на коды
    country_mapping = {
        "uzbekistan": "UZ",
        "узбекистан": "UZ",
        "russia": "RU",
        "россия": "RU",
        "russian federation": "RU",
        "российская федерация": "RU",
        "kazakhstan": "KZ",
        "казахстан": "KZ",
        "kyrgyzstan": "KG",
        "киргизия": "KG",
        "tajikistan": "TJ",
        "таджикистан": "TJ",
        "turkmenistan": "TM",
        "туркменистан": "TM",
    }
    
    # Проверяем точное совпадение
    if country_name_lower in country_mapping:
        return country_mapping[country_name_lower]
    
    # Проверяем частичное совпадение
    for key, code in country_mapping.items():
        if key in country_name_lower or country_name_lower in key:
            return code
    
    # Если страна уже является кодом из 2 символов
    if len(country_name.strip()) == 2:
        return country_name.strip().upper()
    
    return None


def _build_chatwoot_contact_additional_attrs(owner: Client, client: Client) -> Dict[str, Any]:
    """
    Типовые additional_attributes для Chatwoot Contact.
    Эти поля должны идти в additional_attributes, а не в custom_attributes.
    
    Формат:
    {
        "city": "г. Ташкент, Мирабадский район",
        "country": "Uzbekistan",
        "country_code": "UZ",
        "company_name": "OOO JASUR",
        "description": "Биография" (опционально)
    }
    """
    # Определяем источник данных: для пользователей берем данные владельца как fallback
    city_to_use = client.city if not client.parent_id else (client.city or owner.city)
    region_to_use = client.region if not client.parent_id else (client.region or owner.region)
    country_to_use = client.country if not client.parent_id else (client.country or owner.country)
    company_to_use = owner.company_name or client.company_name
    
    # Формируем строку города в формате: "<region>, <city>"
    city_str = None
    if region_to_use and city_to_use:
        city_str = f"{region_to_use}, {city_to_use}"
    elif region_to_use:
        city_str = str(region_to_use)
    elif city_to_use:
        city_str = str(city_to_use)
    
    # Получаем код страны из названия
    country_code = _get_country_code(country_to_use)
    
    attrs: Dict[str, Any] = {}
    if city_str:
        attrs["city"] = city_str
    if country_to_use:
        attrs["country"] = str(country_to_use)
    if country_code:
        attrs["country_code"] = country_code
    # company_name всегда добавляем, если есть значение (даже пустая строка не должна передаваться)
    if company_to_use and company_to_use.strip():
        attrs["company_name"] = str(company_to_use).strip()
    # description не заполняем, так как не знаем что туда добавить
    
    return attrs


async def _sync_client_to_chatwoot(
    client: Client,
    owner_client: Client
) -> None:
    """
    Создает или обновляет контакт в Chatwoot при создании/обновлении клиента.
    
    Args:
        client: Клиент для синхронизации
        owner_client: Владелец клиента (для пользователей - родитель, для владельцев - сам клиент)
    """
    
    # Если нет настроек Chatwoot, пропускаем
    if not settings.CHATWOOT_API_URL or not settings.CHATWOOT_API_TOKEN:
        logger.debug("Chatwoot not configured, skipping contact sync")
        return
    
    try:
        chatwoot_client = ChatwootClient()
        
        # Формируем данные контакта
        # ВАЖНО: В Chatwoot имя идет 1 в 1 без префикса CLOBUS
        contact_name = client.name or client.contact_name or owner_client.name or owner_client.contact_name or "Клиент"
        contact_email = client.email or owner_client.email
        contact_phone = client.phone_number or owner_client.phone_number
        
        # Валидируем email перед отправкой в Chatwoot
        if contact_email and not is_valid_email(contact_email):
            logger.warning(f"Invalid email format '{contact_email}' for client {client.client_id}, skipping email field")
            contact_email = None  # Не отправляем невалидный email
        
        # Если нет валидного email и phone, не создаем контакт
        if not contact_email and not contact_phone:
            logger.debug(f"Skipping Chatwoot contact creation for client {client.client_id}: no valid email or phone")
            return
        
        # Custom attributes и типовые additional_attributes
        contact_custom_attrs = _build_chatwoot_contact_custom_attrs(owner_client, client)
        contact_additional_attrs = _build_chatwoot_contact_additional_attrs(owner_client, client)
        
        # ВАЖНО: НЕ передаем source_id при создании contact - Chatwoot создает его автоматически
        # source_id будет извлечен из ответа создания contact
        
        # Проверяем, существует ли контакт по identifier
        existing_contact = await chatwoot_client.find_contact_by_identifier(str(client.client_id))
        
        if existing_contact:
            # Контакт существует - обновляем его через PATCH и извлекаем source_id
            logger.info(f"Contact already exists in Chatwoot for client {client.client_id}, updating via PATCH")
            
            # Извлекаем contact_id из существующего contact
            contact_id = existing_contact.get("id")
            if not contact_id:
                # Пробуем извлечь из payload
                payload_contact = existing_contact.get("payload", {})
                if isinstance(payload_contact, dict):
                    contact_id = payload_contact.get("contact", {}).get("id") if isinstance(payload_contact.get("contact"), dict) else None
            
            if contact_id:
                # ВАЖНО: Обновляем контакт через PATCH с актуальными данными
                try:
                    logger.info(f"Updating existing Chatwoot contact {contact_id} via PATCH for client {client.client_id}")
                    updated_contact = await chatwoot_client.update_contact(
                        contact_id=contact_id,
                        name=contact_name,
                        email=contact_email,
                        phone_number=contact_phone,
                        custom_attributes=contact_custom_attrs,
                        additional_attributes=contact_additional_attrs
                    )
                    logger.info(f"✓ Updated Chatwoot contact {contact_id} via PATCH for client {client.client_id}")
                except Exception as update_error:
                    logger.warning(f"Failed to update contact {contact_id} via PATCH: {update_error}, continuing with source_id extraction")
            
            # Извлекаем source_id из существующего contact
            # source_id может быть в contact_inbox.source_id или в корне ответа
            contact_source_id = None
            
            # Проверяем contact_inbox (может быть список или объект)
            contact_inboxes = existing_contact.get("contact_inboxes", [])
            if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                # Берем первый contact_inbox для нужного inbox_id
                for ci in contact_inboxes:
                    if ci.get("inbox_id") == settings.CHATWOOT_INBOX_ID:
                        contact_source_id = ci.get("source_id")
                        break
                # Если не нашли по inbox_id, берем первый доступный
                if not contact_source_id and len(contact_inboxes) > 0:
                    contact_source_id = contact_inboxes[0].get("source_id")
            
            # Если source_id не найден в найденном контакте, пробуем получить через GET
            if not contact_source_id and contact_id:
                try:
                    fetched_contact = await chatwoot_client.get_contact(contact_id)
                    payload_fetched = fetched_contact.get("payload", {})
                    contact_inboxes = payload_fetched.get("contact_inboxes", [])
                    if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                        for ci in contact_inboxes:
                            inbox_info = ci.get("inbox", {})
                            if inbox_info.get("id") == settings.CHATWOOT_INBOX_ID:
                                contact_source_id = ci.get("source_id")
                                break
                        if not contact_source_id and len(contact_inboxes) > 0:
                            contact_source_id = contact_inboxes[0].get("source_id")
                except Exception as get_error:
                    logger.warning(f"Failed to get contact {contact_id} to extract source_id: {get_error}")
            
            if contact_source_id:
                client.source_id = contact_source_id
                logger.info(f"✓ Found and updated existing Chatwoot contact source_id: {contact_source_id} for client {client.client_id}")
            else:
                logger.warning(f"Existing contact found but source_id not found in response for client {client.client_id}")
        else:
            # Создаем новый контакт
            logger.info(f"Creating Chatwoot contact for client {client.client_id}")
            try:
                # ВАЖНО: НЕ передаем source_id при создании contact - Chatwoot создает его автоматически
                new_contact = await chatwoot_client.create_contact(
                    name=contact_name,
                    identifier=str(client.client_id),  # Глобальный внешний ID (UUID)
                    email=contact_email,
                    phone_number=contact_phone,
                    custom_attributes=contact_custom_attrs,
                    inbox_id=settings.CHATWOOT_INBOX_ID,
                    additional_attributes=contact_additional_attrs
                    # source_id НЕ передаем - Chatwoot создает его автоматически
                )
                
                # Извлекаем contact_id из ответа
                contact_id = new_contact.get("payload", {}).get("contact", {}).get("id")
                if not contact_id:
                    contact_id = new_contact.get("contact", {}).get("id") if isinstance(new_contact.get("contact"), dict) else None
                
                # Извлекаем source_id из ответа создания contact
                # Структура ответа Chatwoot 4.7:
                # - payload.contact_inbox.source_id (наиболее вероятно)
                # - payload.contact.contact_inboxes[].source_id (если массив)
                contact_source_id = None
                
                # Вариант 1: payload.contact_inbox.source_id (наиболее вероятно)
                payload_contact_inbox = new_contact.get("payload", {}).get("contact_inbox")
                if isinstance(payload_contact_inbox, dict):
                    contact_source_id = payload_contact_inbox.get("source_id")
                    if contact_source_id:
                        logger.info(f"✓ Found source_id in payload.contact_inbox: {contact_source_id}")
                
                # Вариант 2: payload.contact.contact_inboxes[].source_id
                if not contact_source_id:
                    payload_contact = new_contact.get("payload", {}).get("contact", {})
                    if isinstance(payload_contact, dict):
                        contact_inboxes = payload_contact.get("contact_inboxes", [])
                        if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                            # Ищем contact_inbox для нужного inbox_id
                            for ci in contact_inboxes:
                                inbox_info = ci.get("inbox", {})
                                if inbox_info.get("id") == settings.CHATWOOT_INBOX_ID:
                                    contact_source_id = ci.get("source_id")
                                    break
                            # Если не нашли по inbox_id, берем первый доступный
                            if not contact_source_id and len(contact_inboxes) > 0:
                                contact_source_id = contact_inboxes[0].get("source_id")
                            if contact_source_id:
                                logger.info(f"✓ Found source_id in payload.contact.contact_inboxes: {contact_source_id}")
                
                # Если source_id не найден в ответе создания, получаем contact через GET для извлечения source_id
                if not contact_source_id and contact_id:
                    try:
                        logger.info(f"source_id not found in create response, fetching contact {contact_id} to get source_id")
                        fetched_contact = await chatwoot_client.get_contact(contact_id)
                        
                        # Извлекаем source_id из contact_inboxes (структура GET запроса)
                        payload_fetched = fetched_contact.get("payload", {})
                        contact_inboxes = payload_fetched.get("contact_inboxes", [])
                        if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                            # Ищем contact_inbox для нужного inbox_id
                            for ci in contact_inboxes:
                                inbox_info = ci.get("inbox", {})
                                if inbox_info.get("id") == settings.CHATWOOT_INBOX_ID:
                                    contact_source_id = ci.get("source_id")
                                    break
                            # Если не нашли по inbox_id, берем первый доступный
                            if not contact_source_id and len(contact_inboxes) > 0:
                                contact_source_id = contact_inboxes[0].get("source_id")
                        
                        if contact_source_id:
                            logger.info(f"✓ Retrieved source_id from GET contact: {contact_source_id}")
                    except Exception as get_contact_error:
                        logger.warning(f"Failed to get contact {contact_id} to extract source_id: {get_contact_error}")
                
                # Сохраняем source_id в БД
                if contact_source_id:
                    client.source_id = contact_source_id
                    logger.info(f"✓ Created Chatwoot contact: {contact_id}, source_id: {contact_source_id} for client {client.client_id}")
                else:
                    logger.warning(f"Created Chatwoot contact {contact_id} but source_id not found in response or get_contact for client {client.client_id}")
                    logger.warning(f"Response structure: {list(new_contact.keys()) if isinstance(new_contact, dict) else 'not a dict'}")
                    # НЕ используем fallback source_id_hash - это MD5 hash, который frontend не примет
                    # source_id будет null, и frontend обработает это корректно
                    logger.warning("source_id will remain null - frontend will handle this case")
                
                if not contact_id:
                    logger.warning(f"Failed to extract contact_id from Chatwoot response: {new_contact}")
            except httpx.HTTPStatusError as http_error:
                # Обработка ошибки 422 - контакт уже существует
                if http_error.response.status_code == 422:
                    logger.warning(f"Contact creation returned 422 (already exists), trying to find and update existing contact for client {client.client_id}")
                    
                    # Инициализируем contact_source_id для случая ошибки
                    contact_source_id = None
                    
                    # Пытаемся найти существующий contact
                    found_contact = None
                    contact_id = None
                    
                    if str(client.client_id):
                        found_contact = await chatwoot_client.find_contact_by_identifier(str(client.client_id))
                    
                    if not found_contact and contact_email:
                        found_contact = await chatwoot_client.find_contact_by_email(contact_email)
                    
                    if not found_contact and contact_phone:
                        found_contact = await chatwoot_client.find_contact_by_phone(contact_phone)
                    
                    if found_contact:
                        # Извлекаем contact_id из найденного contact
                        contact_id = found_contact.get("id")
                        if not contact_id:
                            # Пробуем извлечь из payload
                            payload_contact = found_contact.get("payload", {})
                            if isinstance(payload_contact, dict):
                                contact_id = payload_contact.get("contact", {}).get("id") if isinstance(payload_contact.get("contact"), dict) else None
                        
                        if contact_id:
                            # ВАЖНО: Обновляем контакт через PATCH вместо повторного создания
                            try:
                                logger.info(f"Updating existing Chatwoot contact {contact_id} via PATCH for client {client.client_id}")
                                updated_contact = await chatwoot_client.update_contact(
                                    contact_id=contact_id,
                                    name=contact_name,
                                    email=contact_email,
                                    phone_number=contact_phone,
                                    custom_attributes=contact_custom_attrs,
                                    additional_attributes=contact_additional_attrs
                                )
                                logger.info(f"✓ Updated Chatwoot contact {contact_id} via PATCH for client {client.client_id}")
                            except Exception as update_error:
                                logger.warning(f"Failed to update contact {contact_id} via PATCH: {update_error}, continuing with source_id extraction")
                        
                        # Извлекаем source_id из найденного contact
                        contact_inboxes = found_contact.get("contact_inboxes", [])
                        if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                            for ci in contact_inboxes:
                                if ci.get("inbox_id") == settings.CHATWOOT_INBOX_ID:
                                    contact_source_id = ci.get("source_id")
                                    break
                            if not contact_source_id and len(contact_inboxes) > 0:
                                contact_source_id = contact_inboxes[0].get("source_id")
                        
                        # Если source_id не найден в найденном контакте, пробуем получить через GET
                        if not contact_source_id and contact_id:
                            try:
                                fetched_contact = await chatwoot_client.get_contact(contact_id)
                                payload_fetched = fetched_contact.get("payload", {})
                                contact_inboxes = payload_fetched.get("contact_inboxes", [])
                                if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                                    for ci in contact_inboxes:
                                        inbox_info = ci.get("inbox", {})
                                        if inbox_info.get("id") == settings.CHATWOOT_INBOX_ID:
                                            contact_source_id = ci.get("source_id")
                                            break
                                    if not contact_source_id and len(contact_inboxes) > 0:
                                        contact_source_id = contact_inboxes[0].get("source_id")
                            except Exception as get_error:
                                logger.warning(f"Failed to get contact {contact_id} to extract source_id: {get_error}")
                        
                        if contact_source_id:
                            client.source_id = contact_source_id
                            logger.info(f"✓ Found and updated existing contact source_id: {contact_source_id} for client {client.client_id}")
                        else:
                            logger.warning(f"Found existing contact but source_id not found for client {client.client_id}")
                    else:
                        logger.error(f"Contact exists (422) but cannot be found by identifier/email/phone for client {client.client_id}")
                        raise ValueError("Contact exists but cannot be found")
                else:
                    # Другие HTTP ошибки - пробрасываем дальше
                    raise
    except Exception as e:
        # Не прерываем создание клиента, если Chatwoot недоступен
        logger.warning(f"Failed to sync client {client.client_id} to Chatwoot: {e}", exc_info=True)


async def _sync_client_to_onec(
    db: AsyncSession,
    client: Client
) -> None:
    """
    Синхронизирует клиента с 1C:ЦЛ.
    Создает или обновляет клиента в 1C:ЦЛ при создании/обновлении в БД.
    
    Логика:
    - Если у клиента есть cl_ref_key - проверяем Parent_Key в ЦЛ, если правильный - обновляем, если нет - создаем нового
    - Если cl_ref_key нет - ищем клиента по коду абонента и ИНН
    - Если найден - проверяем Parent_Key, если правильный - обновляем, если нет - создаем нового (дубль)
    - Если не найден - создаем нового
    
    ВАЖНО: 
    - В 1C:ЦЛ имя формируется с префиксом CLOBUS: CLOBUS + Наименование + КодАбонента + ИНН
    - Нужный Parent_Key: "7ccd31ca-887b-11eb-938b-00e04cd03b68"
    - Если найденный клиент имеет другой Parent_Key, создаем дубль с нужным Parent_Key
    """
    from ..services.onec_client import OneCClient
    from ..routers.consultations import _build_client_display_name
    
    # Синхронизируем только владельцев (is_parent=True)
    if not client.is_parent:
        logger.debug(f"Skipping 1C sync for child client {client.client_id} (only owners are synced)")
        return
    
    # Проверяем обязательные поля
    if not client.org_inn:
        logger.warning(f"Cannot sync client {client.client_id} to 1C: missing org_inn")
        return
    
    # Нужный Parent_Key для клиентов из Clobus
    REQUIRED_PARENT_KEY = "7ccd31ca-887b-11eb-938b-00e04cd03b68"
    
    onec_client = OneCClient()
    
    try:
        # Если у клиента есть cl_ref_key, проверяем его Parent_Key в ЦЛ напрямую
        if client.cl_ref_key:
            logger.info(f"Client {client.client_id} has cl_ref_key={client.cl_ref_key[:20]}, checking Parent_Key in ЦЛ")
            
            # ВАЖНО: Получаем данные клиента напрямую по Ref_Key для проверки Parent_Key
            # Это предотвращает создание дублей, когда клиент уже существует с правильным Parent_Key
            existing_client_data = await onec_client.get_client_by_ref_key(client.cl_ref_key)
            
            if existing_client_data:
                existing_parent_key = existing_client_data.get("Parent_Key")
                existing_ref_key = existing_client_data.get("Ref_Key")
                existing_inn = existing_client_data.get("ИНН") or existing_client_data.get("ИННФизЛица")
                existing_code = existing_client_data.get("КодАбонентаClobus")
                
                # Проверяем, что Ref_Key совпадает (на всякий случай)
                if existing_ref_key != client.cl_ref_key:
                    logger.warning(
                        f"Client {client.client_id} has cl_ref_key={client.cl_ref_key[:20]}, "
                        f"but ЦЛ returned different Ref_Key={existing_ref_key[:20] if existing_ref_key else 'None'}. "
                        f"This should not happen. Resetting cl_ref_key and searching again."
                    )
                    client.cl_ref_key = None
                    await db.flush()
                    # Продолжаем выполнение - найдем клиента по коду и ИНН ниже
                elif existing_parent_key != REQUIRED_PARENT_KEY:
                    # Parent_Key не тот - создаем дубль с нужным Parent_Key
                    logger.warning(
                        f"Client {client.client_id} has cl_ref_key={client.cl_ref_key[:20]}, "
                        f"but Parent_Key in ЦЛ={existing_parent_key} (incorrect, required: {REQUIRED_PARENT_KEY}). "
                        f"Creating duplicate client with correct Parent_Key."
                    )
                    # Сбрасываем cl_ref_key и создаем нового клиента
                    client.cl_ref_key = None
                    await db.flush()
                    # Продолжаем выполнение - создадим нового клиента ниже
                else:
                    # Parent_Key правильный - проверяем соответствие ИНН и кода абонента
                    # ВАЖНО: Если ИНН или код абонента не совпадают (или пустые в ЦЛ),
                    # это может быть дубль, который был очищен. Ищем правильного клиента по ИНН и коду.
                    inn_matches = (
                        existing_inn and client.org_inn and 
                        existing_inn.strip() == client.org_inn.strip()
                    ) or (not existing_inn and not client.org_inn)
                    
                    code_matches = (
                        existing_code and client.code_abonent and 
                        str(existing_code).strip() == str(client.code_abonent).strip()
                    ) or (not existing_code and not client.code_abonent)
                    
                    if not inn_matches or not code_matches:
                        # ИНН или код не совпадают - это может быть дубль
                        logger.warning(
                            f"Client {client.client_id} has cl_ref_key={client.cl_ref_key[:20]}, "
                            f"but ИНН/code mismatch: ЦЛ ИНН={existing_inn}, БД ИНН={client.org_inn}, "
                            f"ЦЛ code={existing_code}, БД code={client.code_abonent}. "
                            f"This might be a duplicate. Searching for correct client by ИНН and code."
                        )
                        # Сбрасываем cl_ref_key и ищем правильного клиента по ИНН и коду
                        client.cl_ref_key = None
                        await db.flush()
                        # Продолжаем выполнение - найдем клиента по коду и ИНН ниже
                    else:
                        # Parent_Key правильный, ИНН и код совпадают - обновляем существующего клиента
                        logger.info(
                            f"Client {client.client_id} has correct Parent_Key and matching ИНН/code, "
                            f"updating in ЦЛ"
                        )
                        display_name = _build_client_display_name(client)
                        
                        update_data = {}
                        if display_name:
                            update_data["name"] = display_name
                        if client.org_inn:
                            update_data["org_inn"] = client.org_inn
                        if client.code_abonent:
                            update_data["code_abonent"] = client.code_abonent
                        if client.phone_number:
                            update_data["phone"] = client.phone_number
                        if client.email:
                            update_data["email"] = client.email
                        
                        if update_data:
                            response = await onec_client.update_client_odata(
                                ref_key=client.cl_ref_key,
                                name=update_data.get("name"),
                                org_inn=update_data.get("org_inn"),
                                code_abonent=update_data.get("code_abonent"),
                                phone=update_data.get("phone"),
                                email=update_data.get("email"),
                            )
                            logger.info(f"✓ Updated client {client.client_id} in 1C:ЦЛ")
                        else:
                            logger.debug(f"No fields to update for client {client.client_id} in 1C")
                        return
            else:
                # Клиент не найден по Ref_Key - возможно был удален в ЦЛ
                logger.warning(
                    f"Client {client.client_id} has cl_ref_key={client.cl_ref_key[:20]}, "
                    f"but client not found in ЦЛ. Resetting cl_ref_key and searching again."
                )
                client.cl_ref_key = None
                await db.flush()
                # Продолжаем выполнение - найдем клиента по коду и ИНН ниже
        
        # Клиента нет в 1C или нужно создать дубль - ищем существующего по коду абонента и ИНН
        # Для создания нужен code_abonent
        if not client.code_abonent:
            logger.warning(f"Cannot create client {client.client_id} in 1C: missing code_abonent")
            return
        
        logger.info(f"Searching/creating client {client.client_id} in 1C:ЦЛ (org_inn={client.org_inn}, code_abonent={client.code_abonent})")
        
        # Ищем клиента по коду абонента и ИНН
        existing_client = await onec_client.find_client_by_code_and_inn(
            code_abonent=client.code_abonent,
            org_inn=client.org_inn
        )
        
        if existing_client:
            existing_ref_key = existing_client.get("Ref_Key")
            existing_parent_key = existing_client.get("Parent_Key")
            
            # Проверяем Parent_Key найденного клиента
            if existing_parent_key == REQUIRED_PARENT_KEY:
                # Parent_Key правильный - обновляем существующего клиента
                logger.info(
                    f"Found existing client in 1C with Ref_Key={existing_ref_key[:20]}, "
                    f"Parent_Key={existing_parent_key} (correct), updating"
                )
                client.cl_ref_key = existing_ref_key
                await db.flush()
                
                display_name = _build_client_display_name(client)
                
                response = await onec_client.update_client_odata(
                    ref_key=existing_ref_key,
                    name=display_name,
                    org_inn=client.org_inn,
                    code_abonent=client.code_abonent,
                    phone=client.phone_number,
                    email=client.email,
                )
                logger.info(f"✓ Updated existing client in 1C:ЦЛ (Ref_Key={existing_ref_key[:20]})")
                return
            else:
                # Parent_Key не тот - создаем дубль с нужным Parent_Key
                logger.warning(
                    f"Found existing client in 1C with Ref_Key={existing_ref_key[:20]}, "
                    f"Parent_Key={existing_parent_key} (incorrect, required: {REQUIRED_PARENT_KEY}). "
                    f"Creating duplicate client with correct Parent_Key."
                )
                # Продолжаем выполнение - создадим нового клиента ниже
        
        # Создаем нового клиента в 1C (или дубль, если найденный имел неправильный Parent_Key)
        display_name = _build_client_display_name(client)
        response = await onec_client.create_client_odata(
            name=display_name,
            org_inn=client.org_inn,
            code_abonent=client.code_abonent,
            phone=client.phone_number,
            email=client.email,
        )
        
        # Сохраняем Ref_Key из ответа 1C
        ref_key = response.get("Ref_Key")
        if ref_key:
            client.cl_ref_key = ref_key
            await db.flush()
            logger.info(f"✓ Created client {client.client_id} in 1C:ЦЛ (Ref_Key={ref_key[:20]})")
        else:
            logger.warning(f"1C returned response without Ref_Key for client {client.client_id}: {response}")
    except Exception as e:
        # Не блокируем создание/обновление клиента в БД при ошибке синхронизации с 1C
        logger.error(f"Failed to sync client {client.client_id} to 1C:ЦЛ: {e}", exc_info=True)


async def _get_parent_client(db: AsyncSession, parent_id: Optional[str]) -> Optional[Client]:
    """Возвращает родителя по UUID или None."""
    if not parent_id:
        return None
    try:
        parent_uuid = uuid.UUID(parent_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid parent_id format")

    result = await db.execute(select(Client).where(Client.client_id == parent_uuid))
    parent = result.scalar_one_or_none()
    if not parent:
        raise HTTPException(status_code=404, detail="Parent client not found")
    if parent.parent_id:
        raise HTTPException(
            status_code=400,
            detail="Parent client must be a top-level owner",
        )
    return parent


async def _migrate_code_abonent_on_inn_change(
    db: AsyncSession,
    current_client: Client,
    new_inn: str,
    new_code_abonent: Optional[str] = None,
) -> None:
    """
    Выполняет миграцию кода абонента при смене ИНН.
    
    Логика:
    1. Если клиент меняет ИНН на тот, который уже есть в БД без кода абонента,
       код абонента должен перейти к этому ИНН
    2. Код абонента из предыдущей записи должен очиститься
    3. Если при миграции на другой ИНН, который уже есть в базе будет иметь свой Код абонента,
       который отличается от текущего кода абонента, то должен быть запрет на миграцию
    
    Args:
        db: Сессия БД
        current_client: Текущий клиент, который меняет ИНН
        new_inn: Новый ИНН
        new_code_abonent: Новый код абонента (если указан)
    """
    if not current_client.code_abonent:
        # У текущего клиента нет кода абонента - миграция не требуется
        return
    
    # Ищем клиента с новым ИНН (исключая текущего)
    # ВАЖНО: Может быть несколько клиентов с одним ИНН, берем первый (самый старый)
    result = await db.execute(
        select(Client).where(
            Client.org_inn == new_inn,
            Client.is_parent == True,
            Client.client_id != current_client.client_id
        ).order_by(Client.created_at.asc()).limit(1)
    )
    existing_client_with_new_inn = result.scalar_one_or_none()
    
    if not existing_client_with_new_inn:
        # Клиента с новым ИНН нет - миграция не требуется
        return
    
    # Клиент с новым ИНН найден
    existing_code = existing_client_with_new_inn.code_abonent
    
    if existing_code:
        # У клиента с новым ИНН уже есть код абонента
        if existing_code != current_client.code_abonent:
            # Коды абонентов отличаются - запрещаем миграцию
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Невозможно изменить ИНН: клиент с ИНН {new_inn} уже существует "
                    f"с кодом абонента {existing_code[:10]}..., который отличается от текущего "
                    f"кода абонента {current_client.code_abonent[:10]}.... "
                    f"Миграция кода абонента запрещена."
                )
            )
        # Коды совпадают - это тот же клиент, миграция не требуется
        return
    
    # У клиента с новым ИНН нет кода абонента - выполняем миграцию
    logger.info(
        f"Migrating code_abonent {current_client.code_abonent} from client {current_client.client_id} "
        f"(old INN: {current_client.org_inn}) to client {existing_client_with_new_inn.client_id} "
        f"(new INN: {new_inn})"
    )
    
    # Переносим код абонента к клиенту с новым ИНН
    existing_client_with_new_inn.code_abonent = current_client.code_abonent
    
    # Очищаем код абонента у текущего клиента
    current_client.code_abonent = None
    
    logger.info(
        f"✓ Code migration completed: code_abonent {existing_client_with_new_inn.code_abonent} "
        f"now belongs to client {existing_client_with_new_inn.client_id} (INN: {new_inn})"
    )


async def _ensure_unique_owner_inn(
    db: AsyncSession,
    org_inn: str,
    code_abonent: Optional[str] = None,
    exclude_client_id: Optional[uuid.UUID] = None,
) -> None:
    """
    Проверяет уникальность ИНН среди владельцев с учетом кода абонента.
    
    Логика:
    1. Если из ЦЛ загрузился клиент с ИНН и кодом абонента - запрещаем создание нового клиента с тем же ИНН
    2. Если из ЦЛ загрузился клиент только с ИНН (без кода абонента) - разрешаем привязку к этому ИНН
    3. Если фронт пытается создать клиента с ИНН и кодом абонента, а в БД есть клиент с тем же ИНН но другим кодом абонента - запрещаем
    
    Args:
        db: Сессия БД
        org_inn: ИНН для проверки
        code_abonent: Код абонента (опционально)
        exclude_client_id: ID клиента для исключения из проверки (при обновлении)
    """
    if not org_inn:
        return
    
    # Ищем всех владельцев с таким ИНН
    query = select(Client).where(
        Client.org_inn == org_inn,
        Client.parent_id.is_(None),
    )
    if exclude_client_id:
        query = query.where(Client.client_id != exclude_client_id)
    
    result = await db.execute(query)
    existing_clients = result.scalars().all()
    
    if not existing_clients:
        return  # ИНН свободен
    
    # Проверяем каждый существующий клиент
    for existing_client in existing_clients:
        existing_code = existing_client.code_abonent
        
        # Случай 1: В БД есть клиент с ИНН и кодом абонента (из ЦЛ)
        if existing_code:
            # Если фронт пытается создать клиента с тем же ИНН
            if code_abonent:
                # Если коды абонентов совпадают - разрешаем обновление (это тот же клиент)
                if code_abonent == existing_code:
                    continue  # Это тот же клиент, можно обновлять
                else:
                    # Разные коды абонентов - запрещаем
                    raise HTTPException(
                        status_code=409,
                        detail=f"Owner with this INN already exists with different code_abonent (existing: {existing_code[:10]}..., new: {code_abonent[:10]}...)",
                    )
            else:
                # Фронт пытается создать клиента без кода абонента, а в БД есть с кодом - запрещаем
                raise HTTPException(
                    status_code=409,
                    detail=f"Owner with this INN already exists with code_abonent from 1C:CL. Cannot create without code_abonent.",
                )
        # Случай 2: В БД есть клиент только с ИНН (без кода абонента)
        else:
            # Если фронт пытается создать клиента с кодом абонента - разрешаем (привязка к существующему ИНН)
            if code_abonent:
                # Разрешаем - фронт может привязаться к этому ИНН
                continue
            else:
                # Оба без кода абонента - это конфликт
                raise HTTPException(
                    status_code=409,
                    detail="Owner with this INN already exists",
                )


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
    logger.info(f"Finding/creating client: email={client_data.email}, phone={client_data.phone_number}, org_inn={client_data.org_inn}, subscriber_id={client_data.subscriber_id}, code_abonent={client_data.code_abonent}")
    
    parent_client = await _get_parent_client(db, client_data.parent_id)
    parent_uuid = parent_client.client_id if parent_client else None

    # Определяем финальные значения is_parent и parent_id из запроса
    # Если в запросе явно указан is_parent, используем его значение
    # Если is_parent=True, то parent_id должен быть None
    # Если is_parent=False, то parent_id должен быть указан
    if client_data.is_parent is not None:
        # Явно указан is_parent в запросе
        if client_data.is_parent:
            # Запрос на владельца - parent_id должен быть None
            if client_data.parent_id is not None:
                raise HTTPException(
                    status_code=400,
                    detail="If is_parent=true, parent_id must be null"
                )
            parent_uuid = None
            parent_client = None
        else:
            # Запрос на пользователя - parent_id должен быть указан
            if not client_data.parent_id:
                raise HTTPException(
                    status_code=400,
                    detail="If is_parent=false, parent_id must be specified"
                )
            parent_client = await _get_parent_client(db, client_data.parent_id)
            parent_uuid = parent_client.client_id if parent_client else None
    else:
        # is_parent не указан в запросе - определяем по parent_id
        # Если parent_id указан, то is_parent=False
        # Если parent_id не указан, то is_parent=True
        if client_data.parent_id:
            parent_client = await _get_parent_client(db, client_data.parent_id)
            parent_uuid = parent_client.client_id if parent_client else None
        else:
            parent_uuid = None
            parent_client = None

    # Определяем финальные значения ИНН и кода абонента
    normalized_inn = client_data.org_inn.strip() if client_data.org_inn else None
    raw_code_abonent = client_data.subscriber_id or client_data.code_abonent
    normalized_code_abonent = raw_code_abonent.strip() if raw_code_abonent else None
    
    # Определяем финальное значение is_parent
    final_is_parent = parent_uuid is None
    
    logger.debug(f"Normalized values: normalized_inn={normalized_inn}, normalized_code_abonent={normalized_code_abonent}, parent_id={parent_uuid}, is_parent={final_is_parent}")

    if parent_client:
        if normalized_inn and normalized_inn != parent_client.org_inn:
            raise HTTPException(
                status_code=400,
                detail="Child client must inherit INN from owner",
            )
        normalized_inn = parent_client.org_inn
        normalized_code_abonent = parent_client.code_abonent
        if not normalized_inn or not normalized_code_abonent:
            raise HTTPException(
                status_code=400,
                detail="Parent client must have INN and code_abonent",
            )

    # 1. По client_id
    if client_data.client_id:
        try:
            client_uuid = uuid.UUID(client_data.client_id)
            result = await db.execute(
                select(Client).where(Client.client_id == client_uuid)
            )
            client = result.scalar_one_or_none()
            if client:
                # ВАЖНО: Если в запросе указан is_parent=True, а в БД is_parent=False,
                # обновляем роль до владельца (повышение роли)
                if final_is_parent and not client.is_parent:
                    logger.info(f"Upgrading client {client.client_id} from user to owner (is_parent: false -> true)")
                    client.is_parent = True
                    client.parent_id = None
                    # После повышения роли разрешаем изменение ИНН и company_name
                elif not final_is_parent and client.is_parent:
                    # Понижение роли - запрещаем без дополнительной проверки
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot downgrade from owner to user. Owner role cannot be changed."
                    )
                
                # ВАЖНО: Запрещаем изменение ИНН и названия организации для не-владельцев
                # Проверяем текущую роль (после возможного обновления)
                if not final_is_parent:
                    # Это пользователь, а не владелец - запрещаем изменение ИНН и company_name
                    if client_data.org_inn and client_data.org_inn != client.org_inn:
                        raise HTTPException(
                            status_code=403,
                            detail="Пользователи абонента не могут изменять ИНН. Только владелец может изменить ИНН."
                        )
                    if client_data.company_name and client_data.company_name != client.company_name:
                        raise HTTPException(
                            status_code=403,
                            detail="Пользователи абонента не могут изменять название организации. Только владелец может изменить название организации."
                        )
                
                # ВАЖНО: Выполняем миграцию кода абонента при смене ИНН (только для владельцев)
                if final_is_parent and normalized_inn and normalized_inn != client.org_inn:
                    await _migrate_code_abonent_on_inn_change(
                        db=db,
                        current_client=client,
                        new_inn=normalized_inn,
                        new_code_abonent=normalized_code_abonent
                    )
                
                if final_is_parent and normalized_inn and normalized_inn != client.org_inn:
                    await _ensure_unique_owner_inn(db, normalized_inn, normalized_code_abonent, exclude_client_id=client_uuid)
                # Обновляем данные если нужно
                if client_data.email:
                    client.email = client_data.email
                if client_data.phone_number:
                    client.phone_number = client_data.phone_number
                if client_data.name:
                    client.name = client_data.name.strip()
                if client_data.contact_name:
                    client.contact_name = client_data.contact_name.strip()
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
                # ВАЖНО: Обновляем ИНН только если это владелец (используем normalized_inn после миграции)
                if normalized_inn and final_is_parent:
                    client.org_inn = normalized_inn
                if normalized_code_abonent:
                    client.code_abonent = normalized_code_abonent
                # ВАЖНО: Обновляем company_name только если это владелец
                if client_data.company_name and final_is_parent:
                    client.company_name = client_data.company_name.strip() if client_data.company_name else None
                # Обновляем географические поля
                if client_data.country is not None:
                    client.country = client_data.country
                if client_data.region is not None:
                    client.region = client_data.region
                if client_data.city is not None:
                    client.city = client_data.city
                # Обновляем обслуживающую организацию
                if client_data.partner is not None:
                    client.partner = client_data.partner
                # Обновляем is_parent и parent_id согласно запросу
                client.is_parent = final_is_parent
                client.parent_id = parent_uuid
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
            # ВАЖНО: Если в запросе указан is_parent=True, а в БД is_parent=False,
            # обновляем роль до владельца (повышение роли)
            if final_is_parent and not client.is_parent:
                logger.info(f"Upgrading client {client.client_id} from user to owner (is_parent: false -> true)")
                client.is_parent = True
                client.parent_id = None
            elif not final_is_parent and client.is_parent:
                # Понижение роли - запрещаем без дополнительной проверки
                raise HTTPException(
                    status_code=403,
                    detail="Cannot downgrade from owner to user. Owner role cannot be changed."
                )
            
            # ВАЖНО: Выполняем миграцию кода абонента при смене ИНН (только для владельцев)
            if final_is_parent and normalized_inn and normalized_inn != client.org_inn:
                await _migrate_code_abonent_on_inn_change(
                    db=db,
                    current_client=client,
                    new_inn=normalized_inn,
                    new_code_abonent=normalized_code_abonent
                )
            
            # ВАЖНО: Запрещаем изменение ИНН и названия организации для не-владельцев
            if not final_is_parent:
                if normalized_inn and normalized_inn != client.org_inn:
                    raise HTTPException(
                        status_code=403,
                        detail="Пользователи абонента не могут изменять ИНН. Только владелец может изменить ИНН."
                    )
                if client_data.company_name and client_data.company_name != client.company_name:
                    raise HTTPException(
                        status_code=403,
                        detail="Пользователи абонента не могут изменять название организации. Только владелец может изменить название организации."
                    )
            
            if final_is_parent and normalized_inn and normalized_inn != client.org_inn:
                await _ensure_unique_owner_inn(db, normalized_inn, normalized_code_abonent, exclude_client_id=client.client_id)
            # Обновляем данные
            if client_data.email:
                client.email = client_data.email
            if client_data.phone_number:
                client.phone_number = client_data.phone_number
            if client_data.name:
                client.name = client_data.name.strip()
            if client_data.contact_name:
                client.contact_name = client_data.contact_name.strip()
            # Обновляем обслуживающую организацию
            if client_data.partner is not None:
                client.partner = client_data.partner
            # ВАЖНО: Обновляем ИНН только если это владелец
            if normalized_inn and final_is_parent:
                client.org_inn = normalized_inn
            if normalized_code_abonent:
                client.code_abonent = normalized_code_abonent
            # ВАЖНО: Обновляем company_name только если это владелец
            if client_data.company_name and final_is_parent:
                client.company_name = client_data.company_name.strip() if client_data.company_name else None
            # Обновляем partner (обслуживающая организация)
            if client_data.partner is not None:
                client.partner = client_data.partner
            # Обновляем географические поля
            if client_data.country is not None:
                client.country = client_data.country
            if client_data.region is not None:
                client.region = client_data.region
            if client_data.city is not None:
                client.city = client_data.city
            client.is_parent = final_is_parent
            client.parent_id = parent_uuid
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
        # ВАЖНО: Если в запросе указан is_parent=True, а в БД is_parent=False,
        # обновляем роль до владельца (повышение роли)
        if final_is_parent and not client.is_parent:
            logger.info(f"Upgrading client {client.client_id} from user to owner (is_parent: false -> true)")
            client.is_parent = True
            client.parent_id = None
        elif not final_is_parent and client.is_parent:
            # Понижение роли - запрещаем без дополнительной проверки
            raise HTTPException(
                status_code=403,
                detail="Cannot downgrade from owner to user. Owner role cannot be changed."
            )
        
        # ВАЖНО: Выполняем миграцию кода абонента при смене ИНН (только для владельцев)
        if final_is_parent and normalized_inn and normalized_inn != client.org_inn:
            await _migrate_code_abonent_on_inn_change(
                db=db,
                current_client=client,
                new_inn=normalized_inn,
                new_code_abonent=normalized_code_abonent
            )
        
        # ВАЖНО: Запрещаем изменение ИНН и названия организации для не-владельцев
        if not final_is_parent:
            if client_data.org_inn and client_data.org_inn != client.org_inn:
                raise HTTPException(
                    status_code=403,
                    detail="Пользователи абонента не могут изменять ИНН. Только владелец может изменить ИНН."
                )
            if client_data.company_name and client_data.company_name != client.company_name:
                raise HTTPException(
                    status_code=403,
                    detail="Пользователи абонента не могут изменять название организации. Только владелец может изменить название организации."
                )
        
        # Обновляем данные
        if client_data.email:
            client.email = client_data.email
        if client_data.phone_number:
            client.phone_number = client_data.phone_number
        if client_data.name:
            client.name = client_data.name.strip()
        if client_data.contact_name:
            client.contact_name = client_data.contact_name.strip()
        # Обновляем обслуживающую организацию
        if client_data.partner is not None:
            client.partner = client_data.partner
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
        # ВАЖНО: Обновляем ИНН только если это владелец (используем normalized_inn после миграции)
        if normalized_inn and final_is_parent:
            client.org_inn = normalized_inn
        if normalized_code_abonent:
            client.code_abonent = normalized_code_abonent
        # ВАЖНО: Обновляем company_name только если это владелец
        if client_data.company_name and final_is_parent:
            client.company_name = client_data.company_name.strip() if client_data.company_name else None
        # Обновляем partner (обслуживающая организация)
        if client_data.partner is not None:
            client.partner = client_data.partner
        # Обновляем географические поля
        if client_data.country is not None:
            client.country = client_data.country
        if client_data.region is not None:
            client.region = client_data.region
        if client_data.city is not None:
            client.city = client_data.city
        client.is_parent = final_is_parent
        client.parent_id = parent_uuid
        return client
    
    # 4. По code_abonent (приоритет выше чем ИНН)
    if normalized_code_abonent:
        result = await db.execute(
            select(Client).where(Client.code_abonent == normalized_code_abonent)
        )
        client_by_code = result.scalar_one_or_none()
        if client_by_code:
            logger.info(f"Found client by code_abonent={normalized_code_abonent}, client_id={client_by_code.client_id}")
            
            # ВАЖНО: Если в запросе указан is_parent=True, а в БД is_parent=False,
            # обновляем роль до владельца (повышение роли)
            if final_is_parent and not client_by_code.is_parent:
                logger.info(f"Upgrading client {client_by_code.client_id} from user to owner (is_parent: false -> true)")
                client_by_code.is_parent = True
                client_by_code.parent_id = None
            elif not final_is_parent and client_by_code.is_parent:
                # Понижение роли - запрещаем без дополнительной проверки
                raise HTTPException(
                    status_code=403,
                    detail="Cannot downgrade from owner to user. Owner role cannot be changed."
                )
            
            # ВАЖНО: Выполняем миграцию кода абонента при смене ИНН (только для владельцев)
            if final_is_parent and normalized_inn and normalized_inn != client_by_code.org_inn:
                await _migrate_code_abonent_on_inn_change(
                    db=db,
                    current_client=client_by_code,
                    new_inn=normalized_inn,
                    new_code_abonent=normalized_code_abonent
                )
            
            # ВАЖНО: Запрещаем изменение ИНН и названия организации для не-владельцев
            if not final_is_parent:
                if normalized_inn and normalized_inn != client_by_code.org_inn:
                    raise HTTPException(
                        status_code=403,
                        detail="Пользователи абонента не могут изменять ИНН. Только владелец может изменить ИНН."
                    )
                if client_data.company_name and client_data.company_name != client_by_code.company_name:
                    raise HTTPException(
                        status_code=403,
                        detail="Пользователи абонента не могут изменять название организации. Только владелец может изменить название организации."
                    )
            
            # Обновляем данные клиента, включая ИНН
            if client_data.email:
                client_by_code.email = client_data.email
            if client_data.phone_number:
                client_by_code.phone_number = client_data.phone_number
            if client_data.name:
                client_by_code.name = client_data.name.strip()
            if client_data.contact_name:
                client_by_code.contact_name = client_data.contact_name.strip()
            if client_data.cl_ref_key:
                client_by_code.cl_ref_key = client_data.cl_ref_key
        if client_data.partner is not None:
            client_by_code.partner = client_data.partner
            if client_data.subs_id:
                client_by_code.subs_id = client_data.subs_id
            if client_data.subs_start:
                client_by_code.subs_start = client_data.subs_start
            if client_data.subs_end:
                client_by_code.subs_end = client_data.subs_end
            if client_data.tariff_id:
                client_by_code.tariff_id = client_data.tariff_id
            if client_data.tariffperiod_id:
                client_by_code.tariffperiod_id = client_data.tariffperiod_id
            # ВАЖНО: Обновляем ИНН только если это владелец
            if normalized_inn and final_is_parent:
                client_by_code.org_inn = normalized_inn
            # ВАЖНО: Обновляем company_name только если это владелец
            if client_data.company_name and final_is_parent:
                client_by_code.company_name = client_data.company_name.strip() if client_data.company_name else None
            # Обновляем географические поля
            if client_data.country is not None:
                client_by_code.country = client_data.country
            if client_data.region is not None:
                client_by_code.region = client_data.region
            if client_data.city is not None:
                client_by_code.city = client_data.city
            # Обновляем обслуживающую организацию
            if client_data.partner is not None:
                client_by_code.partner = client_data.partner
            client_by_code.code_abonent = normalized_code_abonent
            client_by_code.is_parent = final_is_parent
            client_by_code.parent_id = parent_uuid
            return client_by_code
    
    # 5. По ИНН (только если не найден по code_abonent)
    if parent_uuid is None and normalized_inn:
        # ВАЖНО: Может быть несколько клиентов с одним ИНН, берем первый (самый старый)
        result = await db.execute(
            select(Client).where(
                Client.org_inn == normalized_inn,
                Client.is_parent == True
            ).order_by(Client.created_at.asc()).limit(1)
        )
        existing_owner = result.scalar_one_or_none()
        if existing_owner:
            # Проверяем, есть ли у найденного клиента code_abonent
            if existing_owner.code_abonent:
                # У клиента уже есть code_abonent, который отличается от запроса - это конфликт
                error_msg = (
                    f"Client with INN {normalized_inn} already exists with code_abonent={existing_owner.code_abonent}. "
                    f"Cannot update with different code_abonent={normalized_code_abonent}. "
                    f"code_abonent has higher priority than INN."
                )
                logger.error(f"✗ {error_msg}")
                raise HTTPException(
                    status_code=409,
                    detail=error_msg
                )
            else:
                # У клиента нет code_abonent - заполняем его из запроса
                logger.info(f"Found existing owner by INN {normalized_inn} without code_abonent, "
                          f"filling code_abonent={normalized_code_abonent}")
                # Обновляем данные клиента
                if client_data.email:
                    existing_owner.email = client_data.email
                if client_data.phone_number:
                    existing_owner.phone_number = client_data.phone_number
                if client_data.name:
                    existing_owner.name = client_data.name.strip()
                if client_data.contact_name:
                    existing_owner.contact_name = client_data.contact_name.strip()
                if client_data.cl_ref_key:
                    existing_owner.cl_ref_key = client_data.cl_ref_key
                if client_data.partner is not None:
                    existing_owner.partner = client_data.partner
                if client_data.subs_id:
                    existing_owner.subs_id = client_data.subs_id
                if client_data.subs_start:
                    existing_owner.subs_start = client_data.subs_start
                if client_data.subs_end:
                    existing_owner.subs_end = client_data.subs_end
                if client_data.tariff_id:
                    existing_owner.tariff_id = client_data.tariff_id
                if client_data.tariffperiod_id:
                    existing_owner.tariffperiod_id = client_data.tariffperiod_id
                # Заполняем code_abonent
                if normalized_code_abonent:
                    existing_owner.code_abonent = normalized_code_abonent
                # Обновляем географические поля
                if client_data.country is not None:
                    existing_owner.country = client_data.country
                if client_data.region is not None:
                    existing_owner.region = client_data.region
                if client_data.city is not None:
                    existing_owner.city = client_data.city
                existing_owner.org_inn = normalized_inn
                existing_owner.is_parent = True
                existing_owner.parent_id = None
                return existing_owner
    
    # 5. Создаем нового
    if not final_is_parent:
        if not normalized_inn:
            raise HTTPException(status_code=400, detail="User client requires INN from owner")
        if not parent_uuid:
            raise HTTPException(status_code=400, detail="User client requires parent_id")
        code_to_use = normalized_code_abonent
    else:
        if not normalized_inn:
            raise HTTPException(status_code=400, detail="Owner client requires INN")
        await _ensure_unique_owner_inn(db, normalized_inn, normalized_code_abonent, exclude_client_id=None)
        code_to_use = normalized_code_abonent

    if final_is_parent and not code_to_use:
        raise HTTPException(status_code=400, detail="Owner client requires code_abonent")

    client = Client(
        client_id_hash=client_hash,
        email=client_data.email,
        phone_number=client_data.phone_number,
        country=client_data.country,
        region=client_data.region,
        city=client_data.city,
        org_inn=normalized_inn,  # Используем normalized_inn вместо client_data.org_inn
        subs_id=client_data.subs_id,
        subs_start=client_data.subs_start,
        subs_end=client_data.subs_end,
        tariff_id=client_data.tariff_id,
        tariffperiod_id=client_data.tariffperiod_id,
        cl_ref_key=client_data.cl_ref_key,
        name=client_data.name.strip() if client_data.name else None,
        contact_name=client_data.contact_name.strip() if client_data.contact_name else None,
        company_name=client_data.company_name.strip() if client_data.company_name else None,
        code_abonent=code_to_use,
        partner=client_data.partner,
        is_parent=final_is_parent,
        parent_id=parent_uuid,
    )

    db.add(client)
    await db.flush()
    return client


@router.post("", response_model=ClientRead)
@router.post("/", response_model=ClientRead)
async def create_or_update_client(
    payload: ClientCreate,
    db: AsyncSession = Depends(get_db)
):
    """
    Создание или обновление клиента.
    
    Если клиент существует (по client_id, client_id_hash или email+phone+inn),
    обновляет его данные. Иначе создает нового.
    
    После создания/обновления синхронизирует контакт в Chatwoot.
    """
    try:
        client = await find_or_create_client(db, payload)
        await db.commit()
        await db.refresh(client)
        
        # Получаем владельца для синхронизации в Chatwoot
        owner_client = client
        if client.parent_id:
            result = await db.execute(
                select(Client).where(Client.client_id == client.parent_id)
            )
            owner = result.scalar_one_or_none()
            if owner:
                owner_client = owner
        
        # Синхронизируем контакт в Chatwoot (не блокируем создание клиента при ошибке)
        await _sync_client_to_chatwoot(client, owner_client)
        
        # Синхронизируем клиента с 1C:ЦЛ (не блокируем создание клиента при ошибке)
        await _sync_client_to_onec(db, client)
        
        # Коммитим изменения cl_ref_key если он был установлен при синхронизации с 1C
        if client.cl_ref_key:
            await db.commit()
            await db.refresh(client)
        
        return ClientRead.model_validate(client)
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating/updating client: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/{client_id}", response_model=ClientRead)
async def get_client(
    client_id: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Получение клиента по ID.
    
    Для владельца (is_parent=true): возвращает все поля клиента, включая country, region, city из БД.
    
    Для пользователя (is_parent=false): возвращает данные с полями company_name, org_inn, country, region, city из владельца.
    Остальные поля (name, email, phone_number и т.д.) возвращаются из самого пользователя.
    """
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
    
    # Если это пользователь (is_parent=false), получаем данные владельца
    if not client.is_parent and client.parent_id:
        owner_result = await db.execute(
            select(Client).where(Client.client_id == client.parent_id)
        )
        owner = owner_result.scalar_one_or_none()
        
        if owner:
            # Создаем временный объект с данными владельца для полей company_name, org_inn, country, region, city
            # Для пользователей всегда используем данные владельца для этих полей
            client_data = ClientRead.model_validate(client)
            
            # Заменяем поля из владельца (всегда для пользователей)
            client_data.company_name = owner.company_name
            client_data.org_inn = owner.org_inn
            client_data.country = owner.country
            client_data.region = owner.region
            client_data.city = owner.city
            
            return client_data
    
    return ClientRead.model_validate(client)


@router.get("/by-hash/{client_hash}", response_model=ClientRead)
async def get_client_by_hash(
    client_hash: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Получение клиента по хешу.
    
    Если клиент является пользователем (is_parent=false), 
    возвращает данные с полями company_name, org_inn, country, region, city из владельца.
    """
    result = await db.execute(
        select(Client).where(Client.client_id_hash == client_hash)
    )
    client = result.scalar_one_or_none()
    
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Если это пользователь (is_parent=false), получаем данные владельца
    if not client.is_parent and client.parent_id:
        owner_result = await db.execute(
            select(Client).where(Client.client_id == client.parent_id)
        )
        owner = owner_result.scalar_one_or_none()
        
        if owner:
            # Создаем временный объект с данными владельца для полей company_name, org_inn, country, region, city
            # Для пользователей всегда используем данные владельца для этих полей
            client_data = ClientRead.model_validate(client)
            
            # Заменяем поля из владельца (всегда для пользователей)
            client_data.company_name = owner.company_name
            client_data.org_inn = owner.org_inn
            client_data.country = owner.country
            client_data.region = owner.region
            client_data.city = owner.city
            
            return client_data
    
    return ClientRead.model_validate(client)


@router.get("/by-subscriber/{code_abonent}", response_model=ClientRead)
async def get_client_by_subscriber(
    code_abonent: str,
    db: AsyncSession = Depends(get_db)
):
    """
    Поиск владельца клиента по code_abonent (subscriberId).
    
    Ищет клиента-владельца (is_parent=true) по полю code_abonent.
    Если найден только пользователь (is_parent=false), возвращает данные его владельца.
    
    Используется для восстановления клиента после очистки localStorage на фронте,
    когда известен только subscriberId.
    
    Returns:
        ClientRead: Данные клиента-владельца
        
    Raises:
        HTTPException 404: Если клиент с таким code_abonent не найден
    """
    try:
        if not code_abonent or not code_abonent.strip():
            raise HTTPException(status_code=400, detail="code_abonent is required")
        
        normalized_code = code_abonent.strip()
        
        # Сначала ищем владельца (is_parent=true)
        # Если найдено несколько, берем первый (самый старый по created_at)
        try:
            owner_result = await db.execute(
                select(Client).where(
                    Client.code_abonent == normalized_code,
                    Client.is_parent == True
                ).order_by(Client.created_at.asc()).limit(1)
            )
            owner = owner_result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Database error while searching for owner client with code_abonent '{normalized_code}': {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Internal server error while searching for client"
            )
        
        if owner:
            # Найден владелец - возвращаем его
            # Логируем предупреждение, если найдено несколько владельцев с таким code_abonent
            try:
                count_result = await db.execute(
                    select(func.count(Client.client_id)).where(
                        Client.code_abonent == normalized_code,
                        Client.is_parent == True
                    )
                )
                count = count_result.scalar()
                if count > 1:
                    logger.warning(
                        f"Found {count} owner clients with code_abonent '{normalized_code}'. "
                        f"Returning the oldest one (client_id: {owner.client_id})"
                    )
            except Exception as e:
                logger.warning(f"Failed to count owner clients: {e}")
                # Продолжаем без подсчета
            
            return ClientRead.model_validate(owner)
        
        # Если владелец не найден, ищем любого клиента с таким code_abonent (включая пользователей)
        try:
            user_result = await db.execute(
                select(Client).where(
                    Client.code_abonent == normalized_code,
                    Client.is_parent == False
                )
            )
            user = user_result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Database error while searching for user client with code_abonent '{normalized_code}': {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Internal server error while searching for client"
            )
        
        if user:
            # Найден пользователь
            if user.parent_id:
                # У пользователя есть parent_id - получаем его владельца
                try:
                    owner_result = await db.execute(
                        select(Client).where(Client.client_id == user.parent_id)
                    )
                    owner = owner_result.scalar_one_or_none()
                except Exception as e:
                    logger.error(f"Database error while fetching owner for user {user.client_id}: {e}", exc_info=True)
                    raise HTTPException(
                        status_code=500,
                        detail="Internal server error while fetching owner client"
                    )
                
                if owner:
                    # Возвращаем данные владельца с is_parent и parent_id
                    return ClientRead.model_validate(owner)
                else:
                    logger.warning(
                        f"User client {user.client_id} has parent_id {user.parent_id}, but parent not found. Returning user itself."
                    )
                    # Если владелец не найден, возвращаем самого пользователя
                    # Возможно, он должен быть владельцем
                    return ClientRead.model_validate(user)
            else:
                # У пользователя нет parent_id - возвращаем его самого
                # Возможно, он должен быть владельцем (is_parent должен быть true)
                logger.info(
                    f"User client {user.client_id} found with code_abonent '{normalized_code}' but parent_id is null. Returning user itself."
                )
                return ClientRead.model_validate(user)
        
        # Клиент с таким code_abonent не найден
        raise HTTPException(
            status_code=404,
            detail=f"Client with code_abonent '{normalized_code}' not found"
        )
    except HTTPException:
        # Пробрасываем HTTPException как есть
        raise
    except Exception as e:
        # Логируем все остальные ошибки и возвращаем 500 с деталями
        logger.error(f"Unexpected error in get_client_by_subscriber for code_abonent '{code_abonent}': {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )