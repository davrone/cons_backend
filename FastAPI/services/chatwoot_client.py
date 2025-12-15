"""Клиент для работы с Chatwoot API"""
import httpx
import logging
import re
from typing import Optional, Dict, Any, List
from ..config import settings

logger = logging.getLogger(__name__)


def is_valid_email(email: str) -> bool:
    """
    Проверяет, является ли строка валидным email адресом.
    
    Args:
        email: Строка для проверки
        
    Returns:
        True если email валидный, False иначе
    """
    if not email or not isinstance(email, str):
        return False
    
    email = email.strip()
    if not email:
        return False
    
    # Базовый паттерн для email (не слишком строгий, но достаточный)
    # Проверяем наличие @ и хотя бы одной точки после @
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email))


class ChatwootClient:
    """Асинхронный клиент для Chatwoot API"""
    
    # Кэш для bot_id (в памяти, для одного процесса)
    _bot_id_cache: Optional[int] = None
    
    # Кэш для созданных labels (в памяти, для одного процесса)
    _labels_cache: set[str] = set()
    
    def __init__(self):
        self.base_url = settings.CHATWOOT_API_URL.rstrip("/")
        # ВАЖНО: Используем только CHATWOOT_API_TOKEN (Platform API token)
        # НЕ используем bot_token - он не имеет доступа к некоторым endpoint'ам
        self.api_token = settings.CHATWOOT_API_TOKEN
        self.account_id = str(settings.CHATWOOT_ACCOUNT_ID).strip()

        if not (self.base_url and self.api_token and self.account_id):
            raise RuntimeError(
                "Chatwoot config missing. Check CHATWOOT_API_URL, CHATWOOT_API_TOKEN, CHATWOOT_ACCOUNT_ID"
            )
        
        # Логируем информацию о токене (без самого токена для безопасности)
        logger.debug(f"Chatwoot client initialized: base_url={self.base_url}, account_id={self.account_id}, token_length={len(self.api_token) if self.api_token else 0}")
        
        # Проверяем, что токен не пустой
        if not self.api_token or len(self.api_token.strip()) == 0:
            raise RuntimeError(
                "CHATWOOT_API_TOKEN is empty. Please set Platform API token in .env file. "
                "Bot tokens are not supported - they don't have access to all endpoints."
            )
    
    def _clean_custom_attributes(
        self,
        custom_attributes: Dict[str, Any],
        required_fields: tuple = ()
    ) -> Dict[str, Any]:
        """
        Очистка и валидация custom_attributes.
        
        Args:
            custom_attributes: Словарь с атрибутами
            required_fields: Кортеж полей, которые должны быть даже если пустые
        
        Returns:
            Очищенный словарь атрибутов
        """
        cleaned_custom_attrs = {}
        
        for key, value in custom_attributes.items():
            key_str = str(key)
            # Пропускаем None
            if value is None:
                logger.debug(f"Skipping None custom attribute: {key_str}")
                continue
            
            # Для обязательных полей разрешаем пустые строки
            # Для остальных - пропускаем пустые строки
            is_required = key_str in required_fields
            if not is_required and value == "":
                logger.debug(f"Skipping empty custom attribute: {key_str}")
                continue
            
            # Chatwoot принимает строки, числа, булевы
            if isinstance(value, bool):
                cleaned_custom_attrs[key_str] = value
            elif isinstance(value, (str, int, float)):
                if isinstance(value, str):
                    # Удаляем управляющие символы и нормализуем пробелы
                    import re
                    value = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', value)
                    value = re.sub(r'\s+', ' ', value).strip()
                    
                    if len(value) > 500:
                        logger.warning(f"Custom attribute {key_str} is too long ({len(value)} chars), truncating to 500")
                        cleaned_custom_attrs[key_str] = value[:500]
                    else:
                        cleaned_custom_attrs[key_str] = value
                else:
                    cleaned_custom_attrs[key_str] = value
            elif isinstance(value, (list, dict)):
                # Сложные типы конвертируем в строку (JSON)
                import json
                json_str = json.dumps(value, ensure_ascii=False)
                if len(json_str) > 500:
                    logger.warning(f"Custom attribute {key_str} JSON is too long ({len(json_str)} chars), truncating")
                    json_str = json_str[:500]
                cleaned_custom_attrs[key_str] = json_str
            else:
                # Остальные типы конвертируем в строку
                str_value = str(value)
                if len(str_value) > 500:
                    str_value = str_value[:500]
                cleaned_custom_attrs[key_str] = str_value
        
        # Финальная проверка ключей
        final_custom_attrs = {}
        for key, value in cleaned_custom_attrs.items():
            # Проверяем длину ключа
            if len(key) > 100:
                logger.warning(f"Custom attribute key '{key}' is too long ({len(key)} chars), skipping")
                continue
            
            # Для строк проверяем наличие проблемных символов
            if isinstance(value, str) and '\x00' in value:
                value = value.replace('\x00', '')
                logger.warning(f"Removed null bytes from custom attribute '{key}'")
            
            final_custom_attrs[key] = value
        
        return final_custom_attrs
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Базовый метод для запросов к Chatwoot Platform API.
        
        ВАЖНО: Токен CHATWOOT_API_TOKEN передается в заголовке 'api_access_token'.
        Это обязательное требование Chatwoot Platform API - токен должен быть именно
        в этом заголовке, иначе запросы не будут работать (401 Unauthorized).
        
        НЕ используем:
        - Authorization header (Bearer token) - это не работает для Platform API
        - Другие варианты заголовков для токена
        - Bot tokens - они не имеют доступа к некоторым endpoint'ам (например, /contacts)
        """
        url = f"{self.base_url}{endpoint}"
        # ВАЖНО: Токен CHATWOOT_API_TOKEN ОБЯЗАТЕЛЬНО должен быть в заголовке 'api_access_token'
        # Это требование Chatwoot Platform API - иначе будет 401 Unauthorized
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "api_access_token": self.api_token,  # CHATWOOT_API_TOKEN в заголовке api_access_token
            "User-Agent": "Clobus-Chatwoot-Client/1.0 (Custom Python Client)" # Добавьте это для Chatwoot
        }
        
        # Логируем запрос детально (без токена для безопасности)
        import json
        request_log = f"Chatwoot API Request: {method} {url}"
        request_log += f" | Using CHATWOOT_API_TOKEN (token length: {len(self.api_token) if self.api_token else 0})"
        if params:
            request_log += f"\n  Query params: {json.dumps(params, ensure_ascii=False, indent=2)}"
        if data:
            request_log += f"\n  Request body: {json.dumps(data, ensure_ascii=False, indent=2)}"
        logger.info(request_log)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params
            )
            
            # Логируем ответ
            response_body_preview = response.text[:500] if response.text else ""
            logger.info(
                f"Chatwoot API Response: {response.status_code} {response.reason_phrase}"
                + (f" | body: {response_body_preview}" if response_body_preview else "")
            )
            
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                body = exc.response.text if exc.response else ""
                # Логируем полный ответ при ошибке для отладки
                logger.error(
                    f"Chatwoot API Error: {exc} | "
                    f"URL: {exc.request.url if exc.request else 'N/A'} | "
                    f"Method: {exc.request.method if exc.request else 'N/A'} | "
                    f"Response body (full): {body}"
                )
                raise httpx.HTTPStatusError(
                    f"{exc} | response body: {body}",
                    request=exc.request,
                    response=exc.response,
                ) from exc
            if not response.content:
                return {}
            return response.json()
    
    def _extract_source_id(self, data: Dict[str, Any], inbox_id: Optional[int] = None) -> Optional[str]:
        """
        Извлечение source_id из ответа Public API или Platform API.
        
        source_id создается автоматически Chatwoot при создании contact через Public API.
        Нужно извлекать его из ответа для использования при создании conversation.
        
        Args:
            data: Ответ от Chatwoot API
            inbox_id: Опционально, для поиска source_id конкретного inbox
            
        Returns:
            source_id или None
        """
        if not data or not isinstance(data, dict):
            return None
        
        # 1. В корне ответа (source_id)
        if data.get("source_id") and isinstance(data.get("source_id"), str):
            return data["source_id"]
        
        # 2. В contact_inboxes[].source_id (наиболее вероятно для Public API)
        contact_inboxes = data.get("contact_inboxes", [])
        if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
            # Если указан inbox_id, ищем по нему
            if inbox_id:
                for ci in contact_inboxes:
                    inbox_info = ci.get("inbox", {})
                    if isinstance(inbox_info, dict) and inbox_info.get("id") == inbox_id:
                        source_id = ci.get("source_id")
                        if source_id:
                            return source_id
            # Берем первый доступный source_id
            source_id = contact_inboxes[0].get("source_id")
            if source_id:
                return source_id
        
        # 3. В contact_inbox.source_id (если один объект)
        if data.get("contact_inbox") and isinstance(data.get("contact_inbox"), dict):
            source_id = data["contact_inbox"].get("source_id")
            if source_id:
                return source_id
        
        # 4. В payload.contact_inbox.source_id или payload.contact.contact_inboxes[].source_id
        if data.get("payload"):
            payload = data["payload"]
            if isinstance(payload, dict):
                # Проверяем payload.contact_inbox
                if payload.get("contact_inbox") and isinstance(payload.get("contact_inbox"), dict):
                    source_id = payload["contact_inbox"].get("source_id")
                    if source_id:
                        return source_id
                
                # Проверяем payload.contact.contact_inboxes[]
                payload_contact = payload.get("contact", {})
                if isinstance(payload_contact, dict):
                    contact_inboxes = payload_contact.get("contact_inboxes", [])
                    if isinstance(contact_inboxes, list) and len(contact_inboxes) > 0:
                        if inbox_id:
                            for ci in contact_inboxes:
                                inbox_info = ci.get("inbox", {})
                                if isinstance(inbox_info, dict) and inbox_info.get("id") == inbox_id:
                                    source_id = ci.get("source_id")
                                    if source_id:
                                        return source_id
                        source_id = contact_inboxes[0].get("source_id")
                        if source_id:
                            return source_id
        
        # 5. В messages[].conversation.contact_inbox.source_id (для conversation)
        messages = data.get("messages", [])
        if isinstance(messages, list) and len(messages) > 0:
            first_message = messages[0]
            if isinstance(first_message, dict):
                conversation_data = first_message.get("conversation", {})
                if isinstance(conversation_data, dict):
                    contact_inbox = conversation_data.get("contact_inbox", {})
                    if isinstance(contact_inbox, dict):
                        source_id = contact_inbox.get("source_id")
                        if source_id:
                            return source_id
        
        return None
    
    def _extract_pubsub_token(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Извлечение pubsub_token из ответа Public API.
        
        Использует логику из chatWidget.js (extractPubsubToken) для поддержки различных структур ответов.
        Проверяет все возможные места, где может находиться pubsub_token согласно документации Chatwoot.
        
        Args:
            data: Ответ от Chatwoot API
            
        Returns:
            pubsub_token или None
        """
        if not data or not isinstance(data, dict):
            return None
        
        # 1. В корне ответа (наиболее часто)
        pubsub_token = data.get("pubsub_token")
        if pubsub_token and isinstance(pubsub_token, str):
            return pubsub_token
        
        # 2. В inbox_contact (часто используется в Public API для contact)
        inbox_contact = data.get("inbox_contact")
        if isinstance(inbox_contact, dict):
            pubsub_token = inbox_contact.get("pubsub_token")
            if pubsub_token and isinstance(pubsub_token, str):
                return pubsub_token
        
        # 2.1. В contact_inbox (альтернативное название)
        contact_inbox = data.get("contact_inbox")
        if isinstance(contact_inbox, dict):
            pubsub_token = contact_inbox.get("pubsub_token")
            if pubsub_token and isinstance(pubsub_token, str):
                return pubsub_token
        
        # 3. В conversation
        conversation = data.get("conversation")
        if isinstance(conversation, dict):
            pubsub_token = conversation.get("pubsub_token")
            if pubsub_token and isinstance(pubsub_token, str):
                return pubsub_token
        
        # 4. В payload (если ответ обернут)
        payload = data.get("payload")
        if isinstance(payload, dict):
            # 4.1. В payload.pubsub_token
            pubsub_token = payload.get("pubsub_token")
            if pubsub_token and isinstance(pubsub_token, str):
                return pubsub_token
            
            # 4.2. В payload.conversation.pubsub_token
            payload_conversation = payload.get("conversation")
            if isinstance(payload_conversation, dict):
                pubsub_token = payload_conversation.get("pubsub_token")
                if pubsub_token and isinstance(pubsub_token, str):
                    return pubsub_token
            
            # 4.3. В payload.contact.pubsub_token
            payload_contact = payload.get("contact")
            if isinstance(payload_contact, dict):
                pubsub_token = payload_contact.get("pubsub_token")
                if pubsub_token and isinstance(pubsub_token, str):
                    return pubsub_token
        
        # 5. В contact
        contact = data.get("contact")
        if isinstance(contact, dict):
            pubsub_token = contact.get("pubsub_token")
            if pubsub_token and isinstance(pubsub_token, str):
                return pubsub_token
        
        # 6. В messages[].conversation (для GET запросов conversation)
        messages = data.get("messages", [])
        if isinstance(messages, list) and len(messages) > 0:
            first_message = messages[0]
            if isinstance(first_message, dict):
                message_conversation = first_message.get("conversation", {})
                if isinstance(message_conversation, dict):
                    pubsub_token = message_conversation.get("pubsub_token")
                    if pubsub_token and isinstance(pubsub_token, str):
                        return pubsub_token
        
        # 7. В contact_inboxes[].pubsub_token (для contact ответов)
        contact_inboxes = data.get("contact_inboxes", [])
        if isinstance(contact_inboxes, list):
            for ci in contact_inboxes:
                if isinstance(ci, dict):
                    pubsub_token = ci.get("pubsub_token")
                    if pubsub_token and isinstance(pubsub_token, str):
                        return pubsub_token
        
        # 8. В payload.contact_inbox.pubsub_token
        if payload and isinstance(payload, dict):
            payload_contact_inbox = payload.get("contact_inbox")
            if isinstance(payload_contact_inbox, dict):
                pubsub_token = payload_contact_inbox.get("pubsub_token")
                if pubsub_token and isinstance(pubsub_token, str):
                    return pubsub_token
        
        return None
    
    async def _request_public_api(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Базовый метод для запросов к Chatwoot Public API.
        
        ВАЖНО: Public API не требует токена авторизации (публичный доступ).
        Используется для создания contacts и conversations, чтобы получить pubsub_token.
        
        Args:
            method: HTTP метод (GET, POST, PUT, DELETE)
            endpoint: Endpoint относительно base_url (например, /public/api/v1/inboxes/{identifier}/contacts)
            data: Тело запроса (для POST, PUT)
            params: Query параметры
            
        Returns:
            Dict с данными ответа
        """
        url = f"{self.base_url}{endpoint}"
        # Public API не требует токена
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Clobus-Chatwoot-Client/1.0 (Public API)"
        }
        
        # Логируем запрос с полным телом
        import json
        request_log = f"Chatwoot Public API Request: {method} {url}"
        if params:
            request_log += f"\n  Query params: {json.dumps(params, ensure_ascii=False, indent=2)}"
        if data:
            request_log += f"\n  Request body (full): {json.dumps(data, ensure_ascii=False, indent=2)}"
        logger.info(request_log)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params
            )
            
            # Логируем ответ с полным телом
            response_body = response.text if response.text else ""
            try:
                response_json = response.json() if response_body else {}
                response_body_formatted = json.dumps(response_json, ensure_ascii=False, indent=2)
            except Exception:
                response_body_formatted = response_body[:2000]  # Первые 2000 символов если не JSON
            
            logger.info(
                f"Chatwoot Public API Response: {response.status_code} {response.reason_phrase}"
                + f"\n  Response body (full): {response_body_formatted}"
            )
            
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                body = exc.response.text if exc.response else ""
                logger.error(
                    f"Chatwoot Public API Error: {exc} | "
                    f"URL: {exc.request.url if exc.request else 'N/A'} | "
                    f"Method: {exc.request.method if exc.request else 'N/A'} | "
                    f"Response body (full): {body}"
                )
                raise httpx.HTTPStatusError(
                    f"{exc} | response body: {body}",
                    request=exc.request,
                    response=exc.response,
                ) from exc
            if not response.content:
                return {}
            return response.json()
    
    async def create_contact_via_public_api(
        self,
        name: str,
        identifier: Optional[str] = None,
        email: Optional[str] = None,
        phone_number: Optional[str] = None,
        custom_attributes: Optional[Dict[str, Any]] = None,
        additional_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Создание контакта через Public API.
        
        Endpoint: POST /public/api/v1/inboxes/{inbox_identifier}/contacts
        
        ВАЖНО: Public API возвращает pubsub_token сразу в ответе создания contact.
        Это необходимо для WebSocket подключения виджета чата.
        
        Args:
            name: Имя контакта
            identifier: Уникальный идентификатор контакта (глобальный внешний ID, например, client_id UUID)
            email: Email контакта (обязательно для склейки контактов)
            phone_number: Номер телефона (обязательно для склейки контактов)
            custom_attributes: Кастомные атрибуты контакта
        
        Returns:
            Dict с данными созданного контакта, включая pubsub_token, source_id, id
        """
        if not settings.CHATWOOT_INBOX_IDENTIFIER:
            raise RuntimeError("Chatwoot inbox_identifier is not configured (CHATWOOT_INBOX_IDENTIFIER)")
        
        payload: Dict[str, Any] = {
            "name": name,
        }
        
        # Обязательно передаем email или phone_number для склейки контактов
        # ВАЖНО: Валидируем email перед отправкой
        if email:
            if is_valid_email(email):
                payload["email"] = email
            else:
                logger.warning(f"Invalid email format '{email}', skipping email field for contact creation")
        
        if phone_number:
            payload["phone_number"] = phone_number
        
        if identifier:
            payload["identifier"] = identifier
        
        if custom_attributes:
            # Очистка custom_attributes
            # Обязательные поля для contact: code_abonent, inn_pinfl, client_type
            cleaned_custom_attrs = self._clean_custom_attributes(
                custom_attributes,
                required_fields=("code_abonent", "inn_pinfl", "client_type")
            )
            if cleaned_custom_attrs:
                payload["custom_attributes"] = cleaned_custom_attrs
        
        if additional_attributes:
            # Типовые атрибуты Chatwoot
            payload["additional_attributes"] = additional_attributes
        
        import json
        logger.info(f"=== Creating Chatwoot contact via Public API ===")
        logger.info(f"  Endpoint: POST /public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts")
        logger.info(f"  Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        if additional_attributes:
            logger.info(f"  Additional attributes: {json.dumps(additional_attributes, ensure_ascii=False, indent=2)}")
        
        endpoint = f"/public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts"
        response = await self._request_public_api("POST", endpoint, data=payload)
        
        logger.info(f"✓ Contact created via Public API, response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
        return response
    
    async def create_conversation_via_public_api(
        self,
        source_id: str,
        message: str = "",
        custom_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Создание conversation через Public API.
        
        Endpoint: POST /public/api/v1/inboxes/{inbox_identifier}/contacts/{source_id}/conversations
        
        ВАЖНО: 
        - Использует source_id контакта для создания conversation
        - pubsub_token НЕ возвращается в ответе создания conversation
        - pubsub_token возвращается ТОЛЬКО в ответе POST создания contact
        
        Args:
            source_id: source_id контакта (из contact_inboxes[].source_id или contact.source_id)
            message: Текст первого сообщения
            custom_attributes: Кастомные атрибуты беседы (conversation)
        
        Returns:
            Dict с данными созданной conversation, включая id, status, contact и т.д.
        """
        if not settings.CHATWOOT_INBOX_IDENTIFIER:
            raise RuntimeError("Chatwoot inbox_identifier is not configured (CHATWOOT_INBOX_IDENTIFIER)")
        
        payload: Dict[str, Any] = {}
        
        # message обязателен, но может быть пустым
        if message:
            payload["message"] = {
                "content": str(message),
                "message_type": "incoming",  # Входящее сообщение от клиента
                "private": False
            }
        else:
            # Если сообщение пустое, все равно отправляем пустое сообщение
            payload["message"] = {
                "content": "",
                "message_type": "incoming",
                "private": False
            }
        
        # Custom attributes для беседы (conversation)
        # ВАЖНО: Обязательное поле (code_abonent) должно быть передано всегда
        if custom_attributes:
            cleaned_custom_attrs = self._clean_custom_attributes(
                custom_attributes,
                required_fields=("code_abonent",)
            )
            # Передаем custom_attributes даже если осталось только обязательное поле
            # Обязательное поле должно быть всегда передано в Chatwoot
            if cleaned_custom_attrs:
                payload["custom_attributes"] = cleaned_custom_attrs
            else:
                # Если после очистки словарь пустой, но обязательное поле должно быть,
                # создаем минимальный словарь с обязательным полем
                required_attrs = {}
                if "code_abonent" in custom_attributes:
                    required_attrs["code_abonent"] = str(custom_attributes.get("code_abonent", ""))
                if required_attrs:
                    payload["custom_attributes"] = required_attrs
        
        import json
        logger.info(f"=== Creating Chatwoot conversation via Public API ===")
        logger.info(f"  Endpoint: POST /public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}/conversations")
        logger.info(f"  Source ID: {source_id}")
        logger.info(f"  Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        endpoint = f"/public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}/conversations"
        response = await self._request_public_api("POST", endpoint, data=payload)
        
        logger.info(f"✓ Conversation created via Public API, response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
        return response
    
    async def get_contact_via_public_api(
        self,
        source_id: str
    ) -> Dict[str, Any]:
        """
        Получение contact через Public API для извлечения pubsub_token.
        
        Endpoint: GET /public/api/v1/inboxes/{inbox_identifier}/contacts/{source_id}
        
        ВАЖНО: pubsub_token возвращается в корне ответа contact endpoint.
        Это основной способ получения pubsub_token для контакта.
        
        Args:
            source_id: source_id контакта
            
        Returns:
            Dict с данными contact, включая pubsub_token в корне ответа
        """
        if not settings.CHATWOOT_INBOX_IDENTIFIER:
            raise RuntimeError("Chatwoot inbox_identifier is not configured (CHATWOOT_INBOX_IDENTIFIER)")
        
        import json
        logger.info(f"Getting Chatwoot contact via Public API")
        logger.info(f"  Endpoint: GET /public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}")
        
        endpoint = f"/public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}"
        response = await self._request_public_api("GET", endpoint)
        
        logger.info(f"✓ Contact retrieved via Public API, response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
        return response
    
    async def get_conversation_via_public_api(
        self,
        source_id: str,
        conversation_id: str
    ) -> Dict[str, Any]:
        """
        Получение conversation через Public API для извлечения pubsub_token.
        
        Endpoint: GET /public/api/v1/inboxes/{inbox_identifier}/contacts/{source_id}/conversations/{conversation_id}
        
        ВАЖНО: pubsub_token обычно НЕ возвращается в conversation endpoint.
        Используйте get_contact_via_public_api() для получения pubsub_token.
        
        Args:
            source_id: source_id контакта
            conversation_id: ID conversation
            
        Returns:
            Dict с данными conversation
        """
        if not settings.CHATWOOT_INBOX_IDENTIFIER:
            raise RuntimeError("Chatwoot inbox_identifier is not configured (CHATWOOT_INBOX_IDENTIFIER)")
        
        import json
        logger.info(f"Getting Chatwoot conversation via Public API")
        logger.info(f"  Endpoint: GET /public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}/conversations/{conversation_id}")
        
        endpoint = f"/public/api/v1/inboxes/{settings.CHATWOOT_INBOX_IDENTIFIER}/contacts/{source_id}/conversations/{conversation_id}"
        response = await self._request_public_api("GET", endpoint)
        
        logger.info(f"✓ Conversation retrieved via Public API, response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
        return response
    
    async def create_conversation(
        self,
        source_id: Optional[str] = None,
        inbox_id: Optional[int] = None,
        message: str = "",
        contact_id: Optional[int] = None,
        priority: Optional[str] = None,
        labels: Optional[List[str]] = None,
        custom_attributes: Optional[Dict[str, Any]] = None,
        contact_custom_attributes: Optional[Dict[str, Any]] = None,
        contact_email: Optional[str] = None,
        contact_phone: Optional[str] = None,
        contact_name: Optional[str] = None,
        contact_identifier: Optional[str] = None,
        status: Optional[str] = None,
        assignee_id: Optional[int] = None,
        team_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Создание новой консультации в Chatwoot согласно Platform API.
        
        ВАЖНО: 
        - Обязательно передавать объект `contact` с `email` или `phone_number` для правильной идентификации
        - `contact_custom_attributes` - атрибуты контакта (клиента)
        - `custom_attributes` - атрибуты беседы (тикета)
        - `source_id` - локальный ID для конкретного Inbox
        
        Args:
            source_id: Локальный ID клиента в рамках одного Inbox
            inbox_id: ID inbox
            message: Текст первого сообщения
            contact_id: ID контакта (опционально, если contact уже существует)
            priority: Приоритет консультации ("low", "medium", "high", "urgent")
            labels: Метки беседы
            custom_attributes: Кастомные атрибуты беседы (conversation)
            contact_custom_attributes: Кастомные атрибуты контакта (contact)
            contact_email: Email контакта (обязательно для склейки контактов)
            contact_phone: Телефон контакта (обязательно для склейки контактов)
            contact_name: Имя контакта
            contact_identifier: Дополнительный внешний ID контакта
            status: Статус беседы ("open", "resolved", "pending")
            assignee_id: ID оператора
            team_id: ID команды
        
        Returns:
            Dict с данными созданной консультации (включая id, contact_id, source_id)
        """
        final_inbox_id = inbox_id or settings.CHATWOOT_INBOX_ID
        if not final_inbox_id:
            raise RuntimeError("Chatwoot inbox_id is not configured (CHATWOOT_INBOX_ID)")
        
        # Валидация и очистка данных
        # source_id должен быть строкой, не UUID
        # ВАЖНО: Chatwoot может требовать определенный формат source_id
        # Если source_id это UUID, оставляем как есть (строка)
        # Если source_id слишком длинный или содержит специальные символы, может быть проблема
        safe_source_id = str(source_id) if source_id else "web"
        
        # Логируем source_id для отладки
        logger.debug(f"source_id: {safe_source_id} (type: {type(safe_source_id)}, length: {len(safe_source_id)})")
        
        # Проверяем, не слишком ли длинный source_id (Chatwoot может иметь ограничения)
        if len(safe_source_id) > 255:
            logger.warning(f"source_id is very long ({len(safe_source_id)} chars), truncating to 255")
            safe_source_id = safe_source_id[:255]
        
        payload = {
            "source_id": safe_source_id,
            "inbox_id": int(final_inbox_id),  # Убеждаемся, что это int
        }
        
        # ВАЖНО: Если contact_id не указан, передаем объект contact для создания/обновления
        # Это fallback на случай, если contact не был создан заранее
        if not contact_id:
            # Создаем объект contact для создания/обновления контакта
            contact_obj = {}
            
            if contact_name:
                contact_obj["name"] = str(contact_name)
            
            # Обязательно передаем email или phone_number для склейки контактов
            # ВАЖНО: Валидируем email перед отправкой - Chatwoot требует валидный email
            if contact_email:
                if is_valid_email(contact_email):
                    contact_obj["email"] = str(contact_email)
                else:
                    logger.warning(f"Invalid email format '{contact_email}', skipping email field for conversation contact")
                    # Не добавляем email, если он невалидный
            
            if contact_phone:
                contact_obj["phone_number"] = str(contact_phone)
            
            if contact_identifier:
                contact_obj["identifier"] = str(contact_identifier)
            
            # Custom attributes для контакта
            if contact_custom_attributes:
                # Обязательные поля для contact: code_abonent, inn_pinfl, client_type
                cleaned_contact_attrs = self._clean_custom_attributes(
                    contact_custom_attributes,
                    required_fields=("code_abonent", "inn_pinfl", "client_type")
                )
                if cleaned_contact_attrs:
                    contact_obj["custom_attributes"] = cleaned_contact_attrs
            
            # Добавляем объект contact только если есть хотя бы email или phone_number
            if contact_obj and (contact_obj.get("email") or contact_obj.get("phone_number")):
                payload["contact"] = contact_obj
                logger.info(f"Adding contact object with email={contact_obj.get('email')}, phone={contact_obj.get('phone_number')}")
            else:
                logger.warning("No email or phone_number provided for contact. Chatwoot may not properly identify the contact.")
        else:
            # Если contact_id указан, используем его (предпочтительный вариант)
            payload["contact_id"] = int(contact_id)
            logger.info(f"Using existing contact_id: {contact_id}")
        
        # message обязателен, но может быть пустым
        if message:
            payload["message"] = {
                "content": str(message),
                "message_type": "incoming",  # Входящее сообщение от клиента
                "private": False
            }
        else:
            # Если сообщение пустое, все равно отправляем пустое сообщение
            payload["message"] = {
                "content": "",
                "message_type": "incoming",
                "private": False
            }
        
        # Status - статус беседы (по умолчанию "open")
        payload["status"] = status or "open"
        
        # Priority - типовое поле conversation
        if priority:
            # Валидация: Chatwoot принимает "low", "medium", "high", "urgent"
            valid_priorities = {"low", "medium", "high", "urgent"}
            if priority.lower() in valid_priorities:
                payload["priority"] = priority.lower()
            else:
                logger.warning(f"Invalid priority '{priority}', ignoring. Valid values: {valid_priorities}")
        
        # Assignee и Team - назначаем оператора и команду
        if assignee_id:
            payload["assignee_id"] = int(assignee_id)
        if team_id:
            payload["team_id"] = int(team_id)
    
    async def get_teams(self) -> List[Dict[str, Any]]:
        """
        Получить список всех команд в Chatwoot.
        
        Returns:
            Список команд с полями id, name и т.д.
        """
        endpoint = f"/api/v1/accounts/{self.account_id}/teams"
        return await self._request("GET", endpoint)
    
    async def find_team_by_name(self, team_name: str) -> Optional[int]:
        """
        Найти команду по имени (регистронезависимый поиск).
        
        Args:
            team_name: Название команды
            
        Returns:
            ID команды или None если не найдена
        """
        try:
            teams = await self.get_teams()
            # teams может быть списком или словарем с ключом "payload"
            if isinstance(teams, dict) and "payload" in teams:
                teams_list = teams["payload"]
            elif isinstance(teams, list):
                teams_list = teams
            else:
                logger.warning(f"Unexpected teams response format: {type(teams)}")
                return None
            
            # Нормализуем имя команды для поиска (регистронезависимо)
            team_name_normalized = team_name.lower().strip()
            
            for team in teams_list:
                if isinstance(team, dict):
                    team_name_in_chatwoot = team.get("name", "")
                    if team_name_in_chatwoot.lower().strip() == team_name_normalized:
                        team_id = team.get("id")
                        logger.info(f"Found team '{team_name}' in Chatwoot: id={team_id}")
                        return team_id
            
            logger.warning(f"Team '{team_name}' not found in Chatwoot. Available teams: {[t.get('name') for t in teams_list if isinstance(t, dict)]}")
            return None
        except Exception as e:
            logger.warning(f"Failed to find team '{team_name}' in Chatwoot: {e}")
            return None
        
        # Labels - метки беседы (используем для language и source)
        # ВАЖНО: Согласно ТЗ, labels лучше добавлять отдельным запросом после создания
        # Но попробуем добавить в payload, если не сработает - добавим отдельно
        if labels:
            payload["labels"] = labels
        
        # Custom attributes для беседы (conversation)
        # ВАЖНО: Это атрибуты беседы, не контакта!
        if custom_attributes:
            cleaned_custom_attrs = self._clean_custom_attributes(custom_attributes, required_fields=("code_abonent",))
            if cleaned_custom_attrs:
                payload["custom_attributes"] = cleaned_custom_attrs
                logger.info(f"Conversation custom attributes ({len(cleaned_custom_attrs)} fields): {list(cleaned_custom_attrs.keys())}")
        
        # Логируем финальный payload перед отправкой для отладки
        import json
        logger.info(f"Final payload for Chatwoot create_conversation: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        logger.info(f"Chatwoot API URL: POST /api/v1/accounts/{self.account_id}/conversations")
        logger.info(f"Chatwoot base_url: {self.base_url}")
        logger.info(f"Chatwoot account_id: {self.account_id} (type: {type(self.account_id)})")
        logger.info(f"Chatwoot inbox_id: {final_inbox_id} (type: {type(final_inbox_id)})")
        logger.info(f"Chatwoot source_id: {safe_source_id} (type: {type(safe_source_id)})")
        
        # Проверяем, что account_id не пустой
        if not self.account_id or self.account_id.strip() == "":
            raise RuntimeError(f"CHATWOOT_ACCOUNT_ID is empty or invalid: '{self.account_id}'")
        
        # Сохраняем custom_attributes для fallback
        saved_custom_attrs = payload.get("custom_attributes")
        
        try:
            endpoint = f"/api/v1/accounts/{self.account_id}/conversations"
            logger.debug(f"Making request to: {self.base_url}{endpoint}")
            return await self._request(
                "POST",
                endpoint,
                data=payload,
            )
        except httpx.HTTPStatusError as e:
            # Дополнительное логирование при ошибке
            response_text = e.response.text if e.response else "N/A"
            response_status = e.response.status_code if e.response else "N/A"
            
            # Пытаемся распарсить JSON ответ для более детальной информации
            error_details = response_text
            try:
                if e.response and e.response.text:
                    import json
                    error_json = e.response.json()
                    error_details = json.dumps(error_json, ensure_ascii=False, indent=2)
            except:
                pass
            
            import json as json_module
            logger.error(
                f"Chatwoot create_conversation failed. "
                f"Status: {response_status}, "
                f"Response: {error_details}, "
                f"URL: {self.base_url}/api/v1/accounts/{self.account_id}/conversations, "
                f"Account ID: {self.account_id}, "
                f"Inbox ID: {final_inbox_id}, "
                f"Payload sent: {json_module.dumps(payload, ensure_ascii=False, indent=2)}"
            )
            
            # Для 404 ошибки добавляем специальную диагностику
            if response_status == 404:
                logger.error(
                    f"404 Not Found - возможные причины:\n"
                    f"  1. Account ID '{self.account_id}' не существует в Chatwoot\n"
                    f"  2. Endpoint '/api/v1/accounts/{self.account_id}/conversations' недоступен\n"
                    f"  3. API токен не имеет доступа к этому account\n"
                    f"  4. Неправильный base_url: {self.base_url}\n"
                    f"  Проверьте CHATWOOT_ACCOUNT_ID и права доступа токена"
                )
            
            # Для 5xx ошибок (серверные ошибки Chatwoot) пробуем fallback
            if response_status and 500 <= response_status < 600:
                logger.warning(
                    f"Chatwoot server error (5xx). "
                    f"Attempting fallback: create conversation with minimal payload."
                )
                
                # Пробуем создать conversation с минимальным набором полей
                # Сначала без custom_attributes
                fallback_payload = {
                    "source_id": safe_source_id,
                    "inbox_id": int(final_inbox_id),
                    "message": payload.get("message", {"content": ""})
                }
                
                try:
                    logger.info(f"Attempting to create conversation with minimal payload (without custom_attributes, priority, labels)...")
                    logger.info(f"Minimal payload: {json_module.dumps(fallback_payload, ensure_ascii=False, indent=2)}")
                    response = await self._request(
                        "POST",
                        f"/api/v1/accounts/{self.account_id}/conversations",
                        data=fallback_payload,
                    )
                    
                    conversation_id = str(response.get("id"))
                    if conversation_id and conversation_id != "None":
                        logger.info(f"✓ Created conversation {conversation_id} with minimal payload")
                        
                        # Пытаемся обновить остальные поля отдельными запросами
                        update_errors = []
                        
                        # Обновляем priority если был
                        if payload.get("priority"):
                            try:
                                await self.update_conversation(
                                    conversation_id=conversation_id,
                                    status=None,
                                    assignee_id=None
                                )
                                # Для priority нужно использовать другой endpoint или добавить в update_conversation
                                logger.info(f"Note: priority update not implemented in update_conversation")
                            except Exception as priority_error:
                                update_errors.append(f"priority: {priority_error}")
                        
                        # Обновляем labels если были (labels обычно не обновляются через API)
                        if payload.get("labels"):
                            logger.info(f"Note: labels cannot be updated after creation")
                        
                        # Пытаемся обновить custom_attributes отдельным запросом
                        if saved_custom_attrs:
                            try:
                                logger.info(f"Attempting to update custom_attributes for conversation {conversation_id}...")
                                await self.update_conversation(
                                    conversation_id=conversation_id,
                                    custom_attributes=saved_custom_attrs
                                )
                                logger.info(f"✓ Updated custom_attributes for conversation {conversation_id}")
                            except Exception as update_error:
                                update_errors.append(f"custom_attributes: {update_error}")
                                logger.warning(
                                    f"Failed to update custom_attributes for conversation {conversation_id}: {update_error}. "
                                    f"Conversation created but without custom_attributes."
                                )
                        
                        if update_errors:
                            logger.warning(f"Some updates failed: {', '.join(update_errors)}")
                        
                        return response
                    else:
                        logger.error("Fallback: Chatwoot returned invalid conversation ID")
                        raise
                except Exception as fallback_error:
                    logger.error(
                        f"Fallback also failed: {fallback_error}. "
                        f"Original error was: {error_details}"
                    )
                    # Пробрасываем оригинальную ошибку
                    raise httpx.HTTPStatusError(
                        f"{e} | response body: {error_details} | Fallback failed: {fallback_error}",
                        request=e.request,
                        response=e.response,
                    ) from e
            
            # Для других ошибок просто логируем
            if response_status and 500 <= response_status < 600:
                logger.error(
                    f"Chatwoot server error (5xx). This is likely a Chatwoot-side issue. "
                    f"Check Chatwoot logs. Payload structure: "
                    f"source_id={safe_source_id}, inbox_id={final_inbox_id}, "
                    f"has_message={bool(payload.get('message'))}, "
                    f"has_priority={bool(payload.get('priority'))}, "
                    f"has_labels={bool(payload.get('labels'))}, "
                    f"custom_attrs_count={len(payload.get('custom_attributes', {}))}"
                )
            
            raise
    
    async def update_conversation(
        self,
        conversation_id: str,
        status: Optional[str] = None,
        assignee_id: Optional[int] = None,
        team_id: Optional[int] = None,
        custom_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Обновление консультации в Chatwoot.
        
        Args:
            conversation_id: ID консультации
            status: Статус консультации
            assignee_id: ID назначенного агента
            team_id: ID команды
            custom_attributes: Кастомные атрибуты для обновления
        """
        payload = {}
        if status:
            payload["status"] = status
        if assignee_id:
            payload["assignee_id"] = assignee_id
        if team_id:
            payload["team_id"] = team_id
        if custom_attributes:
            # Применяем ту же очистку, что и при создании
            cleaned_custom_attrs = {}
            for key, value in custom_attributes.items():
                key_str = str(key)
                if value is None:
                    continue
                if isinstance(value, bool):
                    cleaned_custom_attrs[key_str] = value
                elif isinstance(value, (str, int, float)):
                    if isinstance(value, str):
                        import re
                        value = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', value)
                        value = re.sub(r'\s+', ' ', value).strip()
                        if len(value) > 500:
                            value = value[:500]
                    cleaned_custom_attrs[key_str] = value
                elif isinstance(value, (list, dict)):
                    import json
                    json_str = json.dumps(value, ensure_ascii=False)
                    if len(json_str) > 500:
                        json_str = json_str[:500]
                    cleaned_custom_attrs[key_str] = json_str
                else:
                    str_value = str(value)
                    if len(str_value) > 500:
                        str_value = str_value[:500]
                    cleaned_custom_attrs[key_str] = str_value
            
            if cleaned_custom_attrs:
                payload["custom_attributes"] = cleaned_custom_attrs
        
        return await self._request(
            "PUT",
            f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}",
            data=payload
        )
    
    async def get_conversation(self, conversation_id: str) -> Dict[str, Any]:
        """Получение консультации из Chatwoot"""
        return await self._request(
            "GET",
            f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}",
        )
    
    async def get_contact(self, contact_id: int) -> Dict[str, Any]:
        """Получение контакта из Chatwoot по ID"""
        return await self._request(
            "GET",
            f"/api/v1/accounts/{self.account_id}/contacts/{contact_id}",
        )
    
    async def ensure_label_exists(self, label_title: str) -> bool:
        """
        Убедиться, что label существует в Chatwoot. Создает label если его нет.
        
        Args:
            label_title: Название label (например, "lang_ru" или "source_web")
            ВАЖНО: Chatwoot разрешает только буквы, цифры, дефисы и подчеркивания
        
        Returns:
            True если label существует или был создан, False если не удалось создать
        """
        # Проверяем кэш
        if label_title in ChatwootClient._labels_cache:
            return True
        
        try:
            # Пытаемся найти существующий label
            labels_response = await self._request(
                "GET",
                f"/api/v1/accounts/{self.account_id}/labels"
            )
            
            if isinstance(labels_response, list):
                existing_labels = [label.get("title") for label in labels_response if isinstance(label, dict)]
                if label_title in existing_labels:
                    ChatwootClient._labels_cache.add(label_title)
                    return True
            
            # Label не найден, создаем его
            # ВАЖНО: Используем человеко-читаемые названия для labels
            # Цвета будут установлены пользователем в UI, здесь создаем только labels
            create_payload = {
                "title": label_title,
                "description": f"Автоматически созданная метка: {label_title}"
            }
            
            try:
                await self._request(
                    "POST",
                    f"/api/v1/accounts/{self.account_id}/labels",
                    data=create_payload
                )
                ChatwootClient._labels_cache.add(label_title)
                logger.info(f"Created label in Chatwoot: {label_title}")
                return True
            except httpx.HTTPStatusError as create_error:
                # Если label уже существует (409 или 422), добавляем в кэш
                status_code = create_error.response.status_code if create_error.response else None
                error_body = create_error.response.text if create_error.response else ""
                error_str = error_body.lower()
                
                if (
                    status_code in (409, 422) or
                    "already exists" in error_str or
                    "already been taken" in error_str or
                    "title has already been taken" in error_str
                ):
                    ChatwootClient._labels_cache.add(label_title)
                    logger.debug(f"Label already exists (status {status_code}): {label_title}")
                    return True
                logger.warning(f"Failed to create label {label_title}: {create_error}")
                return False
            except Exception as create_error:
                # Для других типов ошибок проверяем строку
                error_str = str(create_error).lower()
                if (
                    "422" in str(create_error) or
                    "409" in str(create_error) or
                    "already exists" in error_str or
                    "already been taken" in error_str or
                    "title has already been taken" in error_str
                ):
                    ChatwootClient._labels_cache.add(label_title)
                    logger.debug(f"Label already exists: {label_title}")
                    return True
                logger.warning(f"Failed to create label {label_title}: {create_error}")
                return False
                
        except Exception as e:
            logger.warning(f"Failed to ensure label exists {label_title}: {e}")
            # В случае ошибки продолжаем - возможно label уже существует
            return False
    
    async def add_conversation_labels(
        self,
        conversation_id: str,
        labels: List[str]
    ) -> Dict[str, Any]:
        """
        Добавление меток (labels) к беседе отдельным запросом.
        
        ВАЖНО: Некоторые версии Chatwoot могут игнорировать поле labels в запросе на создание беседы.
        Используйте этот метод для добавления labels после успешного создания conversation.
        
        Перед добавлением убеждается, что все labels существуют в Chatwoot.
        
        ВАЖНО: Chatwoot API требует передачи labels как массива строк в теле запроса.
        Используем PUT метод для замены всех labels или POST для добавления.
        
        Args:
            conversation_id: ID беседы
            labels: Список меток для добавления
        
        Returns:
            Dict с данными обновленной беседы
        """
        if not labels:
            logger.debug(f"No labels to add for conversation {conversation_id}")
            return {}
        
        # Убеждаемся, что все labels существуют
        for label in labels:
            if label:
                await self.ensure_label_exists(label)
        
        # Chatwoot API требует передачи labels как массива строк
        # Используем PUT для замены всех labels беседы
        payload = labels  # Передаем массив напрямую, не в объекте
        
        import json
        logger.info(f"Adding labels to conversation {conversation_id}: {json.dumps(labels, ensure_ascii=False)}")
        
        try:
            # Пробуем PUT метод (замена всех labels)
            response = await self._request(
                "PUT",
                f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/labels",
                data=payload
            )
            logger.info(f"✓ Successfully added labels to conversation {conversation_id}")
            return response
        except Exception as e:
            # Если PUT не работает, пробуем POST (добавление labels)
            logger.warning(f"PUT method failed for labels, trying POST: {e}")
            try:
                response = await self._request(
                    "POST",
                    f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/labels",
                    data={"labels": payload}  # Для POST используем объект с полем labels
                )
                logger.info(f"✓ Successfully added labels to conversation {conversation_id} via POST")
                return response
            except Exception as post_error:
                logger.error(f"Failed to add labels via both PUT and POST: {post_error}", exc_info=True)
                raise
    
    async def send_message(
        self,
        conversation_id: str,
        content: str,
        message_type: str = "outgoing"
    ) -> Dict[str, Any]:
        """Отправка сообщения в консультацию"""
        payload = {
            "content": content,
            "message_type": message_type
        }
        return await self._request(
            "POST",
            f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/messages",
            data=payload
        )
    
    async def send_message_with_attachment(
        self,
        conversation_id: str,
        content: str,
        attachment_url: str,
        attachment_type: str = "file",
        message_type: str = "incoming"
    ) -> Dict[str, Any]:
        """
        Отправка сообщения с вложением в консультацию.
        
        Args:
            conversation_id: ID консультации в Chatwoot
            content: Текст сообщения
            attachment_url: URL файла для загрузки
            attachment_type: Тип файла (image, file, audio, video)
            message_type: Тип сообщения (incoming/outgoing)
        """
        import httpx
        from io import BytesIO
        
        # Сначала загружаем файл по URL
        async with httpx.AsyncClient(timeout=60.0) as client:
            file_response = await client.get(attachment_url)
            file_response.raise_for_status()
            file_content = file_response.content
            file_name = attachment_url.split("/")[-1] or f"file.{attachment_type}"
        
        # Определяем content_type по типу файла
        content_type_map = {
            "image": "image/jpeg",
            "audio": "audio/mpeg",
            "video": "video/mp4",
            "file": "application/octet-stream"
        }
        content_type = content_type_map.get(attachment_type, "application/octet-stream")
        
        # Отправляем сообщение с вложением через multipart/form-data
        url = f"{self.base_url}/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/messages"
        headers = {
            "api_access_token": self.api_token,
            "User-Agent": "Clobus-Chatwoot-Client/1.0"
        }
        
        # Используем multipart/form-data для отправки файла
        # Chatwoot ожидает attachments[] как массив файлов
        files = {
            "attachments[]": (file_name, BytesIO(file_content), content_type)
        }
        data = {
            "content": content,
            "message_type": message_type
        }
        
        logger.info(f"Sending message with attachment to conversation {conversation_id}: type={attachment_type}, size={len(file_content)} bytes, filename={file_name}")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(
                    url,
                    headers=headers,
                    data=data,
                    files=files
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Successfully sent message with attachment to conversation {conversation_id}")
                return result
            except httpx.HTTPStatusError as e:
                error_body = e.response.text if e.response else ""
                logger.error(
                    f"Failed to send message with attachment: {e} | "
                    f"Status: {e.response.status_code if e.response else 'N/A'} | "
                    f"Response: {error_body}"
                )
                raise
    
    async def send_note(
        self,
        conversation_id: str,
        content: str,
        private: bool = True
    ) -> Dict[str, Any]:
        """
        Отправка служебного сообщения (note) в консультацию.
        
        Args:
            conversation_id: ID консультации в Chatwoot
            content: Текст сообщения
            private: Если True, сообщение видно только агенту
        
        Returns:
            Dict с данными созданного сообщения
        """
        payload = {
            "content": content,
            "message_type": "incoming",
            "private": private
        }
        return await self._request(
            "POST",
            f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/messages",
            data=payload
        )
    
    async def get_messages(
        self,
        conversation_id: str,
        page: int = 1,
        per_page: int = 50
    ) -> Dict[str, Any]:
        """
        Получение истории сообщений из conversation.
        
        Args:
            conversation_id: ID консультации в Chatwoot
            page: Номер страницы (начиная с 1)
            per_page: Количество сообщений на странице
        
        Returns:
            Dict с данными сообщений и метаданными пагинации
        """
        params = {
            "page": page,
            "per_page": per_page
        }
        return await self._request(
            "GET",
            f"/api/v1/accounts/{self.account_id}/conversations/{conversation_id}/messages",
            params=params
        )
    
    async def create_user(
        self,
        name: str,
        email: Optional[str] = None,
        custom_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Создание пользователя в Chatwoot.
        
        ВАЖНО: При создании устанавливает confirmed=True и available=True,
        чтобы пользователь был доступен для выбора в Chatwoot.
        
        Args:
            name: Имя пользователя
            email: Email (обязательно для Application API)
            custom_attributes: Кастомные поля (например, cl_ref_key для маппинга)
                              ВАЖНО: Application API может не возвращать custom_attributes
                              в GET запросах и не показывать их в UI. Для доступа к ним
                              может потребоваться Platform API.
        
        Returns:
            Dict с данными созданного пользователя (включая id)
        """
        payload: Dict[str, Any] = {
            "name": name,
            "role": "agent",  # По умолчанию создаём агентом
            "confirmed": True,  # Подтверждаем пользователя сразу
            "available": True,  # Делаем доступным для работы
        }
        if email:
            payload["email"] = email
        if custom_attributes:
            payload["custom_attributes"] = custom_attributes
        
        user_response = await self._request("POST", f"/api/v1/accounts/{self.account_id}/agents", data=payload)
        
        # Добавляем пользователя в inbox после создания
        user_id = user_response.get("id") if isinstance(user_response, dict) else None
        if user_id and self.account_id:
            from ..config import settings
            if settings.CHATWOOT_INBOX_ID:
                try:
                    await self.add_user_to_inbox(user_id, settings.CHATWOOT_INBOX_ID)
                except Exception as e:
                    logger.warning(f"Failed to add user {user_id} to inbox {settings.CHATWOOT_INBOX_ID}: {e}")
        
        return user_response
    
    async def add_user_to_inbox(
        self,
        user_id: int,
        inbox_id: int
    ) -> Dict[str, Any]:
        """
        Добавить пользователя в inbox.
        
        Args:
            user_id: ID пользователя в Chatwoot
            inbox_id: ID inbox
        
        Returns:
            Dict с результатом операции
        """
        return await self._request(
            "POST",
            f"/api/v1/accounts/{self.account_id}/inboxes/{inbox_id}/members",
            data={"user_ids": [user_id]}
        )
    
    async def get_agent(self, agent_id: int) -> Dict[str, Any]:
        """
        Получение агента по ID.
        
        ВАЖНО: Application API может не возвращать custom_attributes в GET запросах.
        Для доступа к custom_attributes может потребоваться Platform API.
        
        Args:
            agent_id: ID агента в Chatwoot
        
        Returns:
            Dict с данными агента (custom_attributes могут отсутствовать)
        """
        return await self._request("GET", f"/api/v1/accounts/{self.account_id}/agents/{agent_id}")
    
    async def find_user_by_custom_attribute(
        self,
        attribute_key: str,
        attribute_value: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск пользователя по кастомному атрибуту.
        
        ВАЖНО: Application API может не возвращать custom_attributes в GET запросах.
        Этот метод может не работать для поиска по custom_attributes через Application API.
        Рекомендуется использовать chatwoot_user_id из БД для маппинга.
        
        Args:
            attribute_key: Ключ кастомного атрибута (например, "cl_ref_key")
            attribute_value: Значение атрибута
        
        Returns:
            Dict с данными пользователя или None
        """
        # Chatwoot Application API может не возвращать custom_attributes в GET запросах
        # Поэтому поиск по custom_attributes может не работать
        # Рекомендуется использовать chatwoot_user_id из БД для маппинга
        try:
            response = await self._request("GET", f"/api/v1/accounts/{self.account_id}/agents")
            # Chatwoot может возвращать список напрямую или в поле payload
            if isinstance(response, list):
                agents = response
            elif isinstance(response, dict):
                agents = response.get("payload", [])
                if not agents and isinstance(response.get("value"), list):
                    agents = response.get("value", [])
            else:
                agents = []
            
            for agent in agents:
                # Application API может не возвращать custom_attributes
                custom_attrs = agent.get("custom_attributes", {})
                if custom_attrs and custom_attrs.get(attribute_key) == attribute_value:
                    return agent
        except Exception:
            pass
        return None
    
    async def find_user_by_email(
        self,
        email: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск пользователя по email.
        
        Args:
            email: Email пользователя
        
        Returns:
            Dict с данными пользователя или None
        """
        try:
            response = await self._request("GET", f"/api/v1/accounts/{self.account_id}/agents")
            # Chatwoot может возвращать список напрямую или в поле payload
            if isinstance(response, list):
                agents = response
            elif isinstance(response, dict):
                agents = response.get("payload", [])
                if not agents and isinstance(response.get("value"), list):
                    agents = response.get("value", [])
            else:
                agents = []
            
            email_lower = email.lower()
            for agent in agents:
                agent_email = agent.get("email")
                if agent_email and agent_email.lower() == email_lower:
                    return agent
        except Exception:
            pass
        return None
    
    async def find_user_by_name(
        self,
        name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск пользователя по имени (name или available_name).
        
        Args:
            name: Имя пользователя
        
        Returns:
            Dict с данными пользователя или None
        """
        try:
            response = await self._request("GET", f"/api/v1/accounts/{self.account_id}/agents")
            # Chatwoot может возвращать список напрямую или в поле payload
            if isinstance(response, list):
                agents = response
            elif isinstance(response, dict):
                agents = response.get("payload", [])
                if not agents and isinstance(response.get("value"), list):
                    agents = response.get("value", [])
            else:
                agents = []
            
            name_lower = name.lower().strip()
            for agent in agents:
                agent_name = agent.get("name", "").lower().strip()
                agent_available_name = agent.get("available_name", "").lower().strip()
                if (agent_name and agent_name == name_lower) or (agent_available_name and agent_available_name == name_lower):
                    return agent
        except Exception:
            pass
        return None
    
    async def list_all_agents(self) -> List[Dict[str, Any]]:
        """
        Получение списка всех агентов в Chatwoot.
        
        Returns:
            Список всех агентов
        """
        try:
            response = await self._request("GET", f"/api/v1/accounts/{self.account_id}/agents")
            # Chatwoot может возвращать список напрямую или в поле payload
            if isinstance(response, list):
                return response
            elif isinstance(response, dict):
                agents = response.get("payload", [])
                if not agents and isinstance(response.get("value"), list):
                    agents = response.get("value", [])
                return agents if isinstance(agents, list) else []
            else:
                return []
        except Exception:
            return []
    
    async def create_contact(
        self,
        name: str,
        identifier: Optional[str] = None,
        email: Optional[str] = None,
        phone_number: Optional[str] = None,
        custom_attributes: Optional[Dict[str, Any]] = None,
        inbox_id: Optional[int] = None,
        additional_attributes: Optional[Dict[str, Any]] = None
        # source_id НЕ передается - Chatwoot создает его автоматически
    ) -> Dict[str, Any]:
        """
        Создание контакта в Chatwoot.
        
        ВАЖНО: source_id НЕ передается при создании contact - Chatwoot создает его автоматически.
        source_id будет возвращен в ответе создания и должен быть извлечен из payload.contact_inbox.source_id
        или payload.contact.contact_inboxes[].source_id
        
        Args:
            name: Имя контакта
            identifier: Уникальный идентификатор контакта (глобальный внешний ID, например, client_id UUID)
            email: Email контакта (обязательно для склейки контактов)
            phone_number: Номер телефона (обязательно для склейки контактов)
            custom_attributes: Кастомные атрибуты контакта
            inbox_id: ID inbox для связи контакта с инбоксом
        
        Returns:
            Dict с данными созданного контакта (включая id, source_id в payload.contact_inbox.source_id)
        """
        payload: Dict[str, Any] = {
            "name": name,
        }
        
        # Обязательно передаем email или phone_number для склейки контактов
        # ВАЖНО: Валидируем email перед отправкой - Chatwoot требует валидный email
        if email:
            if is_valid_email(email):
                payload["email"] = email
            else:
                logger.warning(f"Invalid email format '{email}', skipping email field for contact creation")
                # Не добавляем email, если он невалидный
                # Chatwoot вернет 422 если email невалидный
        
        if phone_number:
            payload["phone_number"] = phone_number
        
        if identifier:
            payload["identifier"] = identifier
        
        if inbox_id:
            payload["inbox_id"] = int(inbox_id)
        
        # ВАЖНО: source_id НЕ передаем - Chatwoot создает его автоматически при создании contact
        
        if custom_attributes:
            # Очистка custom_attributes
            # Обязательные поля для contact: code_abonent, inn_pinfl, client_type
            cleaned_custom_attrs = self._clean_custom_attributes(
                custom_attributes,
                required_fields=("code_abonent", "inn_pinfl", "client_type")
            )
            if cleaned_custom_attrs:
                payload["custom_attributes"] = cleaned_custom_attrs
        
        if additional_attributes:
            payload["additional_attributes"] = additional_attributes
        
        logger.info(f"Creating Chatwoot contact: {payload}")
        return await self._request(
            "POST",
            f"/api/v1/accounts/{self.account_id}/contacts",
            data=payload
        )
    
    async def find_contact_by_identifier(
        self,
        identifier: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск контакта по identifier.
        
        Args:
            identifier: Уникальный идентификатор контакта
        
        Returns:
            Dict с данными контакта или None
        """
        try:
            # Пытаемся получить список контактов и найти по identifier
            # Chatwoot API может поддерживать поиск через query параметр
            response = await self._request(
                "GET",
                f"/api/v1/accounts/{self.account_id}/contacts",
                params={"identifier": identifier}
            )
            # Ответ может содержать массив контактов или объект с payload
            if isinstance(response, dict):
                contacts = response.get("payload", [])
                if isinstance(contacts, list):
                    for contact in contacts:
                        if contact.get("identifier") == identifier:
                            return contact
                elif isinstance(contacts, dict) and contacts.get("identifier") == identifier:
                    return contacts
        except Exception as e:
            logger.debug(f"Failed to search contact by identifier {identifier}: {e}")
        return None
    
    async def find_contact_by_email(
        self,
        email: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск контакта по email.
        
        Args:
            email: Email контакта
        
        Returns:
            Dict с данными контакта или None
        """
        try:
            response = await self._request(
                "GET",
                f"/api/v1/accounts/{self.account_id}/contacts",
                params={"email": email}
            )
            if isinstance(response, dict):
                contacts = response.get("payload", [])
                if isinstance(contacts, list):
                    for contact in contacts:
                        if contact.get("email") == email:
                            return contact
                elif isinstance(contacts, dict) and contacts.get("email") == email:
                    return contacts
        except Exception as e:
            logger.debug(f"Failed to search contact by email {email}: {e}")
        return None
    
    async def find_contact_by_phone(
        self,
        phone_number: str
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск контакта по номеру телефона.
        
        Args:
            phone_number: Номер телефона контакта
        
        Returns:
            Dict с данными контакта или None
        """
        try:
            response = await self._request(
                "GET",
                f"/api/v1/accounts/{self.account_id}/contacts",
                params={"phone_number": phone_number}
            )
            if isinstance(response, dict):
                contacts = response.get("payload", [])
                if isinstance(contacts, list):
                    for contact in contacts:
                        if contact.get("phone_number") == phone_number:
                            return contact
                elif isinstance(contacts, dict) and contacts.get("phone_number") == phone_number:
                    return contacts
        except Exception as e:
            logger.debug(f"Failed to search contact by phone {phone_number}: {e}")
        return None
    
    async def find_or_create_contact(
        self,
        name: str,
        identifier: Optional[str] = None,
        email: Optional[str] = None,
        phone_number: Optional[str] = None,
        custom_attributes: Optional[Dict[str, Any]] = None,
        inbox_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Находит существующий контакт или создает новый.
        
        Args:
            name: Имя контакта
            identifier: Уникальный идентификатор контакта (для поиска и создания)
            email: Email контакта
            phone_number: Номер телефона
            custom_attributes: Кастомные атрибуты контакта
            inbox_id: ID inbox для связи контакта с инбоксом
        
        Returns:
            Dict с данными контакта (включая id, source_id)
        """
        # Пытаемся найти существующий контакт по identifier
        if identifier:
            existing = await self.find_contact_by_identifier(identifier)
            if existing:
                logger.info(f"Found existing contact with identifier {identifier}: {existing.get('id')}")
                return existing
        
        # Если не нашли, создаем новый
        logger.info(f"Creating new contact with identifier {identifier or 'N/A'}")
        try:
            return await self.create_contact(
                name=name,
                identifier=identifier,
                email=email,
                phone_number=phone_number,
                custom_attributes=custom_attributes,
                inbox_id=inbox_id
            )
        except httpx.HTTPStatusError as e:
            # Если контакт уже существует (возможно, Chatwoot вернул ошибку)
            # Пытаемся найти его еще раз
            if identifier and e.response.status_code in (400, 409, 422):
                logger.warning(f"Contact creation failed, trying to find existing contact: {e}")
                existing = await self.find_contact_by_identifier(identifier)
                if existing:
                    logger.info(f"Found existing contact after creation error: {existing.get('id')}")
                    return existing
            raise
