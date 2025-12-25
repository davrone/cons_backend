"""Клиент для работы с 1C:ЦЛ через OData"""
import httpx
import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from urllib.parse import quote
from ..config import settings

logger = logging.getLogger(__name__)


class ConsultationLimitExceeded(Exception):
    """
    Исключение для случая превышения лимита создания консультаций в 1C:ЦЛ.
    
    В 1C:ЦЛ есть лимит: максимум 3 документа ТелефонныйЗвонок на один день
    (по полю ДатаКонсультации). При превышении лимита возвращается ошибка 500.
    """
    pass

# Константы для языков (из load_dicts.py)
LANG_RU_KEY = "15d38cda-1812-11ef-b824-c67597d01fa8"
LANG_UZ_KEY = "15d38cdb-1812-11ef-b824-c67597d01fa8"

def get_language_key(lang_code: Optional[str]) -> Optional[str]:
    """Маппинг кода языка (ru/uz) в Язык_Key для 1C"""
    if not lang_code:
        return LANG_RU_KEY  # По умолчанию русский
    lang_code_lower = lang_code.lower()
    if lang_code_lower == "ru":
        return LANG_RU_KEY
    elif lang_code_lower == "uz":
        return LANG_UZ_KEY
    return LANG_RU_KEY  # По умолчанию русский

def map_source_to_contact_method(source: Optional[str]) -> str:
    """Маппинг источника в СпособСвязи для 1C"""
    if not source:
        return "ПоТелефону"
    source_lower = source.lower()
    if "telegram" in source_lower or "tg" in source_lower:
        return "ПоТелеграм"
    elif "site" in source_lower or "web" in source_lower:
        return "ПоСайту"
    elif "phone" in source_lower or "call" in source_lower:
        return "ПоТелефону"
    return "ПоТелефону"  # По умолчанию

def map_importance_to_1c(importance: Optional[int]) -> str:
    """
    Маппинг важности в формат 1C:ЦЛ.
    
    Args:
        importance: Числовое значение важности (1 = Низкая, 2 = Обычная, >=3 = Высокая)
    
    Returns:
        Строковое значение для поля "Важность" в 1C:ЦЛ:
        - "Низкая" для importance == 1
        - "Обычная" для importance == 2 или None (по умолчанию)
        - "Высокая" для importance >= 3
    """
    if importance is None:
        return "Обычная"
    if importance >= 3:
        return "Высокая"
    elif importance == 1:
        return "Низкая"
    return "Обычная"


class OneCClient:
    """Асинхронный клиент для 1C:ЦЛ OData API"""
    
    def __init__(self):
        # Используем ODATA_BASEURL_CL если доступен, иначе ODATA_BASE_URL
        self.odata_base_url = (settings.ODATA_BASEURL_CL or settings.ODATA_BASE_URL).rstrip("/")
        self.odata_user = settings.ODATA_USER
        self.odata_password = settings.ODATA_PASSWORD
        self.entity = "Document_ТелефонныйЗвонок"
        # В 1C сущность клиентов — Catalog_Контрагенты
        self.clients_entity = "Catalog_Контрагенты"
    
    async def _odata_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Базовый метод для OData запросов с retry и backoff.
        
        Args:
            method: HTTP метод (GET, POST, PATCH, etc.)
            endpoint: OData endpoint
            data: Тело запроса (для POST/PATCH)
            params: Query параметры
            max_retries: Максимальное количество попыток
        
        Returns:
            Dict с ответом от сервера
        """
        base_url = self.odata_base_url.rstrip("/")
        endpoint = (endpoint or "").lstrip("/")
        # Формируем URL - httpx автоматически кодирует URL, но для кириллицы в OData
        # может потребоваться явное кодирование. Используем quote для имени сущности.
        if endpoint:
            # Для OData с кириллицей нужно кодировать имя сущности
            # Разбиваем endpoint на части
            endpoint_parts = endpoint.split('/')
            encoded_parts = []
            for part in endpoint_parts:
                # Если есть скобки (например, Document_ТелефонныйЗвонок(guid'...'))
                if '(' in part:
                    entity_name, rest = part.split('(', 1)
                    # Кодируем имя сущности, но оставляем скобки и параметры как есть
                    encoded_parts.append(f"{quote(entity_name, safe='')}({rest}")
                else:
                    # Кодируем имя сущности
                    encoded_parts.append(quote(part, safe=''))
            encoded_endpoint = '/'.join(encoded_parts)
            url = f"{base_url}/{encoded_endpoint}"
        else:
            url = base_url
        auth = (self.odata_user, self.odata_password)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        attempt = 0
        while attempt < max_retries:
            try:
                # Логируем запрос детально
                import json
                # Логируем исходный endpoint для отладки
                original_endpoint = endpoint if endpoint else "(root)"
                request_log = f"1C OData Request [{attempt + 1}/{max_retries}]: {method}\n"
                request_log += f"  Original endpoint: {original_endpoint}\n"
                request_log += f"  Final URL: {url}"
                if params:
                    request_log += f"\n  Query params: {json.dumps(params, ensure_ascii=False, indent=2)}"
                if data:
                    request_log += f"\n  Request body: {json.dumps(data, ensure_ascii=False, indent=2)}"
                logger.info(request_log)
                
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        auth=auth,
                        headers=headers,
                        json=data if data else None,
                        params=params
                    )
                    
                    # Улучшенное логирование ошибок
                    if response.status_code >= 400:
                        error_body = ""
                        try:
                            error_body = response.text
                        except:
                            try:
                                error_body = str(response.content)
                            except:
                                error_body = "Unable to read response body"
                        
                        logger.error(
                            f"1C OData Error [{attempt + 1}/{max_retries}]: "
                            f"Client error '{response.status_code} {response.reason_phrase}' for url '{url}'\n"
                            f"  Request body: {json.dumps(data, ensure_ascii=False, indent=2) if data else 'None'}\n"
                            f"  Response body: {error_body}"
                        )
                    
                    # Логируем ответ
                    logger.info(
                        f"1C OData Response: {response.status_code} {response.reason_phrase}"
                        + (f" | body: {response.text[:500]}" if response.text else "")
                    )
                    
                    # Если 429 (Too Many Requests) или 5xx ошибка, делаем retry
                    # ВАЖНО: 500 может быть временной ошибкой на стороне 1C, поэтому делаем retry
                    if response.status_code in (429, 500, 502, 503, 504):
                        if attempt < max_retries - 1:
                            wait_time = min(2 ** attempt, 60)  # Экспоненциальный backoff, макс 60 сек
                            logger.warning(f"1C OData retry {attempt + 1}/{max_retries} after {wait_time}s for status {response.status_code}")
                            await asyncio.sleep(wait_time)
                            attempt += 1
                            continue
                    
                    response.raise_for_status()
                    # Для DELETE запросов может быть пустой ответ (204 No Content)
                    if method == "DELETE":
                        return {}
                    if response.content:
                        return response.json()
                    return {}
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                error_log = f"1C OData Error [{attempt + 1}/{max_retries}]: {e}"
                if hasattr(e, 'response') and e.response:
                    # Показываем полный ответ для отладки (первые 2000 символов)
                    response_text = e.response.text
                    error_log += f"\n  Response body: {response_text[:2000]}"
                    # Если это 400 ошибка, логируем также request body для отладки
                    if e.response.status_code == 400 and data:
                        import json
                        error_log += f"\n  Request body that caused error: {json.dumps(data, ensure_ascii=False, indent=2)}"
                    # Пытаемся распарсить JSON если возможно
                    try:
                        if response_text:
                            import json
                            response_json = e.response.json()
                            error_log += f"\n  Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)[:1000]}"
                    except:
                        pass
                logger.error(error_log)
                if attempt < max_retries - 1:
                    wait_time = min(2 ** attempt, 60)
                    logger.warning(f"1C OData retry {attempt + 1}/{max_retries} after {wait_time}s")
                    await asyncio.sleep(wait_time)
                    attempt += 1
                    continue
                raise
    
    def _map_status_to_vid_obrascheniya(self, status: str) -> str:
        """Маппинг нашего статуса в ВидОбращения ЦЛ"""
        status_map = {
            "closed": "КонсультацияИТС",
            "pending": "ВОчередьНаКонсультацию",
            "other": "Другое",
            "new": "ВОчередьНаКонсультацию"
        }
        return status_map.get(status, "ВОчередьНаКонсультацию")
    
    def _map_chatwoot_status_to_vid_obrascheniya(self, chatwoot_status: str) -> str:
        """Маппинг статуса Chatwoot в ВидОбращения ЦЛ"""
        # Chatwoot статусы: open, resolved, pending, snoozed
        status_map = {
            "open": "ВОчередьНаКонсультацию",
            "resolved": "КонсультацияИТС",
            "pending": "ВОчередьНаКонсультацию",
            "snoozed": "ВОчередьНаКонсультацию",
            "closed": "КонсультацияИТС",
        }
        return status_map.get(chatwoot_status.lower(), "ВОчередьНаКонсультацию")
    
    async def create_consultation_odata(
        self,
        client_key: Optional[str] = None,
        manager_key: Optional[str] = None,
        author_key: Optional[str] = None,
        description: str = "",
        topic: Optional[str] = None,
        scheduled_at: Optional[datetime] = None,
        question_category_key: Optional[str] = None,
        question_key: Optional[str] = None,
        language_code: Optional[str] = None,
        contact_method: Optional[str] = None,
        contact_hint: Optional[str] = None,
        client_display_name: Optional[str] = None,
        importance: Optional[int] = None,
        comment: Optional[str] = None,
        db_session: Optional[Any] = None  # AsyncSession для поиска автора по имени
    ) -> Dict[str, Any]:
        """
        Создание новой консультации в 1C:ЦЛ через OData.
        
        ВАЖНО: При создании (статус "ВОчередьНаКонсультацию") НЕ отправляем КонсультацииИТС.
        КонсультацииИТС заполняется операторами после завершения консультации.
        
        Args:
            client_key: Абонент_Key (UUID клиента в ЦЛ)
            manager_key: Менеджер_Key
            author_key: Автор_Key
            description: Описание/Вопрос
            topic: Тема
            scheduled_at: ДатаКонсультации
            question_category_key: КатегорияВопроса_Key
            question_key: ВопросНаКонсультацию_Key (может быть пустым UUID)
            language_code: Код языка (ru/uz) - маппится в Язык_Key
            contact_method: Способ связи (маппится из source)
            contact_hint: АбонентКакСвязаться (строка вида "Телефон / ФИО / Способ")
            client_display_name: АбонентПредставление (название клиента)
            importance: Важность (маппится в строку)
            comment: Комментарий
        
        Returns:
            Dict с данными созданной консультации (Ref_Key, Number и т.д.)
        """
        # ВАЖНО: Используем UTC+5 для дат в ЦЛ (как указано в требованиях)
        from datetime import timezone, timedelta
        utc_plus_5 = timezone(timedelta(hours=5))
        now = datetime.now(utc_plus_5)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
        
        # Используем scheduled_at для Date, если есть, иначе текущее время
        date_str = scheduled_at.strftime("%Y-%m-%dT%H:%M:%S") if scheduled_at else now_str
        
        payload = {
            "Date": date_str,
            "Posted": False,
            "Описание": description,
            "ВидОбращения": "ВОчередьНаКонсультацию",  # При создании всегда очередь
            "Входящий": True,
            "ДатаСоздания": now_str,  # Используем текущее время создания
        }
        
        if client_key:
            payload["Абонент_Key"] = client_key
        
        # ВАЖНО: Автор_Key должен быть сервисным пользователем из справочника users
        # Ищем пользователя по названию (description) из переменной окружения ONEC_DEFAULT_AUTHOR_NAME
        # Если не найден, используем пустой UUID как fallback
        effective_author_key = author_key
        if not effective_author_key and db_session:
            try:
                from sqlalchemy import select
                from ..models import User
                author_name = settings.ONEC_DEFAULT_AUTHOR_NAME
                if author_name:
                    result = await db_session.execute(
                        select(User.cl_ref_key)
                        .where(User.description == author_name)
                        .where(User.deletion_mark == False)
                        .limit(1)
                    )
                    found_author_key = result.scalar_one_or_none()
                    if found_author_key:
                        effective_author_key = found_author_key
                        logger.debug(f"Found author '{author_name}' with cl_ref_key: {found_author_key}")
                    else:
                        logger.warning(f"Author '{author_name}' not found in users table, using empty UUID")
            except Exception as e:
                logger.warning(f"Failed to find author by name: {e}")
        
        # Если автор не найден, используем пустой UUID
        payload["Автор_Key"] = effective_author_key or "00000000-0000-0000-0000-000000000000"
        if manager_key:
            payload["Менеджер_Key"] = manager_key
            payload["Ответственный_Key"] = manager_key  # Всегда тот же менеджер
        if topic:
            payload["Тема"] = topic
        if scheduled_at:
            payload["ДатаКонсультации"] = scheduled_at.strftime("%Y-%m-%dT%H:%M:%S")
        
        # Поля для статуса "ВОчередьНаКонсультацию" (вместо КонсультацииИТС)
        language_key = get_language_key(language_code)
        if language_key:
            payload["Язык_Key"] = language_key
        
        # КатегорияВопроса_Key - всегда передаем, даже если пустая
        if question_category_key and question_category_key != "00000000-0000-0000-0000-000000000000":
            payload["КатегорияВопроса_Key"] = question_category_key
        else:
            payload["КатегорияВопроса_Key"] = "00000000-0000-0000-0000-000000000000"
        
        # ВопросНаКонсультацию_Key - всегда передаем, даже если пустая
        if question_key and question_key != "00000000-0000-0000-0000-000000000000":
            payload["ВопросНаКонсультацию_Key"] = question_key
        else:
            payload["ВопросНаКонсультацию_Key"] = "00000000-0000-0000-0000-000000000000"
        
        if contact_method:
            payload["СпособСвязи"] = contact_method
        else:
            payload["СпособСвязи"] = "ПоТелефону"  # По умолчанию
        
        if contact_hint:
            payload["АбонентКакСвязаться"] = contact_hint
        
        if client_display_name:
            payload["АбонентПредставление"] = client_display_name
        
        if importance is not None:
            payload["Важность"] = map_importance_to_1c(importance)
        else:
            payload["Важность"] = "Обычная"
        
        # Формируем комментарий с информацией о выбранной категории и вопросе
        comment_parts = []
        
        # Добавляем пометку о источнике создания
        comment_parts.append("Создано из Clobus.uz")
        
        # Добавляем информацию о категории и вопросе, если они есть
        if question_category_key and question_category_key != "00000000-0000-0000-0000-000000000000":
            # Получаем название категории из БД, если доступна сессия
            category_name = None
            if db_session:
                try:
                    from sqlalchemy import select
                    from ..models import OnlineQuestionCat
                    result = await db_session.execute(
                        select(OnlineQuestionCat.description)
                        .where(OnlineQuestionCat.ref_key == question_category_key)
                        .limit(1)
                    )
                    category_name = result.scalar_one_or_none()
                except Exception as e:
                    logger.debug(f"Failed to get category name: {e}")
            
            if category_name:
                comment_parts.append(f"Категория вопроса: {category_name}")
        
        if question_key and question_key != "00000000-0000-0000-0000-000000000000":
            # Получаем название вопроса из БД, если доступна сессия
            question_name = None
            if db_session:
                try:
                    from sqlalchemy import select
                    from ..models import OnlineQuestion
                    result = await db_session.execute(
                        select(OnlineQuestion.description)
                        .where(OnlineQuestion.ref_key == question_key)
                        .limit(1)
                    )
                    question_name = result.scalar_one_or_none()
                except Exception as e:
                    logger.debug(f"Failed to get question name: {e}")
            
            if question_name:
                comment_parts.append(f"Вопрос: {question_name}")
        
        # Добавляем пользовательский комментарий, если есть
        if comment:
            comment_parts.append(comment)
        
        # Объединяем все части комментария
        final_comment = "\n".join(comment_parts) if comment_parts else ""
        payload["Комментарий"] = final_comment
        
        # ВАЖНО: Обязательные поля КонсультацииИТС и ВопросыИОтветы должны быть пустыми массивами
        # Они заполняются операторами после завершения консультации
        payload["КонсультацииИТС"] = []
        payload["ВопросыИОтветы"] = []
        
        # НЕ отправляем Ref_Key и Number - они создаются автоматически
        
        # ВАЖНО: Абонент_Key обязателен для создания консультации
        if not client_key:
            raise ValueError("client_key (Абонент_Key) is required for creating consultation in 1C")
        
        # Логируем финальный payload перед отправкой
        import json
        logger.info(f"1C create_consultation_odata payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        endpoint = f"/{self.entity}"
        try:
            return await self._odata_request("POST", endpoint, data=payload)
        except httpx.HTTPStatusError as e:
            # Проверяем, не является ли ошибка 500 результатом превышения лимита консультаций
            # В 1C:ЦЛ есть лимит: максимум 3 документа ТелефонныйЗвонок на один день
            # (по полю ДатаКонсультации). При превышении лимита возвращается ошибка 500.
            if e.response.status_code == 500:
                # Проверяем, есть ли ДатаКонсультации в payload
                consultation_date = payload.get("ДатаКонсультации") or payload.get("Date")
                if consultation_date:
                    # Если это POST запрос на создание консультации и ошибка 500,
                    # скорее всего это превышение лимита
                    logger.error(
                        f"1C returned 500 error when creating consultation with ДатаКонсультации={consultation_date}. "
                        f"This might be due to consultation limit exceeded (max 3 per day)."
                    )
                    raise ConsultationLimitExceeded(
                        f"Превышен лимит создания консультаций в 1C:ЦЛ. "
                        f"Максимум 3 консультации на один день (по дате консультации: {consultation_date}). "
                        f"Попробуйте выбрать другую дату."
                    ) from e
            # Для других ошибок пробрасываем исключение как есть
            raise
    
    async def update_consultation_odata(
        self,
        ref_key: str,
        number: Optional[str] = None,
        status: Optional[str] = None,
        manager_key: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        description: Optional[str] = None,
        consultations_its: Optional[List[Dict[str, Any]]] = None,
        is_chatwoot_status: bool = False,
        check_changes: bool = True
    ) -> Dict[str, Any]:
        """
        Обновление консультации в 1C:ЦЛ через OData (PATCH).
        
        Args:
            ref_key: Ref_Key документа (обязательно для поиска)
            number: Number документа (опционально, для проверки)
            status: Статус (маппится в ВидОбращения)
            manager_key: Менеджер_Key
            start_date: ДатаКонсультации
            end_date: Конец
            description: Описание
            consultations_its: Обновление КонсультацииИТС
            is_chatwoot_status: Если True, использует маппинг Chatwoot статусов
            check_changes: Если True, проверяет текущие значения в ЦЛ перед обновлением
        
        Returns:
            Обновленный документ
        """
        # Формируем endpoint для PATCH
        # OData: Document_ТелефонныйЗвонок(guid'ref_key')
        endpoint = f"/{self.entity}(guid'{ref_key}')"
        
        # Если включена проверка изменений, получаем текущие данные из ЦЛ
        current_data = None
        if check_changes:
            try:
                current_data = await self.get_consultation_odata(ref_key)
            except Exception as e:
                logger.warning(f"Failed to get current data for {ref_key}, proceeding without check: {e}")
        
        payload = {}
        
        # Проверяем изменения для каждого поля
        if status:
            new_vid_obrascheniya = self._map_chatwoot_status_to_vid_obrascheniya(status) if is_chatwoot_status else self._map_status_to_vid_obrascheniya(status)
            if not current_data or current_data.get("ВидОбращения") != new_vid_obrascheniya:
                payload["ВидОбращения"] = new_vid_obrascheniya
        
        if manager_key:
            current_manager = current_data.get("Менеджер_Key") if current_data else None
            if not current_data or current_manager != manager_key:
                payload["Менеджер_Key"] = manager_key
        
        if start_date:
            start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            current_start = current_data.get("ДатаКонсультации") if current_data else None
            if not current_data or current_start != start_date_str:
                payload["ДатаКонсультации"] = start_date_str
        
        if end_date:
            end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            current_end = current_data.get("Конец") if current_data else None
            if not current_data or current_end != end_date_str:
                payload["Конец"] = end_date_str
        
        if description is not None:
            current_desc = current_data.get("Описание") if current_data else None
            if not current_data or current_desc != description:
                payload["Описание"] = description
        
        if consultations_its is not None:
            # Для КонсультацииИТС всегда обновляем, так как сравнение сложное
            payload["КонсультацииИТС"] = consultations_its
        
        # Если нет изменений - возвращаем текущие данные без запроса
        if not payload:
            logger.debug(f"No changes detected for consultation {ref_key}, skipping update")
            return current_data or {}
        
        # Используем PATCH для частичного обновления
        response = await self._odata_request("PATCH", endpoint, data=payload)
        
        # Логируем успешное обновление
        logger.info(f"Updated 1C consultation: Ref_Key={ref_key}, updated fields: {list(payload.keys())}")
        
        return response
    
    async def get_consultation_odata(self, ref_key: str) -> Dict[str, Any]:
        """Получение консультации из 1C:ЦЛ через OData"""
        endpoint = f"/{self.entity}(guid'{ref_key}')"
        return await self._odata_request("GET", endpoint)
    
    async def delete_consultation_odata(self, ref_key: str) -> None:
        """
        Удаление консультации (документа ТелефонныйЗвонок) в 1C:ЦЛ через OData.
        
        ВАЖНО: Удаление документа освобождает лимит на создание консультаций.
        Лимит: максимум 3 документа ТелефонныйЗвонок на один день (по полю ДатаКонсультации).
        
        Args:
            ref_key: Ref_Key документа для удаления
        """
        endpoint = f"/{self.entity}(guid'{ref_key}')"
        await self._odata_request("DELETE", endpoint)
        logger.info(f"Deleted 1C consultation: Ref_Key={ref_key}")

    async def mark_consultation_deleted(self, ref_key: str) -> Dict[str, Any]:
        """
        Пометить консультацию на удаление (DeletionMark = true) вместо физического удаления.
        """
        endpoint = f"/{self.entity}(guid'{ref_key}')"
        payload = {"DeletionMark": True}
        response = await self._odata_request("PATCH", endpoint, data=payload)
        logger.info(f"Marked 1C consultation as deleted (DeletionMark=true): Ref_Key={ref_key}")
        return response

    async def get_client_by_ref_key(self, ref_key: str) -> Optional[Dict[str, Any]]:
        """
        Получить клиента (Catalog_Контрагенты) по Ref_Key напрямую.
        
        Args:
            ref_key: Ref_Key клиента в 1C:ЦЛ
        
        Returns:
            Dict с данными клиента, включая Parent_Key, или None если не найден
        """
        if not ref_key:
            return None
        
        endpoint = f"/{self.clients_entity}(guid'{ref_key}')"
        params = {
            "$format": "json",
            "$select": "Ref_Key,Description,ИНН,КодАбонентаClobus,Parent_Key",
        }
        try:
            response = await self._odata_request("GET", endpoint, params=params)
            return response if isinstance(response, dict) else None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Клиент не найден
                return None
            raise

    async def find_client_by_inn(self, org_inn: Optional[str]) -> Optional[Dict[str, Any]]:
        """Поиск клиента (Catalog_Контрагенты) по ИНН."""
        if not org_inn:
            return None
        filter_value = org_inn.replace("'", "''")
        params = {
            "$format": "json",
            "$top": "1",
            "$select": "Ref_Key,Description,ИНН,КодАбонентаClobus,Parent_Key",
            "$filter": f"ИНН eq '{filter_value}'",
        }
        response = await self._odata_request("GET", f"/{self.clients_entity}", params=params)
        rows = response.get("value", []) if isinstance(response, dict) else []
        return rows[0] if rows else None

    async def find_client_by_code_and_inn(
        self, 
        code_abonent: Optional[str], 
        org_inn: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Поиск клиента (Catalog_Контрагенты) по коду абонента и ИНН.
        
        Возвращает клиента, если найден по обоим параметрам.
        Если code_abonent не указан, ищет только по ИНН.
        
        Returns:
            Dict с данными клиента, включая Parent_Key, или None если не найден
        """
        if not org_inn:
            return None
        
        filter_value_inn = org_inn.replace("'", "''")
        
        # Формируем фильтр
        if code_abonent:
            filter_value_code = code_abonent.replace("'", "''")
            filter_str = f"ИНН eq '{filter_value_inn}' and КодАбонентаClobus eq '{filter_value_code}'"
        else:
            filter_str = f"ИНН eq '{filter_value_inn}'"
        
        params = {
            "$format": "json",
            "$top": "1",
            "$select": "Ref_Key,Description,ИНН,КодАбонентаClobus,Parent_Key",
            "$filter": filter_str,
        }
        response = await self._odata_request("GET", f"/{self.clients_entity}", params=params)
        rows = response.get("value", []) if isinstance(response, dict) else []
        return rows[0] if rows else None

    async def create_client_odata(
        self,
        name: str,
        org_inn: str,
        code_abonent: str,
        phone: Optional[str] = None,
        email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Создание клиента в Catalog_Контрагенты."""
        payload: Dict[str, Any] = {
            "Description": name,
            "ИНН": org_inn,
            "КодАбонентаClobus": code_abonent,
            "Parent_Key": "7ccd31ca-887b-11eb-938b-00e04cd03b68",  # Родитель по умолчанию
        }
        
        # Определяем тип лица по ИНН:
        # 9 знаков → "ЮридическоеЛицо" (без пробела!)
        # 14 знаков → "ФизическоеЛицо" (без пробела!)
        inn_cleaned = org_inn.replace(" ", "").replace("-", "")
        if len(inn_cleaned) == 9:
            payload["ЮридическоеФизическоеЛицо"] = "ЮридическоеЛицо"
        elif len(inn_cleaned) == 14:
            payload["ЮридическоеФизическоеЛицо"] = "ФизическоеЛицо"
        # Если ИНН не соответствует ни одному формату, не добавляем поле
        
        # Формируем массив КонтактнаяИнформация
        contact_info = []
        line_number = 1
        
        # Константы для Вид_Key из примера
        PHONE_TYPE_KEY = "c07763c7-2ae8-4708-a608-7691be9d782b"  # Телефон
        EMAIL_TYPE_KEY = "f0dddb0f-1cac-49ab-ad37-f9480b0ef654"  # АдресЭлектроннойПочты
        
        if phone:
            contact_info.append({
                "LineNumber": str(line_number),
                "Тип": "Телефон",
                "Вид_Key": PHONE_TYPE_KEY,
                "Представление": phone,
            })
            line_number += 1
        
        if email:
            contact_info.append({
                "LineNumber": str(line_number),
                "Тип": "АдресЭлектроннойПочты",
                "Вид_Key": EMAIL_TYPE_KEY,
                "Представление": email,
            })
        
        if contact_info:
            payload["КонтактнаяИнформация"] = contact_info

        # Добавляем ?$format=json через params (правильный способ для OData)
        endpoint = f"/{self.clients_entity}"
        params = {"$format": "json"}
        return await self._odata_request("POST", endpoint, data=payload, params=params)
    
    async def update_client_odata(
        self,
        ref_key: str,
        name: Optional[str] = None,
        org_inn: Optional[str] = None,
        code_abonent: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Обновление клиента в Catalog_Контрагенты через OData (PATCH).
        
        Args:
            ref_key: Ref_Key клиента в 1C:ЦЛ (обязательно)
            name: Наименование клиента
            org_inn: ИНН
            code_abonent: Код абонента
            phone: Телефон
            email: Email
        
        Returns:
            Обновленный клиент
        """
        endpoint = f"/{self.clients_entity}(guid'{ref_key}')"
        payload: Dict[str, Any] = {}
        
        if name:
            payload["Description"] = name
        if org_inn:
            payload["ИНН"] = org_inn
            # Обновляем тип лица по ИНН
            inn_cleaned = org_inn.replace(" ", "").replace("-", "")
            if len(inn_cleaned) == 9:
                payload["ЮридическоеФизическоеЛицо"] = "ЮридическоеЛицо"
            elif len(inn_cleaned) == 14:
                payload["ЮридическоеФизическоеЛицо"] = "ФизическоеЛицо"
        if code_abonent:
            payload["КодАбонентаClobus"] = code_abonent
        
        # Обновление контактной информации (если указаны phone или email)
        if phone or email:
            contact_info = []
            line_number = 1
            
            # Константы для Вид_Key
            PHONE_TYPE_KEY = "c07763c7-2ae8-4708-a608-7691be9d782b"  # Телефон
            EMAIL_TYPE_KEY = "f0dddb0f-1cac-49ab-ad37-f9480b0ef654"  # АдресЭлектроннойПочты
            
            if phone:
                contact_info.append({
                    "LineNumber": str(line_number),
                    "Тип": "Телефон",
                    "Вид_Key": PHONE_TYPE_KEY,
                    "Представление": phone,
                })
                line_number += 1
            
            if email:
                contact_info.append({
                    "LineNumber": str(line_number),
                    "Тип": "АдресЭлектроннойПочты",
                    "Вид_Key": EMAIL_TYPE_KEY,
                    "Представление": email,
                })
            
            if contact_info:
                payload["КонтактнаяИнформация"] = contact_info
        
        if not payload:
            logger.warning(f"No fields to update for client {ref_key}")
            return {}
        
        params = {"$format": "json"}
        response = await self._odata_request("PATCH", endpoint, data=payload, params=params)
        logger.info(f"Updated 1C client: Ref_Key={ref_key}, updated fields: {list(payload.keys())}")
        return response
    
    async def create_rating_odata(
        self,
        cons_key: str,
        client_key: str,
        manager_key: str,
        question_number: int,
        rating: int,
        question_text: Optional[str] = None,
        comment: Optional[str] = None,
        period: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Создание записи оценки консультации в InformationRegister_ОценкаКонсультацийПоЗаявкам.
        
        Args:
            cons_key: Ref_Key консультации (Обращение_Key)
            client_key: Ref_Key клиента (Контрагент_Key)
            manager_key: Ref_Key менеджера (Менеджер_Key)
            question_number: Номер вопроса
            rating: Оценка (число)
            question_text: Текст вопроса
            comment: Комментарий
            period: Дата/время оценки (по умолчанию текущее время)
        
        Returns:
            Dict с данными созданной записи
        """
        if not period:
            period = datetime.now()
        
        # Валидация: manager_key должен быть валидным GUID
        if not manager_key or manager_key == "FRONT" or len(manager_key) != 36 or manager_key.count("-") != 4:
            raise ValueError(f"Invalid manager_key format: '{manager_key}'. Must be a valid GUID.")
        
        # Форматируем дату в формате ISO 8601 для 1C
        period_str = period.strftime("%Y-%m-%dT%H:%M:%S")
        
        payload = {
            "Обращение_Key": cons_key,
            "Контрагент_Key": client_key,
            "Менеджер_Key": manager_key,
            "НомерВопроса": question_number,
            "Оценка": rating,
            "Period": period_str,
            "ДатаОценки": period_str,  # ДатаОценки устанавливаем в текущую дату
        }
        
        if question_text:
            payload["Вопрос"] = question_text
        if comment:
            payload["Комментарий"] = comment
        
        endpoint = "/InformationRegister_ОценкаКонсультацийПоЗаявкам"
        return await self._odata_request("POST", endpoint, data=payload)
    
    async def create_redate_odata(
        self,
        cons_key: str,
        client_key: str,
        manager_key: str,
        old_date: Optional[datetime],
        new_date: datetime,
        comment: Optional[str] = None,
        period: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Создание записи переноса консультации в InformationRegister_РегистрацияПереносаКонсультации.
        
        Args:
            cons_key: Ref_Key консультации (ДокументОбращения_Key)
            client_key: Ref_Key клиента (Абонент_Key)
            manager_key: Ref_Key менеджера (Менеджер_Key)
            old_date: Старая дата консультации
            new_date: Новая дата консультации
            comment: Комментарий к переносу
            period: Дата/время переноса (по умолчанию текущее время)
        
        Returns:
            Dict с данными созданной записи
        """
        if not period:
            period = datetime.now()
        
        # Валидация: manager_key должен быть валидным GUID
        if not manager_key or manager_key == "FRONT" or len(manager_key) != 36 or manager_key.count("-") != 4:
            raise ValueError(f"Invalid manager_key format: '{manager_key}'. Must be a valid GUID.")
        
        # Форматируем даты в формате ISO 8601 для 1C
        new_date_str = new_date.strftime("%Y-%m-%dT%H:%M:%S")
        period_str = period.strftime("%Y-%m-%dT%H:%M:%S")
        
        payload = {
            "ДокументОбращения_Key": cons_key,
            "Абонент_Key": client_key,
            "Менеджер_Key": manager_key,
            "НоваяДата": new_date_str,
            "Period": period_str,
        }
        
        if old_date:
            payload["СтараяДата"] = old_date.strftime("%Y-%m-%dT%H:%M:%S")
        if comment:
            payload["Комментарий"] = comment
        
        endpoint = "/InformationRegister_РегистрацияПереносаКонсультации"
        return await self._odata_request("POST", endpoint, data=payload)
    
    # Старые методы для обратной совместимости (если используется REST API)
    async def create_consultation(
        self,
        client_ref_key: Optional[str] = None,
        org_inn: Optional[str] = None,
        description: str = "",
        scheduled_at: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Создание новой консультации (REST API, устаревший метод).
        Используйте create_consultation_odata для OData.
        """
        # Если есть REST API URL, используем его
        if hasattr(settings, 'ONEC_API_URL') and settings.ONEC_API_URL:
            url = f"{settings.ONEC_API_URL.rstrip('/')}/api/consultations"
            headers = {
                "Authorization": f"Bearer {settings.ONEC_API_TOKEN}",
                "Content-Type": "application/json"
            }
            payload = {"description": description}
            if client_ref_key:
                payload["client_ref_key"] = client_ref_key
            if org_inn:
                payload["org_inn"] = org_inn
            if scheduled_at:
                payload["scheduled_at"] = scheduled_at.isoformat()
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                return response.json()
        else:
            # Fallback на OData
            return await self.create_consultation_odata(
                client_key=client_ref_key,
                description=description,
                scheduled_at=scheduled_at
            )
    
    async def update_consultation(
        self,
        cl_ref_key: str,
        status: Optional[str] = None,
        manager: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Обновление консультации (REST API, устаревший метод)"""
        return await self.update_consultation_odata(
            ref_key=cl_ref_key,
            status=status,
            manager_key=manager,
            start_date=start_date,
            end_date=end_date
        )
    
    async def get_consultation(self, cl_ref_key: str) -> Dict[str, Any]:
        """Получение консультации (REST API, устаревший метод)"""
        return await self.get_consultation_odata(cl_ref_key)
    
    async def close_consultation(
        self,
        cl_ref_key: str,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Закрытие консультации"""
        return await self.update_consultation_odata(
            ref_key=cl_ref_key,
            status="closed",
            end_date=end_date
        )
