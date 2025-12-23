"""
Сервис для автоматического выбора менеджеров для консультаций.

Логика выбора:
1. По навыкам (users_skill) - если консультация по определенному разделу программы
2. По загрузке - выбираем менеджера с меньшей очередью
3. По лимитам (con_limit) - только менеджеры с установленными лимитами
4. По времени работы (start_hour, end_hour) - только работающие в текущее время
5. Универсальные менеджеры (знают все разделы) - распределяются по очереди
"""
import logging
from datetime import datetime, time, timezone, timedelta
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy import select, func, and_, or_, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..models import (
    User, UserSkill, Consultation, QAndA, UserMapping, QueueClosing, OnlineQuestionCat
)

logger = logging.getLogger(__name__)


class ManagerSelector:
    """Сервис для выбора менеджеров"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_available_managers(
        self,
        current_time: Optional[datetime] = None,
        po_section_key: Optional[str] = None,
        po_type_key: Optional[str] = None,
        category_key: Optional[str] = None,
        consultation_type: Optional[str] = None,
        filter_by_working_hours: bool = True,
        language: Optional[str] = None,
    ) -> List[User]:
        """
        Получить список доступных менеджеров.
        
        Args:
            current_time: Текущее время (по умолчанию now())
            po_section_key: Ключ раздела ПО из консультации
            po_type_key: Ключ типа ПО из консультации
            category_key: Ключ категории вопроса (из online_question_cat)
            consultation_type: Тип консультации ("Консультация по ведению учёта" или "Техническая поддержка")
            filter_by_working_hours: Фильтровать по рабочему времени
            language: Язык консультации ("ru" или "uz")
        
        Returns:
            Список доступных менеджеров, отсортированный по приоритету
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # Базовый запрос: активные менеджеры с лимитами и разрешением на консультации
        query = select(User).where(
            User.deletion_mark == False,
            User.invalid == False,
            User.consultation_enabled == True,  # Только менеджеры с разрешением на консультации
            User.con_limit.isnot(None),
            User.con_limit > 0,
        )
        
        # ВАЖНО: Для "Консультация по ведению учёта" применяем дополнительные фильтры:
        # - department = "ИТС консультанты"
        # - start_hour и end_hour обязательны (должно быть установлено рабочее время)
        if consultation_type == "Консультация по ведению учёта":
            query = query.where(
                User.department == "ИТС консультанты",
                User.start_hour.isnot(None),  # Обязательно должно быть установлено рабочее время начала
                User.end_hour.isnot(None),    # Обязательно должно быть установлено рабочее время окончания
            )
        
        # Фильтр по времени работы (только если требуется)
        if filter_by_working_hours:
            current_time_only = current_time.time()
            
            # ВАЖНО: Для "Консультация по ведению учёта" start_hour и end_hour уже проверены выше (is not null)
            # Поэтому для них проверяем только соответствие текущего времени рабочему времени
            if consultation_type == "Консультация по ведению учёта":
                # Для консультаций по ведению учета проверяем только рабочее время
                # (start_hour и end_hour уже гарантированно не null)
                query = query.where(
                    or_(
                        # Текущее время в пределах рабочего времени (обычный случай: start_hour < end_hour)
                        and_(
                            User.start_hour <= User.end_hour,  # Обычный рабочий день
                            User.start_hour <= current_time_only,
                            User.end_hour >= current_time_only,
                        ),
                        # Рабочее время переходит через полночь (start_hour > end_hour)
                        and_(
                            User.start_hour > User.end_hour,  # Работа через полночь
                            or_(
                                current_time_only >= User.start_hour,  # После начала работы (вечер)
                                current_time_only <= User.end_hour,    # До окончания работы (утро)
                            )
                        )
                    )
                )
            else:
                # Для других типов консультаций: если время не установлено, считаем что менеджер работает всегда
                query = query.where(
                    or_(
                        # Менеджер работает всегда (start_hour и end_hour не установлены)
                        and_(
                            User.start_hour.is_(None),
                            User.end_hour.is_(None)
                        ),
                        # Или текущее время в пределах рабочего времени
                        and_(
                            User.start_hour.isnot(None),
                            User.end_hour.isnot(None),
                            User.start_hour <= User.end_hour,  # Обычный рабочий день
                            User.start_hour <= current_time_only,
                            User.end_hour >= current_time_only,
                        ),
                        # Или рабочее время переходит через полночь (start_hour > end_hour)
                        and_(
                            User.start_hour.isnot(None),
                            User.end_hour.isnot(None),
                            User.start_hour > User.end_hour,  # Работа через полночь
                            or_(
                                current_time_only >= User.start_hour,  # После начала работы (вечер)
                                current_time_only <= User.end_hour,   # До окончания работы (утро)
                            )
                        )
                    )
                )
        # Если filter_by_working_hours=False, не применяем фильтр по времени работы
        
        managers = await self.db.execute(query)
        all_managers = managers.scalars().all()
        
        if not all_managers:
            logger.warning(
                f"No available managers found (with limits and working hours). "
                f"consultation_type={consultation_type}, current_time={current_time}. "
                f"Filters: deletion_mark=False, invalid=False, consultation_enabled=True, "
                f"con_limit > 0"
                + (f", department='ИТС консультанты', start_hour IS NOT NULL, end_hour IS NOT NULL" 
                   if consultation_type == "Консультация по ведению учёта" else "")
            )
            return []
        
        logger.debug(
            f"Found {len(all_managers)} managers after initial filtering "
            f"(consultation_type={consultation_type})"
        )
        
        # Фильтруем менеджеров с закрытой очередью на текущую дату
        # Проверяем закрытие очереди по дате (period) - если есть запись с period = текущая дата
        current_date = current_time.date()
        available_managers = []
        
        for manager in all_managers:
            if not manager.cl_ref_key:
                continue
            
            # Проверяем, закрыта ли очередь для этого менеджера на текущую дату
            # Period в QueueClosing - это дата закрытия очереди
            # Используем date_trunc для сравнения только по дате (без времени)
            # Сравниваем дату закрытия очереди с текущей датой
            queue_closing_query = select(QueueClosing).where(
                QueueClosing.manager_key == manager.cl_ref_key,
                func.date_trunc('day', QueueClosing.period) == func.date_trunc('day', current_time)
            ).limit(1)
            
            queue_closing_result = await self.db.execute(queue_closing_query)
            queue_closed = queue_closing_result.scalar_one_or_none() is not None
            
            if queue_closed:
                logger.debug(f"Manager {manager.cl_ref_key} has closed queue on {current_date}, excluding from available")
                continue
            
            available_managers.append(manager)
        
        if not available_managers:
            logger.warning("No available managers found after filtering closed queues")
            return []
        
        # Фильтруем по навыкам, если указан раздел программы или категория
        if po_section_key or category_key:
            skilled_managers = []
            universal_managers = []
            
            # Для "Консультация по ведению учёта" получаем информацию о категории вопроса
            category_language = None
            if consultation_type == "Консультация по ведению учёта" and category_key:
                category_query = select(OnlineQuestionCat.language).where(
                    OnlineQuestionCat.ref_key == category_key
                )
                category_result = await self.db.execute(category_query)
                category_language = category_result.scalar_one_or_none()
                logger.debug(
                    f"Category {category_key} has language: {category_language}, "
                    f"consultation language: {language}"
                )
            
            for manager in available_managers:
                # Проверяем навыки менеджера
                skills_query = select(UserSkill).where(
                    UserSkill.user_key == manager.cl_ref_key
                )
                skills_result = await self.db.execute(skills_query)
                manager_skills = skills_result.scalars().all()
                
                # Получаем список category_key, которые знает менеджер
                manager_category_keys = {skill.category_key for skill in manager_skills}
                
                # Если у менеджера нет навыков, считаем его универсальным
                # (знает все разделы) - но только если это не "Консультация по ведению учёта"
                if not manager_category_keys:
                    if consultation_type == "Консультация по ведению учёта":
                        # Для консультаций по ведению учета требуются точные навыки
                        continue
                    universal_managers.append(manager)
                    continue
                
                # Для "Консультация по ведению учёта" применяем строгую проверку:
                # 1. Точное совпадение category_key
                # 2. Соответствие языка менеджера языку категории вопроса
                if consultation_type == "Консультация по ведению учёта" and category_key:
                    # Проверяем точное совпадение категории
                    if category_key not in manager_category_keys:
                        continue
                    
                    # Проверяем соответствие языка
                    # Если указан язык консультации, проверяем его
                    if language:
                        # Проверяем, что менеджер знает нужный язык
                        if language.lower() == "ru" and not manager.ru:
                            logger.debug(
                                f"Manager {manager.cl_ref_key} doesn't know Russian, skipping"
                            )
                            continue
                        if language.lower() == "uz" and not manager.uz:
                            logger.debug(
                                f"Manager {manager.cl_ref_key} doesn't know Uzbek, skipping"
                            )
                            continue
                    
                    # Если есть информация о языке категории, проверяем соответствие
                    if category_language:
                        # Проверяем, что менеджер знает язык категории
                        if category_language.lower() == "ru" and not manager.ru:
                            logger.debug(
                                f"Manager {manager.cl_ref_key} doesn't know Russian "
                                f"(required by category language), skipping"
                            )
                            continue
                        if category_language.lower() == "uz" and not manager.uz:
                            logger.debug(
                                f"Manager {manager.cl_ref_key} doesn't know Uzbek "
                                f"(required by category language), skipping"
                            )
                            continue
                    
                    # Все проверки пройдены - менеджер подходит
                    skilled_managers.append(manager)
                else:
                    # Для других типов консультаций используем старую логику
                    # Проверяем, знает ли менеджер нужную категорию
                    # category_key из users_skill соответствует КатегорияВопроса_Key
                    # Если указан category_key, проверяем его
                    if category_key and category_key in manager_category_keys:
                        skilled_managers.append(manager)
                    # Если category_key не указан, но указан po_section_key,
                    # то пока считаем менеджера подходящим (в будущем можно добавить
                    # прямую связь между po_section_key и category_key)
                    elif not category_key and po_section_key:
                        # Пока считаем всех менеджеров с навыками подходящими
                        # TODO: добавить маппинг po_section_key -> category_key если нужно
                        skilled_managers.append(manager)
                    # Если указан только po_section_key без category_key,
                    # и у менеджера есть навыки, считаем его подходящим
                    elif po_section_key and not category_key:
                        skilled_managers.append(manager)
            
            # Возвращаем сначала менеджеров с навыками, потом универсальных
            return skilled_managers + universal_managers
        
        # Если раздел не указан, возвращаем всех доступных менеджеров (без закрытой очереди)
        return available_managers
    
    async def get_manager_queue_count(
        self,
        manager_key: str,
        current_time: Optional[datetime] = None,
    ) -> int:
        """
        Получить количество консультаций в очереди у менеджера.
        
        ВАЖНО: Считает ВСЕ заявки менеджера, включая созданные вручную в ЦЛ,
        а не только созданные через бэкенд. Это необходимо для корректного
        расчета очереди, так как у менеджера могут быть заявки из разных источников.
        
        Считаются только консультации со статусом "pending" или "open".
        
        Args:
            manager_key: cl_ref_key менеджера
            current_time: Текущее время (опционально)
        
        Returns:
            Количество консультаций в очереди
        """
        # ВАЖНО: Убрали фильтрацию по source - считаем все заявки менеджера
        # Это включает заявки созданные через бэкенд, вручную в ЦЛ, и через другие источники
        query = select(func.count(Consultation.cons_id)).where(
            Consultation.manager == manager_key,
            Consultation.status.in_(["pending", "open"]),
            Consultation.denied == False,
        )
        
        result = await self.db.execute(query)
        count = result.scalar() or 0
        
        return count
    
    async def get_manager_current_load(
        self,
        manager_key: str,
    ) -> Dict[str, Any]:
        """
        Получить текущую загрузку менеджера.
        
        Returns:
            Dict с информацией о загрузке:
            - queue_count: количество в очереди
            - limit: лимит менеджера
            - load_percent: процент загрузки (0-100)
            - available_slots: свободные слоты
        """
        # Получаем менеджера
        manager_query = select(User).where(User.cl_ref_key == manager_key)
        manager_result = await self.db.execute(manager_query)
        manager = manager_result.scalar_one_or_none()
        
        if not manager:
            return {
                "queue_count": 0,
                "limit": 0,
                "load_percent": 0,
                "available_slots": 0,
            }
        
        queue_count = await self.get_manager_queue_count(manager_key)
        limit = manager.con_limit or 0
        
        if limit == 0:
            load_percent = 0
            available_slots = 0
        else:
            load_percent = min(100, (queue_count / limit) * 100)
            available_slots = max(0, limit - queue_count)
        
        return {
            "queue_count": queue_count,
            "limit": limit,
            "load_percent": round(load_percent, 2),
            "available_slots": available_slots,
        }
    
    async def select_manager_for_consultation(
        self,
        consultation: Optional[Consultation] = None,
        po_section_key: Optional[str] = None,
        po_type_key: Optional[str] = None,
        category_key: Optional[str] = None,
        current_time: Optional[datetime] = None,
        consultation_type: Optional[str] = None,
        language: Optional[str] = None,
    ) -> Optional[str]:
        """
        Выбрать менеджера для консультации.
        
        Алгоритм:
        1. Получаем доступных менеджеров (по навыкам, лимитам, времени работы)
        2. Считаем очередь для каждого менеджера
        3. Выбираем менеджера с наименьшей загрузкой (очередью)
        
        Args:
            consultation: Консультация для назначения
            po_section_key: Ключ раздела ПО
            po_type_key: Ключ типа ПО
            category_key: Ключ категории вопроса
            current_time: Текущее время
            consultation_type: Тип консультации ("Консультация по ведению учёта" или "Техническая поддержка")
            language: Язык консультации ("ru" или "uz")
        
        Returns:
            cl_ref_key выбранного менеджера или None
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # Получаем доступных менеджеров
        available_managers = await self.get_available_managers(
            current_time=current_time,
            po_section_key=po_section_key,
            po_type_key=po_type_key,
            category_key=category_key,
            consultation_type=consultation_type,
            language=language,
        )
        
        if not available_managers:
            consultation_id = consultation.cons_id if consultation else "N/A (not created yet)"
            logger.warning(
                f"No available managers for consultation {consultation_id}. "
                f"consultation_type={consultation_type}, "
                f"po_section_key={po_section_key}, category_key={category_key}, "
                f"language={language}, current_time={current_time}. "
                f"Check: managers must have con_limit > 0, consultation_enabled=True, "
                f"and for 'Консультация по ведению учёта' also department='ИТС консультанты', "
                f"start_hour/end_hour must be set, and must have matching skills and language."
            )
            return None
        
        logger.info(
            f"Found {len(available_managers)} available managers for consultation_type={consultation_type}, "
            f"category_key={category_key}, language={language}"
        )
        
        # Считаем очередь для каждого менеджера
        manager_loads = []
        for manager in available_managers:
            if not manager.cl_ref_key:
                continue
            
            queue_count = await self.get_manager_queue_count(manager.cl_ref_key)
            limit = manager.con_limit or 0
            
            # Вычисляем приоритет: меньше очередь = выше приоритет
            # Если лимит не установлен, считаем приоритет = 0 (низкий)
            if limit == 0:
                priority = 999999  # Низкий приоритет
            else:
                # Приоритет = очередь / лимит (меньше = лучше)
                # Добавляем небольшой бонус за наличие лимита
                priority = queue_count / limit if limit > 0 else 999999
            
            manager_loads.append({
                "manager": manager,
                "queue_count": queue_count,
                "limit": limit,
                "priority": priority,
            })
        
        # Сортируем по приоритету (меньше = лучше)
        manager_loads.sort(key=lambda x: x["priority"])
        
        if not manager_loads:
            return None
        
        # Улучшенный алгоритм: выбираем из менеджеров с примерно одинаковой загрузкой
        # Это обеспечивает более равномерное распределение консультаций между менеджерами
        best_priority = manager_loads[0]["priority"]
        
        # Находим всех менеджеров с приоритетом близким к лучшему (разница < 0.1)
        # Это позволяет распределять консультации между несколькими менеджерами
        candidates = [
            m for m in manager_loads
            if abs(m["priority"] - best_priority) < 0.1
        ]
        
        # Если есть несколько кандидатов с одинаковой загрузкой, выбираем случайно
        # Это обеспечивает равномерное распределение
        import random
        if len(candidates) > 1:
            selected = random.choice(candidates)
            logger.info(
                f"Selected manager {selected['manager'].cl_ref_key} from {len(candidates)} candidates "
                f"with similar load for consultation {consultation.cons_id if consultation else 'N/A (not created yet)'}. "
                f"Queue: {selected['queue_count']}/{selected['limit']}, priority: {selected['priority']:.2f}"
            )
        else:
            selected = manager_loads[0]
            selected_manager = selected["manager"]
            logger.info(
                f"Selected manager {selected_manager.cl_ref_key} for consultation {consultation.cons_id if consultation else 'N/A (not created yet)'}. "
                f"Queue: {selected['queue_count']}/{selected['limit']}, priority: {selected['priority']:.2f}"
            )
        
        return selected["manager"].cl_ref_key
    
    async def get_all_managers_load(
        self,
        current_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Получить загрузку всех менеджеров.
        
        Returns:
            Список словарей с информацией о загрузке каждого менеджера
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # Получаем всех менеджеров с лимитами (без фильтрации по времени работы)
        # Это нужно для отображения информации о загрузке всех менеджеров
        available_managers = await self.get_available_managers(
            current_time=current_time,
            filter_by_working_hours=False  # Показываем всех менеджеров, независимо от времени работы
        )
        
        result = []
        for manager in available_managers:
            if not manager.cl_ref_key:
                continue
            
            load_info = await self.get_manager_current_load(manager.cl_ref_key)
            
            result.append({
                "manager_key": manager.cl_ref_key,
                "manager_id": str(manager.account_id),
                "chatwoot_user_id": manager.chatwoot_user_id,
                "name": manager.description or manager.user_id or "Unknown",
                "queue_count": load_info["queue_count"],
                "limit": load_info["limit"],
                "load_percent": load_info["load_percent"],
                "available_slots": load_info["available_slots"],
                "start_hour": manager.start_hour.isoformat() if manager.start_hour else None,
                "end_hour": manager.end_hour.isoformat() if manager.end_hour else None,
            })
        
        # Сортируем по загрузке (меньше загрузка = выше в списке)
        result.sort(key=lambda x: x["load_percent"])
        
        return result
    
    async def get_average_consultation_duration_minutes(
        self,
        manager_key: str,
        default_minutes: int = 15,
    ) -> int:
        """
        Рассчитать среднее время закрытия заявок для менеджера из статистики БД.
        
        Args:
            manager_key: cl_ref_key менеджера
            default_minutes: Значение по умолчанию если статистики нет (15 минут)
        
        Returns:
            Среднее время закрытия заявки в минутах (минимум 15 минут)
        """
        # Получаем статистику: среднее время между start_date и end_date для закрытых консультаций
        # Берем только консультации со статусом resolved или closed
        # И только те, где есть и start_date и end_date
        stats_query = select(
            func.avg(
                func.extract('epoch', Consultation.end_date - Consultation.start_date) / 60
            ).label('avg_duration_minutes')
        ).where(
            Consultation.manager == manager_key,
            Consultation.status.in_(["resolved", "closed"]),
            Consultation.start_date.isnot(None),
            Consultation.end_date.isnot(None),
            Consultation.denied == False,
            # Берем только консультации за последние 30 дней для актуальности статистики
            Consultation.end_date >= datetime.now(timezone.utc) - timedelta(days=30)
        )
        
        result = await self.db.execute(stats_query)
        avg_duration = result.scalar()
        
        if avg_duration is None or avg_duration <= 0:
            # Статистики нет или некорректная - используем дефолт
            logger.debug(f"No statistics for manager {manager_key}, using default {default_minutes} minutes")
            return default_minutes
        
        # Округляем до целого числа минут
        avg_minutes = int(round(avg_duration))
        
        # Применяем минимум 15 минут (даже если статистика показывает меньше)
        avg_minutes = max(avg_minutes, default_minutes)
        
        logger.debug(f"Average consultation duration for manager {manager_key}: {avg_minutes} minutes")
        return avg_minutes
    
    async def calculate_wait_time(
        self,
        manager_key: str,
        average_consultation_duration_minutes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Рассчитать примерное время ожидания для менеджера.
        
        Args:
            manager_key: cl_ref_key менеджера
            average_consultation_duration_minutes: Средняя длительность консультации в минутах (опционально, если не указано - вычисляется из статистики)
        
        Returns:
            Dict с информацией о времени ожидания:
            - queue_position: позиция в очереди
            - estimated_wait_minutes_min: минимальное время ожидания в минутах (статистика * очередь)
            - estimated_wait_minutes_max: максимальное время ожидания в минутах (15 минут * очередь)
            - estimated_wait_minutes: среднее время ожидания в минутах (для обратной совместимости)
            - estimated_wait_hours: примерное время ожидания в часах (округлено)
            - show_range: нужно ли показывать диапазон (True если статистика < 15 минут)
        """
        load_info = await self.get_manager_current_load(manager_key)
        queue_count = load_info["queue_count"]
        
        # Получаем реальную статистику (без применения минимума)
        stats_query = select(
            func.avg(
                func.extract('epoch', Consultation.end_date - Consultation.start_date) / 60
            ).label('avg_duration_minutes')
        ).where(
            Consultation.manager == manager_key,
            Consultation.status.in_(["resolved", "closed"]),
            Consultation.start_date.isnot(None),
            Consultation.end_date.isnot(None),
            Consultation.denied == False,
            Consultation.end_date >= datetime.now(timezone.utc) - timedelta(days=30)
        )
        
        result = await self.db.execute(stats_query)
        real_avg_duration = result.scalar()
        
        # Если статистика не указана явно, используем реальную статистику или дефолт
        if average_consultation_duration_minutes is None:
            if real_avg_duration is None or real_avg_duration <= 0:
                # Статистики нет - используем дефолт 15 минут
                stats_minutes = 15
                show_range = False
            else:
                stats_minutes = int(round(real_avg_duration))
                # Если статистика меньше 15 минут, показываем диапазон
                show_range = stats_minutes < 15
        else:
            stats_minutes = average_consultation_duration_minutes
            show_range = stats_minutes < 15
        
        # Минимальное время ожидания = очередь * статистика
        estimated_wait_minutes_min = queue_count * stats_minutes
        
        # Максимальное время ожидания = очередь * 15 минут (запас)
        estimated_wait_minutes_max = queue_count * 15
        
        # Среднее время для обратной совместимости
        estimated_wait_minutes = estimated_wait_minutes_max if show_range else estimated_wait_minutes_min
        
        # Округляем до часов
        estimated_wait_hours = round(estimated_wait_minutes / 60)
        if estimated_wait_hours == 0 and estimated_wait_minutes > 0:
            estimated_wait_hours = 1  # Минимум 1 час если есть очередь
        
        return {
            "queue_position": queue_count + 1,  # +1 потому что новая консультация будет следующей
            "estimated_wait_minutes_min": estimated_wait_minutes_min,
            "estimated_wait_minutes_max": estimated_wait_minutes_max,
            "estimated_wait_minutes": estimated_wait_minutes,
            "estimated_wait_hours": estimated_wait_hours,
            "show_range": show_range,
        }

