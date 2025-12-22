"""
Планировщик задач для ETL процессов.
Использует APScheduler для запуска периодических задач.
"""
import os
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# Блокировки для предотвращения параллельных запусков
running_tasks = set()

def get_etl_interval(env_var: str, default: int) -> int:
    """
    Получить интервал ETL из переменной окружения с обработкой пустых значений.
    
    Args:
        env_var: Имя переменной окружения
        default: Значение по умолчанию
    
    Returns:
        Интервал в минутах (int)
    """
    value = os.getenv(env_var)
    if not value or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid value for {env_var}: '{value}', using default {default}")
        return default

# Переменные окружения для частоты запуска ETL процессов (в минутах)
# Значения по умолчанию для обратной совместимости
ETL_CLIENTS_INTERVAL = get_etl_interval("ETL_CLIENTS_INTERVAL", 1)
ETL_CONS_INCREMENTAL_INTERVAL = get_etl_interval("ETL_CONS_INCREMENTAL_INTERVAL", 5)
ETL_CONS_OPEN_UPDATE_INTERVAL = get_etl_interval("ETL_CONS_OPEN_UPDATE_INTERVAL", 30)
ETL_CONS_REDATE_INTERVAL = get_etl_interval("ETL_CONS_REDATE_INTERVAL", 1)
ETL_CONS_RATES_INTERVAL = get_etl_interval("ETL_CONS_RATES_INTERVAL", 1)
ETL_CALLS_INTERVAL = get_etl_interval("ETL_CALLS_INTERVAL", 1)
ETL_QUEUE_CLOSING_INTERVAL = get_etl_interval("ETL_QUEUE_CLOSING_INTERVAL", 1)
ETL_USERS_INTERVAL = get_etl_interval("ETL_USERS_INTERVAL", 60)  # По умолчанию каждый час

# ВАЖНО: Если интервал = 0, ETL процесс отключается


async def run_etl_script(script_name: str):
    """Запуск ETL скрипта с защитой от параллельных запусков"""
    if script_name in running_tasks:
        logger.warning(f"Task {script_name} is already running, skipping...")
        print(f"⚠ Task {script_name} is already running, skipping...")
        return
    
    running_tasks.add(script_name)
    try:
        logger.info(f"Starting ETL task: {script_name}")
        print(f"🔄 Starting ETL task: {script_name}")
        
        # Запускаем скрипт как subprocess с перенаправлением вывода в реальном времени
        process = await asyncio.create_subprocess_exec(
            'python', '-m', f'FastAPI.catalog_scripts.{script_name}',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,  # Объединяем stderr с stdout
            cwd='/app'
        )
        
        # Читаем вывод в реальном времени и логируем
        output_lines = []
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line_str = line.decode('utf-8', errors='replace').strip()
            if line_str:
                output_lines.append(line_str)
                # Логируем каждую строку вывода ETL скрипта
                logger.info(f"[{script_name}] {line_str}")
                print(f"[{script_name}] {line_str}")
        
        await process.wait()
        
        if process.returncode == 0:
            logger.info(f"ETL task {script_name} completed successfully")
            print(f"✅ ETL task {script_name} completed successfully")
        else:
            logger.error(f"ETL task {script_name} failed with code {process.returncode}")
            print(f"❌ ETL task {script_name} failed with code {process.returncode}")
            # Выводим последние строки для диагностики
            if output_lines:
                last_lines = '\n'.join(output_lines[-20:])  # Последние 20 строк
                logger.error(f"Last output lines:\n{last_lines}")
    except Exception as e:
        logger.error(f"Error running ETL task {script_name}: {e}", exc_info=True)
        print(f"❌ Error running ETL task {script_name}: {e}")
    finally:
        running_tasks.discard(script_name)


async def run_clients_then_consultations():
    """Запуск загрузки клиентов, затем консультаций (после завершения клиентов)"""
    # Сначала запускаем загрузку клиентов и ждем её завершения
    logger.info("Starting clients sync, then consultations sync")
    print("🔄 Starting clients sync, then consultations sync")
    
    await run_etl_script('pull_clients_cl')
    
    # Только после завершения загрузки клиентов запускаем загрузку консультаций (инкремент)
    # await гарантирует, что pull_clients_cl уже завершился
    logger.info("Clients sync completed, starting consultations incremental sync")
    print("✅ Clients sync completed, starting consultations incremental sync")
    await run_etl_script('pull_cons_cl')


async def run_consultations_open_update():
    """Запуск обновления открытых консультаций по Ref_Key"""
    logger.info("Starting open consultations update")
    print("🔄 Starting open consultations update")
    # Передаем режим через переменную окружения
    import os
    old_mode = os.environ.get('ETL_CONS_MODE')
    os.environ['ETL_CONS_MODE'] = 'open_update'
    try:
        await run_etl_script('pull_cons_cl')
    finally:
        if old_mode:
            os.environ['ETL_CONS_MODE'] = old_mode
        elif 'ETL_CONS_MODE' in os.environ:
            del os.environ['ETL_CONS_MODE']


def setup_scheduler():
    """Настройка планировщика задач"""
    
    # Загрузка клиентов и консультаций (инкремент) - частота из env
    # ВАЖНО: pull_cons_cl запускается только после завершения pull_clients_cl
    # Используем ETL_CONS_INCREMENTAL_INTERVAL для частоты запуска инкремента консультаций
    # (клиенты загружаются перед консультациями, но частота определяется по консультациям)
    # Если интервал = 0, задача не добавляется (ETL отключен)
    if ETL_CONS_INCREMENTAL_INTERVAL > 0:
        scheduler.add_job(
            run_clients_then_consultations,
            IntervalTrigger(minutes=ETL_CONS_INCREMENTAL_INTERVAL),
            id='pull_clients_then_cons',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_CONS_INCREMENTAL_INTERVAL * 2,  # Пропустить если опоздал больше чем в 2 раза
        )
    else:
        logger.info("ETL incremental consultations disabled (ETL_CONS_INCREMENTAL_INTERVAL=0)")
        print("⚠ ETL incremental consultations disabled (ETL_CONS_INCREMENTAL_INTERVAL=0)")
    
    # Обновление открытых консультаций по Ref_Key - частота из env
    # Запускается отдельно от инкремента для обновления открытых заявок
    if ETL_CONS_OPEN_UPDATE_INTERVAL > 0:
        scheduler.add_job(
            run_consultations_open_update,
            IntervalTrigger(minutes=ETL_CONS_OPEN_UPDATE_INTERVAL),
            id='pull_cons_open_update',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_CONS_OPEN_UPDATE_INTERVAL * 2,
        )
    else:
        logger.info("ETL open consultations update disabled (ETL_CONS_OPEN_UPDATE_INTERVAL=0)")
        print("⚠ ETL open consultations update disabled (ETL_CONS_OPEN_UPDATE_INTERVAL=0)")
    
    # Загрузка переносов - частота из env
    if ETL_CONS_REDATE_INTERVAL > 0:
        scheduler.add_job(
            run_etl_script,
            IntervalTrigger(minutes=ETL_CONS_REDATE_INTERVAL),
            args=['pull_cons_redate_cl'],
            id='pull_cons_redate_cl',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_CONS_REDATE_INTERVAL * 2,
        )
    else:
        logger.info("ETL consultations redate disabled (ETL_CONS_REDATE_INTERVAL=0)")
        print("⚠ ETL consultations redate disabled (ETL_CONS_REDATE_INTERVAL=0)")
    
    # Загрузка оценок - частота из env
    if ETL_CONS_RATES_INTERVAL > 0:
        scheduler.add_job(
            run_etl_script,
            IntervalTrigger(minutes=ETL_CONS_RATES_INTERVAL),
            args=['pull_cons_rates_cl'],
            id='pull_cons_rates_cl',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_CONS_RATES_INTERVAL * 2,
        )
    else:
        logger.info("ETL consultations rates disabled (ETL_CONS_RATES_INTERVAL=0)")
        print("⚠ ETL consultations rates disabled (ETL_CONS_RATES_INTERVAL=0)")
    
    # Загрузка дозвонов - частота из env
    if ETL_CALLS_INTERVAL > 0:
        scheduler.add_job(
            run_etl_script,
            IntervalTrigger(minutes=ETL_CALLS_INTERVAL),
            args=['pull_calls_cl'],
            id='pull_calls_cl',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_CALLS_INTERVAL * 2,
        )
    else:
        logger.info("ETL calls disabled (ETL_CALLS_INTERVAL=0)")
        print("⚠ ETL calls disabled (ETL_CALLS_INTERVAL=0)")
    
    # Загрузка закрытия очереди - частота из env
    if ETL_QUEUE_CLOSING_INTERVAL > 0:
        scheduler.add_job(
            run_etl_script,
            IntervalTrigger(minutes=ETL_QUEUE_CLOSING_INTERVAL),
            args=['pull_queue_closing_cl'],
            id='pull_queue_closing_cl',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_QUEUE_CLOSING_INTERVAL * 2,
        )
    else:
        logger.info("ETL queue closing disabled (ETL_QUEUE_CLOSING_INTERVAL=0)")
        print("⚠ ETL queue closing disabled (ETL_QUEUE_CLOSING_INTERVAL=0)")
    
    # Загрузка пользователей - частота из env (в минутах)
    # По умолчанию каждый час (60 минут)
    if ETL_USERS_INTERVAL > 0:
        scheduler.add_job(
            run_etl_script,
            IntervalTrigger(minutes=ETL_USERS_INTERVAL),
            args=['pull_users_cl'],
            id='pull_users_cl',
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=ETL_USERS_INTERVAL * 2,
        )
    else:
        logger.info("ETL users disabled (ETL_USERS_INTERVAL=0)")
        print("⚠ ETL users disabled (ETL_USERS_INTERVAL=0)")
    
    logger.info("Scheduler configured with ETL tasks")
    logger.info(f"ETL intervals: clients={ETL_CLIENTS_INTERVAL}min, "
                f"cons_incremental={ETL_CONS_INCREMENTAL_INTERVAL}min, "
                f"cons_open_update={ETL_CONS_OPEN_UPDATE_INTERVAL}min, "
                f"redate={ETL_CONS_REDATE_INTERVAL}min, "
                f"rates={ETL_CONS_RATES_INTERVAL}min, "
                f"calls={ETL_CALLS_INTERVAL}min, "
                f"queue_closing={ETL_QUEUE_CLOSING_INTERVAL}min, "
                f"users={ETL_USERS_INTERVAL}min")
    print(f"✓ Scheduler configured with {len(scheduler.get_jobs())} ETL tasks")
    print(f"  Intervals: clients={ETL_CLIENTS_INTERVAL}min, "
          f"cons_incremental={ETL_CONS_INCREMENTAL_INTERVAL}min, "
          f"cons_open_update={ETL_CONS_OPEN_UPDATE_INTERVAL}min, "
          f"redate={ETL_CONS_REDATE_INTERVAL}min, "
          f"rates={ETL_CONS_RATES_INTERVAL}min, "
          f"calls={ETL_CALLS_INTERVAL}min, "
          f"queue_closing={ETL_QUEUE_CLOSING_INTERVAL}min, "
          f"users={ETL_USERS_INTERVAL}min")


def start_scheduler():
    """Запуск планировщика"""
    if not scheduler.running:
        scheduler.start()
        logger.info("Scheduler started")
        print("✓ Scheduler started")
        # Выводим информацию о запланированных задачах
        jobs = scheduler.get_jobs()
        print(f"  Scheduled {len(jobs)} tasks:")
        for job in jobs:
            next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S UTC") if job.next_run_time else "Not scheduled"
            print(f"    - {job.id}: next run at {next_run}")


def shutdown_scheduler():
    """Остановка планировщика"""
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")

