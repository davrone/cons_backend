"""
Планировщик задач для ETL процессов.
Использует APScheduler для запуска периодических задач.
"""
import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

# Блокировки для предотвращения параллельных запусков
running_tasks = set()


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
    
    # Только после завершения загрузки клиентов запускаем загрузку консультаций
    # await гарантирует, что pull_clients_cl уже завершился
    logger.info("Clients sync completed, starting consultations sync")
    print("✅ Clients sync completed, starting consultations sync")
    await run_etl_script('pull_cons_cl')


def setup_scheduler():
    """Настройка планировщика задач"""
    
    # Загрузка клиентов и консультаций - каждую минуту
    # ВАЖНО: pull_cons_cl запускается только после завершения pull_clients_cl
    scheduler.add_job(
        run_clients_then_consultations,
        IntervalTrigger(minutes=1),
        id='pull_clients_then_cons',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,  # Пропустить если опоздал больше 1 минуты
    )
    
    # Загрузка переносов - каждую минуту
    scheduler.add_job(
        run_etl_script,
        IntervalTrigger(minutes=1),
        args=['pull_cons_redate_cl'],
        id='pull_cons_redate_cl',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )
    
    # Загрузка оценок - каждую минуту
    scheduler.add_job(
        run_etl_script,
        IntervalTrigger(minutes=1),
        args=['pull_cons_rates_cl'],
        id='pull_cons_rates_cl',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )
    
    # Загрузка дозвонов - каждую минуту
    scheduler.add_job(
        run_etl_script,
        IntervalTrigger(minutes=1),
        args=['pull_calls_cl'],
        id='pull_calls_cl',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )
    
    # Загрузка закрытия очереди - каждую минуту
    scheduler.add_job(
        run_etl_script,
        IntervalTrigger(minutes=1),
        args=['pull_queue_closing_cl'],
        id='pull_queue_closing_cl',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )
    
    # Загрузка пользователей - ежедневно в 3:00 UTC
    scheduler.add_job(
        run_etl_script,
        CronTrigger(minute=15),
        args=['pull_users_cl'],
        id='pull_users_cl',
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600,  # 1 час для ежедневных задач
    )
    
    logger.info("Scheduler configured with ETL tasks")
    print(f"✓ Scheduler configured with {len(scheduler.get_jobs())} ETL tasks")


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

