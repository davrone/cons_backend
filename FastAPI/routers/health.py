"""Роуты для проверки здоровья сервиса"""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ..database import get_db
from ..scheduler import scheduler

router = APIRouter()


@router.get("/health")
async def health():
    """Базовая проверка здоровья сервиса"""
    return {"status": "ok"}


@router.get("/health/db")
async def health_db(db: AsyncSession = Depends(get_db)):
    """Проверка подключения к БД"""
    try:
        result = await db.execute(text("SELECT 1"))
        result.scalar()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": "disconnected", "error": str(e)}


@router.get("/health/scheduler")
async def health_scheduler():
    """Проверка статуса планировщика задач"""
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'next_run': str(job.next_run_time) if job.next_run_time else None,
                'trigger': str(job.trigger),
            })
        return {
            'scheduler_running': scheduler.running,
            'jobs_count': len(jobs),
            'jobs': jobs,
        }
    except Exception as e:
        return {
            'scheduler_running': False,
            'error': str(e),
        }
