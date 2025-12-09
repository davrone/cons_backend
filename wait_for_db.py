#!/usr/bin/env python3
"""Простая утилита для ожидания доступности БД"""
import asyncio
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import os

async def check_db():
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME')
    
    if not all([db_user, db_pass, db_host, db_name]):
        print(f"Missing DB config: USER={bool(db_user)}, PASS={bool(db_pass)}, HOST={bool(db_host)}, NAME={bool(db_name)}", file=sys.stderr)
        return False
    
    db_url = (
        f"postgresql+asyncpg://{db_user}:{db_pass}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    engine = create_async_engine(db_url, pool_pre_ping=True, pool_timeout=5)
    try:
        async with engine.begin() as conn:
            await conn.execute(text('SELECT 1'))
        await engine.dispose()
        return True
    except Exception as e:
        print(f"Database check failed: {type(e).__name__}: {e}", file=sys.stderr)
        await engine.dispose()
        return False

if __name__ == "__main__":
    if asyncio.run(check_db()):
        sys.exit(0)
    else:
        sys.exit(1)

