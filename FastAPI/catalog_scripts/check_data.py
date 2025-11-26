"""Проверка загруженных данных"""
from sqlalchemy import create_engine, text
from FastAPI.config import settings

engine = create_engine(
    f"postgresql://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)

with engine.connect() as conn:
    tables = [
        "dict.po_types",
        "dict.po_sections",
        "dict.online_question_cat",
        "dict.online_question",
        "dict.knowledge_base",
        "dict.consultation_interference",
    ]
    for table in tables:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        count = result.scalar()
        print(f"{table}: {count} rows")

