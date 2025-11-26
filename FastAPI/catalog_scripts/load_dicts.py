"""
Асинхронная загрузка всех справочников 1C:CL (dict.*).

Запуск:
    python -m FastAPI.catalog_scripts.load_dicts
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from FastAPI import models
from FastAPI.config import settings

LOGGER = logging.getLogger("load_dicts")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LANG_RU_KEY = "15d38cda-1812-11ef-b824-c67597d01fa8"
LANG_UZ_KEY = "15d38cdb-1812-11ef-b824-c67597d01fa8"

DATABASE_URL = (
    f"postgresql+asyncpg://{settings.DB_USER}:{settings.DB_PASS}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
)


def clean_uuid(value: Optional[str]) -> Optional[str]:
    if not value or value == "00000000-0000-0000-0000-000000000000":
        return None
    return value


def language_from_key(lang_key: Optional[str]) -> Optional[str]:
    if not lang_key:
        return None
    if lang_key == LANG_RU_KEY:
        return "ru"
    if lang_key == LANG_UZ_KEY:
        return "uz"
    return None


class ODataAsyncClient:
    def __init__(self):
        import os

        base_url = settings.ODATA_BASEURL_CL or os.getenv("ODATA_BASEURL_CL")
        if not (base_url and settings.ODATA_USER and settings.ODATA_PASSWORD):
            raise RuntimeError("ODATA credentials missing (ODATA_BASEURL_CL, ODATA_USER, ODATA_PASSWORD)")

        self.base_url = base_url.rstrip("/")
        self.auth = httpx.BasicAuth(settings.ODATA_USER, settings.ODATA_PASSWORD)
        self.page_size = settings.ODATA_PAGE_SIZE or 1000
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": "cons-middleware/dicts-loader",
                "Accept": "application/json",
            },
            timeout=120,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client:
            await self._client.aclose()

    async def fetch_entity(self, entity_name: str) -> List[Dict[str, Any]]:
        if not self._client:
            raise RuntimeError("Client is not initialized")

        url = f"{self.base_url}/{entity_name}?$format=json"
        data: List[Dict[str, Any]] = []
        skip = 0

        while True:
            page_url = f"{url}&$top={self.page_size}&$skip={skip}"
            resp = await self._client.get(page_url, auth=self.auth)
            if resp.status_code == 404:
                LOGGER.error("Entity %s not found (404)", entity_name)
                break
            resp.raise_for_status()

            batch = resp.json().get("value", [])
            if not batch:
                break

            data.extend(batch)
            LOGGER.info("Fetched %s rows from %s (skip=%s)", len(batch), entity_name, skip)

            if len(batch) < self.page_size:
                break
            skip += self.page_size

        return data


async def upsert_rows(session: AsyncSession, table, rows: List[Dict[str, Any]], pk_field: str):
    if not rows:
        return
    stmt = insert(table).values(rows)
    update_columns = {
        col.name: stmt.excluded[col.name]
        for col in table.columns
        if col.name != pk_field
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[getattr(table.c, pk_field)],
        set_=update_columns,
    )
    await session.execute(stmt)


async def load_po_types(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_ВидыПОДляКонсультаций")
    rows = [
        {
            "ref_key": ref_key,
            "description": item.get("Description"),
            "details": item.get("Описание"),
        }
        for item in raw_items
        if (ref_key := clean_uuid(item.get("Ref_Key")))
    ]
    await upsert_rows(session, models.POType.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.po_types", len(rows))


async def load_po_sections(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_РазделыПОДляКонсультаций")
    rows = [
        {
            "ref_key": ref_key,
            "owner_key": clean_uuid(item.get("Owner_Key")),
            "description": item.get("Description"),
            "details": item.get("Описание"),
        }
        for item in raw_items
        if (ref_key := clean_uuid(item.get("Ref_Key")))
    ]
    await upsert_rows(session, models.POSection.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.po_sections", len(rows))


async def load_question_categories(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_КатегорииВопросов")
    rows: List[Dict[str, Any]] = []
    for item in raw_items:
        ref_key = clean_uuid(item.get("Ref_Key"))
        if not ref_key:
            continue
        language = language_from_key(clean_uuid(item.get("Язык_Key")))
        if not language:
            continue
        rows.append(
            {
                "ref_key": ref_key,
                "code": item.get("Code"),
                "description": item.get("Description"),
                "language": language,
            }
        )
    await upsert_rows(session, models.OnlineQuestionCat.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.online_question_cat", len(rows))


async def load_online_questions(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_ВопросыНаКонсультацию")
    rows: List[Dict[str, Any]] = []
    for item in raw_items:
        ref_key = clean_uuid(item.get("Ref_Key"))
        if not ref_key:
            continue
        language = language_from_key(clean_uuid(item.get("Язык_Key")))
        if not language:
            continue
        rows.append(
            {
                "ref_key": ref_key,
                "code": item.get("Code"),
                "description": item.get("Description"),
                "language": language,
                "category_key": clean_uuid(item.get("Категория_Key")),
                "useful_info": item.get("ПолезнаяИнформация"),
                "question": item.get("Вопрос"),
            }
        )
    await upsert_rows(session, models.OnlineQuestion.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.online_question", len(rows))


async def load_knowledge_base(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_БазаЗнанийДляКонсультаций")
    rows = [
        {
            "ref_key": ref_key,
            "description": item.get("Description"),
            "po_type_key": clean_uuid(item.get("ВидПО_Key")),
            "po_section_key": clean_uuid(item.get("РазделПО_Key")),
            "author_key": clean_uuid(item.get("Автор_Key")),
            "question": item.get("Вопрос"),
            "answer": item.get("Ответ"),
        }
        for item in raw_items
        if (ref_key := clean_uuid(item.get("Ref_Key")))
    ]
    await upsert_rows(session, models.KnowledgeBase.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.knowledge_base", len(rows))


async def load_consultation_interference(client: ODataAsyncClient, session: AsyncSession):
    raw_items = await client.fetch_entity("Catalog_ПомехиДляКонсультаций")
    rows = [
        {
            "ref_key": ref_key,
            "code": item.get("Code"),
            "description": item.get("Description"),
        }
        for item in raw_items
        if (ref_key := clean_uuid(item.get("Ref_Key")))
    ]
    await upsert_rows(session, models.ConsultationInterference.__table__, rows, "ref_key")
    LOGGER.info("Upserted %s rows into dict.consultation_interference", len(rows))


async def pull_dicts():
    engine = create_async_engine(DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with ODataAsyncClient() as client, Session() as session:
            await load_po_types(client, session)
            await load_po_sections(client, session)
            await load_question_categories(client, session)
            await load_online_questions(client, session)
            await load_knowledge_base(client, session)
            await load_consultation_interference(client, session)
            await session.commit()
            LOGGER.info("Dictionary load completed")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(pull_dicts())