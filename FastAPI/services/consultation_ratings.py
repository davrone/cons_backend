"""Сервисы для работы с оценками консультаций."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Set

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ConsRatingAnswer, Consultation


async def recalc_consultation_ratings(db: AsyncSession, cons_keys: Set[str]) -> None:
    """
    Пересчитывает среднюю оценку и ответы для списка консультаций и
    обновляет поле con_rates в таблице cons.cons.
    """
    if not cons_keys:
        return

    for cons_key in cons_keys:
        answers_result = await db.execute(
            select(
                ConsRatingAnswer.question_number,
                ConsRatingAnswer.rating,
                ConsRatingAnswer.question_text,
                ConsRatingAnswer.comment,
                ConsRatingAnswer.manager_key,
            )
            .where(ConsRatingAnswer.cons_key == cons_key)
            .order_by(ConsRatingAnswer.question_number.asc())
        )
        answers = answers_result.all()
        if not answers:
            continue

        ratings = [row[1] for row in answers if row[1] is not None]
        avg_rating = round(sum(ratings) / len(ratings), 2) if ratings else None
        payload = {
            "average": avg_rating,
            "count": len(ratings),
            "answers": [
                {
                    "question_number": row[0],
                    "rating": row[1],
                    "question": row[2],
                    "comment": row[3],
                    "manager_key": row[4],
                }
                for row in answers
            ],
        }

        await db.execute(
            update(Consultation)
            .where(Consultation.cl_ref_key == cons_key)
            .values(con_rates=payload, updated_at=datetime.now(timezone.utc))
        )

