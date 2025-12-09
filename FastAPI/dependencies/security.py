"""Зависимости для авторизации фронтенда."""
import secrets
from fastapi import Header, HTTPException, status
from typing import Optional

from ..config import settings


def _safe_equals(expected: str, received: Optional[str]) -> bool:
    """Сравнивает строки в константное время, если обе заданы."""
    if not expected or received is None:
        return False
    return secrets.compare_digest(expected, received)


async def verify_front_secret(
    x_front_secret: Optional[str] = Header(default=None),
    authorization: Optional[str] = Header(default=None),
) -> None:
    """
    Проверяет статичный токен фронта.

    Клиент может передавать:
    - `X-Front-Secret: <FRONT_SECRET>`
    - `Authorization: Bearer <FRONT_BEARER_TOKEN>`
    """
    header_secret = settings.FRONT_SECRET
    bearer_secret = settings.FRONT_BEARER_TOKEN or header_secret

    if not header_secret and not bearer_secret:
        # Если секреты не заданы, пропускаем проверку (dev окружение)
        return

    if _safe_equals(header_secret, x_front_secret):
        return

    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1].strip()
        if _safe_equals(bearer_secret, token):
            return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid frontend token",
    )

