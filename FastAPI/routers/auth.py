"""Роуты для аутентификации"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from ..schemas.auth import LoginRequest, LoginResponse
from ..config import settings
import httpx

router = APIRouter()
security = HTTPBearer()


async def verify_openid_token(token: str) -> dict:
    """
    Проверка OpenID токена через провайдера.
    
    В реальной реализации здесь должна быть проверка токена
    через OpenID Connect провайдера (например, Keycloak, Auth0).
    """
    if not settings.OPENID_ISSUER:
        # В режиме разработки пропускаем проверку
        return {"sub": "dev_user", "email": "dev@example.com"}
    
    try:
        async with httpx.AsyncClient() as client:
            # Проверяем токен через introspection endpoint
            response = await client.post(
                f"{settings.OPENID_ISSUER}/protocol/openid-connect/token/introspect",
                data={
                    "token": token,
                    "client_id": settings.OPENID_CLIENT_ID,
                    "client_secret": settings.OPENID_CLIENT_SECRET,
                }
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Ошибка проверки токена: {str(e)}"
        )


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Вход через OpenID токен.
    
    Проверяет токен и возвращает access_token для дальнейших запросов.
    """
    try:
        # Проверяем токен
        token_info = await verify_openid_token(request.token)
        
        # В реальной реализации здесь:
        # 1. Создаем/обновляем пользователя в БД
        # 2. Генерируем JWT токен для API
        # 3. Возвращаем токен
        
        # Заглушка: возвращаем тот же токен
        return LoginResponse(
            access_token=request.token,
            user_id=token_info.get("sub")
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка входа: {str(e)}"
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """Dependency для получения текущего пользователя из токена"""
    token = credentials.credentials
    token_info = await verify_openid_token(token)
    return token_info

