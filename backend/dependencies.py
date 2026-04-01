import os
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from dotenv import load_dotenv

load_dotenv()

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "").strip()


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Проверяет наличие и корректность X-API-Key."""
    if not INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="INTERNAL_API_KEY не задан в .env файле",
        )

    if api_key != INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Неверный или отсутствующий API ключ",
        )

    return api_key
