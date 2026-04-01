import time
from collections import defaultdict
from fastapi import HTTPException, Request, status

_request_counts: dict = defaultdict(list)

AI_RATE_LIMIT = 10
AI_RATE_WINDOW = 60  # секунд


def check_rate_limit(request: Request, limit: int = AI_RATE_LIMIT, window: int = AI_RATE_WINDOW):
    client_ip = request.client.host
    now = time.time()
    key = f"{client_ip}"

    _request_counts[key] = [t for t in _request_counts[key] if now - t < window]

    if len(_request_counts[key]) >= limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Превышен лимит: не более {limit} запросов в {window} секунд",
            headers={"Retry-After": str(window)},
        )

    _request_counts[key].append(now)
