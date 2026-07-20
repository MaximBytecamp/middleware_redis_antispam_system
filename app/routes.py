"""HTTP routes for applications, service health, statistics, and maintenance."""

from __future__ import annotations

import json
import logging
import secrets
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, Request, status
from redis.exceptions import RedisError
from starlette.responses import JSONResponse

from .models import (
    ApplicationAccepted,
    ApplicationCreate,
    ApplicationRead,
    HealthResponse,
    MaintenanceResponse,
    StatsResponse,
)


router = APIRouter()
logger = logging.getLogger(__name__)


def _text(value: Any) -> str:
    """Normalize Redis values for clients with or without response decoding."""

    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _integer(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(_text(value))
    except (TypeError, ValueError):
        return 0


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(_text(value))
    except (TypeError, ValueError):
        return 0.0


@router.post(
    "/applications",
    response_model=ApplicationAccepted,
    status_code=status.HTTP_201_CREATED,
)
async def create_application(
    application: ApplicationCreate,
    request: Request,
) -> ApplicationAccepted:
    """Persist an accepted application and announce it over Redis Pub/Sub."""

    redis = request.app.state.redis
    application_id = f"app_{uuid4()}"
    stored_application = {
        "id": application_id,
        "name": application.name,
        "email": str(application.email),
        "message": application.message,
        "status": "accepted",
    }

    await redis.hset(f"application:{application_id}", mapping=stored_application)
    try:
        await redis.publish(
            "application.created",
            json.dumps(
                {
                    "application_id": application_id,
                    "email": str(application.email),
                },
                ensure_ascii=False,
            ),
        )
    except RedisError:
        # Persistence is the acceptance boundary.  A transient Pub/Sub
        # failure must not turn an already stored application into a 503.
        logger.warning(
            "Application %s was stored, but its creation event was not published",
            application_id,
            exc_info=True,
        )

    return ApplicationAccepted(id=application_id)


@router.get("/applications/{application_id}", response_model=ApplicationRead)
async def get_application(application_id: str, request: Request) -> ApplicationRead:
    redis = request.app.state.redis
    stored = await redis.hgetall(f"application:{application_id}")
    if not stored:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена",
        )

    normalized = {_text(key): _text(value) for key, value in stored.items()}
    # Old/external records may not duplicate their id inside the hash.
    normalized.setdefault("id", application_id)
    normalized.setdefault("status", "accepted")
    return ApplicationRead.model_validate(normalized)


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse | JSONResponse:
    """Report Redis connectivity while containing Redis failures locally."""

    try:
        connected = bool(await request.app.state.redis.ping())
    except RedisError:
        connected = False

    if not connected:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "degraded", "redis": "disconnected"},
        )
    return HealthResponse(status="ok", redis="connected")


@router.get("/stats", response_model=StatsResponse)
async def stats(request: Request) -> StatsResponse:
    """Build a statistics snapshot from the counters maintained by middleware."""

    redis = request.app.state.redis
    keys = (
        "stats:requests:total",
        "stats:requests:blocked",
        "stats:requests:GET",
        "stats:requests:POST",
        "stats:requests:DELETE",
        "stats:responses:4xx",
        "stats:responses:5xx",
        "stats:process_time:total",
        "stats:process_time:count",
    )
    values = await redis.mget(*keys)

    total_requests = _integer(values[0])
    blocked_requests = _integer(values[1])
    requests_by_method = {
        "GET": _integer(values[2]),
        "POST": _integer(values[3]),
        "DELETE": _integer(values[4]),
    }
    responses_4xx = _integer(values[5])
    responses_5xx = _integer(values[6])
    total_process_time = _number(values[7])
    process_time_count = _integer(values[8])
    average_process_time = (
        total_process_time / process_time_count if process_time_count else 0.0
    )

    return StatsResponse(
        total_requests=total_requests,
        blocked_requests=blocked_requests,
        requests_by_method=requests_by_method,
        errors={"4xx": responses_4xx, "5xx": responses_5xx},
        responses_4xx=responses_4xx,
        responses_5xx=responses_5xx,
        process_time_count=process_time_count,
        average_process_time_ms=average_process_time,
    )


def _admin_key_is_valid(provided: str | None, expected: str) -> bool:
    if provided is None:
        return False
    # Compare bytes so configured keys are not artificially restricted to
    # ASCII (``compare_digest`` rejects non-ASCII ``str`` values).
    return secrets.compare_digest(
        provided.encode("utf-8"),
        expected.encode("utf-8"),
    )


async def _set_maintenance(
    request: Request,
    enabled: bool,
    x_admin_key: str | None,
) -> MaintenanceResponse:
    expected_key = request.app.state.settings.admin_key
    if not _admin_key_is_valid(x_admin_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Неверный ключ администратора",
        )

    await request.app.state.redis.set("service:maintenance", "1" if enabled else "0")
    return MaintenanceResponse(maintenance=enabled)


@router.post("/admin/maintenance/on", response_model=MaintenanceResponse)
async def maintenance_on(
    request: Request,
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
) -> MaintenanceResponse:
    return await _set_maintenance(request, True, x_admin_key)


@router.post("/admin/maintenance/off", response_model=MaintenanceResponse)
async def maintenance_off(
    request: Request,
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
) -> MaintenanceResponse:
    return await _set_maintenance(request, False, x_admin_key)
