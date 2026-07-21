"""Maintenance mode middleware."""

from __future__ import annotations

from redis.exceptions import RedisError
from starlette.types import ASGIApp, Receive, Scope, Send

from .common import (
    json_response,
    redis_from_scope,
    redis_text,
    redis_unavailable,
    request_state,
)


class MaintenanceMiddleware:
    """Short-circuit ordinary routes while maintenance mode is enabled."""

    EXEMPT_PATHS = frozenset(
        {
            "/health",
            "/docs",
            "/openapi.json",
            # This must stay reachable so maintenance mode can be disabled.
            "/admin/maintenance/off",
        }
    )

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") in self.EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return

        try:
            enabled = await redis_from_scope(scope).get("service:maintenance")
        except RedisError:
            await redis_unavailable(scope, receive, send)
            return

        if enabled is not None and redis_text(enabled) == "1":
            request_state(scope)["response_reason"] = "maintenance"
            await json_response(
                scope,
                receive,
                send,
                503,
                "Сервис временно находится на техническом обслуживании",
            )
            return

        await self.app(scope, receive, send)
