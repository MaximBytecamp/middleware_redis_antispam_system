"""Temporary IP block middleware."""

from __future__ import annotations

from redis.exceptions import RedisError
from starlette.types import ASGIApp, Receive, Scope, Send

from .common import (
    client_ip,
    json_response,
    redis_from_scope,
    redis_unavailable,
    request_state,
    setting,
)


class TemporaryBlockMiddleware:
    """Reject clients carrying an unexpired temporary block key."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        key = f"blocked:{client_ip(scope)}"
        try:
            redis = redis_from_scope(scope)
            ttl = int(await redis.ttl(key))
            if ttl == -1:
                ttl = setting(scope, "block_ttl", 600)
                if not await redis.expire(key, ttl):
                    await self.app(scope, receive, send)
                    return
        except RedisError:
            await redis_unavailable(scope, receive, send)
            return

        if ttl != -2:
            request_state(scope)["response_reason"] = "temporarily_blocked"
            await json_response(
                scope,
                receive,
                send,
                403,
                "Ваш IP временно заблокирован",
                {"Retry-After": str(max(ttl, 1))},
            )
            return

        await self.app(scope, receive, send)
