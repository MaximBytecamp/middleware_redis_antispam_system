"""Stats endpoint cache middleware."""

from __future__ import annotations

from redis.exceptions import RedisError
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

from .common import (
    capture_response,
    redis_from_scope,
    redis_unavailable,
    replay_response,
    request_state,
    response_body,
    response_header,
    response_status,
    set_response_header,
    setting,
)


class StatsCacheMiddleware:
    """Cache successful JSON responses from ``GET /stats`` for a short TTL."""

    CACHE_KEY = "cache:stats"

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not (
            scope.get("method") == "GET" and scope.get("path") == "/stats"
        ):
            await self.app(scope, receive, send)
            return

        redis = redis_from_scope(scope)
        state = request_state(scope)
        try:
            cached = await redis.get(self.CACHE_KEY)
        except RedisError:
            await redis_unavailable(scope, receive, send)
            return

        if cached is not None:
            state["cache"] = "HIT"
            content = cached if isinstance(cached, (bytes, str)) else str(cached)
            await Response(
                content=content,
                status_code=200,
                media_type="application/json",
                headers={"X-Cache": "HIT"},
            )(scope, receive, send)
            return

        state["cache"] = "MISS"
        messages = await capture_response(self.app, scope, receive)
        status = response_status(messages)
        content_type = response_header(messages, b"content-type") or ""

        if status == 200 and content_type.lower().startswith("application/json"):
            try:
                await redis.set(
                    self.CACHE_KEY,
                    response_body(messages),
                    ex=setting(scope, "stats_cache_ttl", 15),
                )
            except RedisError:
                await redis_unavailable(scope, receive, send)
                return

        for message in messages:
            if message["type"] == "http.response.start":
                set_response_header(message, "X-Cache", "MISS")
        await replay_response(messages, send)
