"""Per-IP fixed-window rate limiting middleware."""

from __future__ import annotations

from typing import Any

from redis.exceptions import RedisError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .common import (
    client_ip,
    json_response,
    pipeline,
    redis_from_scope,
    redis_unavailable,
    request_state,
    set_response_header,
    setting,
)


class RateLimitMiddleware:
    """Apply a per-IP fixed-window request limit and track violations."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    async def _increment_window(redis: Any, key: str, window: int) -> tuple[int, int]:
        redis_pipeline = await pipeline(redis)
        async with redis_pipeline:
            redis_pipeline.incr(key)
            # Redis 7's NX option sets an expiry only if one is absent. Keeping
            # this in MULTI makes creation/repair atomic without sliding the
            # fixed window on every request.
            redis_pipeline.expire(key, window, nx=True)
            redis_pipeline.ttl(key)
            count, _, ttl = await redis_pipeline.execute()
        return count, ttl

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        redis = redis_from_scope(scope)
        ip = client_ip(scope)
        limit = setting(scope, "rate_limit", 10)
        window = setting(scope, "rate_window", 60)
        state = request_state(scope)

        try:
            count, ttl = await self._increment_window(
                redis,
                f"rate_limit:{ip}",
                window,
            )
            state["rate_limit_count"] = count
            state["rate_limit_limit"] = limit

            if count > limit:
                violation_limit = setting(scope, "violation_limit", 3)
                violation_window = setting(scope, "violation_window", 300)
                violations, _ = await self._increment_window(
                    redis,
                    f"violations:{ip}",
                    violation_window,
                )
                if violations >= violation_limit:
                    await redis.set(
                        f"blocked:{ip}",
                        "1",
                        ex=setting(scope, "block_ttl", 600),
                    )
        except RedisError:
            await redis_unavailable(scope, receive, send)
            return

        remaining = max(limit - count, 0)
        rate_headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": str(remaining),
        }

        if count > limit:
            state["response_reason"] = "rate_limit_exceeded"
            rate_headers["Retry-After"] = str(max(ttl, 1))
            await json_response(
                scope,
                receive,
                send,
                429,
                "Слишком много запросов. Попробуйте позже",
                rate_headers,
            )
            return

        async def send_with_rate_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                for name, value in rate_headers.items():
                    set_response_header(message, name, value)
            await send(message)

        await self.app(scope, receive, send_with_rate_headers)
