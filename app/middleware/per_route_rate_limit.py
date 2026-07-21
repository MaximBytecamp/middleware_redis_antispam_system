"""Alternative rate limiter with an independent window for every route.

This module is an educational replacement for ``rate_limit.py``.  It is not
registered in ``main.py`` so that requests are not counted by two rate-limit
middlewares at the same time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Pattern, Sequence

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


@dataclass(frozen=True, slots=True)
class RouteLimitRule:
    """One fixed-window limit for one HTTP method and path pattern."""

    name: str
    method: str
    path_pattern: Pattern[str]
    limit: int
    window: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "method", self.method.upper())
        if self.limit < 1:
            raise ValueError("limit must be greater than zero")
        if self.window < 1:
            raise ValueError("window must be greater than zero")

    def matches(self, method: str, path: str) -> bool:
        return (
            self.method == method
            and self.path_pattern.fullmatch(path) is not None
        )


# Each rule has its own request limit and its own window in seconds.
# A stable rule name is used in Redis instead of the concrete URL.  Therefore
# /applications/app_1 and /applications/app_2 share the get_application quota.
DEFAULT_ROUTE_RULES: tuple[RouteLimitRule, ...] = (
    RouteLimitRule(
        name="create_application",
        method="POST",
        path_pattern=re.compile(r"/applications/?"),
        limit=5,
        window=60,
    ),
    RouteLimitRule(
        name="get_application",
        method="GET",
        path_pattern=re.compile(r"/applications/[^/]+/?"),
        limit=30,
        window=60,
    ),
    RouteLimitRule(
        name="health",
        method="GET",
        path_pattern=re.compile(r"/health/?"),
        limit=120,
        window=60,
    ),
    RouteLimitRule(
        name="stats",
        method="GET",
        path_pattern=re.compile(r"/stats/?"),
        limit=10,
        window=30,
    ),
    RouteLimitRule(
        name="maintenance_on",
        method="POST",
        path_pattern=re.compile(r"/admin/maintenance/on/?"),
        limit=3,
        window=300,
    ),
    RouteLimitRule(
        name="maintenance_off",
        method="POST",
        path_pattern=re.compile(r"/admin/maintenance/off/?"),
        limit=3,
        window=300,
    ),
)


class PerRouteRateLimitMiddleware:
    """Apply an independent fixed-window quota to each configured route."""

    def __init__(
        self,
        app: ASGIApp,
        rules: Sequence[RouteLimitRule] = DEFAULT_ROUTE_RULES,
    ) -> None:
        self.app = app
        self.rules = tuple(rules)

    def _rule_for(self, method: str, path: str) -> RouteLimitRule | None:
        # The first matching rule wins, so put more specific patterns first.
        return next(
            (rule for rule in self.rules if rule.matches(method, path)),
            None,
        )

    @staticmethod
    async def _increment_window(redis: Any, key: str, window: int) -> tuple[int, int]:
        redis_pipeline = await pipeline(redis)
        async with redis_pipeline:
            redis_pipeline.incr(key)
            redis_pipeline.expire(key, window, nx=True)
            redis_pipeline.ttl(key)
            count, _, ttl = await redis_pipeline.execute()
        return int(count), int(ttl)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = str(scope.get("method", "GET")).upper()
        path = str(scope.get("path", "/"))
        rule = self._rule_for(method, path)

        # Routes absent from DEFAULT_ROUTE_RULES are not rate-limited.
        if rule is None:
            await self.app(scope, receive, send)
            return

        redis = redis_from_scope(scope)
        ip = client_ip(scope)
        state = request_state(scope)

        try:
            # Blocking remains global per IP, exactly as in rate_limit.py.
            blocked_key = f"blocked:{ip}"
            is_blocked = await redis.get(blocked_key)
            if is_blocked:
                ttl = int(await redis.ttl(blocked_key))
                if ttl < 1:
                    ttl = setting(scope, "block_ttl", 600)
                    await redis.expire(blocked_key, ttl, nx=True)

                state["response_reason"] = "temporary_blocked"
                await json_response(
                    scope,
                    receive,
                    send,
                    403,
                    "Ваш IP временно заблокирован",
                    {"Retry-After": str(ttl)},
                )
                return

            # The rule name and method split one IP into independent counters.
            rate_key = f"rate_limit:route:{rule.name}:{method}:{ip}"
            count, ttl = await self._increment_window(
                redis,
                rate_key,
                rule.window,
            )

            state["rate_limit_route"] = rule.name
            state["rate_limit_count"] = count
            state["rate_limit_limit"] = rule.limit
            is_rate_limited = count > rule.limit

            if is_rate_limited:
                # Violations are intentionally global: abuse across different
                # routes can still result in one temporary IP block.
                violations, _ = await self._increment_window(
                    redis,
                    f"violations:{ip}",
                    setting(scope, "violation_window", 300),
                )
                if violations >= setting(scope, "violation_limit", 3):
                    await redis.set(
                        blocked_key,
                        "1",
                        ex=setting(scope, "block_ttl", 600),
                    )
        except RedisError:
            await redis_unavailable(scope, receive, send)
            return

        rate_headers = {
            "X-RateLimit-Limit": str(rule.limit),
            "X-RateLimit-Remaining": str(max(rule.limit - count, 0)),
            "X-RateLimit-Window": str(rule.window),
        }

        if is_rate_limited:
            state["response_reason"] = "rate_limit_exceeded"
            rate_headers["Retry-After"] = str(max(ttl, 1))
            await json_response(
                scope,
                receive,
                send,
                429,
                "Слишком много запросов к этому маршруту. Попробуйте позже",
                rate_headers,
            )
            return

        async def send_with_rate_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                for name, value in rate_headers.items():
                    set_response_header(message, name, value)
            await send(message)

        await self.app(scope, receive, send_with_rate_headers)
