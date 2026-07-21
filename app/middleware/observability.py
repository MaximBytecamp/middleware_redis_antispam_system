"""Request metrics, timing, and access logging middleware."""

from __future__ import annotations

import logging
import time
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
)


logger = logging.getLogger("app.requests")


class ObservabilityMiddleware:
    """Count requests/responses, measure latency, and emit one access log."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    async def _record_response(
        redis: Any,
        status: int,
        elapsed_ms: float,
        response_reason: str | None,
    ) -> None:
        redis_pipeline = await pipeline(redis)
        async with redis_pipeline:
            if 400 <= status < 500:
                redis_pipeline.incr("stats:responses:4xx")
            elif status >= 500:
                redis_pipeline.incr("stats:responses:5xx")

            if response_reason in {
                "rate_limit_exceeded",
                "temporarily_blocked",
            }:
                redis_pipeline.incr("stats:requests:blocked")

            redis_pipeline.incrbyfloat("stats:process_time:total", elapsed_ms)
            redis_pipeline.incr("stats:process_time:count")
            await redis_pipeline.execute()

    @staticmethod
    def _log(scope: Scope, status: int, elapsed_ms: float) -> None:
        state = request_state(scope)
        request_id = str(state.get("request_id", "-"))[:8]
        request_id = request_id.replace("\n", "_").replace("\r", "_")
        method = str(scope.get("method", "-"))
        path = str(scope.get("path", "-"))
        parts = [
            f"[{request_id}] {method} {path}",
            f"IP={client_ip(scope)}",
            f"status={status}",
            f"time={elapsed_ms:.1f}ms",
        ]
        if "rate_limit_count" in state:
            parts.append(
                f"rate_limit={state['rate_limit_count']}/{state['rate_limit_limit']}"
            )
        if "cache" in state:
            parts.append(f"cache={state['cache']}")
        if reason := state.get("response_reason"):
            parts.append(f"reason={reason}")
        logger.info(" ".join(parts))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started_at = time.perf_counter()
        redis = redis_from_scope(scope)
        method = str(scope.get("method", "UNKNOWN")).upper()

        try:
            # These counters deliberately happen before any downstream guard.
            redis_pipeline = await pipeline(redis)
            async with redis_pipeline:
                redis_pipeline.incr("stats:requests:total")
                redis_pipeline.incr(f"stats:requests:{method}")
                await redis_pipeline.execute()
        except RedisError:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            await redis_unavailable(
                scope,
                receive,
                send,
                headers={"X-Process-Time": f"{elapsed_ms:.1f}ms"},
            )
            self._log(scope, 503, elapsed_ms)
            return

        status_code = 500
        response_started = False
        redis_failed = False

        async def observe_send(message: Message) -> None:
            nonlocal status_code, response_started, redis_failed
            if redis_failed:
                return

            if message["type"] == "http.response.start" and not response_started:
                status_code = int(message["status"])
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                try:
                    await self._record_response(
                        redis,
                        status_code,
                        elapsed_ms,
                        request_state(scope).get("response_reason"),
                    )
                except RedisError:
                    redis_failed = True
                    status_code = 503
                    response_started = True
                    await redis_unavailable(
                        scope,
                        receive,
                        send,
                        headers={"X-Process-Time": f"{elapsed_ms:.1f}ms"},
                    )
                    return

                set_response_header(
                    message,
                    "X-Process-Time",
                    f"{elapsed_ms:.1f}ms",
                )
                response_started = True

            await send(message)

        try:
            await self.app(scope, receive, observe_send)
        except RedisError:
            if response_started:
                elapsed_ms = (time.perf_counter() - started_at) * 1000
                logger.exception(
                    "Redis error after response start: %s %s",
                    scope.get("method"),
                    scope.get("path"),
                )
                self._log(scope, status_code, elapsed_ms)
                raise
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            redis_failed = True
            status_code = 503
            await redis_unavailable(
                scope,
                receive,
                send,
                headers={"X-Process-Time": f"{elapsed_ms:.1f}ms"},
            )
        except Exception:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            logger.exception(
                "Unhandled request error: %s %s",
                scope.get("method"),
                scope.get("path"),
            )
            if response_started:
                self._log(scope, status_code, elapsed_ms)
                raise

            status_code = 500
            try:
                await self._record_response(
                    redis,
                    status_code,
                    elapsed_ms,
                    request_state(scope).get("response_reason"),
                )
            except RedisError:
                status_code = 503
                await redis_unavailable(
                    scope,
                    receive,
                    send,
                    headers={"X-Process-Time": f"{elapsed_ms:.1f}ms"},
                )
            else:
                request_state(scope)["response_reason"] = "internal_error"
                await json_response(
                    scope,
                    receive,
                    send,
                    500,
                    "Внутренняя ошибка сервера",
                    {"X-Process-Time": f"{elapsed_ms:.1f}ms"},
                )
            self._log(scope, status_code, elapsed_ms)
            return

        elapsed_ms = (time.perf_counter() - started_at) * 1000
        if redis_failed:
            status_code = 503
        self._log(scope, status_code, elapsed_ms)
