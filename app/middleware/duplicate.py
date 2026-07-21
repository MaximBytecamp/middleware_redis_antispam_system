"""Duplicate application protection middleware."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

from pydantic import ValidationError
from redis.exceptions import RedisError, WatchError
from starlette.types import ASGIApp, Receive, Scope, Send

from app.models import ApplicationCreate

from .common import (
    buffer_request_body,
    capture_response,
    client_ip,
    json_response,
    pipeline,
    redis_from_scope,
    redis_text,
    redis_unavailable,
    replay_response,
    request_state,
    response_status,
    setting,
)


def application_fingerprint(ip: str, email: str, message: str) -> str:
    """Create a collision-resistant fingerprint for a submitted application."""

    source = f"{ip}:{email}:{message}".encode("utf-8")
    return hashlib.sha256(source).hexdigest()


class DuplicateApplicationMiddleware:
    """Reject a recently accepted application with the same IP and content."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    async def _release_reservation(redis: object, key: str, token: str) -> None:
        """Delete ``key`` atomically, but only while it still contains token."""

        while True:
            redis_pipeline = await pipeline(redis)
            try:
                async with redis_pipeline:
                    await redis_pipeline.watch(key)
                    current = await redis_pipeline.get(key)
                    if current is None or redis_text(current) != token:
                        await redis_pipeline.unwatch()
                        return
                    redis_pipeline.multi()
                    redis_pipeline.delete(key)
                    await redis_pipeline.execute()
                    return
            except WatchError:
                # A concurrent change invalidated the read. Re-read rather than
                # risk deleting a newer request's reservation.
                continue

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not (
            scope.get("method") == "POST" and scope.get("path") == "/applications"
        ):
            await self.app(scope, receive, send)
            return

        body, replay_receive = await buffer_request_body(receive)
        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            await self.app(scope, replay_receive, send)
            return

        try:
            application = ApplicationCreate.model_validate(payload)
        except ValidationError:
            await self.app(scope, replay_receive, send)
            return

        ip = client_ip(scope)
        fingerprint = application_fingerprint(
            ip,
            str(application.email),
            application.message,
        )
        duplicate_key = f"duplicate:{ip}:{fingerprint}"
        redis = redis_from_scope(scope)
        reservation_token = str(uuid4())

        try:
            reserved = bool(
                await redis.set(
                    duplicate_key,
                    reservation_token,
                    nx=True,
                    ex=setting(scope, "duplicate_ttl", 120),
                )
            )
        except RedisError:
            await redis_unavailable(scope, replay_receive, send)
            return

        if not reserved:
            request_state(scope)["response_reason"] = "duplicate_application"
            await json_response(
                scope,
                receive,
                send,
                409,
                "Такая заявка уже была отправлена недавно",
            )
            return

        try:
            messages = await capture_response(self.app, scope, replay_receive)
        except Exception:
            try:
                await self._release_reservation(
                    redis,
                    duplicate_key,
                    reservation_token,
                )
            except RedisError:
                await redis_unavailable(scope, replay_receive, send)
                return
            raise

        if response_status(messages) != 201:
            try:
                await self._release_reservation(
                    redis,
                    duplicate_key,
                    reservation_token,
                )
            except RedisError:
                await redis_unavailable(scope, replay_receive, send)
                return

        await replay_response(messages, send)
