"""Shared helpers for pure ASGI middleware."""

from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any

from redis.exceptions import RedisError
from starlette.datastructures import MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


REDIS_UNAVAILABLE_DETAIL = "Redis недоступен"


def request_state(scope: Scope) -> dict[str, Any]:
    """Return Starlette's per-request state dictionary."""

    state = scope.setdefault("state", {})
    if not isinstance(state, dict):  # Defensive support for hand-written scopes.
        raise TypeError("scope['state'] must be a dictionary")
    return state


def redis_from_scope(scope: Scope) -> Any:
    return scope["app"].state.redis


def setting(scope: Scope, name: str, default: int) -> int:
    settings = getattr(scope["app"].state, "settings", None)
    value = getattr(settings, name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def client_ip(scope: Scope) -> str:
    client = scope.get("client")
    return str(client[0]) if client else "unknown"


def header(scope: Scope, name: bytes) -> str | None:
    for raw_name, raw_value in scope.get("headers", ()):
        if raw_name.lower() == name:
            return raw_value.decode("latin-1")
    return None


def set_response_header(message: Message, name: str, value: str) -> None:
    MutableHeaders(scope=message)[name] = value


def redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


async def pipeline(redis: Any) -> Any:
    """Create a transaction pipeline, including support for async test doubles."""

    redis_pipeline = redis.pipeline(transaction=True)
    if inspect.isawaitable(redis_pipeline):
        redis_pipeline = await redis_pipeline
    return redis_pipeline


def clone_message(message: Message) -> Message:
    cloned = dict(message)
    if "headers" in cloned:
        cloned["headers"] = list(cloned["headers"])
    if "body" in cloned:
        cloned["body"] = bytes(cloned["body"])
    return cloned


async def json_response(
    scope: Scope,
    receive: Receive,
    send: Send,
    status_code: int,
    detail: str,
    headers: dict[str, str] | None = None,
) -> None:
    await JSONResponse(
        {"detail": detail},
        status_code=status_code,
        headers=headers,
    )(scope, receive, send)


async def redis_unavailable(
    scope: Scope,
    receive: Receive,
    send: Send,
    *,
    headers: dict[str, str] | None = None,
) -> None:
    request_state(scope)["response_reason"] = "redis_unavailable"
    await json_response(
        scope,
        receive,
        send,
        503,
        REDIS_UNAVAILABLE_DETAIL,
        headers,
    )


async def capture_response(
    app: ASGIApp,
    scope: Scope,
    receive: Receive,
) -> list[Message]:
    """Run an ASGI app and retain every response event without modifying it."""

    messages: list[Message] = []

    async def capture(message: Message) -> None:
        messages.append(clone_message(message))

    await app(scope, receive, capture)
    return messages


async def replay_response(messages: Sequence[Message], send: Send) -> None:
    for message in messages:
        await send(clone_message(message))


def response_status(messages: Sequence[Message]) -> int | None:
    for message in messages:
        if message["type"] == "http.response.start":
            return int(message["status"])
    return None


def response_body(messages: Sequence[Message]) -> bytes:
    return b"".join(
        bytes(message.get("body", b""))
        for message in messages
        if message["type"] == "http.response.body"
    )


def response_header(messages: Sequence[Message], name: bytes) -> str | None:
    for message in messages:
        if message["type"] != "http.response.start":
            continue
        for raw_name, raw_value in message.get("headers", ()):
            if raw_name.lower() == name:
                return raw_value.decode("latin-1")
    return None


async def buffer_request_body(receive: Receive) -> tuple[bytes, Receive]:
    messages: list[Message] = []
    chunks: list[bytes] = []

    while True:
        message = await receive()
        messages.append(clone_message(message))
        if message["type"] == "http.disconnect":
            break
        if message["type"] == "http.request":
            chunks.append(bytes(message.get("body", b"")))
            if not message.get("more_body", False):
                break

    index = 0

    async def replay() -> Message:
        nonlocal index
        if index < len(messages):
            message = clone_message(messages[index])
            index += 1
            return message
        return await receive()

    return b"".join(chunks), replay


async def safe_redis_unavailable(
    scope: Scope,
    receive: Receive,
    send: Send,
    error: RedisError,
) -> None:
    del error
    await redis_unavailable(scope, receive, send)
