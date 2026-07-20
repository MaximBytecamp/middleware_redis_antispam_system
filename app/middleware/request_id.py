"""Request ID middleware."""

from __future__ import annotations

from uuid import uuid4

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .common import header, request_state, set_response_header


class RequestIdMiddleware:
    """Propagate a client request ID or generate one and expose it in state."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = header(scope, b"x-request-id") or str(uuid4())
        request_state(scope)["request_id"] = request_id

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                set_response_header(message, "X-Request-ID", request_id)
            await send(message)

        await self.app(scope, receive, send_with_request_id)
