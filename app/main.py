"""FastAPI application factory and ASGI entry point."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, status
from redis.exceptions import RedisError
from starlette.middleware import Middleware
from starlette.responses import JSONResponse

from .config import Settings, get_settings
from .middleware import (
    DuplicateApplicationMiddleware,
    MaintenanceMiddleware,
    ObservabilityMiddleware,
    RateLimitMiddleware,
    RequestIdMiddleware,
    StatsCacheMiddleware,
)
from .redis_client import close_redis_client, create_redis_client
from .routes import router


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def create_app(
    settings: Settings | None = None,
    redis_client: Any | None = None,
) -> FastAPI:
    """Create an application, optionally injecting Redis for integration tests."""

    runtime_settings = settings or get_settings()
    owns_redis_client = redis_client is None
    client = redis_client or create_redis_client(runtime_settings.redis_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_redis_client:
                await close_redis_client(client)

    # The first item is the outermost user middleware. This guarantees that
    # request IDs and timing headers are also present on early 403/429/503
    # responses produced by inner Redis-backed middleware.
    middleware = [
        Middleware(RequestIdMiddleware),
        Middleware(ObservabilityMiddleware),
        Middleware(MaintenanceMiddleware),
        Middleware(RateLimitMiddleware),
        Middleware(DuplicateApplicationMiddleware),
        Middleware(StatsCacheMiddleware),
    ]

    application = FastAPI(
        title="Антиспам для формы заявок",
        description="API заявок с Redis rate limiting, блокировками и метриками.",
        version="1.0.0",
        lifespan=lifespan,
        middleware=middleware,
    )
    application.state.redis = client
    application.state.settings = runtime_settings
    application.include_router(router)

    @application.exception_handler(RedisError)
    async def redis_error_handler(_: Request, __: RedisError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "Redis недоступен"},
        )

    return application


app = create_app()
