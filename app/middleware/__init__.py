"""ASGI middleware package for anti-spam checks and Redis observability."""

from .duplicate import DuplicateApplicationMiddleware, application_fingerprint
from .maintenance import MaintenanceMiddleware
from .observability import ObservabilityMiddleware
from .rate_limit import RateLimitMiddleware
from .request_id import RequestIdMiddleware
from .stats_cache import StatsCacheMiddleware


MIDDLEWARE_CLASSES = (
    RequestIdMiddleware,
    ObservabilityMiddleware,
    MaintenanceMiddleware,
    RateLimitMiddleware,
    DuplicateApplicationMiddleware,
    StatsCacheMiddleware,
)

__all__ = [
    "DuplicateApplicationMiddleware",
    "MIDDLEWARE_CLASSES",
    "MaintenanceMiddleware",
    "ObservabilityMiddleware",
    "RateLimitMiddleware",
    "RequestIdMiddleware",
    "StatsCacheMiddleware",
    "application_fingerprint",
]
