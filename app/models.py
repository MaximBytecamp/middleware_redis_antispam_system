"""Pydantic request and response models used by the API."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class ApplicationCreate(BaseModel):
    name: str = Field(max_length=100)
    email: EmailStr
    message: str = Field(max_length=2_000)

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("name", "message")
    @classmethod
    def reject_blank_text(cls, value: str) -> str:
        """Reject values which only contained whitespace before stripping."""

        if not value:
            raise ValueError("must not be blank")
        return value


class ApplicationAccepted(BaseModel):
    id: str
    status: Literal["accepted"] = "accepted"


class ApplicationRead(ApplicationAccepted):
    name: str
    email: EmailStr
    message: str


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    redis: Literal["connected", "disconnected"]


class StatsResponse(BaseModel):
    total_requests: int
    blocked_requests: int
    requests_by_method: dict[str, int]
    errors: dict[str, int]
    responses_4xx: int
    responses_5xx: int
    process_time_count: int
    average_process_time_ms: float


class MaintenanceResponse(BaseModel):
    maintenance: bool
