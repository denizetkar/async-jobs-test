"""Pydantic v2 request/response schemas."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class SimulationParams(BaseModel):
    chunks: int = Field(default=4, ge=1, le=100, description="Number of simulation chunks (fan-out)")


class SimulationRequest(BaseModel):
    dataset_id: UUID
    parameters: SimulationParams = Field(default_factory=SimulationParams)


class DatasetResponse(BaseModel):
    id: UUID
    filename: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class SimulationResponse(BaseModel):
    id: UUID
    dataset_id: UUID
    status: str
    parameters: dict
    result: dict | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
