"""Application configuration via pydantic-settings."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIMAPP_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://simapp:simapp@localhost:5432/simapp"
    upload_dir: str = "/tmp/simapp_uploads"


settings = Settings()
