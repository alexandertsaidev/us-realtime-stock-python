# /app/src/config/settings.py

from typing import Annotated

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_ignore_empty=True,
    )

    # ── MinIO ──────────────────────────────────────────
    # 類型：str（必填）｜ 用途：對應 MINIO_ENDPOINT
    minio_endpoint: str
    # 類型：str（必填）｜ 用途：對應 MINIO_ACCESS_KEY，帳號不算機密，用一般 str
    minio_access_key: str
    # 類型：SecretStr（必填）｜ 用途：對應 MINIO_SECRET_KEY，print 或 log 時會被遮蔽
    minio_secret_key: SecretStr
    # 類型：bool（選填）
    minio_secure: bool = False
    # 類型：str（必填）｜ 用途：對應 MINIO_BUCKET
    minio_bucket: str

    # ── Kafka ──────────────────────────────────────────
    # 類型：str（必填）｜ 用途：對應 KAFKA_BOOTSTRAP_SERVERS
    kafka_bootstrap_servers: str

    # ── Slack ──────────────────────────────────────────
    # 類型：SecretStr（必填）｜ 用途：對應 SLACK_RT_PIPE_WEBHOOK_URL
    slack_rt_pipe_webhook_url: SecretStr

    # ── Finnhub ────────────────────────────────────────
    finnhub_api_keys: Annotated[list[SecretStr], Field(min_length=1)]

    # 類型：SecretStr（必填）｜ 用途：對應 FINNHUB_API_KEY_X
    # finnhub_api_key_1: SecretStr
    # finnhub_api_key_2: SecretStr
    # finnhub_api_key_3: SecretStr

    # @property
    # def finnhub_api_keys(self) -> list[str]:
    #     return [self.finnhub_api_key_1.get_secret_value(),
    #             self.finnhub_api_key_2.get_secret_value(),
    #             self.finnhub_api_key_3.get_secret_value(),]


