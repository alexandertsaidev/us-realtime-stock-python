# /app/src/storage/minio_s3_factory.py

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

from config.settings import Settings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

@dataclass(frozen=True)
class MinioS3Factory:
    """負責建立連到 MinIO 的 boto3 S3 client。"""

    endpoint_url: str
    access_key: str
    secret_key: str
    region_name: str = "us-east-1"  # MinIO 不在意,但 boto3 需要一個值

    @classmethod
    def from_settings(cls, settings: Settings) -> MinioS3Factory:
        endpoint = settings.minio_endpoint
        if not endpoint.startswith(("http://", "https://")):
            scheme = "https" if settings.minio_secure else "http"
            endpoint = f"{scheme}://{endpoint}"

        return cls(
            endpoint_url=endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key.get_secret_value(),
        )

    def create_client(self) -> S3Client :
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            config=Config(s3={"addressing_style": "path"}),
        )
