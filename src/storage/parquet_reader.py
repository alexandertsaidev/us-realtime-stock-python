# /app/src/storage/parquet_reader.py

from types import TracebackType
from typing import Self

import duckdb

from config.settings import Settings


class ParquetReader:
    """只負責用 DuckDB 從 MinIO 讀取 parquet。"""

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.minio_bucket
        self._conn = duckdb.connect()

        self._conn.execute("INSTALL httpfs")
        self._conn.execute("LOAD httpfs")
        self._conn.execute(f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                KEY_ID {self._q(settings.minio_access_key)},
                SECRET {self._q(settings.minio_secret_key.get_secret_value())},
                ENDPOINT {self._q(settings.minio_endpoint)},
                USE_SSL {str(settings.minio_secure).lower()},
                URL_STYLE 'path'
            )
        """)

    @staticmethod
    def _q(value: str) -> str:
        """SQL 字串常值跳脫（單引號加倍）。CREATE SECRET 不支援參數綁定。"""
        return "'" + value.replace("'", "''") + "'"

    def recent_tickers(self, object_name: str) -> list[str]:
        """近 5 年內有資料的 ticker，依字母排序、去重。"""
        return self._conn.execute(f"""
            SELECT DISTINCT "ticker"
            FROM read_parquet('s3://{self._bucket}/{object_name}')
            WHERE "created_at" >= CURRENT_DATE - INTERVAL '5 years'
            ORDER BY "ticker" ASC
        """).df()["ticker"].tolist()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
