"""S3 source connector."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import boto3

from sources.base import SourceBase

if TYPE_CHECKING:
    from config import Config


class S3Source(SourceBase):
    """Reads from an S3 bucket."""

    type_name = "s3"

    def __init__(self, config: Config, region: str | None = None,
                 bucket: str | None = None):
        super().__init__(config)
        self._region = region or self.config.get("source.aws.region", "us-east-1")
        self._bucket = bucket or self.config.get("source.s3.bucket")
        self._client: Any | None = None

    @property
    def client(self) -> boto3.client:
        """Lazy-initialized S3 client."""
        if self._client is None:
            self._client = boto3.client("s3", region_name=self._region)
        return self._client

    def list_files(self) -> list[str]:
        """List S3 objects matching the configured prefix/pattern."""
        path = self.config.get("source.path", "")
        pattern = self.config.get("source.file_pattern", "*")
        prefix = path.lstrip("/")

        files: list[str] = []
        continuation_token = None
        while True:
            kwargs: dict = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**kwargs)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if self._matches_pattern(os.path.basename(key), pattern):
                    files.append(key)
            if response.get("IsTruncated"):
                continuation_token = response["NextContinuationToken"]
            else:
                break
        return sorted(files)

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """Simple glob-like pattern matching for S3 keys."""
        import fnmatch
        return fnmatch.fnmatch(name, pattern)

    def read_file(self, path: str) -> bytes:
        """Read object content from S3."""
        response = self.client.get_object(Bucket=self._bucket, Key=path)
        return response["Body"].read()
