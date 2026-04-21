"""Local filesystem source connector."""

from __future__ import annotations

import glob
import os
from typing import Any

from sources.base import SourceBase


class LocalSource(SourceBase):
    """Reads from the local filesystem."""

    type_name = "local"

    def list_files(self) -> list[str]:
        """Find files matching the configured pattern."""
        path = self.config.get("source.path", "./test_data")
        pattern = self.config.get("source.file_pattern", "*")
        full_pattern = os.path.join(path, pattern)
        return sorted(glob.glob(full_pattern))

    def read_file(self, path: str) -> bytes:
        """Read file bytes from the local filesystem."""
        with open(path, "rb") as f:
            return f.read()


def get_local_source(config: Any) -> LocalSource:
    """Convenience function to create a LocalSource."""
    return LocalSource(config)
