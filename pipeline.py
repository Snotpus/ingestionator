"""Pipeline orchestrator for ingestionator."""

import logging
import os

from config import Config
from error_handling import IngestionError, retry_with_backoff
from factories import Factory

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates a full ingestion run."""

    def __init__(self, config: Config, factory: Factory):
        self._config = config
        self._factory = factory
        self._source = factory.create(config.get("source.type"))
        self._ingestor = factory.create(config.get("ingestor.type"))
        self._target = factory.create(config.get("target.type"))
        self._retry_attempts = config.get("error_handling.retry_attempts", 3)
        self._backoff_factor = config.get("error_handling.backoff_factor", 2)
        self._max_delay = config.get("error_handling.max_delay", 300)
        self._total_rows = 0
        self._run_file = retry_with_backoff(
            self._run_file,
            attempts=self._retry_attempts,
            base_delay=1.0,
            max_delay=self._max_delay,
            backoff_factor=self._backoff_factor,
        )

    @property
    def total_rows(self) -> int:
        return self._total_rows

    def _run_file(self, path: str, filename: str | None = None) -> int:
        """Ingest a single file. Decorated with retry logic."""
        logger.info("Processing: %s", path)
        if filename is None:
            source_path = self._source.config.get("source.path", "").rstrip(os.sep)
            rel_path = path.replace(source_path, "").lstrip(os.sep)
            if rel_path:
                filename = rel_path
        data = self._source.read_file(path)
        df = self._ingestor.ingest(data)
        self._target.write(df, filename=filename)
        rows = len(df)
        logger.info("Ingested %d rows from %s", rows, path)
        return rows

    def run_file(self, path: str) -> int:
        """Ingest a single file (public, with retry)."""
        return self._run_file(path)

    def run(self) -> int:
        """Run the full pipeline: list files, ingest each, return total rows."""
        files = self._source.list_files()
        logger.info("Found %d files to process", len(files))
        self._total_rows = 0
        for path in files:
            try:
                rows = self.run_file(path)
                self._total_rows += rows
            except Exception as e:
                raise IngestionError(f"Pipeline failed: {e}") from e
        logger.info("Total rows ingested: %d", self._total_rows)
        return self._total_rows
