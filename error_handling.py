"""Error handling, custom exceptions, and retry logic for the pipeline."""

import functools
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Base exception for all pipeline errors."""


class SourceError(IngestionError):
    """Raised on source connector failures."""


class IngestorError(IngestionError):
    """Raised on ingestor failures."""


class TargetError(IngestionError):
    """Raised on target connector failures."""


def retry_with_backoff(
    func: Callable | None = None,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    backoff_factor: float = 2.0,
) -> Callable[..., Any]:
    """Retry a function with exponential backoff.

    Retries on IngestionError (and all subclasses). Logs each attempt.
    """
    def decorator(fn: Callable) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except IngestionError as exc:
                    last_exc = exc
                    if attempt == attempts:
                        logger.error("%s failed after %d attempts: %s", fn.__name__, attempts, exc)
                        raise
                    logger.warning(
                        "%s attempt %d/%d failed: %s. Retrying in %.1fs...",
                        fn.__name__, attempt, attempts, exc, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
            assert last_exc is not None
            raise last_exc
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
