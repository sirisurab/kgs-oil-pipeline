"""Shared utilities: logging setup, retry decorator, timer decorator."""

import functools
import logging
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure and return a named logger with console handler."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def ensure_dirs(*dirs: str | Path) -> None:
    """Create directories if they do not exist."""
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)


def retry(max_retries: int = 3, base_delay: float = 1.0, factor: float = 2.0) -> Callable[[F], F]:
    """Decorator: retry with exponential backoff on any exception.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds.
        factor: Multiplicative backoff factor.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < max_retries:
                        time.sleep(delay)
                        delay *= factor
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


def timer(func: F) -> F:
    """Decorator: log elapsed time for the wrapped function."""
    logger = setup_logging(__name__)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info("%-30s completed in %.2f s", func.__name__, elapsed)
        return result

    return wrapper  # type: ignore[return-value]
