"""Shared utility helpers for the KGS oil production data pipeline.

Provides: setup_logging, retry decorator, timer decorator,
compute_file_hash, ensure_dir, is_valid_raw_file.
"""

import hashlib
import logging
import logging.handlers
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_logger = logging.getLogger(__name__)


def setup_logging(name: str, log_dir: Path, level: str = "INFO") -> logging.Logger:
    """Configure and return a named logger writing to both file and stdout.

    Args:
        name: Logger name (also used as the log filename stem).
        log_dir: Directory for log files (created if absent).
        level: Python logging level string.

    Returns:
        Configured logger.
    """
    ensure_dir(log_dir)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / f"{name}.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def retry(
    max_attempts: int = 3,
    backoff_s: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator factory: retry the wrapped function with exponential backoff.

    Args:
        max_attempts: Maximum total call attempts (includes the first).
        backoff_s: Base backoff in seconds; sleep = backoff_s * 2**attempt.
        exceptions: Exception types that trigger a retry.

    Returns:
        Decorator that wraps the target function.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    sleep_time = backoff_s * (2**attempt)
                    _logger.warning(
                        "Attempt %d/%d for %s failed: %s. Retrying in %.2fs.",
                        attempt + 1,
                        max_attempts,
                        func.__name__,
                        exc,
                        sleep_time,
                    )
                    if attempt < max_attempts - 1:
                        time.sleep(sleep_time)
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


def timer(logger: logging.Logger | None = None) -> Callable[[F], F]:
    """Decorator factory: measure and optionally log wall-clock execution time.

    Args:
        logger: If provided, log elapsed time at DEBUG level.

    Returns:
        Decorator that wraps the target function.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.monotonic()
            result = func(*args, **kwargs)
            elapsed = time.monotonic() - start
            if logger is not None:
                logger.debug("%s completed in %.3fs.", func.__name__, elapsed)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def compute_file_hash(path: Path, algorithm: str = "sha256") -> str:
    """Compute and return the hex digest of a file.

    Args:
        path: Path to the file.
        algorithm: Hash algorithm name (default: sha256).

    Returns:
        Lowercase hex digest string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    h = hashlib.new(algorithm)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> Path:
    """Create directory (and all parents) if it does not exist.

    Args:
        path: Directory path to create.

    Returns:
        The same path (idempotent).
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def is_valid_raw_file(path: Path) -> bool:
    """Return True if the file exists, is non-empty, is UTF-8 decodable, and has >= 2 lines.

    Args:
        path: Path to the file to validate.

    Returns:
        True if all validation conditions pass, False otherwise.
    """
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        text = path.read_text(encoding="utf-8")
        lines = [ln for ln in text.splitlines() if ln]
        return len(lines) >= 2
    except Exception:
        return False
