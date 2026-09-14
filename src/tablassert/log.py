"""Pipeline logging, enabled only when the optional ``log`` extra is installed.

Without loguru, the logger is a silent no-op: base installs do not create log
files or directories and do not emit warnings about an optional dependency.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from tablassert.utils import BASE

if TYPE_CHECKING:
    from loguru import Logger

LOGASSERT: Path = BASE / "log"
LOG_FORMAT: str = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra[category]} | {message}"
_LOG_FILE: Final[Path] = LOGASSERT / "tablassert.log"


class _NullLogger:
    """Loguru-shaped logger used when the optional ``log`` extra is absent."""

    def __init__(self: _NullLogger, category: str) -> None:
        self._category: str = category

    def configure(self: _NullLogger, **_: Any) -> None:
        return

    def bind(self: _NullLogger, *, category: str, **_: Any) -> _NullLogger:
        return _NullLogger(category)

    def add(self: _NullLogger, sink: Any, **_: Any) -> int:
        return 0

    def remove(self: _NullLogger, sink_id: int | None = None) -> None:
        return

    def info(self: _NullLogger, message: str, /, **fields: Any) -> None:
        return

    def warning(self: _NullLogger, message: str, /, **fields: Any) -> None:
        return

    def error(self: _NullLogger, message: str, /, **fields: Any) -> None:
        return

    def debug(self: _NullLogger, message: str, /, **fields: Any) -> None:
        return


try:
    from loguru import logger
except ImportError:
    logger = _NullLogger("PIPELINE")
else:
    LOGASSERT.mkdir(parents=True, exist_ok=True)
    logger.configure(extra={"category": "PIPELINE"})
    logger.remove()
    # mode="a" + enqueue=True: under multiprocessing Pool() (spawn) each worker re-imports
    # this module and reopens the log. mode="w" would truncate the parent's log mid-run, so
    # append mode (O_APPEND) is what makes concurrent cross-process writes safe; enqueue=True
    # additionally serializes writes through a per-process queue so threads within one process
    # don't interleave partial lines.
    logger.add(_LOG_FILE, level="INFO", format=LOG_FORMAT, rotation="100 MB", encoding="utf-8", mode="a", enqueue=True)


def cat(name: str) -> Logger | _NullLogger:
    return logger.bind(category=name)
