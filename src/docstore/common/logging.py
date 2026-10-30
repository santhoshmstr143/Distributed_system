"""
Structured JSON logging for Sharded-Docstore nodes.

Rules:
- All log records are emitted as single-line JSON to stdout.
- Every record includes: timestamp (UTC ISO-8601), level, logger name,
  message, node_id, shard_id, and term (when known).
- Secrets and full document bodies are NEVER logged.
- Stack traces are logged only in DEBUG/ERROR but without request payloads.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a compact JSON object on one line."""

    def __init__(
        self,
        node_id: str = "",
        shard_id: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._node_id = node_id
        self._shard_id = shard_id

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "node_id": self._node_id,
            "shard_id": self._shard_id,
        }

        # Include extra fields passed via logger.info(..., extra={...})
        for key in ("term", "seq", "request_id", "shard_id", "op"):
            if hasattr(record, key):
                log_data[key] = getattr(record, key)

        # Include exception TYPE only — never the full traceback in the JSON
        # value (it would expose internal paths and logic).  The raw traceback
        # is still written to the stream by the handler's StreamHandler.emit()
        # only if the log level is DEBUG.
        if record.exc_info and record.exc_info[0] is not None:
            log_data["exc_type"] = record.exc_info[0].__name__
            if record.levelno <= logging.DEBUG:
                log_data["exc_detail"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


def get_logger(
    name: str,
    *,
    node_id: str = "",
    shard_id: str = "",
    level: int | str = logging.INFO,
) -> logging.Logger:
    """Return a logger that emits structured JSON to stdout.

    Args:
        name:      Logger name (use ``__name__`` in production code).
        node_id:   Node identifier injected into every record.
        shard_id:  Shard identifier injected into every record.
        level:     Log level (default INFO).

    Returns:
        A configured :class:`logging.Logger` instance.

    Note:
        This function is idempotent — calling it twice with the same *name*
        returns the same logger without adding duplicate handlers.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter(node_id=node_id, shard_id=shard_id))
        logger.addHandler(handler)
        logger.propagate = False

    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level)

    return logger


def configure_root_logger(level: str = "INFO") -> None:
    """Configure the root logger for structured JSON output.

    Call once at application startup before any other logging calls.
    """
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
