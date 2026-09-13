"""Structured logging with secret redaction.

Never log raw secrets. The REDACTION_PATTERNS list is applied to every
log record before emission.
"""

from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

REDACTION_PATTERNS = [
    (re.compile(r"(api_?secret|secret|password|token)\s*[=:]\s*\S+", re.I), r"\1=***"),
    (re.compile(r"\b[0-9a-fA-F]{64}\b"), "***"),

]


class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        s = record.getMessage()
        for pattern, repl in REDACTION_PATTERNS:
            s = pattern.sub(repl, s)
        record.msg = s
        record.args = ()
        return True


_STANDARD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "message", "asctime",
    "extra_fields",
}


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        kv = []
        for key, value in sorted(record.__dict__.items()):
            if key.startswith("_") or key in _STANDARD_ATTRS:
                continue
            kv.append(f"{key}={value}")
        if kv:
            return f"{base} | " + " ".join(kv)
        return base


def setup_logging(
    level: str = "INFO",
    log_dir: Optional[Path] = None,
    enable_file: bool = True,
) -> None:
    log_dir = Path(log_dir) if log_dir else Path("logs")
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers = [logging.StreamHandler()]
    if enable_file:
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_dir / "crypto_scalper.log",
                maxBytes=50 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
        )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=handlers,
        format=fmt,
    )
    for h in logging.getLogger().handlers:
        h.addFilter(RedactFilter())
        h.setFormatter(KeyValueFormatter(fmt))


def _kv(logger_name: str, message: str, fields: Dict[str, object]) -> None:
    logging.getLogger(logger_name).info(message, extra={"extra_fields": fields})