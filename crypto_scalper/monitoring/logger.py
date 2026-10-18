"""Structured logging with secret redaction.

Never log raw secrets. Redaction runs on the FINAL formatted line — message,
``extra`` fields and tracebacks alike — so a secret can't leak through a
key=value field or an exception message (e.g. a Telegram bot URL).

``LOG_FORMAT=kv`` (default, human friendly) or ``json`` (one object per line,
for log aggregators). ``LOG_TO_FILE=false`` keeps containers stdout-only.
"""

from __future__ import annotations

import json
import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional

REDACTION_PATTERNS = [
    (re.compile(r"(api_?secret|api_?key|secret|password|token|signature)(\"?\s*[=:]\s*\"?)[^\s&\"',}]+", re.I),
     r"\1\2***"),
    (re.compile(r"\bbot\d{6,}:[A-Za-z0-9_-]{20,}"), "bot***"),     # Telegram bot token
    (re.compile(r"\b[0-9a-fA-F]{64}\b"), "***"),                    # HMAC signatures / secrets
    (re.compile(r"\b[A-Za-z0-9]{64}\b"), "***"),                    # Binance API keys
    (re.compile(r"(listenKey=)[A-Za-z0-9]+"), r"\1***"),
]


def redact(text: str) -> str:
    for pattern, repl in REDACTION_PATTERNS:
        text = pattern.sub(repl, text)
    return text


_STANDARD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "message", "asctime",
    "extra_fields",
}


def _extras(record: logging.LogRecord) -> Dict[str, object]:
    return {k: v for k, v in sorted(record.__dict__.items())
            if not k.startswith("_") and k not in _STANDARD_ATTRS}


class RedactFilter(logging.Filter):
    """Kept for backwards compatibility; formatters redact the full line."""

    def filter(self, record: logging.LogRecord) -> bool:
        return True


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        kv = [f"{key}={value}" for key, value in _extras(record).items()]
        line = f"{base} | " + " ".join(kv) if kv else base
        return redact(line)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, object] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in _extras(record).items()})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return redact(json.dumps(payload, default=str, ensure_ascii=False))


def setup_logging(
    level: str = "INFO",
    log_dir: Optional[Path] = None,
    enable_file: bool = True,
    fmt: str = "kv",
) -> None:
    log_dir = Path(log_dir) if log_dir else Path("logs")
    line_fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers: list = [logging.StreamHandler()]
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
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    formatter: logging.Formatter = (
        JsonFormatter() if fmt.lower() == "json" else KeyValueFormatter(line_fmt)
    )
    for h in handlers:
        h.setFormatter(formatter)
        root.addHandler(h)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # third-party chatter
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


def _kv(logger_name: str, message: str, fields: Dict[str, object]) -> None:
    logging.getLogger(logger_name).info(message, extra={"extra_fields": fields})
