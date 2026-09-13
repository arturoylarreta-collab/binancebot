"""Monitoring / alerts configuration (observability layer)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MonitoringConfig:
    """Telegram alerts + dashboard heartbeat knobs.

    The package never hardcodes secrets: token/chat come from the environment.
    Alerts only fire when `telegram_enabled` is true AND both token and chat
    id are present.
    """

    bot_token: str = ""
    chat_id: str = ""
    telegram_enabled: bool = False
    http_timeout_s: float = 5.0
    queue_size: int = 256
    heartbeat_interval_s: float = 5.0
    alert_cooldown_s: float = 300.0