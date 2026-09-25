"""Execution venue + runtime server configuration (FASE 9).

``EXECUTION_VENUE`` selects where orders go:

  * ``paper``   — SimulatedExecutionAdapter (default, no keys, no network orders)
  * ``testnet`` — Binance USD-M Futures demo/testnet via BinanceFuturesAdapter;
                  requires ``BINANCE_TESTNET_API_KEY`` / ``BINANCE_TESTNET_API_SECRET``

Real-money trading is intentionally NOT selectable here.

Secrets are ``repr=False`` so they never reach logs through a dataclass repr.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from crypto_scalper.core.exceptions import ConfigurationError

VENUES = ("paper", "testnet")


@dataclass(frozen=True)
class VenueConfig:
    venue: str = "paper"
    testnet_rest_url: str = "https://demo-fapi.binance.com"
    testnet_ws_url: str = "wss://demo-fstream.binance.com"
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    recv_window_ms: int = 5000
    poll_interval_s: float = 2.0
    # When keys are missing for `testnet`, run on paper instead of refusing
    # to start (keeps a 24/7 deployment alive until the keys are added).
    fallback_to_paper: bool = True

    def __post_init__(self) -> None:
        if self.venue not in VENUES:
            raise ConfigurationError(f"EXECUTION_VENUE must be one of {VENUES}, got {self.venue!r}")
        if not 1000 <= self.recv_window_ms <= 60000:
            raise ConfigurationError("BINANCE_RECV_WINDOW_MS must be in [1000, 60000]")
        if self.poll_interval_s <= 0:
            raise ConfigurationError("BINANCE_POLL_INTERVAL_S must be > 0")

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def effective_venue(self) -> str:
        if self.venue == "testnet" and not self.has_credentials:
            if self.fallback_to_paper:
                return "paper"
            raise ConfigurationError(
                "EXECUTION_VENUE=testnet requires BINANCE_TESTNET_API_KEY and "
                "BINANCE_TESTNET_API_SECRET"
            )
        return self.venue


@dataclass(frozen=True)
class ServerConfig:
    """Embedded HTTP server: /healthz, JSON API and the web dashboard."""
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080
    # Bearer token required for control actions (pause/resume/flatten).
    # Empty → control endpoints are disabled; the dashboard is read-only.
    control_token: str = field(default="", repr=False)
    # Optional basic protection for the whole dashboard (user "admin").
    dashboard_password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not 0 < self.port < 65536:
            raise ConfigurationError(f"invalid PORT {self.port}")
