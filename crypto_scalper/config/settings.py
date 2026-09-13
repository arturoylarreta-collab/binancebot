from dataclasses import dataclass, field
import json as _json
import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from crypto_scalper.config.strategies import DEFAULT_SIGNAL_WEIGHTS, StrategyConfig
from crypto_scalper.core.enums import Environment, Regime
from crypto_scalper.core.exceptions import ConfigurationError

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv_for_environment() -> None:
    """Loads .env then the environment-specific override file (.env.<APP_ENV>)."""
    app_env = os.environ.get("APP_ENV", "dev")
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    load_dotenv(PROJECT_ROOT / f".env.{app_env}", override=False)


def _as_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be a float, got {raw!r}")


def _as_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be an int, got {raw!r}")


def _as_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _as_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw if raw is not None and raw != "" else default


@dataclass(frozen=True)
class WebSocketConfig:
    url: str
    batch_size: int
    conn_timeout_s: float
    ping_interval_s: float
    recv_timeout_s: float
    reconnect_base_s: float
    reconnect_max_s: float
    reconnect_factor: float
    event_queue_maxsize: int
    depth_snapshot_limit: int


@dataclass(frozen=True)
class UniverseConfig:
    max_symbols: int
    min_quote_volume_24h: float
    max_spread_pct: float
    min_price_change_pct: float = -50.0
    max_price_change_pct: float = 50.0
    # Weights for the tradability score (order of columns in the metric row).
    score_weights: tuple = (0.35, 0.35, 0.20, 0.10)


@dataclass(frozen=True)
class FeatureConfig:
    interval_s: float = 1.0
    rolling_history_size: int = 120
    depth_pct_buckets: tuple = (0.0005, 0.0010, 0.0025)


@dataclass(frozen=True)
class DataConfig:
    quote_symbols: tuple = ("USDT", "FDUSD", "USDC")


@dataclass(frozen=True)
class Settings:
    """Typed, immutable application configuration loaded from environment."""

    environment: Environment
    run_mode: str                    # market_data / backtest / paper / live
    live_trading_enabled: bool
    rest_url: str
    ws: WebSocketConfig
    universe: UniverseConfig
    features: FeatureConfig
    data: DataConfig
    log_level: str
    log_format: str
    log_dir: Path
    explicit_symbols: List[str] = field(default_factory=list)
    extras: dict = field(default_factory=dict)
    strategies: StrategyConfig = field(default_factory=StrategyConfig)

    @classmethod
    def load(cls) -> "Settings":
        _load_dotenv_for_environment()

        env_name = _as_str("APP_ENV", "dev").lower()
        try:
            environment = Environment[env_name.upper()]
        except KeyError:
            raise ConfigurationError(f"APP_ENV must be one of dev/paper/live, got {env_name!r}")

        if environment is Environment.LIVE and not _as_bool("LIVE_TRADING_ENABLED", False):
            raise ConfigurationError(
                "Refusing to start in LIVE without LIVE_TRADING_ENABLED=true. "
                "Add an explicit .env.live file and keep it out of source control."
            )

        _validate_ws_numbers()
        ws = WebSocketConfig(
            url=_as_str("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/stream"),
            batch_size=_as_int("WS_BATCH_SIZE", 20),
            conn_timeout_s=_as_float("WS_CONN_TIMEOUT_S", 30.0),
            ping_interval_s=_as_float("WS_PING_INTERVAL_S", 20.0),
            recv_timeout_s=_as_float("WS_RECV_TIMEOUT_S", 45.0),
            reconnect_base_s=_as_float("WS_RECONNECT_BASE_S", 1.0),
            reconnect_max_s=_as_float("WS_RECONNECT_MAX_S", 30.0),
            reconnect_factor=_as_float("WS_RECONNECT_FACTOR", 2.0),
            event_queue_maxsize=_as_int("WS_EVENT_QUEUE_MAXSIZE", 100000),
            depth_snapshot_limit=_as_int("ORDERBOOK_SNAPSHOT_LIMIT", 100),
        )

        symbols_raw = _as_str("SYMBOLS", "")
        explicit = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]

        return cls(
            environment=environment,
            run_mode=_as_str("RUN_MODE", "market_data").lower(),
            live_trading_enabled=_as_bool("LIVE_TRADING_ENABLED", False),
            rest_url=_as_str("BINANCE_FUTURES_REST_URL", "https://fapi.binance.com"),
            ws=ws,
            universe=UniverseConfig(
                max_symbols=_as_int("UNIVERSE_MAX_SYMBOLS", 100),
                min_quote_volume_24h=_as_float("UNIVERSE_MIN_QUOTE_VOLUME_24H", 5_000_000),
                max_spread_pct=_as_float("UNIVERSE_MAX_SPREAD_PCT", 0.0015),
            ),
            features=FeatureConfig(
                interval_s=_as_float("FEATURE_INTERVAL_S", 1.0),
                rolling_history_size=_as_int("ROLLING_HISTORY_SIZE", 120),
            ),
            data=DataConfig(),
            log_level=_as_str("LOG_LEVEL", "INFO").upper(),
            log_format=_as_str("LOG_FORMAT", "kv"),
            log_dir=Path(_as_str("LOG_DIR", "logs")),
            explicit_symbols=explicit,
            strategies=_load_strategy_config(),
        )


def _load_strategy_config() -> StrategyConfig:
    weights = dict(DEFAULT_SIGNAL_WEIGHTS)
    raw_weights = _as_str("SIGNAL_WEIGHTS_JSON", "")
    if raw_weights:
        try:
            parsed = _json.loads(raw_weights)
        except ValueError:
            raise ConfigurationError("SIGNAL_WEIGHTS_JSON must be a valid JSON object")
        if not isinstance(parsed, dict):
            raise ConfigurationError("SIGNAL_WEIGHTS_JSON must be a JSON object")
        for key, value in parsed.items():
            try:
                weights[key] = float(value)
            except (TypeError, ValueError):
                raise ConfigurationError(
                    f"SIGNAL_WEIGHTS_JSON key {key!r} must map to a number"
                )

    allowed_raw = _as_str("STRATEGY_ALLOWED_REGIMES", "")
    allowed = tuple(
        r.strip().lower() for r in allowed_raw.split(",") if r.strip()
    )
    known = {r.name.lower() for r in Regime}
    for r in allowed:
        if r not in known:
            raise ConfigurationError(f"unknown regime in STRATEGY_ALLOWED_REGIMES: {r}")

    return StrategyConfig(
        enabled=_as_bool("STRATEGY_ENABLED", False),
        allowed_regimes=allowed,
        signal_weights=weights,
        long_threshold=_as_float("STRATEGY_LONG_THRESHOLD", 60.0),
        short_threshold=_as_float("STRATEGY_SHORT_THRESHOLD", 40.0),
    )


def _validate_ws_numbers() -> None:
    pairs = [
        ("WS_BATCH_SIZE", _as_int("WS_BATCH_SIZE", 20), 1, 100),
        ("WS_RECV_TIMEOUT_S", _as_float("WS_RECV_TIMEOUT_S", 45.0), 1.0, 300.0),
        ("WS_RECONNECT_BASE_S", _as_float("WS_RECONNECT_BASE_S", 1.0), 0.1, 60.0),
    ]
    for name, value, low, high in pairs:
        if not (low <= value <= high):
            raise ConfigurationError(f"{name} out of range: {value}")