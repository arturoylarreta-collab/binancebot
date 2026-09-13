"""Offline tests for configuration loading and validation."""

import pytest

from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.config.settings import Settings, _load_dotenv_for_environment
from crypto_scalper.core.enums import Environment
from crypto_scalper.core.exceptions import ConfigurationError


def _clear_env(monkeypatch):
    for key in (
        "APP_ENV", "LIVE_TRADING_ENABLED", "BINANCE_FUTURES_REST_URL", "SYMBOLS",
        "RISK_MAX_RISK_PER_TRADE_PCT",
    ):
        monkeypatch.delenv(key, raising=False)


class TestSettings:
    def test_defaults_dev(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "dev")
        settings = Settings.load()
        assert settings.environment is Environment.DEV
        assert settings.live_trading_enabled is False
        assert settings.ws.batch_size == 20
        assert settings.ws.reconnect_base_s == 1.0
        assert settings.universe.max_symbols == 100

    def test_env_defaults_when_unset(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "dev")
        settings = Settings.load()
        assert settings.features.interval_s == 1.0
        assert settings.log_level == "INFO"

    def test_explicit_symbols(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "dev")
        monkeypatch.setenv("SYMBOLS", "btcusdt, ethusdt")
        settings = Settings.load()
        assert settings.explicit_symbols == ["BTCUSDT", "ETHUSDT"]

    def test_live_requires_explicit_flag(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "live")
        with pytest.raises(ConfigurationError):
            Settings.load()

    def test_live_with_flag(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "live")
        monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
        settings = Settings.load()
        assert settings.environment is Environment.LIVE
        assert settings.live_trading_enabled is True

    def test_invalid_env(self, monkeypatch):
        _clear_env(monkeypatch)
        monkeypatch.setenv("APP_ENV", "prod")
        with pytest.raises(ConfigurationError):
            Settings.load()


class TestRiskConfig:
    def test_defaults_valid(self):
        RiskConfig()

    def test_risk_over_hard_cap_rejected(self):
        with pytest.raises(ConfigurationError):
            RiskConfig(risk_per_trade_pct=0.02)

    def test_leverage_cap(self):
        with pytest.raises(ConfigurationError):
            RiskConfig(max_leverage=5)

    def test_daily_loss_bounds(self):
        with pytest.raises(ConfigurationError):
            RiskConfig(daily_loss_limit_pct=0.5)