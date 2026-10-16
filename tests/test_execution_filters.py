"""Exchange filters: grid snapping, minimums and router integration."""

import pytest

from crypto_scalper.core.enums import RiskVerdict, SignalType
from crypto_scalper.core.models import RiskDecision, Signal
from crypto_scalper.execution.execution_router import _apply_filters
from crypto_scalper.execution.filters import (
    FilterRegistry,
    SymbolFilters,
    format_decimal,
    parse_symbol_filters,
)

BTC = SymbolFilters("BTCUSDT", tick_size=0.1, step_size=0.001, min_qty=0.001, min_notional=100.0)


def _decision(qty, sl, tp):
    return RiskDecision(symbol="BTCUSDT", ts_ms=1, verdict=RiskVerdict.APPROVED.name,
                        position_size=qty, stop_loss_price=sl, take_profit_price=tp,
                        notional_value=qty * 50_000, risk_amount=10.0)


def _signal(kind):
    return Signal(symbol="BTCUSDT", timestamp_ms=1, signal_type=kind, score=70.0,
                  eligible=True, regime="trending_up")


def test_price_rounding_directions():
    assert BTC.round_price(50_000.07, "down") == pytest.approx(50_000.0)
    assert BTC.round_price(50_000.01, "up") == pytest.approx(50_000.1)
    assert BTC.round_price(50_000.05) == pytest.approx(50_000.1)


def test_qty_floors_to_step_without_float_noise():
    assert BTC.floor_qty(0.0029999999) == pytest.approx(0.002)
    assert BTC.floor_qty(0.3 * 3) == pytest.approx(0.9)
    assert format_decimal(0.1 + 0.2, 0.001) == "0.300"


def test_long_sl_rounds_away_tp_toward_entry():
    d, violation = _apply_filters(_decision(0.0123456, 49_899.97, 50_200.03),
                                  _signal(SignalType.LONG), BTC, 50_000.0)
    assert violation is None
    assert d.position_size == pytest.approx(0.012)
    assert d.stop_loss_price == pytest.approx(49_899.9)   # further from entry
    assert d.take_profit_price == pytest.approx(50_200.0)  # closer to entry


def test_short_rounding_is_mirrored():
    d, violation = _apply_filters(_decision(0.01, 50_100.01, 49_799.99),
                                  _signal(SignalType.SHORT), BTC, 50_000.0)
    assert violation is None
    assert d.stop_loss_price == pytest.approx(50_100.1)
    assert d.take_profit_price == pytest.approx(49_800.0)


def test_below_min_notional_is_rejected():
    _d, violation = _apply_filters(_decision(0.001, 49_900.0, 50_200.0),
                                   _signal(SignalType.LONG), BTC, 50_000.0)
    assert violation and "minNotional" in violation


def test_parse_exchange_info_entry():
    f = parse_symbol_filters({"symbol": "ETHUSDT", "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "maxQty": "2000"},
        {"filterType": "MIN_NOTIONAL", "notional": "20"},
    ]})
    assert (f.tick_size, f.step_size, f.min_notional, f.market_max_qty) == (0.01, 0.001, 20.0, 2000.0)
    reg = FilterRegistry.from_exchange_info({"symbols": [{"symbol": "ETHUSDT", "filters": []}]})
    assert "ETHUSDT" in reg and "BTCUSDT" not in reg
