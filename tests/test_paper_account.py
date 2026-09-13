"""FASE 7 — PaperAccount: simulated balance, fees and mark-to-market."""

from __future__ import annotations

import pytest

from crypto_scalper.core.enums import PositionStatus
from crypto_scalper.core.models import ManagedPosition
from crypto_scalper.trading.account import PaperAccount


def _closed_pos(side: str = "BUY", entry: float = 50000.0, qty: float = 1.0,
                pnl: float = 1000.0, notional: float = 50000.0) -> ManagedPosition:
    return ManagedPosition(
        position_id="pos-1", symbol="BTCUSDT", side=side, quantity=qty,
        ordered_quantity=qty, entry_price=entry, stop_loss_price=49250.0,
        take_profit_price=51500.0, notional_value=notional, risk_amount=100.0,
        regime="trending_up", status=PositionStatus.CLOSED,
        entry_client_order_id="ENTRY-POS-1",
        stop_client_order_id="SL-POS-1",
        take_profit_client_order_id="TP-POS-1",
        opened_ts_ms=1, closed_ts_ms=2, realized_pnl=pnl, close_reason="take_profit",
    )


class TestPaperAccount:
    def test_start_equity(self):
        acc = PaperAccount(10_000.0)
        assert acc.cash == 10_000.0
        assert acc.equity() == 10_000.0
        assert acc.realized_pnl == 0.0
        assert acc.fees_paid == 0.0

    def test_validation(self):
        with pytest.raises(ValueError):
            PaperAccount(0.0)
        with pytest.raises(ValueError):
            PaperAccount(100.0, fee_pct=-1.0)
        with pytest.raises(ValueError):
            PaperAccount(100.0, fee_pct=0.5)

    def test_realize_close_long_profit_no_fees(self):
        acc = PaperAccount(10_000.0)
        acc.realize_close(_closed_pos(pnl=1000.0))
        assert acc.realized_pnl == pytest.approx(1000.0)
        assert acc.cash == pytest.approx(11_000.0)
        assert acc.fees_paid == 0.0

    def test_realize_close_short_loss(self):
        acc = PaperAccount(10_000.0)
        acc.realize_close(_closed_pos(side="SELL", pnl=-2000.0))
        assert acc.realized_pnl == pytest.approx(-2000.0)
        assert acc.cash == pytest.approx(8_000.0)

    def test_fees_charged_on_entry_and_exit_notional(self):
        acc = PaperAccount(10_000.0, fee_pct=0.001)
        pos = _closed_pos(qty=2.0, entry=5000.0, pnl=500.0, notional=10_000.0)
        # entry notional = 2*5000 = 10_000; exit notional = 2*(5000+250) = 10_500
        acc.realize_close(pos)
        assert acc.fees_paid == pytest.approx((10000.0 + 10500.0) * 0.001)

    def test_unrealized_mark_to_market(self):
        acc = PaperAccount(10_000.0)
        pos = ManagedPosition(
            position_id="pos-2", symbol="BTCUSDT", side="BUY", quantity=2.0,
            ordered_quantity=2.0, entry_price=50000.0, stop_loss_price=49250.0,
            take_profit_price=51500.0, notional_value=100000.0, risk_amount=100.0,
            regime="trending_up", status=PositionStatus.ACTIVE,
            entry_client_order_id="ENTRY-POS-2",
            stop_client_order_id="SL-POS-2",
            take_profit_client_order_id="TP-POS-2",
            opened_ts_ms=1,
        )
        unreal = acc.unrealized_pnl([pos], lambda s: 50500.0)
        assert unreal == pytest.approx(1000.0)
        assert acc.equity(unreal) == pytest.approx(11_000.0)

    def test_peak_equity_tracks_high_water(self):
        acc = PaperAccount(10_000.0)
        acc.equity(500.0)
        assert acc.peak_equity == pytest.approx(10_500.0)
        acc.equity(-300.0)
        assert acc.peak_equity == pytest.approx(10_500.0)
        stats = acc.stats(-300.0)  # current marked-to-market unrealized pnl
        assert stats.drawdown_pct == pytest.approx((10500 - 9700) / 10500)

    def test_stats_shape(self):
        acc = PaperAccount(10_000.0)
        stats = acc.stats(unrealized_pnl=50.0)
        assert stats.to_dict()["equity"] == pytest.approx(10_050.0)
        assert stats.drawdown_pct == 0.0