"""Observability tests: formato de alertas, notificador no bloqueante
(seam HTTP sin red) y ObservabilityRepository (transiciones, marks, halt
dedupe, heartbeat).  La suite completa juega asyncio_mode=auto.
"""

from __future__ import annotations

import asyncio

import pytest

from crypto_scalper.core.enums import PositionStatus, SignalType
from crypto_scalper.core.models import HeartbeatSnapshot, ManagedPosition, RiskDecision, Signal
from crypto_scalper.monitoring.alerts import AlertEvent, TelegramNotifier, format_message
from crypto_scalper.monitoring.observability import ObservabilityRepository
from crypto_scalper.storage.sqlite_repo import SqliteRepository


# ── helpers ─────────────────────────────────────────────────────────────────


def mk_position(
    pid: str = "p1",
    *,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    status: PositionStatus = PositionStatus.ACTIVE,
    entry: float = 100.0,
    qty: float = 0.1,
    notional: float = 10.0,
    realized: float = 0.0,
    close_reason: str = "",
    closed_ts: int = 0,
) -> ManagedPosition:
    return ManagedPosition(
        position_id=pid,
        symbol=symbol,
        side=side,
        quantity=qty,
        ordered_quantity=qty,
        entry_price=entry,
        stop_loss_price=entry * 0.99,
        take_profit_price=entry * 1.01,
        notional_value=notional,
        risk_amount=notional * 0.01,
        regime="trending_up",
        status=status,
        entry_client_order_id=f"entry-{pid}",
        stop_client_order_id=f"stop-{pid}",
        take_profit_client_order_id=f"tp-{pid}",
        entry_order_id=f"order-{pid}",
        opened_ts_ms=1000,
        closed_ts_ms=closed_ts,
        realized_pnl=realized,
        close_reason=close_reason,
    )


class FakeNotifier(TelegramNotifier):
    """Captura de alertas sin cola HTTP."""

    def __init__(self) -> None:
        super().__init__("fake-token", "fake-chat", enabled=True)
        self.events: list = []

    def notify_trade_opened(self, *a, **kw) -> bool:
        self.events.append(("opened", kw))
        return True

    def notify_trade_closed(self, *a, **kw) -> bool:
        self.events.append(("closed", kw))
        return True

    def notify_risk_halt(self, *a, **kw) -> bool:
        self.events.append(("risk_halt", kw or {"reason": a[0] if a else ""}))
        return True

    def notify_kill_switch(self, *a, **kw) -> bool:
        self.events.append(("kill_switch", kw or {"reason": a[0] if a else ""}))
        return True

    def notify_critical_error(self, *a, **kw) -> bool:
        self.events.append(("critical_error", kw or {"content": a[0] if a else ""}))
        return True


# ── format_message (pura) ────────────────────────────────────────────────────


def test_format_message_all_events():
    assert "TRADE_OPENED" in format_message(
        AlertEvent.TRADE_OPENED,
        {"symbol": "BTCUSDT", "entry_price": 100.0, "quantity": 0.1,
         "stop_loss": 99.0, "take_profit": 101.0},
    )
    msg = format_message(
        AlertEvent.TRADE_CLOSED,
        {"symbol": "BTCUSDT", "pnl_pct": 1.5, "pnl_usd": 0.15, "reason": "tp"},
    )
    assert "TRADE_CLOSED" in msg and "1.5%" in msg
    assert "RISK_HALT" in format_message(AlertEvent.RISK_HALT, {"reason": "x"})
    assert "KILL_SWITCH" in format_message(AlertEvent.KILL_SWITCH, {"reason": "x"})
    assert "CRITICAL_ERROR" in format_message(AlertEvent.CRITICAL_ERROR, {"content": "boom"})


# ── TelegramNotifier — no red en tests ──────────────────────────────────────


async def test_notifier_disabled_emit_false():
    n = TelegramNotifier("token", "chat", enabled=False)
    assert not n.enabled
    assert n.emit(AlertEvent.TRADE_OPENED, symbol="X") is False


async def test_notifier_queue_full_drops_without_blocking():
    n = TelegramNotifier("token", "chat", enabled=True, queue_maxsize=2)
    kwargs = dict(symbol="A", entry_price=1.0, quantity=0.1, stop_loss=0.9, take_profit=1.1)
    assert n.emit(AlertEvent.TRADE_OPENED, **kwargs) is True
    assert n.emit(AlertEvent.TRADE_OPENED, **kwargs) is True
    assert n.emit(AlertEvent.TRADE_OPENED, **kwargs) is False  # cola llena: descarta
    assert n.pending == 2


async def test_notifier_worker_posts_through_seam():
    n = TelegramNotifier("token", "chat", enabled=True, queue_maxsize=16)
    received: list = []

    async def fake_post(url, params):
        received.append((url, params["text"]))
        return True, "ok"

    n._post = fake_post  # type: ignore[method-assign]  # seam de transporte
    await n.start()
    assert n.emit(
        AlertEvent.TRADE_OPENED, symbol="SOL", entry_price=150, quantity=0.2,
        stop_loss=149, take_profit=151,
    ) is True
    n.emit(AlertEvent.RISK_HALT, reason="daily_loss")
    for _ in range(50):
        if len(received) >= 2:
            break
        await asyncio.sleep(0.01)
    await n.stop()
    assert len(received) == 2
    assert "api.telegram.org/bot" in received[0][0]
    assert all("TRADE_OPENED" in t or "RISK_HALT" in t for _, t in received)


# ── ObservabilityRepository ─────────────────────────────────────────────────


async def test_open_and_close_alerts_with_mark():
    inner = SqliteRepository(":memory:")
    notifier = FakeNotifier()
    obs = ObservabilityRepository(inner, notifier=notifier, mode="paper",
                                  start_equity=1000.0, heartbeat_interval_ms=10 ** 9)
    obs.attach_mark(lambda sym: 110.0)

    opened = mk_position("p1", status=PositionStatus.ACTIVE, entry=100.0, qty=0.1)
    await obs.save_position(opened)
    await obs.save_position(opened)  # misma posición: sin alerta duplicada
    assert inner.count_rows("positions") == 1
    # pnl_unrealized persistido con mark (110-100)*0.1 = 1.0
    stored = dict(inner._conn.execute(
        "SELECT * FROM positions WHERE position_id='p1'").fetchone())
    assert stored["pnl_unrealized"] == pytest.approx(1.0)

    closed = mk_position("p1", status=PositionStatus.CLOSED, entry=100.0, qty=0.1,
                         realized=-0.5, close_reason="tp", closed_ts=2000)
    await obs.save_position(closed)

    kinds = [e[0] for e in notifier.events]
    assert kinds == ["opened", "closed"]
    args = dict(notifier.events[1][1])
    assert args["symbol"] == "BTCUSDT"
    assert args["reason"] == "tp"
    assert "pnl_usd" in args and args["pnl_usd"] == pytest.approx(-0.5)


async def test_risk_halt_dedupe_and_kill_switch():
    inner = SqliteRepository(":memory:")
    notifier = FakeNotifier()
    obs = ObservabilityRepository(inner, notifier=notifier, mode="paper",
                                  start_equity=1000.0, heartbeat_interval_ms=10 ** 9)
    await obs.save_risk_decision(RiskDecision(
        symbol="BTCUSDT", ts_ms=1000, verdict="TRADING_HALTED",
        reason="DAILY_LOSS_LIMIT", verifier="risk_engine",
    ))
    await obs.save_risk_decision(RiskDecision(
        symbol="BTCUSDT", ts_ms=2000, verdict="TRADING_HALTED",
        reason="DAILY_LOSS_LIMIT", verifier="risk_engine",  # dentro del cooldown
    ))
    await obs.save_risk_decision(RiskDecision(
        symbol="BTCUSDT", ts_ms=3000, verdict="SAFE_MODE",
        reason="KILL_SWITCH", verifier="risk_engine",
    ))
    kinds = [e[0] for e in notifier.events]
    assert kinds.count("risk_halt") == 1
    assert kinds.count("kill_switch") == 1


async def test_heartbeat_written_throttled():
    inner = SqliteRepository(":memory:")
    obs = ObservabilityRepository(inner, notifier=None, mode="backtest",
                                  start_equity=1000.0, heartbeat_interval_ms=1)
    await obs.save_position(mk_position("p1", status=PositionStatus.ACTIVE, entry=100.0, qty=0.1))
    await asyncio.sleep(0.01)
    await obs.save_position(mk_position("p2", status=PositionStatus.ACTIVE, entry=50.0, qty=0.2))
    assert inner.count_rows("heartbeats") >= 1


async def test_signal_and_heartbeat_delegate():
    inner = SqliteRepository(":memory:")
    obs = ObservabilityRepository(inner, notifier=None, mode="paper",
                                  start_equity=1000.0, heartbeat_interval_ms=10 ** 9)
    signal = Signal(
        symbol="BTCUSDT", timestamp_ms=1234, signal_type=SignalType.LONG,
        score=0.9, eligible=True, regime="trending_up", reason="signal",
    )
    await obs.save_signal(signal, rejection_reason="risk")
    snapshot = HeartbeatSnapshot(
        ts_ms=111, mode="paper", status="running", equity=1000.0,
        realized_pnl=0.0, unrealized_pnl=0.0, drawdown_pct=0.0,
        total_exposure=0.0, open_count=0, trades_today=0, uptime_ms=0,
    )
    await obs.save_heartbeat(snapshot)
    assert inner.count_rows("signals") == 1
    assert inner.count_rows("heartbeats") == 1


async def test_save_feature_snapshot_feeds_mark_fallback():
    from crypto_scalper.core.models import FeatureSnapshot

    inner = SqliteRepository(":memory:")
    notifier = FakeNotifier()
    obs = ObservabilityRepository(inner, notifier=notifier, mode="paper",
                                  start_equity=1000.0, heartbeat_interval_ms=10 ** 9)
    snap = FeatureSnapshot(
        symbol="BTCUSDT", timestamp_ms=100, price=120.0,
        vwap=100.0, rsi=50.0, atr=1.0, atr_pct=0.01, ema9=100.0, ema21=100.0,
        ema50=100.0, adx=20.0, boll_upper=110.0, boll_mid=100.0, boll_lower=90.0,
        roc=0.0, volume_zscore=0.0, relative_volume=1.0, buy_volume=1.0,
        sell_volume=1.0, trade_count=10, avg_trade_size=0.2, aggressive_volume=0.5,
        order_book_imbalance=0.0, microprice=100.0, spread=0.01, spread_pct=0.0001,
        bid_depth=5.0, ask_depth=5.0, news_sentiment=0.0, news_impact="",
        news_age_ms=0, mention_zscore=0.0, regime="range",
    )
    await obs.save_feature_snapshot(snap)
    await obs.save_position(mk_position("p1", status=PositionStatus.ACTIVE, entry=100.0, qty=0.1))
    stored = dict(inner._conn.execute(
        "SELECT * FROM positions WHERE position_id='p1'").fetchone())
    assert stored["pnl_unrealized"] == pytest.approx(2.0)  # (120-100)*0.1


async def test_noop_passthrough_config():
    inner = SqliteRepository(":memory:")
    obs = ObservabilityRepository(inner, notifier=None, mode="paper", start_equity=1.0)
    assert obs.inner is inner