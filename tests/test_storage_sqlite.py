"""FASE 7 — SqliteRepository audit trail: roundtrips, idempotency, reopen."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from crypto_scalper.config.execution import ExecutionConfig
from crypto_scalper.config.risk import RiskConfig
from crypto_scalper.core.enums import OrderStatus, PositionStatus, RiskVerdict
from crypto_scalper.core.models import (
    ExecutionReport,
    FeatureSnapshot,
    Fill,
    ManagedPosition,
    RiskDecision,
)
from crypto_scalper.execution.execution_router import ExecutionRouter
from crypto_scalper.execution.reconciliation import ReconciliationIssue, ReconciliationReport
from crypto_scalper.risk.portfolio import PortfolioState
from crypto_scalper.risk.risk_engine import RiskEngine
from crypto_scalper.storage.sqlite_repo import SqliteRepository
from crypto_scalper.strategies.signal_engine import SignalEngine
from crypto_scalper.config.strategies import StrategyConfig

SYMBOL = "BTCUSDT"
ENTRY = 50000.0


def _position(position_id: str = "pos-1", status: PositionStatus = PositionStatus.CLOSED) -> ManagedPosition:
    return ManagedPosition(
        position_id=position_id, symbol=SYMBOL, side="BUY",
        quantity=0.1, ordered_quantity=0.1, entry_price=ENTRY,
        stop_loss_price=ENTRY - 750.0, take_profit_price=ENTRY + 1500.0,
        notional_value=5000.0, risk_amount=75.0, regime="trending_up",
        status=status, entry_client_order_id="ENTRY-1",
        stop_client_order_id="SL-1", take_profit_client_order_id="TP-1",
        entry_order_id="sim-1", opened_ts_ms=1000, closed_ts_ms=2000,
        realized_pnl=150.0, close_reason="take_profit",
    )


def _report(client_order_id: str = "ENTRY-1", ts_ms: int = 1000) -> ExecutionReport:
    return ExecutionReport(
        order_id="sim-1", client_order_id=client_order_id, symbol=SYMBOL,
        side="BUY", order_type="MARKET", status=OrderStatus.FILLED.name,
        original_quantity=0.1, executed_quantity=0.1, avg_price=ENTRY,
        fills=(
            Fill(symbol=SYMBOL, client_order_id=client_order_id, side="BUY",
                 quantity=0.1, price=ENTRY, ts_ms=1000, order_id="sim-1"),
        ),
        ts_ms=ts_ms,
    )


def _snapshot() -> FeatureSnapshot:
    long_cats = {
        "trend": {"trend_alignment": 0.8, "adx_strength": 0.6},
        "momentum": {"rsi": 65.0, "roc_10": 0.003, "roc_30": 0.02},
        "volume": {"volume_zscore": 1.0, "relative_volume": 2.0, "buy_ratio_30s": 0.6},
        "order_book": {"obi": 0.15, "obi_delta": 0.01, "spread_pct": 0.00002},
        "volatility": {"atr_pct": 0.01},
        "price": {"price_vs_vwap_pct": 0.001, "boll_position": 0.5, "return_60s": 0.001},
        "news": {"news_age_ms": 0, "news_ttl_ms": 0, "news_relevance": 0},
        "ml": {"ml_score": 50.0},
    }
    return FeatureSnapshot(
        symbol=SYMBOL, timestamp_ms=int(ENTRY), price=ENTRY, vwap=ENTRY + 200.0,
        rsi=60.0, atr=500.0, atr_pct=0.01, ema9=ENTRY - 100, ema21=ENTRY - 150,
        ema50=ENTRY - 200, adx=28.0, boll_upper=ENTRY * 1.02, boll_mid=ENTRY,
        boll_lower=ENTRY * 0.98, roc=1.2, volume_zscore=1.5, relative_volume=2.0,
        buy_volume=10.0, sell_volume=8.0, trade_count=50, avg_trade_size=0.05,
        aggressive_volume=0.3, order_book_imbalance=0.1, microprice=ENTRY,
        spread=1.0, spread_pct=0.00002, bid_depth=10.0, ask_depth=10.0,
        news_sentiment=0.0, news_impact="low", news_age_ms=5000,
        mention_zscore=0.2, regime="trending_up", features_by_category=long_cats,
    )


class TestRoundTrips:
    async def test_position_roundtrip(self):
        repo = SqliteRepository(":memory:")
        pos = _position()
        await repo.save_position(pos)
        loaded = await repo.load_positions()
        assert len(loaded) == 1
        got = loaded[0]
        assert got.position_id == pos.position_id
        assert got.entry_price == pytest.approx(ENTRY)
        assert got.status is PositionStatus.CLOSED
        assert got.close_reason == "take_profit"
        assert got.realized_pnl == pytest.approx(150.0)
        await repo.close()

    async def test_position_status_roundtrip_active(self):
        repo = SqliteRepository(":memory:")
        await repo.save_position(_position("pos-a", PositionStatus.ACTIVE))
        await repo.save_position(_position("pos-b", PositionStatus.ABORTED))
        by_id = {p.position_id: p for p in await repo.load_positions()}
        assert by_id["pos-a"].status is PositionStatus.ACTIVE
        assert by_id["pos-b"].status is PositionStatus.ABORTED
        await repo.close()

    async def test_order_roundtrip_with_fills(self):
        repo = SqliteRepository(":memory:")
        await repo.save_order(_report())
        loaded = await repo.load_orders()
        assert len(loaded) == 1
        got = loaded[0]
        assert got.client_order_id == "ENTRY-1"
        assert got.status == OrderStatus.FILLED.name
        assert len(got.fills) == 1
        assert got.fills[0].price == pytest.approx(ENTRY)
        assert got.fills[0].fee == 0.0
        await repo.close()

    async def test_snapshot_and_event_land_in_events(self):
        repo = SqliteRepository(":memory:")
        await repo.save_feature_snapshot(_snapshot())
        await repo.save_event("test_event", {"foo": 1})
        assert repo.count_rows("events") == 2
        await repo.close()

    async def test_risk_decision_roundtrip(self):
        repo = SqliteRepository(":memory:")
        await repo.save_risk_decision(RiskDecision(
            symbol=SYMBOL, ts_ms=1000, verdict=RiskVerdict.APPROVED.name,
            reason="", position_size=0.1, stop_loss_price=49250.0,
            take_profit_price=51500.0, risk_amount=75.0,
            risk_checks={"max_positions": True},
        ))
        await repo.save_risk_decision(RiskDecision(
            symbol=SYMBOL, ts_ms=1001, verdict=RiskVerdict.REJECTED.name,
            reason="RISK_EXPOSURE_EXCEEDED",
        ))
        assert repo.count_rows("risk_decisions") == 2
        await repo.close()

    async def test_reconciliation_roundtrip(self):
        repo = SqliteRepository(":memory:")
        report = ReconciliationReport((
            ReconciliationIssue("MISSING_STOP", SYMBOL, "no resting stop-loss", "fatal"),
        ))
        await repo.save_reconciliation(report)
        await repo.save_reconciliation(ReconciliationReport())
        rows = repo.reconciliation_rows()
        assert len(rows) == 2
        assert rows[0][1] is False
        assert rows[0][2][0]["kind"] == "MISSING_STOP"
        assert rows[1][1] is True
        await repo.close()


class TestIdempotency:
    async def test_position_upsert(self):
        repo = SqliteRepository(":memory:")
        await repo.save_position(_position())
        closed = _position()
        closed.realized_pnl = 999.0
        closed.close_reason = "manual"
        await repo.save_position(closed)
        assert repo.count_rows("positions") == 1
        got = (await repo.load_positions())[0]
        assert got.realized_pnl == pytest.approx(999.0)
        assert got.close_reason == "manual"
        await repo.close()

    async def test_order_upsert(self):
        repo = SqliteRepository(":memory:")
        await repo.save_order(_report())
        updated = replace(_report(), status=OrderStatus.CANCELED.name,
                          executed_quantity=0.05)
        await repo.save_order(updated)
        assert repo.count_rows("orders") == 1
        got = (await repo.load_orders())[0]
        assert got.status == OrderStatus.CANCELED.name
        await repo.close()

    async def test_duplicate_fills_deduped(self):
        repo = SqliteRepository(":memory:")
        report = _report()
        await repo.save_fills(report.fills)
        await repo.save_fills(report.fills)  # same leg replayed
        row = repo._conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()
        assert int(row["n"]) == 1
        await repo.close()

    async def test_duplicate_fills_deduped_across_calls(self):
        repo = SqliteRepository(":memory:")
        report = _report()
        await repo.save_fills(report.fills)
        report2 = replace(
            report,
            client_order_id="ENTRY-2", order_id="sim-2", ts_ms=1001,
            fills=(Fill(symbol=SYMBOL, client_order_id="ENTRY-2", side="BUY",
                        quantity=0.1, price=ENTRY, ts_ms=1001, order_id="sim-2"),),
        )
        await repo.save_fills(report2.fills)
        row = repo._conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()
        assert int(row["n"]) == 2
        await repo.close()


class TestReopenPersistence:
    async def test_file_db_survives_reopen(self, tmp_path):
        db = tmp_path / "paper.db"
        repo = SqliteRepository(db)
        await repo.save_position(_position())
        await repo.save_order(_report())
        await repo.close()

        repo2 = SqliteRepository(db)
        assert len(await repo2.load_positions()) == 1
        assert len(await repo2.load_orders()) == 1
        assert repo2.count_rows("reconciliations") == 0
        await repo2.close()


class TestEndToEndUnion:
    """The full output of one routed trade wrote to the same file + reload."""

    async def test_routed_trade_persists_to_sqlite_then_reloads(self, tmp_path):
        cfg = ExecutionConfig(retries=0, backoff_base_s=0.01, slippage_pct=0.0)
        from crypto_scalper.execution.order_manager import OrderManager
        from crypto_scalper.execution.position_manager import PositionManager
        from crypto_scalper.execution.simulated import SimulatedExecutionAdapter
        from crypto_scalper.trading.orchestrator import TradeOrchestrator
        from crypto_scalper.config.paper import PaperConfig
        from crypto_scalper.trading.account import PaperAccount

        adapter = SimulatedExecutionAdapter(cfg)
        adapter.set_price(SYMBOL, ENTRY)
        om = OrderManager(adapter, cfg)
        pm = PositionManager(om, cfg)
        router = ExecutionRouter(RiskEngine(RiskConfig(
            risk_per_trade_pct=0.01, max_total_open_risk_pct=0.03,
            max_positions=10, max_leverage=1, daily_loss_limit_pct=0.03,
            max_drawdown_pct=0.10, correlated_group_exposure_cap_pct=0.06,
        )), pm, adapter)
        repo = SqliteRepository(tmp_path / "paper.db")
        orch = TradeOrchestrator(router, SignalEngine(StrategyConfig(enabled=True)),
                                 PaperAccount(10_000.0), repo, order_manager=om,
                                 config=PaperConfig())
        await orch.start()
        try:
            snap = _snapshot()
            outcome = await orch.on_signal(orch._signals.evaluate(snap), snap)
            assert outcome.submitted
            await orch.on_price(SYMBOL, ENTRY + 2000.0)
            import asyncio
            from crypto_scalper.core.enums import PositionStatus as PS
            end = asyncio.get_running_loop().time() + 3
            while outcome.position.status is not PS.CLOSED:
                if asyncio.get_running_loop().time() >= end:
                    raise AssertionError("position did not close")
                await asyncio.sleep(0.02)
            await orch.persist()
            assert repo.count_rows("positions") >= 1
            assert repo.count_rows("orders") >= 2  # entry + tp
            assert repo.count_rows("fills") >= 2
        finally:
            await orch.close()
            await repo.close()

        repo2 = SqliteRepository(tmp_path / "paper.db")
        positions = await repo2.load_positions()
        assert positions and positions[-1].close_reason == "take_profit"
        orders = await repo2.load_orders()
        assert any(o.client_order_id == positions[-1].entry_client_order_id for o in orders)
        await repo2.close()