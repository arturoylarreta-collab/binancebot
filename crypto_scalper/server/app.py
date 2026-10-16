"""aiohttp application served from inside the bot process.

  GET  /healthz              liveness/readiness for the platform (no auth)
  GET  /                     web dashboard (single page, polls the API)
  GET  /api/live             in-memory state: account, open positions, signals, health
  GET  /api/equity?hours=24  equity curve from heartbeats
  GET  /api/trades?limit=50  closed trades (net of fees)
  GET  /api/stats            performance statistics
  GET  /api/rejections       why signals did not trade (last 24h)
  GET  /api/signals?symbol=  signal score history
  POST /api/control/{pause|resume|flatten}   operator actions (Bearer token)

Why in-process: one service, one SQLite file on one disk, and the dashboard
can show live state (unrealized PnL, feed health) that is never in the DB.
Reads use a separate read-only SQLite connection in a worker thread, so the
dashboard can never block or lock the trading loop.
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import math
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aiohttp import web

from crypto_scalper.config.settings import Settings
from crypto_scalper.core.enums import PositionStatus
from crypto_scalper.monitoring import dashboard_data as dd
from crypto_scalper.monitoring.runtime import RuntimeState

log = logging.getLogger(__name__)

_STATIC = Path(__file__).resolve().parent.parent / "dashboard" / "static"
_RUNTIME_KEY = web.AppKey("runtime", RuntimeState)
_SETTINGS_KEY = web.AppKey("settings", Settings)


def _json(data: Any, status: int = 200) -> web.Response:
    return web.json_response(data, status=status, dumps=lambda o: json.dumps(o, default=_default))


def _default(o: Any) -> Any:
    if isinstance(o, float) and (math.isinf(o) or math.isnan(o)):
        return None
    if hasattr(o, "name"):
        return o.name
    return str(o)


async def _db(request: web.Request, fn: Callable, *args, **kwargs) -> Any:
    runtime: RuntimeState = request.app[_RUNTIME_KEY]
    path = runtime.db_path
    if not path or not Path(path).exists():
        return None

    def _run():
        conn = dd.connect(path)
        try:
            return fn(conn, *args, **kwargs)
        finally:
            conn.close()

    return await asyncio.to_thread(_run)


# ── middleware ──────────────────────────────────────────────────────────────


def _auth_middleware(password: str):
    @web.middleware
    async def mw(request: web.Request, handler):
        if not password or request.path == "/healthz":
            return await handler(request)
        header = request.headers.get("Authorization", "")
        if header.startswith("Basic "):
            try:
                _user, _, supplied = base64.b64decode(header[6:]).decode().partition(":")
            except (ValueError, UnicodeDecodeError):
                supplied = ""
            if hmac.compare_digest(supplied, password):
                return await handler(request)
        return web.Response(status=401, headers={"WWW-Authenticate": 'Basic realm="crypto-scalper"'})
    return mw


@web.middleware
async def _errors_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - dashboard errors never reach the bot
        log.exception("http handler failed", extra={"path": request.path})
        return _json({"error": type(exc).__name__, "detail": str(exc)[:200]}, status=500)


# ── handlers ────────────────────────────────────────────────────────────────


async def healthz(request: web.Request) -> web.Response:
    health = request.app[_RUNTIME_KEY].health()
    alive = health["engine_alive"] or health["status"] == "warming_up"
    return _json(health, status=200 if alive else 503)


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(_STATIC / "index.html", headers={"Cache-Control": "no-cache"})


async def live(request: web.Request) -> web.Response:
    runtime: RuntimeState = request.app[_RUNTIME_KEY]
    settings: Settings = request.app[_SETTINGS_KEY]
    orch = runtime.orchestrator
    adapter = runtime.adapter
    out: Dict[str, Any] = {"ts_ms": int(time.time() * 1000), "health": runtime.health()}
    if orch is not None:
        summary = orch.summary().to_dict()
        summary["net_realized_pnl"] = summary["realized_pnl"] - summary["fees_paid"]
        summary["start_equity"] = orch.account.start_equity
        summary["return_pct"] = (summary["equity"] / orch.account.start_equity - 1.0) \
            if orch.account.start_equity else 0.0
        out["summary"] = summary
        get_price = getattr(adapter, "get_price", lambda s: None)
        positions = []
        for p in orch.open_positions():
            px = get_price(p.symbol) or p.entry_price
            sign = 1.0 if p.side == "BUY" else -1.0
            unreal = (px - p.entry_price) * sign * p.quantity
            span = abs(p.take_profit_price - p.stop_loss_price) or 1.0
            progress = ((px - p.stop_loss_price) / span) if sign > 0 else ((p.stop_loss_price - px) / span)
            positions.append({
                "position_id": p.position_id, "symbol": p.symbol,
                "side": "LONG" if p.side == "BUY" else "SHORT",
                "quantity": p.quantity, "entry_price": p.entry_price, "mark": px,
                "stop_loss_price": p.stop_loss_price, "take_profit_price": p.take_profit_price,
                "unrealized": unreal, "notional": p.quantity * px,
                "progress": max(0.0, min(1.0, progress)),
                "status": p.status.name if isinstance(p.status, PositionStatus) else str(p.status),
                "opened_ts_ms": p.opened_ts_ms,
                "age_s": max(0, (out["ts_ms"] - p.opened_ts_ms) // 1000) if p.opened_ts_ms else 0,
            })
        out["positions"] = positions
        out["last_signal"] = orch.last_signal
    if adapter is not None and hasattr(adapter, "account_snapshot"):
        out["venue_account"] = adapter.account_snapshot()
    out["prices"] = runtime.last_price
    out["config"] = {
        "venue": runtime.venue,
        "requested_venue": runtime.requested_venue,
        "symbols": runtime.symbols,
        "risk_per_trade_pct": settings.risk.risk_per_trade_pct,
        "max_leverage": settings.risk.max_leverage,
        "daily_loss_limit_pct": settings.risk.daily_loss_limit_pct,
        "max_drawdown_pct": settings.risk.max_drawdown_pct,
        "min_stop_pct": settings.risk.min_stop_pct,
        "rr_ratio": settings.execution.default_rr_ratio,
        "long_threshold": settings.strategies.long_threshold,
        "short_threshold": settings.strategies.short_threshold,
        "control_enabled": bool(settings.server.control_token),
    }
    out["notes"] = runtime.notes[-8:]
    return _json(out)


async def equity(request: web.Request) -> web.Response:
    hours = float(request.query.get("hours", "24"))
    since = int(time.time() * 1000 - hours * 3_600_000) if hours > 0 else 0
    return _json(await _db(request, dd.equity_series, since) or [])


async def trades(request: web.Request) -> web.Response:
    limit = max(1, min(500, int(request.query.get("limit", "50"))))
    return _json(await _db(request, dd.closed_trades, limit) or [])


async def stats(request: web.Request) -> web.Response:
    return _json(await _db(request, dd.performance_stats) or {})


async def rejections(request: web.Request) -> web.Response:
    since = int(time.time() * 1000 - 24 * 3_600_000)
    return _json(await _db(request, dd.rejection_breakdown, since) or [])


async def signals(request: web.Request) -> web.Response:
    symbol = request.query.get("symbol", "")
    hours = float(request.query.get("hours", "6"))
    since = int(time.time() * 1000 - hours * 3_600_000)
    if symbol:
        return _json(await _db(request, dd.signal_series, symbol.upper(), since) or [])
    return _json(await _db(request, dd.recent_signals, 30) or [])


async def reconciliations(request: web.Request) -> web.Response:
    return _json(await _db(request, dd.recent_reconciliations, 5) or [])


async def control(request: web.Request) -> web.Response:
    settings: Settings = request.app[_SETTINGS_KEY]
    token = settings.server.control_token
    if not token:
        return _json({"error": "control disabled (set DASHBOARD_CONTROL_TOKEN)"}, status=403)
    supplied = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(supplied, token):
        return _json({"error": "invalid token"}, status=401)
    runtime: RuntimeState = request.app[_RUNTIME_KEY]
    orch = runtime.orchestrator
    if orch is None:
        return _json({"error": "engine not running"}, status=409)
    action = request.match_info["action"]
    if action == "pause":
        orch.pause("operator")
        runtime.note("paused by operator")
        return _json({"ok": True, "paused": True})
    if action == "resume":
        orch.resume()
        runtime.note("resumed by operator")
        return _json({"ok": True, "paused": False})
    if action == "flatten":
        orch.pause("operator_flatten")
        closed = await orch.flatten_all()
        runtime.note(f"flatten: closed {closed} position(s); bot paused")
        return _json({"ok": True, "closed": closed, "paused": True})
    return _json({"error": f"unknown action {action}"}, status=404)


def build_app(settings: Settings, runtime: RuntimeState) -> web.Application:
    app = web.Application(middlewares=[_errors_middleware,
                                       _auth_middleware(settings.server.dashboard_password)])
    app[_RUNTIME_KEY] = runtime
    app[_SETTINGS_KEY] = settings
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", index)
    app.router.add_get("/api/live", live)
    app.router.add_get("/api/equity", equity)
    app.router.add_get("/api/trades", trades)
    app.router.add_get("/api/stats", stats)
    app.router.add_get("/api/rejections", rejections)
    app.router.add_get("/api/signals", signals)
    app.router.add_get("/api/reconciliations", reconciliations)
    app.router.add_post("/api/control/{action}", control)
    if _STATIC.exists():
        app.router.add_static("/static/", _STATIC, show_index=False)
    return app


async def start_server(settings: Settings, runtime: RuntimeState) -> Optional[web.AppRunner]:
    if not settings.server.enabled:
        return None
    runner = web.AppRunner(build_app(settings, runtime), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, settings.server.host, settings.server.port)
    await site.start()
    log.info("http server listening", extra={"host": settings.server.host,
                                              "port": settings.server.port})
    return runner
