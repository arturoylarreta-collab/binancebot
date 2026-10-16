"""In-process runtime state shared by the engine, /healthz and the dashboard.

A single mutable object (no locks needed: everything runs on one event loop)
that answers "is the bot alive and trading right now?" without touching the
database. The HTTP layer only *reads* it; operator actions go through the
orchestrator methods it references.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class RuntimeState:
    venue: str = "paper"
    requested_venue: str = "paper"
    mode: str = "paper"
    symbols: List[str] = field(default_factory=list)
    started_ms: int = field(default_factory=now_ms)
    db_path: str = ""

    engine_alive: bool = False
    engine_error: str = ""
    snapshots_processed: int = 0
    stale_snapshots_dropped: int = 0
    loop_errors: int = 0
    last_error: str = ""
    last_snapshot_ms: Dict[str, int] = field(default_factory=dict)
    last_price: Dict[str, float] = field(default_factory=dict)

    # live references (set by main / engine); never serialized directly
    orchestrator: Any = None
    adapter: Any = None
    ws_manager: Any = None
    mirror: Any = None
    keepalive: Any = None
    states: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    def note(self, text: str) -> None:
        self.notes.append(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z {text}")
        del self.notes[:-20]

    # ── health ──────────────────────────────────────────────────────────────

    def health(self, stale_after_ms: int = 30_000) -> Dict[str, Any]:
        from crypto_scalper.core import clock
        t = clock.now_ms()
        feed_age = {s: t - ts for s, ts in self.last_snapshot_ms.items()}
        books = {}
        for sym, st in self.states.items():
            ob = getattr(st, "orderbook", None)
            books[sym] = {
                "synced": bool(ob.is_synced) if ob is not None else False,
                "resyncs": getattr(ob, "resyncs", 0),
                "data_age_ms": st.data_age_ms(t) if hasattr(st, "data_age_ms") else None,
            }
        ws = self.ws_manager
        ws_info = {
            "connections": getattr(ws, "connected", 0) if ws else 0,
            "last_message_age_s": None,
        }
        if ws is not None and getattr(ws, "last_message_mono", 0):
            ws_info["last_message_age_s"] = round(time.monotonic() - ws.last_message_mono, 1)

        warming = now_ms() - self.started_ms < 120_000
        fresh = bool(feed_age) and min(feed_age.values()) < stale_after_ms
        ok = self.engine_alive and (fresh or warming)
        orch = self.orchestrator
        adapter = self.adapter
        out: Dict[str, Any] = {
            "ok": ok,
            "status": "warming_up" if (warming and not fresh) else ("ok" if ok else "degraded"),
            "venue": self.venue,
            "requested_venue": self.requested_venue,
            "uptime_s": round((now_ms() - self.started_ms) / 1000),
            "clock_offset_ms": clock.offset_ms(),
            "engine_alive": self.engine_alive,
            "engine_error": self.engine_error,
            "paused": bool(getattr(orch, "paused", False)),
            "pause_reason": getattr(orch, "pause_reason", ""),
            "snapshots_processed": self.snapshots_processed,
            "stale_snapshots_dropped": self.stale_snapshots_dropped,
            "loop_errors": self.loop_errors,
            "last_error": self.last_error,
            "feed_age_ms": feed_age,
            "books": books,
            "ws": ws_info,
        }
        out["keepalive"] = dict(self.keepalive) if self.keepalive else None
        m = self.mirror
        out["firestore"] = None if m is None else {
            "project": m.project, "bot_id": m.bot_id, "writes_ok": m.writes_ok,
            "pending": len(m._pending), "last_error": m.last_error,
            "last_flush_ms": m.last_flush_ms,
        }
        if adapter is not None and hasattr(adapter, "stream_connected"):
            out["user_stream_connected"] = bool(adapter.stream_connected)
        return out
