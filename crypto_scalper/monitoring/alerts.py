"""Async Telegram alerts — observability notifications (fire-and-forget).

Desacoplamiento del hot path: `TelegramNotifier.emit()` NUNCA hace await del
HTTP. Solo hace `put_nowait` síncrono en una `asyncio.Queue` acotada; un único
worker en background drena la cola y hace el POST aiohttp. Si la cola está
llena el evento se descarta y se cuenta (métrica `alerts.dropped_queue_full`),
nunca bloquea al `ExecutionRouter`/`TradeOrchestrator`.

Seam de transporte para tests: `_post()` se puede monkeypatchear; los tests
unitarios jamás hacen HTTP real.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto
from typing import Dict, Optional, Tuple

import aiohttp

from crypto_scalper.monitoring.metrics import METRICS

log = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"


class AlertEvent(Enum):
    TRADE_OPENED = auto()
    TRADE_CLOSED = auto()
    RISK_HALT = auto()
    KILL_SWITCH = auto()
    CRITICAL_ERROR = auto()


def format_message(event: AlertEvent, fields: Dict[str, object]) -> str:
    """Mensaje human-readable para un evento (función pura, testeable)."""
    if event is AlertEvent.TRADE_OPENED:
        return "\n".join([
            "[TRADE_OPENED] {symbol}",
            "Entrada: {entry_price}",
            "Tamanio: {quantity}",
            "SL: {stop_loss} | TP: {take_profit}",
        ]).format(**{k: v for k, v in fields.items()})
    if event is AlertEvent.TRADE_CLOSED:
        return "\n".join([
            "[TRADE_CLOSED] {symbol}",
            "PnL: {pnl_pct}%  ({pnl_usd} USDT)",
            "Razon: {reason}",
        ]).format(**{k: v for k, v in fields.items()})
    if event is AlertEvent.RISK_HALT:
        return "[RISK_HALT] Motivo: {reason}".format(**fields)
    if event is AlertEvent.KILL_SWITCH:
        return "[KILL_SWITCH] Motivo: {reason}".format(**fields)
    if event is AlertEvent.CRITICAL_ERROR:
        return "[CRITICAL_ERROR]\n{content}".format(**fields)
    return str(fields)  # pragma: no cover - unknowable future events


class TelegramNotifier:
    """Cola acotada + worker único; el emisor nunca se bloquea."""

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        enabled: bool = True,
        api_base: str = TELEGRAM_API_BASE,
        http_timeout_s: float = 5.0,
        queue_maxsize: int = 256,
    ) -> None:
        self._token = bot_token
        self._chat_id = chat_id
        self._enabled = bool(enabled) and bool(bot_token) and bool(chat_id)
        self._api_base = api_base.rstrip("/")
        self._timeout = float(http_timeout_s)
        self._queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=int(queue_maxsize))
        self._worker: Optional[asyncio.Task] = None
        self._started = False

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    async def start(self) -> None:
        if not self._enabled or self._started:
            return
        self._started = True
        self._worker = asyncio.create_task(self._run())
        METRICS.incr("alerts.worker_started")

    async def stop(self) -> None:
        """Cancela el worker y drena lo pendiente de forma best-effort."""
        if self._started and self._worker is not None:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
            self._worker = None
        self._started = False
        while True:
            try:
                text = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await self._send_text(text)
            except Exception:  # noqa: BLE001 - best-effort drain
                log.warning("alert drain failed", exc_info=True)
            finally:
                self._queue.task_done()

    # ── non-blocking emitters ──────────────────────────────────────────────

    def emit(self, event: AlertEvent, **fields) -> bool:
        """Encolar sin esperar. Devuelve True si se aceptó, False si se descartó."""
        if not self._enabled:
            return False
        text = format_message(event, fields)
        try:
            self._queue.put_nowait(text)
            METRICS.incr("alerts.enqueued")
        except asyncio.QueueFull:
            METRICS.incr("alerts.dropped_queue_full")
            log.warning("alert dropped (queue full)",
                        extra={"event": event.name})
            return False
        return True

    def notify_trade_opened(self, symbol: str, entry_price: float, quantity: float,
                            stop_loss: float, take_profit: float) -> bool:
        return self.emit(AlertEvent.TRADE_OPENED, symbol=symbol, entry_price=round(entry_price, 6),
                         quantity=round(quantity, 6), stop_loss=round(stop_loss, 6),
                         take_profit=round(take_profit, 6))

    def notify_trade_closed(self, symbol: str, pnl_pct: float, pnl_usd: float,
                            reason: str) -> bool:
        return self.emit(AlertEvent.TRADE_CLOSED, symbol=symbol, pnl_pct=round(pnl_pct, 3),
                         pnl_usd=round(pnl_usd, 4), reason=reason or "unknown")

    def notify_risk_halt(self, reason: str) -> bool:
        return self.emit(AlertEvent.RISK_HALT, reason=reason)

    def notify_kill_switch(self, reason: str) -> bool:
        return self.emit(AlertEvent.KILL_SWITCH, reason=reason)

    def notify_critical_error(self, content: str) -> bool:
        return self.emit(AlertEvent.CRITICAL_ERROR, content=str(content)[:4000])

    # ── worker ──────────────────────────────────────────────────────────────

    async def _run(self) -> None:
        while True:
            try:
                text = await self._queue.get()
            except asyncio.CancelledError:
                raise
            try:
                await self._send_text(text)
            except Exception:  # noqa: BLE001 - un solo fallo no tira el worker
                log.warning("alert send failed", exc_info=True)
            finally:
                self._queue.task_done()

    async def _send_text(self, text: str) -> None:
        url = f"{self._api_base}/bot{self._token}/sendMessage"
        params = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        ok, status = await self._post(url, params)
        if not ok:
            METRICS.incr("alerts.http_failed")
            log.warning("telegram send failed", extra={"status": status})

    async def _post(self, url: str, params: Dict[str, object]) -> Tuple[bool, str]:
        """Seam HTTP: los tests lo sobrescriben para no tocar la red."""
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            async with session.post(url, json=params, timeout=timeout) as resp:
                body = await resp.text()
                return resp.status == 200, body