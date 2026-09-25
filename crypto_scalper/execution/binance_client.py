"""Signed REST client for Binance USD-M Futures (testnet/demo or live).

Responsibilities kept deliberately narrow:
  * HMAC-SHA256 request signing with a server-time offset (no -1021 drift);
  * one typed exception per failure class so the OrderManager can decide
    between retry / lookup / give up (see ``_raise_for``);
  * weight-aware throttling from ``X-MBX-USED-WEIGHT-1M`` and ``Retry-After``
    so 429s never escalate into a 418 IP ban.

Secrets never appear in logs, exceptions or URLs that get logged: the
signature and API key are only ever placed on the wire.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import aiohttp

from crypto_scalper.core.exceptions import (
    AlreadyFlatError,
    DuplicateOrderError,
    ExchangeAuthenticationError,
    ExchangeConnectionError,
    ExchangeError,
    ExchangeRateLimitError,
    ExchangeTimeoutError,
    InvalidOrderError,
    OrderNotFoundError,
    OrderRejectedError,
)
from crypto_scalper.monitoring.metrics import METRICS

log = logging.getLogger(__name__)

# Binance error codes → exception class. Anything unlisted with a 4xx is a
# rejection of THIS request (not retryable); 5xx / network are transient.
_NOT_FOUND = {-2011, -2013}
_AUTH = {-2014, -2015, -1022, -1002}
_RATE = {-1003, -1015}
_TIMEOUT_UNKNOWN = {-1007, -1006}          # "status unknown": look the order up
_DISCONNECTED = {-1001}
_TIMESTAMP = {-1021}
_DUPLICATE = {-4116}
_REDUCE_ONLY_REJECTED = {-2022}
_INVALID = {-1102, -1106, -1111, -1116, -1117, -4015}


class BinanceAPIError(ExchangeError):
    def __init__(self, status: int, code: int, msg: str, path: str) -> None:
        super().__init__(f"{path} http={status} code={code} msg={msg}")
        self.status = status
        self.code = code
        self.msg = msg


class BinanceFuturesClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        *,
        recv_window_ms: int = 5000,
        timeout_s: float = 10.0,
        weight_limit_1m: int = 2400,
    ) -> None:
        if not api_key or not api_secret:
            raise ExchangeAuthenticationError("Binance API key/secret are required")
        self._base = base_url.rstrip("/")
        self._key = api_key
        self._secret = api_secret.encode()
        self._recv_window = int(recv_window_ms)
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._time_offset_ms = 0
        self._last_time_sync = 0.0
        self._weight_limit = weight_limit_1m
        self._used_weight = 0
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    @property
    def base_url(self) -> str:
        return self._base

    @property
    def used_weight(self) -> int:
        return self._used_weight

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"X-MBX-APIKEY": self._key, "User-Agent": "crypto-scalper/0.9"},
            )
        await self.sync_time()

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def sync_time(self) -> None:
        t0 = time.time()
        data = await self.request("GET", "/fapi/v1/time")
        t1 = time.time()
        local_mid = int((t0 + t1) / 2 * 1000)
        self._time_offset_ms = int(data["serverTime"]) - local_mid
        self._last_time_sync = t1
        METRICS.set_gauge("binance.time_offset_ms", self._time_offset_ms)

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    # ── public request API ──────────────────────────────────────────────────

    async def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        signed: bool = False,
    ) -> Any:
        if self._session is None or self._session.closed:
            raise ExchangeConnectionError("client not started")
        if signed and time.time() - self._last_time_sync > 1800:
            await self.sync_time()
        await self._throttle()

        query = {k: _fmt(v) for k, v in (params or {}).items() if v is not None}
        if signed:
            query["recvWindow"] = str(self._recv_window)
            query["timestamp"] = str(self.now_ms())
            qs = urlencode(query)
            sig = hmac.new(self._secret, qs.encode(), hashlib.sha256).hexdigest()
            qs = f"{qs}&signature={sig}"
        else:
            qs = urlencode(query)
        url = f"{self._base}{path}" + (f"?{qs}" if qs else "")

        try:
            async with self._session.request(method, url) as resp:
                self._track_weight(resp)
                text = await resp.text()
                status = resp.status
                retry_after = resp.headers.get("Retry-After")
        except asyncio.TimeoutError as exc:
            METRICS.incr("binance.http_timeout")
            raise ExchangeTimeoutError(f"{method} {path} timed out") from exc
        except aiohttp.ClientError as exc:
            METRICS.incr("binance.http_error")
            raise ExchangeConnectionError(f"{method} {path}: {type(exc).__name__}") from exc

        payload: Any
        try:
            import json
            payload = json.loads(text) if text else {}
        except ValueError:
            payload = {"code": 0, "msg": text[:200]}

        # Some endpoints answer HTTP 200 with {"code": <negative>, "msg": ...}.
        embedded_error = isinstance(payload, dict) and int(payload.get("code", 0) or 0) < 0
        if status < 400 and not embedded_error:
            return payload

        code = int(payload.get("code", 0)) if isinstance(payload, dict) else 0
        msg = str(payload.get("msg", "")) if isinstance(payload, dict) else str(payload)[:200]
        if status in (418, 429) or code in _RATE:
            wait = float(retry_after) if retry_after else (60.0 if status == 418 else 5.0)
            self._blocked_until = time.time() + wait
            log.error("binance rate limited", extra={"status": status, "retry_after_s": wait})
        if code in _TIMESTAMP:
            self._last_time_sync = 0.0  # force a resync on the next signed call
        raise _map_error(status, code, msg, f"{method} {path}")

    # ── internals ───────────────────────────────────────────────────────────

    async def _throttle(self) -> None:
        now = time.time()
        if self._blocked_until > now:
            await asyncio.sleep(self._blocked_until - now)
        if self._used_weight > 0.85 * self._weight_limit:
            METRICS.incr("binance.weight_backoff")
            await asyncio.sleep(1.0)

    def _track_weight(self, resp: aiohttp.ClientResponse) -> None:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT-1m")
        if raw:
            try:
                self._used_weight = int(raw)
                METRICS.set_gauge("binance.used_weight_1m", self._used_weight)
            except ValueError:
                pass


def _map_error(status: int, code: int, msg: str, path: str) -> Exception:
    detail = f"{path} http={status} code={code} msg={msg}"
    if code in _NOT_FOUND:
        return OrderNotFoundError(detail)
    if code in _DUPLICATE:
        return DuplicateOrderError(detail)
    if code in _REDUCE_ONLY_REJECTED:
        return AlreadyFlatError(detail)
    if code in _AUTH or status in (401,):
        return ExchangeAuthenticationError(detail)
    if code in _RATE or status in (418, 429):
        return ExchangeRateLimitError(detail)
    if code in _TIMEOUT_UNKNOWN or status == 408:
        return ExchangeTimeoutError(detail)
    if code in _DISCONNECTED or code in _TIMESTAMP or status >= 500:
        return ExchangeConnectionError(detail)
    if code in _INVALID:
        return InvalidOrderError(detail)
    if 400 <= status < 500:
        return OrderRejectedError(detail)
    return BinanceAPIError(status, code, msg, path)


def _fmt(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)
