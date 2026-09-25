"""REST client for Binance USDⓈ-M Futures (public endpoints).

Weight-aware with a token bucket so batch universe probes stay under the
2400 weight/minute limit. Only public market-data endpoints for FASE 2;
authenticated endpoints arrive with the Execution Engine (FASE 6).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp

from crypto_scalper.core.exceptions import (
    ExchangeConnectionError,
    ExchangeRateLimitError,
    ExchangeTimeoutError,
)


class RateLimiter:
    """Continuous token bucket keyed on Binance request weight.

    Refills at ``capacity/60`` tokens per second up to ``capacity`` (the old
    queue-based version only refilled when EMPTY, so the burst allowance was
    gone after the first minute). ``block_for`` honours ``Retry-After``.
    """

    def __init__(self, capacity_per_minute: int = 2400) -> None:
        self._capacity = float(capacity_per_minute)
        self._tokens = float(capacity_per_minute)
        self._rate = capacity_per_minute / 60.0
        self._last = 0.0
        self._blocked_until = 0.0
        self._lock: Optional[asyncio.Lock] = None
        self._refiller = None

    async def start(self) -> None:
        self._lock = asyncio.Lock()
        self._last = asyncio.get_running_loop().time()

    def block_for(self, seconds: float) -> None:
        loop_t = asyncio.get_running_loop().time()
        self._blocked_until = max(self._blocked_until, loop_t + max(0.0, seconds))

    async def acquire(self, weight: int = 1) -> None:
        assert self._lock is not None, "RateLimiter.start() not called"
        async with self._lock:
            loop = asyncio.get_running_loop()
            while True:
                now = loop.time()
                if now < self._blocked_until:
                    await asyncio.sleep(self._blocked_until - now)
                    continue
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= weight:
                    self._tokens -= weight
                    return
                await asyncio.sleep((weight - self._tokens) / self._rate)

    async def close(self) -> None:
        return None


_DEPTH_WEIGHTS = {5: 2, 10: 2, 20: 5, 50: 5, 100: 10, 500: 20, 1000: 20}


def _agg_trades_weight(limit: int) -> int:
    if limit <= 99:
        return 1
    if limit <= 499:
        return 2
    return 5


def _klines_weight(limit: int) -> int:
    if limit <= 99:
        return 1
    if limit <= 499:
        return 2
    return 5


class BinanceFuturesRest:
    def __init__(
        self,
        base_url: str,
        timeout_s: float = 10.0,
        rate_limiter: Optional[RateLimiter] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._limiter = rate_limiter or RateLimiter()

    async def start(self) -> None:
        await self._limiter.start()
        self._session = aiohttp.ClientSession(timeout=self._timeout)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
        await self._limiter.close()

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None, weight: int = 1) -> Any:
        if self._session is None:
            raise RuntimeError("client not started")
        await self._limiter.acquire(weight)
        url = f"{self._base_url}{path}"
        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 429 or resp.status == 418:
                    # 418 = temporary IP ban (common on shared cloud egress IPs):
                    # stop ALL requests for Retry-After instead of digging deeper.
                    retry_after = float(resp.headers.get("Retry-After") or
                                        (30.0 if resp.status == 418 else 5.0))
                    self._limiter.block_for(min(retry_after, 300.0))
                    raise ExchangeRateLimitError(
                        f"rate limited GET {path}: {resp.status} retry_after={retry_after}s")
                if resp.status == 403:
                    raise ExchangeConnectionError(f"forbidden GET {path}: {resp.status}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise ExchangeConnectionError(f"GET {path} -> {resp.status}: {body[:300]}")
                return await resp.json(content_type=None)
        except asyncio.TimeoutError:
            raise ExchangeTimeoutError(f"timeout GET {path}")
        except aiohttp.ClientError as exc:
            raise ExchangeConnectionError(f"connection error GET {path}: {exc}")

    async def ping(self) -> bool:
        await self._get("/fapi/v1/ping", weight=1)
        return True

    async def server_time(self) -> int:
        data = await self._get("/fapi/v1/time", weight=1)
        return int(data["serverTime"])

    async def exchange_info(self) -> List[Dict[str, Any]]:
        data = await self._get("/fapi/v1/exchangeInfo", weight=1)
        return data["symbols"]

    async def ticker_24h(self) -> List[Dict[str, Any]]:
        return await self._get("/fapi/v1/ticker/24hr", weight=40)

    async def depth(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        weight = _DEPTH_WEIGHTS.get(limit, 20)
        return await self._get("/fapi/v1/depth", {"symbol": symbol, "limit": limit}, weight=weight)

    async def agg_trades(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        return await self._get(
            "/fapi/v1/aggTrades", {"symbol": symbol, "limit": limit}, weight=_agg_trades_weight(limit)
        )

    async def klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[List[Any]]:
        """Kline history. Paging (FASE 8) uses start_time/end_time in ms."""
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        return await self._get(
            "/fapi/v1/klines",
            params,
            weight=_klines_weight(limit),
        )

    def utc_now_ms(self) -> int:
        return int(time.time() * 1000)