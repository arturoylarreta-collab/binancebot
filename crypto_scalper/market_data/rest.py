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
    """Token-bucket rate limiter keyed on Binance request weight."""

    def __init__(self, capacity_per_minute: int = 2400) -> None:
        self._capacity = capacity_per_minute
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=capacity_per_minute)

    async def start(self) -> None:
        for _ in range(self._capacity):
            self._queue.put_nowait(1.0)
        self._refiller = asyncio.create_task(self._refill())

    async def _refill(self) -> None:
        try:
            while True:
                await asyncio.sleep(60.0 / self._capacity)
                if self._queue.empty():
                    self._queue.put_nowait(1.0)
        except asyncio.CancelledError:
            pass

    async def acquire(self, weight: int = 1) -> None:
        for _ in range(weight):
            await self._queue.get()

    async def close(self) -> None:
        if self._refiller:
            self._refiller.cancel()


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
                    raise ExchangeRateLimitError(f"rate limited GET {path}: {resp.status}")
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

    async def klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> List[List[Any]]:
        return await self._get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            weight=_klines_weight(limit),
        )

    def utc_now_ms(self) -> int:
        return int(time.time() * 1000)