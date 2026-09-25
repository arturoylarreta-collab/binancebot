"""Durable state mirror on Google Firestore (Firebase free "Spark" plan).

Why: free hosting tiers (e.g. Render Free) have an ephemeral filesystem, so the
local SQLite audit trail disappears on every restart/deploy. The mirror keeps
the small set of records needed to resume and to keep history:

    bots/{bot_id}                    account state (cash, realized, fees, peak,
                                     trades, daily PnL, loss streak) — 1 doc
    bots/{bot_id}/trades/{pos_id}    every closed trade
    bots/{bot_id}/equity/{ts_ms}     equity curve at a coarse cadence

Quota-aware by design (Spark: 20k writes / 50k reads per day):
  * writes are queued and committed in batches (``:commit``, ≤ 500 per call);
  * pending writes to the same document are coalesced, so a state doc updated
    every second costs one write per flush window;
  * equity points default to one every 5 minutes (≈ 288 writes/day).

Never on the hot path: ``put_*`` only enqueues; a single worker talks HTTPS.
If the mirror is misconfigured or Google is unreachable the bot keeps trading
(logged + counted), it just loses durability.

Credentials: ``FIREBASE_SERVICE_ACCOUNT_JSON`` (raw JSON or base64 of the
service-account key file). Project id is read from the key.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from typing import Any, Dict, List, Optional

import aiohttp

from crypto_scalper.monitoring.metrics import METRICS

log = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/datastore"]
_API = "https://firestore.googleapis.com/v1"


# ── Firestore value codec ───────────────────────────────────────────────────


def encode_value(v: Any) -> Dict[str, Any]:
    if v is None:
        return {"nullValue": None}
    if isinstance(v, bool):
        return {"booleanValue": v}
    if isinstance(v, int):
        return {"integerValue": str(v)}
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return {"nullValue": None}
        return {"doubleValue": v}
    if isinstance(v, str):
        return {"stringValue": v}
    if isinstance(v, dict):
        return {"mapValue": {"fields": {str(k): encode_value(x) for k, x in v.items()}}}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [encode_value(x) for x in v]}}
    return {"stringValue": str(v)}


def decode_value(v: Dict[str, Any]) -> Any:
    if "nullValue" in v:
        return None
    if "booleanValue" in v:
        return bool(v["booleanValue"])
    if "integerValue" in v:
        return int(v["integerValue"])
    if "doubleValue" in v:
        return float(v["doubleValue"])
    if "stringValue" in v:
        return v["stringValue"]
    if "timestampValue" in v:
        return v["timestampValue"]
    if "mapValue" in v:
        return {k: decode_value(x) for k, x in v["mapValue"].get("fields", {}).items()}
    if "arrayValue" in v:
        return [decode_value(x) for x in v["arrayValue"].get("values", [])]
    return None


def encode_fields(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k): encode_value(x) for k, x in d.items()}


def decode_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    return {k: decode_value(x) for k, x in (fields or {}).items()}


def parse_service_account(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("empty service account")
    if not raw.startswith("{"):
        raw = base64.b64decode(raw).decode("utf-8")
    info = json.loads(raw)
    for key in ("project_id", "client_email", "private_key"):
        if not info.get(key):
            raise ValueError(f"service account JSON lacks {key}")
    return info


# ── mirror ──────────────────────────────────────────────────────────────────


class FirestoreMirror:
    def __init__(
        self,
        service_account: Dict[str, Any],
        *,
        bot_id: str = "paper",
        flush_interval_s: float = 15.0,
        equity_interval_s: float = 300.0,
        session: Optional[aiohttp.ClientSession] = None,
        token_provider=None,
        api_base: str = _API,
    ) -> None:
        self._api = api_base.rstrip("/")
        self._info = service_account
        self.project = service_account["project_id"]
        self.bot_id = bot_id
        self._flush_interval_s = flush_interval_s
        self.equity_interval_s = equity_interval_s
        self._session = session
        self._own_session = session is None
        self._token_provider = token_provider
        self._creds = None
        self._token: Optional[str] = None
        self._token_exp = 0.0
        self._pending: Dict[str, Dict[str, Any]] = {}   # path → fields (coalesced)
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self.last_error = ""
        self.writes_ok = 0
        self.last_flush_ms = 0

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls, raw_json: str, *, bot_id: str, equity_interval_s: float = 300.0,
                 flush_interval_s: float = 15.0) -> Optional["FirestoreMirror"]:
        if not raw_json:
            return None
        try:
            info = parse_service_account(raw_json)
        except Exception as exc:  # noqa: BLE001 - misconfig must not stop trading
            log.error("firestore disabled: invalid service account", extra={"error": str(exc)})
            return None
        return cls(info, bot_id=bot_id, equity_interval_s=equity_interval_s,
                   flush_interval_s=flush_interval_s)

    @property
    def root(self) -> str:
        return f"projects/{self.project}/databases/(default)/documents"

    def _doc(self, path: str) -> str:
        return f"{self.root}/{path}"

    @property
    def bot_path(self) -> str:
        return f"bots/{self.bot_id}"

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        await self._auth_header()          # fail fast on bad credentials
        self._task = asyncio.create_task(self._worker(), name="firestore-mirror")
        log.info("firestore mirror ready", extra={"project": self.project, "bot": self.bot_id})

    async def close(self, timeout_s: float = 8.0) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        try:
            await asyncio.wait_for(self.flush(), timeout=timeout_s)
        except Exception as exc:  # noqa: BLE001 - shutdown best effort
            log.warning("final firestore flush failed", extra={"error": repr(exc)})
        if self._own_session and self._session is not None:
            await self._session.close()

    # ── writes (non-blocking) ───────────────────────────────────────────────

    def put(self, path: str, data: Dict[str, Any]) -> None:
        self._pending[path] = data
        if len(self._pending) >= 400:
            self._wake.set()

    def put_state(self, state: Dict[str, Any]) -> None:
        self.put(self.bot_path, {**state, "updated_ms": int(time.time() * 1000)})

    def put_trade(self, trade: Dict[str, Any]) -> None:
        self.put(f"{self.bot_path}/trades/{trade['position_id']}", trade)

    def put_equity(self, ts_ms: int, equity: float, drawdown_pct: float,
                   unrealized: float, open_count: int, realized: float) -> None:
        self.put(f"{self.bot_path}/equity/{int(ts_ms)}", {
            "ts_ms": int(ts_ms), "equity": float(equity), "drawdown_pct": float(drawdown_pct),
            "unrealized": float(unrealized), "open_count": int(open_count),
            "realized": float(realized)})

    async def flush(self) -> int:
        if not self._pending:
            return 0
        batch, self._pending = self._pending, {}
        items = list(batch.items())
        sent = 0
        for i in range(0, len(items), 450):
            chunk = items[i:i + 450]
            body = {"writes": [{"update": {"name": self._doc(p), "fields": encode_fields(d)}}
                               for p, d in chunk]}
            try:
                await self._call("POST", f"{self._api}/{self.root}:commit", body)
                sent += len(chunk)
            except Exception as exc:  # noqa: BLE001 - re-queue, retry next window
                for p, d in chunk:
                    self._pending.setdefault(p, d)
                self.last_error = f"{type(exc).__name__}: {exc}"[:200]
                METRICS.incr("firestore.flush_errors")
                log.warning("firestore flush failed", extra={"error": self.last_error,
                                                             "pending": len(self._pending)})
                break
        self.writes_ok += sent
        self.last_flush_ms = int(time.time() * 1000)
        METRICS.incr("firestore.writes", sent)
        return sent

    async def _worker(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._flush_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self.flush()

    # ── reads (startup restore) ─────────────────────────────────────────────

    async def load_state(self) -> Optional[Dict[str, Any]]:
        try:
            data = await self._call("GET", f"{self._api}/{self._doc(self.bot_path)}")
        except FileNotFoundError:
            return None
        return decode_fields(data.get("fields", {}))

    async def load_collection(self, collection: str, *, order_field: str,
                              since: Optional[int] = None, limit: int = 500) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {
            "from": [{"collectionId": collection}],
            "orderBy": [{"field": {"fieldPath": order_field}, "direction": "DESCENDING"}],
            "limit": int(limit),
        }
        if since is not None:
            query["where"] = {"fieldFilter": {"field": {"fieldPath": order_field},
                                              "op": "GREATER_THAN_OR_EQUAL",
                                              "value": {"integerValue": str(int(since))}}}
        rows = await self._call("POST", f"{self._api}/{self._doc(self.bot_path)}:runQuery",
                                {"structuredQuery": query})
        out = [decode_fields(r["document"].get("fields", {})) for r in rows or [] if "document" in r]
        out.reverse()   # chronological
        return out

    # ── transport ───────────────────────────────────────────────────────────

    async def _auth_header(self) -> Dict[str, str]:
        if self._token and time.time() < self._token_exp - 60:
            return {"Authorization": f"Bearer {self._token}"}
        if self._token_provider is not None:
            self._token, self._token_exp = await self._token_provider()
        else:
            self._token, self._token_exp = await asyncio.to_thread(self._refresh_token)
        return {"Authorization": f"Bearer {self._token}"}

    def _refresh_token(self):
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account

        if self._creds is None:
            self._creds = service_account.Credentials.from_service_account_info(
                self._info, scopes=_SCOPES)
        self._creds.refresh(Request())
        exp = self._creds.expiry.timestamp() if self._creds.expiry else time.time() + 3000
        return self._creds.token, exp

    async def _call(self, method: str, url: str, body: Any = None) -> Any:
        assert self._session is not None
        headers = await self._auth_header()
        async with self._session.request(method, url, json=body, headers=headers) as resp:
            text = await resp.text()
            if resp.status == 404:
                raise FileNotFoundError(url.rsplit("/", 1)[-1])
            if resp.status >= 400:
                raise RuntimeError(f"firestore http {resp.status}: {text[:200]}")
            return json.loads(text) if text else {}
