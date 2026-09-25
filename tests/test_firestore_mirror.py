"""FirestoreMirror against a fake Firestore REST server + engine state round-trip."""

from __future__ import annotations

import base64
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from crypto_scalper.storage.firestore_mirror import (
    FirestoreMirror,
    decode_fields,
    encode_fields,
    parse_service_account,
)

SA = {"project_id": "demo-proj", "client_email": "bot@demo-proj.iam.gserviceaccount.com",
      "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n"}


def _fake_firestore():
    docs = {}
    commits = []

    async def commit(request):
        body = await request.json()
        commits.append(len(body["writes"]))
        for w in body["writes"]:
            name = w["update"]["name"].split("/documents/", 1)[1]
            docs[name] = w["update"]["fields"]
        return web.json_response({"writeResults": [{} for _ in body["writes"]]})

    async def get_doc(request):
        path = request.match_info["path"]
        if path.endswith(":runQuery"):
            return web.Response(status=405)
        if path not in docs:
            return web.json_response({"error": {"code": 404}}, status=404)
        return web.json_response({"name": path, "fields": docs[path]})

    async def run_query(request):
        parent = request.match_info["path"].removesuffix(":runQuery")
        q = (await request.json())["structuredQuery"]
        coll = q["from"][0]["collectionId"]
        field = q["orderBy"][0]["field"]["fieldPath"]
        prefix = f"{parent}/{coll}/"
        rows = [decode_fields(f) for n, f in docs.items()
                if n.startswith(prefix) and "/" not in n[len(prefix):]]
        rows.sort(key=lambda r: r[field], reverse=True)
        rows = rows[: q["limit"]]
        return web.json_response([{"document": {"fields": encode_fields(r)}} for r in rows])

    app = web.Application()
    base = "/v1/projects/demo-proj/databases/(default)/documents"
    app.router.add_post(base + ":commit", commit)
    app.router.add_post(base + "/{path:.+:runQuery}", run_query)
    app.router.add_get(base + "/{path:.+}", get_doc)
    return app, docs, commits


async def _token():
    return "fake-token", 9e18


def test_codec_roundtrip():
    data = {"a": 1, "b": 2.5, "c": "x", "d": True, "e": None, "f": {"g": [1, "h"]}}
    assert decode_fields(encode_fields(data)) == data


def test_service_account_accepts_base64():
    raw = base64.b64encode(json.dumps(SA).encode()).decode()
    assert parse_service_account(raw)["project_id"] == "demo-proj"
    with pytest.raises(ValueError):
        parse_service_account('{"project_id": "p"}')


async def test_batched_coalesced_writes_and_restore():
    app, docs, commits = _fake_firestore()
    server = TestServer(app)
    await server.start_server()
    try:
        m = FirestoreMirror(SA, bot_id="paper", token_provider=_token,
                            api_base=str(server.make_url("/v1")), flush_interval_s=3600)
        await m.start()
        for i in range(50):                      # 50 updates of the same doc …
            m.put_state({"cash": 10_000.0 + i, "trades_closed": i})
        m.put_trade({"position_id": "pos-1", "closed_ts_ms": 2000, "symbol": "BTCUSDT"})
        m.put_trade({"position_id": "pos-2", "closed_ts_ms": 3000, "symbol": "ETHUSDT"})
        m.put_equity(1000, 10_010.0, 0.0, 0.0, 0, 10.0)
        assert await m.flush() == 4              # … cost ONE write
        assert commits == [4]

        state = await m.load_state()
        assert state["cash"] == 10_049.0 and state["trades_closed"] == 49
        trades = await m.load_collection("trades", order_field="closed_ts_ms", limit=10)
        assert [t["position_id"] for t in trades] == ["pos-1", "pos-2"]   # chronological
        await m.close()
    finally:
        await server.close()


async def test_flush_failure_requeues():
    m = FirestoreMirror(SA, bot_id="paper", token_provider=_token,
                        api_base="http://127.0.0.1:9/v1", flush_interval_s=3600)
    await m.start()
    m.put_state({"cash": 1.0})
    assert await m.flush() == 0
    assert m._pending and m.last_error
    await m.close(timeout_s=0.5)


async def test_engine_state_roundtrip():
    from crypto_scalper.config.settings import Settings
    from crypto_scalper.paper.engine import PaperTradingEngine
    from crypto_scalper.strategies.signal_engine import SignalEngine

    async def _src():
        if False:
            yield None

    settings = Settings.load()
    eng = PaperTradingEngine(settings=settings, signal_engine=SignalEngine(settings.strategies),
                             source=_src())
    eng._account.restore(cash=10_250.0, realized_pnl=300.0, fees_paid=50.0, peak_equity=10_400.0)
    eng.orchestrator._trades_closed = 7
    state = eng.export_state()

    fresh = PaperTradingEngine(settings=settings, signal_engine=SignalEngine(settings.strategies),
                               source=_src())
    fresh.restore_state(state)
    assert fresh._account.cash == 10_250.0 and fresh._account.peak_equity == 10_400.0
    assert fresh.orchestrator.trades_closed == 7
