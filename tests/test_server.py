"""Embedded HTTP server: health, JSON API, auth and operator controls."""

from __future__ import annotations

import dataclasses

from aiohttp.test_utils import TestClient, TestServer

from crypto_scalper.config.settings import Settings
from crypto_scalper.config.venue import ServerConfig
from crypto_scalper.monitoring.runtime import RuntimeState
from crypto_scalper.server.app import build_app


class _Orch:
    paused = False
    pause_reason = ""

    def __init__(self):
        self.flattened = 0

    def pause(self, reason):
        self.paused, self.pause_reason = True, reason

    def resume(self):
        self.paused = False

    async def flatten_all(self):
        self.flattened += 1
        return 0


def _settings(**server):
    s = Settings.load()
    return dataclasses.replace(s, server=ServerConfig(**server))


async def _client(settings, runtime):
    client = TestClient(TestServer(build_app(settings, runtime)))
    await client.start_server()
    return client


async def test_healthz_reports_engine_state(tmp_path):
    rt = RuntimeState(db_path=str(tmp_path / "none.db"))
    c = await _client(_settings(), rt)
    try:
        rt.engine_alive = True
        r = await c.get("/healthz")
        assert r.status == 200 and (await r.json())["engine_alive"] is True
        rt.engine_alive = False
        rt.started_ms -= 10 * 60_000   # past warm-up
        assert (await c.get("/healthz")).status == 503
    finally:
        await c.close()


async def test_dashboard_and_api_without_db(tmp_path):
    rt = RuntimeState(db_path=str(tmp_path / "missing.db"))
    c = await _client(_settings(), rt)
    try:
        assert (await c.get("/")).status == 200
        assert await (await c.get("/api/trades")).json() == []
        live = await (await c.get("/api/live")).json()
        assert live["config"]["venue"] == "paper"
    finally:
        await c.close()


async def test_basic_auth_protects_everything_but_healthz(tmp_path):
    rt = RuntimeState(db_path=str(tmp_path / "x.db"))
    c = await _client(_settings(dashboard_password="pw"), rt)
    try:
        assert (await c.get("/api/live")).status == 401
        assert (await c.get("/healthz")).status in (200, 503)
        import base64
        auth = {"Authorization": "Basic " + base64.b64encode(b"admin:pw").decode()}
        assert (await c.get("/api/live", headers=auth)).status == 200
    finally:
        await c.close()


async def test_controls_require_token(tmp_path):
    rt = RuntimeState(db_path=str(tmp_path / "x.db"))
    rt.orchestrator = _Orch()
    c = await _client(_settings(control_token="tok"), rt)
    try:
        assert (await c.post("/api/control/pause")).status == 401
        r = await c.post("/api/control/pause", headers={"Authorization": "Bearer tok"})
        assert r.status == 200 and rt.orchestrator.paused
        r = await c.post("/api/control/flatten", headers={"Authorization": "Bearer tok"})
        assert r.status == 200 and rt.orchestrator.flattened == 1
    finally:
        await c.close()
    c = await _client(_settings(), rt)   # no token configured → disabled
    try:
        assert (await c.post("/api/control/resume", headers={"Authorization": "Bearer x"})).status == 403
    finally:
        await c.close()
