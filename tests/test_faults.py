import json
from datetime import datetime, timezone
from urllib.error import URLError

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine

from payments_lab.api import create_app
from payments_lab.config import Settings
from payments_lab.consumers import _dlq_payload
from payments_lab import faults


class Response:
    def __init__(self, payload=None):
        self.payload = payload
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def read(self): return json.dumps(self.payload).encode() if self.payload is not None else b""


@pytest.fixture()
def captured(monkeypatch):
    calls = []
    def fake_urlopen(request, timeout):
        calls.append((request.method, request.full_url, json.loads(request.data) if request.data else None, timeout))
        if request.full_url.endswith("/proxies/postgres-proxy"):
            return Response({"listen": "0.0.0.0:15432", "upstream": "postgres:5432", "enabled": True})
        if request.full_url.endswith("/proxies/redpanda-proxy"):
            return Response({"listen": "0.0.0.0:29092", "upstream": "redpanda:29093", "enabled": True})
        return Response()
    monkeypatch.setattr(faults, "urlopen", fake_urlopen)
    return calls


def test_populate_creates_both_dependency_proxies(captured):
    client = faults.ToxiproxyClient("http://toxi")
    proxies = [{"name": "postgres-proxy"}, {"name": "redpanda-proxy"}]
    client.populate(proxies)
    assert captured[0][:3] == ("POST", "http://toxi/populate", proxies)

def test_reset_uses_server_reset_endpoint(captured):
    faults.ToxiproxyClient("http://toxi").reset()
    assert captured[0][0:2] == ("POST", "http://toxi/reset")

@pytest.mark.parametrize("enabled", [True, False])
def test_proxy_can_be_enabled_and_disabled(captured, enabled):
    faults.ToxiproxyClient("http://toxi").set_enabled("postgres-proxy", enabled)
    assert captured[-1][0] == "PATCH"
    assert captured[-1][2]["enabled"] is enabled

@pytest.mark.parametrize("scenario,proxy,toxic_type,attributes", [
    ("postgres-latency", "postgres-proxy", "latency", {"latency": 750, "jitter": 100}),
    ("broker-latency", "redpanda-proxy", "latency", {"latency": 750, "jitter": 100}),
    ("postgres-timeout", "postgres-proxy", "timeout", {"timeout": 1000}),
    ("broker-timeout", "redpanda-proxy", "timeout", {"timeout": 1000}),
    ("postgres-reset", "postgres-proxy", "reset_peer", {"timeout": 0}),
    ("broker-reset", "redpanda-proxy", "reset_peer", {"timeout": 0}),
    ("broker-bandwidth", "redpanda-proxy", "bandwidth", {"rate": 32}),
])
def test_named_fault_profile_is_deterministic(monkeypatch, scenario, proxy, toxic_type, attributes):
    calls = []
    class Client:
        def add_toxic(self, *args): calls.append(args)
    monkeypatch.setattr(faults, "configured_client", lambda: Client())
    faults.apply_scenario(scenario)
    assert calls == [(proxy, scenario, toxic_type, attributes)]

@pytest.mark.parametrize("scenario,proxy", [("postgres-disconnect", "postgres-proxy"), ("broker-disconnect", "redpanda-proxy")])
def test_disconnect_disables_only_target_proxy(monkeypatch, scenario, proxy):
    calls = []
    class Client:
        def set_enabled(self, *args): calls.append(args)
    monkeypatch.setattr(faults, "configured_client", lambda: Client())
    faults.apply_scenario(scenario)
    assert calls == [(proxy, False)]

def test_toxic_can_be_removed_without_resetting_other_proxy(captured):
    faults.ToxiproxyClient("http://toxi").remove_toxic("redpanda-proxy", "latency")
    assert captured[0][0:2] == ("DELETE", "http://toxi/proxies/redpanda-proxy/toxics/latency")

def test_wait_ready_retries_transient_control_api_failure(monkeypatch):
    attempts = {"count": 0}
    def flaky(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3: raise URLError("starting")
        return Response({"version": "2.12.0"})
    monkeypatch.setattr(faults, "urlopen", flaky)
    monkeypatch.setattr(faults.time, "sleep", lambda _: None)
    faults.ToxiproxyClient("http://toxi").wait_ready(1)
    assert attempts["count"] == 3

def test_settings_read_recovery_timeouts_from_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_CONNECT_TIMEOUT", "7")
    monkeypatch.setenv("WORKER_RECOVERY_BACKOFF", "1.25")
    settings = Settings.from_env()
    assert (settings.database_connect_timeout, settings.worker_recovery_backoff) == (7, 1.25)

@pytest.mark.parametrize("message", ["token=abc", "password: hunter2", "SECRET = value"])
def test_dlq_diagnostics_redact_obvious_secrets(message):
    payload = _dlq_payload("ledger", {}, 3, "transient", message, datetime.now(timezone.utc))
    assert "abc" not in payload["error_message"]
    assert "hunter2" not in payload["error_message"]
    assert "value" not in payload["error_message"].lower()
    assert "[REDACTED]" in payload["error_message"]

def unavailable_app():
    engine = create_engine("postgresql+psycopg://payments:payments@127.0.0.1:1/payments", pool_timeout=1, connect_args={"connect_timeout": 1})
    return TestClient(create_app(engine=engine))

def test_database_outage_changes_readiness_but_not_liveness():
    client = unavailable_app()
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 503

def test_database_outage_returns_controlled_503_for_payment_command():
    response = unavailable_app().post("/payments", headers={"Idempotency-Key": "outage"}, json={"amount_minor": 1, "currency": "RUB"})
    assert response.status_code == 503
    assert response.json() == {"detail": "database unavailable"}

def test_unknown_fault_profile_fails_explicitly(monkeypatch):
    monkeypatch.setattr(faults, "configured_client", lambda: object())
    with pytest.raises(KeyError): faults.apply_scenario("not-a-profile")

def test_control_api_error_is_not_silently_ignored(monkeypatch):
    monkeypatch.setattr(faults, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("down")))
    with pytest.raises(URLError): faults.ToxiproxyClient("http://toxi").reset()

def test_default_fault_configuration_is_fast_and_bounded():
    settings = Settings()
    assert settings.database_connect_timeout == 2
    assert 0 < settings.worker_recovery_backoff <= 1

def test_engine_connect_timeout_is_propagated():
    from payments_lab.db import make_engine
    engine = make_engine("postgresql+psycopg://payments:payments@127.0.0.1:1/payments", connect_timeout=4)
    assert engine.pool._timeout == 4
