from __future__ import annotations

import json
import os
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")
TOXIPROXY_URL = os.getenv("TOXIPROXY_URL", "http://127.0.0.1:8474")


def request(method, path, payload=None, key=None):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    try:
        with urlopen(Request(API_BASE + path, data=data, headers=headers, method=method), timeout=5) as response:
            return response.status, json.loads(response.read() or b"{}")
    except HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def toxiproxy(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    with urlopen(Request(TOXIPROXY_URL + path, data=data, headers={"Content-Type": "application/json"}, method=method), timeout=5) as response:
        return json.loads(response.read() or b"{}")


def wait_for(check, timeout=45):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (HTTPError, URLError, TimeoutError):
            pass
        time.sleep(0.25)
    raise AssertionError(f"condition did not converge; last result: {last!r}")


def reset_faults():
    toxiproxy("POST", "/reset")


def disable_proxy(name):
    reset_faults()
    toxiproxy("PATCH", f"/proxies/{name}", {"enabled": False})


def is_converged(payment_id):
    status, body = request("GET", f"/payments/{payment_id}")
    return status == 200 and body.get("reconciliation") == "matched" and all(
        body.get("projection_versions", {}).get(name) == 2
        for name in ("ledger", "notification", "reconciliation")
    )


def create_and_authorize(run, amount):
    status, created = request("POST", "/payments", {"amount_minor": amount, "currency": "RUB"}, f"{run}-create")
    assert status == 202, (status, created)
    payment_id = created["payment_id"]
    status, authorized = request("POST", f"/payments/{payment_id}/authorize", key=f"{run}-authorize")
    assert status == 202, (status, authorized)
    return payment_id


def main():
    run = uuid4().hex
    wait_for(lambda: request("GET", "/health/ready")[0] == 200)
    lifecycle_id = create_and_authorize(f"{run}-lifecycle", 4242)
    wait_for(lambda: is_converged(lifecycle_id))

    try:
        disable_proxy("postgres-proxy")
        assert request("GET", "/health/live")[0] == 200
        wait_for(lambda: request("GET", "/health/ready")[0] == 503)
        assert request("POST", "/payments", {"amount_minor": 1, "currency": "RUB"}, f"{run}-db") == (503, {"detail": "database unavailable"})
    finally:
        reset_faults()
    wait_for(lambda: request("GET", "/health/ready")[0] == 200)

    try:
        disable_proxy("redpanda-proxy")
        fault_id = create_and_authorize(f"{run}-broker", 8181)
        time.sleep(1)
        _, pending = request("GET", f"/payments/{fault_id}")
        assert any(version < 2 for version in pending["projection_versions"].values())
    finally:
        reset_faults()
    wait_for(lambda: is_converged(fault_id))

    query = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "payments", "-d", "payments", "-Atc", f"select count(*) from ledger_entries where payment_id='{fault_id}';"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert query.stdout.strip() == "1", query.stdout
    print(json.dumps({"lifecycle": "converged-v2", "postgres_outage": "controlled-503-and-recovered", "broker_outage": "pending-then-converged", "ledger_entries_for_fault_payment": 1}))


if __name__ == "__main__":
    main()
