from __future__ import annotations

import argparse
import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ToxiproxyClient:
    def __init__(self, base_url: str, timeout: float = 3) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = Request(self.base_url + path, data=data, method=method, headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            body = response.read()
            return json.loads(body) if body else None

    def wait_ready(self, timeout: float = 30) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                self._request("GET", "/version")
                return
            except (OSError, HTTPError, URLError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)

    def populate(self, proxies: list[dict]) -> None:
        self._request("POST", "/populate", proxies)

    def reset(self) -> None:
        self._request("POST", "/reset")

    def set_enabled(self, proxy: str, enabled: bool) -> None:
        current = self._request("GET", f"/proxies/{proxy}")
        self._request("PATCH", f"/proxies/{proxy}", {"listen": current["listen"], "upstream": current["upstream"], "enabled": enabled})

    def add_toxic(self, proxy: str, name: str, toxic_type: str, attributes: dict, stream: str = "downstream", toxicity: float = 1.0) -> None:
        self._request("POST", f"/proxies/{proxy}/toxics", {"name": name, "type": toxic_type, "stream": stream, "toxicity": toxicity, "attributes": attributes})

    def remove_toxic(self, proxy: str, name: str) -> None:
        self._request("DELETE", f"/proxies/{proxy}/toxics/{name}")


def configured_client() -> ToxiproxyClient:
    return ToxiproxyClient(os.getenv("TOXIPROXY_URL", "http://localhost:8474"))


def initialize() -> None:
    client = configured_client()
    client.wait_ready()
    client.populate([
        {"name": "postgres-proxy", "listen": "0.0.0.0:15432", "upstream": os.getenv("POSTGRES_UPSTREAM", "postgres:5432"), "enabled": True},
        {"name": "redpanda-proxy", "listen": "0.0.0.0:29092", "upstream": os.getenv("REDPANDA_UPSTREAM", "redpanda:29093"), "enabled": True},
    ])


def apply_scenario(name: str) -> None:
    client = configured_client()
    profiles = {
        "postgres-latency": ("postgres-proxy", "latency", {"latency": 750, "jitter": 100}),
        "postgres-timeout": ("postgres-proxy", "timeout", {"timeout": 1000}),
        "postgres-reset": ("postgres-proxy", "reset_peer", {"timeout": 0}),
        "broker-latency": ("redpanda-proxy", "latency", {"latency": 750, "jitter": 100}),
        "broker-timeout": ("redpanda-proxy", "timeout", {"timeout": 1000}),
        "broker-reset": ("redpanda-proxy", "reset_peer", {"timeout": 0}),
        "broker-bandwidth": ("redpanda-proxy", "bandwidth", {"rate": 32}),
    }
    if name.endswith("disconnect"):
        client.set_enabled("postgres-proxy" if name.startswith("postgres") else "redpanda-proxy", False)
        return
    proxy, toxic_type, attributes = profiles[name]
    client.add_toxic(proxy, name, toxic_type, attributes)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reproducible fault scenarios for the payments lab")
    parser.add_argument("action", choices=("init", "reset", "apply"))
    parser.add_argument("scenario", nargs="?")
    args = parser.parse_args()
    client = configured_client()
    if args.action == "init":
        initialize()
    elif args.action == "reset":
        client.reset()
    elif not args.scenario:
        parser.error("scenario is required for apply")
    else:
        apply_scenario(args.scenario)


if __name__ == "__main__":
    main()
