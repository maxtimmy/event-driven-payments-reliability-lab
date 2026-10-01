import pytest
from testcontainers.core.container import DockerContainer

from payments_lab.faults import ToxiproxyClient


@pytest.fixture(scope="module")
def toxiproxy():
    with DockerContainer("ghcr.io/shopify/toxiproxy:2.12.0").with_exposed_ports(8474) as container:
        client = ToxiproxyClient(
            f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8474)}"
        )
        client.wait_ready(timeout=20)
        yield client


def test_generic_container_creates_isolated_dependency_proxies(toxiproxy):
    toxiproxy.populate(
        [
            {"name": "postgres-proxy", "listen": "0.0.0.0:15432", "upstream": "127.0.0.1:1"},
            {"name": "redpanda-proxy", "listen": "0.0.0.0:29092", "upstream": "127.0.0.1:2"},
        ]
    )

    postgres = toxiproxy._request("GET", "/proxies/postgres-proxy")
    redpanda = toxiproxy._request("GET", "/proxies/redpanda-proxy")

    assert postgres["upstream"] == "127.0.0.1:1"
    assert redpanda["upstream"] == "127.0.0.1:2"


def test_real_control_api_applies_and_resets_latency(toxiproxy):
    toxiproxy.add_toxic(
        "postgres-proxy",
        "postgres-latency",
        "latency",
        {"latency": 750, "jitter": 100},
    )
    toxic = toxiproxy._request("GET", "/proxies/postgres-proxy/toxics/postgres-latency")
    assert toxic["type"] == "latency"
    assert toxic["attributes"] == {"latency": 750, "jitter": 100}

    toxiproxy.reset()
    assert toxiproxy._request("GET", "/proxies/postgres-proxy/toxics") == []
