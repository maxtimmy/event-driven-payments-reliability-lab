import os
import socket
import subprocess
import sys
import time
import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from confluent_kafka.admin import AdminClient, NewTopic
from payments_lab.db import make_session_factory

@pytest.fixture(scope="session")
def database_url():
    with PostgresContainer("postgres:16.4-alpine", driver="psycopg") as postgres:
        url = postgres.get_connection_url()
        env = {**os.environ, "DATABASE_URL": url}
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True, env=env, capture_output=True)
        yield url

@pytest.fixture()
def engine(database_url):
    engine = create_engine(database_url)
    yield engine
    with engine.begin() as connection:
        for table in ("dlq_outbox", "consumer_aggregate_versions", "consumer_inbox", "processed_events", "reconciliation_results", "notification_deliveries", "ledger_entries", "outbox_events", "payments"):
            connection.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    engine.dispose()

@pytest.fixture()
def session_factory(engine):
    return make_session_factory(engine)

@pytest.fixture(scope="session")
def redpanda_bootstrap():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        host_port = sock.getsockname()[1]
    host = os.getenv("TESTCONTAINERS_HOST_OVERRIDE", "localhost")
    command = ["redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "0.0.0.0:19092", "--advertise-kafka-addr", f"{host}:{host_port}"]
    with DockerContainer("redpandadata/redpanda:v24.2.9").with_bind_ports(19092, host_port).with_command(command) as container:
        bootstrap = f"{host}:{host_port}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        deadline = time.time() + 30
        while True:
            try:
                admin.list_topics(timeout=2)
                break
            except Exception:
                if time.time() >= deadline:
                    raise
                time.sleep(0.25)
        topics = ["payments.events.v1", "payments.ledger.dlq.v1", "payments.notification.dlq.v1", "payments.reconciliation.dlq.v1"]
        futures = admin.create_topics([NewTopic(name, 3, 1) for name in topics])
        for future in futures.values():
            future.result(10)
        yield bootstrap
