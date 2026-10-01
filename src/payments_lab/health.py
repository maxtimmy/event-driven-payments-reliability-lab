from __future__ import annotations

from collections.abc import Callable
from threading import Thread
from fastapi import FastAPI, HTTPException
from confluent_kafka.admin import AdminClient
from sqlalchemy import text
from sqlalchemy.engine import Engine
import uvicorn

def create_worker_health_app(engine: Engine, kafka_bootstrap_servers: str, broker_check: Callable[[], None] | None = None) -> FastAPI:
    app = FastAPI(title="Payments worker health")

    def default_broker_check() -> None:
        AdminClient({"bootstrap.servers": kafka_bootstrap_servers}).list_topics(timeout=2)

    check_broker = broker_check or default_broker_check

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            check_broker()
        except Exception:
            raise HTTPException(status_code=503, detail="dependency unavailable")
        return {"status": "ready"}

    return app

def start_worker_health_server(engine: Engine, kafka_bootstrap_servers: str, port: int = 8081) -> None:
    app = create_worker_health_app(engine, kafka_bootstrap_servers)
    thread = Thread(target=uvicorn.run, args=(app,), kwargs={"host": "0.0.0.0", "port": port, "log_level": "warning"}, daemon=True)
    thread.start()
