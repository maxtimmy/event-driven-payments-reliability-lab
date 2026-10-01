from __future__ import annotations

from datetime import datetime, timezone
import time
from confluent_kafka import Producer
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from payments_lab.config import Settings
from payments_lab.db import make_engine, make_session_factory
from payments_lab.events import PaymentCreated
from payments_lab.health import start_worker_health_server
from payments_lab.models import DlqOutbox, OutboxEvent

def publish_pending(session: Session, producer: Producer, topic: str, limit: int = 100) -> int:
    events = list(session.scalars(select(OutboxEvent).where(OutboxEvent.publication_status == "pending").order_by(OutboxEvent.created_at).limit(limit).with_for_update(skip_locked=True)))
    published = 0
    for row in events:
        event = PaymentCreated.from_dict(row.payload)
        error = []
        producer.produce(topic, key=str(row.aggregate_id).encode(), value=event.to_json(), on_delivery=lambda err, _msg: error.append(err) if err else None)
        producer.flush(10)
        if error:
            raise RuntimeError(str(error[0]))
        row.publication_status = "published"
        row.published_at = datetime.now(timezone.utc)
        published += 1
    return published

def publish_dlq_pending(session: Session, producer: Producer, limit: int = 100) -> int:
    rows = list(session.scalars(select(DlqOutbox).where(DlqOutbox.publication_status == "pending").order_by(DlqOutbox.created_at).limit(limit).with_for_update(skip_locked=True)))
    published = 0
    for row in rows:
        errors = []
        topic = f"payments.{row.consumer_name}.dlq.v1"
        producer.produce(topic, key=str(row.event_id).encode(), value=__import__("json").dumps(row.payload, separators=(",", ":"), sort_keys=True).encode(), on_delivery=lambda err, _msg: errors.append(err) if err else None)
        producer.flush(10)
        if errors:
            raise RuntimeError(str(errors[0]))
        row.publication_status = "published"
        row.published_at = datetime.now(timezone.utc)
        published += 1
    return published

def run() -> None:
    settings = Settings.from_env()
    engine = make_engine(settings.database_url, settings.database_connect_timeout)
    factory = make_session_factory(engine)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "enable.idempotence": True})
    start_worker_health_server(engine, settings.kafka_bootstrap_servers)
    while True:
        try:
            with factory.begin() as session:
                count = publish_pending(session, producer, settings.kafka_topic)
                count += publish_dlq_pending(session, producer)
            if not count:
                time.sleep(0.25)
        except Exception:
            time.sleep(settings.worker_recovery_backoff)

if __name__ == "__main__":
    run()
