from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID
from confluent_kafka import Consumer
from sqlalchemy import select
from sqlalchemy.orm import Session
from payments_lab.config import Settings
from payments_lab.db import make_engine, make_session_factory
from payments_lab.events import NonRetryableEventError, PaymentCreated
from payments_lab.health import start_worker_health_server
from payments_lab.models import ConsumerAggregateVersion, ConsumerInbox, DlqOutbox, LedgerEntry, NotificationDelivery, Payment, ProcessedEvent, ReconciliationResult

CONSUMERS = {"ledger", "notification", "reconciliation"}

def process_event(session: Session, consumer_name: str, event: PaymentCreated) -> bool:
    if consumer_name not in CONSUMERS:
        raise ValueError("unknown consumer")
    done = session.scalar(select(ProcessedEvent).where(ProcessedEvent.consumer_name == consumer_name, ProcessedEvent.event_id == event.event_id))
    if done:
        return False
    if consumer_name == "ledger":
        ledger = session.scalar(select(LedgerEntry).where(LedgerEntry.payment_id == event.payment_id))
        if event.event_type == "payment.created":
            if ledger is None:
                session.add(LedgerEntry(payment_id=event.payment_id, event_id=event.event_id, amount_minor=event.amount_minor, currency=event.currency, aggregate_version=event.aggregate_version))
        elif ledger is not None:
            ledger.aggregate_version = event.aggregate_version
    elif consumer_name == "notification":
        session.add(NotificationDelivery(payment_id=event.payment_id, event_id=event.event_id, status="recorded", aggregate_version=event.aggregate_version))
    else:
        payment = session.get(Payment, event.payment_id)
        ledger = session.scalar(select(LedgerEntry).where(LedgerEntry.payment_id == event.payment_id))
        actual = ledger.amount_minor if ledger else None
        status = "pending" if ledger is None else ("matched" if payment and actual == payment.amount_minor else "mismatch")
        session.add(ReconciliationResult(payment_id=event.payment_id, event_id=event.event_id, expected_amount_minor=payment.amount_minor if payment else event.amount_minor, actual_amount_minor=actual, status=status, aggregate_version=event.aggregate_version))
    session.add(ProcessedEvent(consumer_name=consumer_name, event_id=event.event_id))
    return True

def _dlq_payload(consumer_name: str, payload: dict, attempts: int, category: str, message: str, now: datetime) -> dict:
    safe_message = re.sub(r"(?i)(token|password|secret)\s*[=:]\s*[^\s,;]+", r"\1=[REDACTED]", message)
    return {"consumer_name": consumer_name, "attempt_count": attempts, "error_category": category, "error_message": safe_message[:500], "failed_at": now.isoformat(), "original_event": payload}

def ingest_event(session: Session, consumer_name: str, payload: dict, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    try:
        event = PaymentCreated.from_dict(payload)
    except NonRetryableEventError as error:
        event_id = UUID(payload["event_id"]) if payload.get("event_id") else UUID(int=0)
        session.add(DlqOutbox(consumer_name=consumer_name, event_id=event_id, payload=_dlq_payload(consumer_name, payload, 1, "non_retryable", str(error), now)))
        return False
    exists = session.scalar(select(ConsumerInbox).where(ConsumerInbox.consumer_name == consumer_name, ConsumerInbox.event_id == event.event_id))
    if exists:
        return False
    session.add(ConsumerInbox(consumer_name=consumer_name, event_id=event.event_id, payment_id=event.payment_id, aggregate_version=event.aggregate_version, payload=payload, status="waiting", next_attempt_at=now))
    return True

def drain_inbox(session: Session, consumer_name: str, now: datetime | None = None, max_attempts: int = 3, backoff_seconds=(0.1, 0.5), handler=process_event) -> int:
    now = now or datetime.now(timezone.utc)
    rows = list(session.scalars(select(ConsumerInbox).where(ConsumerInbox.consumer_name == consumer_name, ConsumerInbox.status.in_(("waiting", "retry")), ConsumerInbox.next_attempt_at <= now).order_by(ConsumerInbox.payment_id, ConsumerInbox.aggregate_version).with_for_update(skip_locked=True)))
    processed = 0
    for row in rows:
        state = session.scalar(select(ConsumerAggregateVersion).where(ConsumerAggregateVersion.consumer_name == consumer_name, ConsumerAggregateVersion.payment_id == row.payment_id))
        last_version = state.last_version if state else 0
        if row.aggregate_version <= last_version:
            row.status = "ignored"
            row.processed_at = now
            continue
        if row.aggregate_version != last_version + 1:
            continue
        event = PaymentCreated.from_dict(row.payload)
        try:
            handler(session, consumer_name, event)
        except Exception as error:
            row.attempt_count += 1
            row.last_error = str(error)[:500]
            if row.attempt_count >= max_attempts:
                row.status = "dead_letter_pending"
                session.add(DlqOutbox(consumer_name=consumer_name, event_id=row.event_id, payload=_dlq_payload(consumer_name, row.payload, row.attempt_count, "transient_exhausted", str(error), now)))
            else:
                row.status = "retry"
                delay = backoff_seconds[min(row.attempt_count - 1, len(backoff_seconds) - 1)]
                row.next_attempt_at = now + timedelta(seconds=delay)
            continue
        if state is None:
            state = ConsumerAggregateVersion(consumer_name=consumer_name, payment_id=row.payment_id, last_version=row.aggregate_version)
            session.add(state)
        else:
            state.last_version = row.aggregate_version
        row.status = "processed"
        row.attempt_count += 1
        row.processed_at = now
        processed += 1
    return processed

def reconcile_pending(session: Session) -> int:
    rows = list(session.scalars(select(ReconciliationResult).where(ReconciliationResult.status == "pending")))
    updated = 0
    for result in rows:
        ledger = session.scalar(select(LedgerEntry).where(LedgerEntry.payment_id == result.payment_id))
        if ledger:
            result.actual_amount_minor = ledger.amount_minor
            result.status = "matched" if ledger.amount_minor == result.expected_amount_minor else "mismatch"
            updated += 1
    return updated

def run(consumer_name: str | None = None) -> None:
    consumer_name = consumer_name or os.environ["CONSUMER_NAME"]
    if consumer_name not in CONSUMERS:
        raise ValueError("CONSUMER_NAME must be ledger, notification or reconciliation")
    settings = Settings.from_env()
    engine = make_engine(settings.database_url, settings.database_connect_timeout)
    factory = make_session_factory(engine)
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": f"payments-{consumer_name}-v1", "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([settings.kafka_topic])
    start_worker_health_server(engine, settings.kafka_bootstrap_servers)
    try:
        while True:
            try:
                message = consumer.poll(1)
                if message is None:
                    with factory.begin() as session:
                        drain_inbox(session, consumer_name)
                        if consumer_name == "reconciliation":
                            reconcile_pending(session)
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                with factory.begin() as session:
                    try:
                        payload = json.loads(message.value())
                    except (TypeError, ValueError):
                        payload = {"raw": message.value().decode(errors="replace") if message.value() else ""}
                    ingest_event(session, consumer_name, payload)
                consumer.commit(message=message, asynchronous=False)
                with factory.begin() as session:
                    drain_inbox(session, consumer_name)
            except Exception:
                time.sleep(settings.worker_recovery_backoff)
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
