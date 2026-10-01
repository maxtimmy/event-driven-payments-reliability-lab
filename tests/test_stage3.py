from datetime import datetime, timedelta, timezone
import json
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import func, select

from payments_lab.api import create_app
from payments_lab.consumers import drain_inbox, ingest_event
from payments_lab.events import NonRetryableEventError, PaymentCreated
from payments_lab.models import ConsumerAggregateVersion, ConsumerInbox, DlqOutbox, LedgerEntry, NotificationDelivery, OutboxEvent, Payment, ReconciliationResult
from payments_lab.relay import publish_dlq_pending
from payments_lab.service import PaymentInput, create_payment

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

class FakeProducer:
    def __init__(self, error=None):
        self.messages, self.error, self.callback = [], error, None
    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value)); self.callback = on_delivery
    def flush(self, timeout):
        self.callback(self.error, None)

def payment(session_factory, key="stage3"):
    with session_factory.begin() as session:
        return create_payment(session, PaymentInput(2500, "RUB"), key)

def events_for(item):
    created = PaymentCreated.new(item.id, item.amount_minor, item.currency)
    authorized = PaymentCreated.authorized(item.id, item.amount_minor, item.currency, created.correlation_id)
    return created, authorized

@pytest.mark.parametrize("kind", ["created", "authorized"])
def test_versioned_event_roundtrip(kind):
    payment_id = uuid4()
    event = PaymentCreated.new(payment_id, 101, "RUB") if kind == "created" else PaymentCreated.authorized(payment_id, 101, "RUB")
    restored = PaymentCreated.from_dict(json.loads(event.to_json()))
    assert restored == event
    assert restored.aggregate_version == (1 if kind == "created" else 2)

def test_event_reader_tolerates_additional_fields():
    payload = PaymentCreated.new(uuid4(), 1, "RUB").to_dict() | {"future_field": "ignored"}
    assert PaymentCreated.from_dict(payload).amount_minor == 1

@pytest.mark.parametrize("mutation", [
    lambda p: {**p, "event_type": "payment.unknown"},
    lambda p: {**p, "schema_version": 99},
    lambda p: {k: v for k, v in p.items() if k != "event_id"},
])
def test_invalid_contract_is_non_retryable(mutation):
    with pytest.raises(NonRetryableEventError):
        PaymentCreated.from_dict(mutation(PaymentCreated.new(uuid4(), 1, "RUB").to_dict()))

def test_authorize_endpoint_creates_version_two(engine, session_factory):
    client = TestClient(create_app(engine=engine))
    created = client.post("/payments", headers={"Idempotency-Key": "create"}, json={"amount_minor": 500, "currency": "RUB"}).json()
    response = client.post(f"/payments/{created['payment_id']}/authorize", headers={"Idempotency-Key": "authorize"})
    assert response.status_code == 202
    assert response.json()["status"] == "authorized"
    with session_factory() as session:
        item = session.get(Payment, UUID(created["payment_id"]))
        assert item.aggregate_version == 2

def test_authorization_repeat_does_not_create_another_event(engine, session_factory):
    client = TestClient(create_app(engine=engine))
    pid = client.post("/payments", headers={"Idempotency-Key": "c"}, json={"amount_minor": 1, "currency": "RUB"}).json()["payment_id"]
    for _ in range(2):
        assert client.post(f"/payments/{pid}/authorize", headers={"Idempotency-Key": "a"}).status_code == 202
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 2

def test_authorization_different_key_conflicts(engine):
    client = TestClient(create_app(engine=engine))
    pid = client.post("/payments", headers={"Idempotency-Key": "c"}, json={"amount_minor": 1, "currency": "RUB"}).json()["payment_id"]
    assert client.post(f"/payments/{pid}/authorize", headers={"Idempotency-Key": "a1"}).status_code == 202
    assert client.post(f"/payments/{pid}/authorize", headers={"Idempotency-Key": "a2"}).status_code == 409

def test_authorization_unknown_payment_is_404(engine):
    assert TestClient(create_app(engine=engine)).post(f"/payments/{uuid4()}/authorize", headers={"Idempotency-Key": "a"}).status_code == 404

def test_authorization_and_outbox_rollback_together(session_factory):
    item = payment(session_factory)
    with pytest.raises(RuntimeError):
        with session_factory.begin() as session:
            from payments_lab.service import authorize_payment
            authorize_payment(session, item.id, "rollback-auth")
            raise RuntimeError("rollback")
    with session_factory() as session:
        assert session.get(Payment, item.id).status == "accepted"
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1

@pytest.mark.parametrize("consumer", ["ledger", "notification", "reconciliation"])
def test_duplicate_delivery_creates_one_inbox_record(session_factory, consumer):
    event = events_for(payment(session_factory))[0]
    with session_factory.begin() as session:
        assert ingest_event(session, consumer, event.to_dict(), NOW) is True
    with session_factory.begin() as session:
        assert ingest_event(session, consumer, event.to_dict(), NOW) is False
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ConsumerInbox)) == 1

@pytest.mark.parametrize("consumer", ["ledger", "notification", "reconciliation"])
def test_out_of_order_delivery_converges_per_consumer(session_factory, consumer):
    created, authorized = events_for(payment(session_factory))
    with session_factory.begin() as session:
        ingest_event(session, consumer, authorized.to_dict(), NOW)
        assert drain_inbox(session, consumer, NOW) == 0
    with session_factory.begin() as session:
        ingest_event(session, consumer, created.to_dict(), NOW)
        assert drain_inbox(session, consumer, NOW) == 2
    with session_factory() as session:
        version = session.scalar(select(ConsumerAggregateVersion))
        assert version.last_version == 2

def test_authorized_event_never_creates_second_ledger_entry(session_factory):
    created, authorized = events_for(payment(session_factory))
    with session_factory.begin() as session:
        ingest_event(session, "ledger", created.to_dict(), NOW); ingest_event(session, "ledger", authorized.to_dict(), NOW)
        drain_inbox(session, "ledger", NOW)
    with session_factory() as session:
        ledger = session.scalar(select(LedgerEntry))
        assert session.scalar(select(func.count()).select_from(LedgerEntry)) == 1
        assert (ledger.amount_minor, ledger.aggregate_version) == (2500, 2)

def failing_until(target):
    calls = {"count": 0}
    def handler(session, consumer, event):
        calls["count"] += 1
        if calls["count"] < target:
            raise RuntimeError("temporary")
        from payments_lab.consumers import process_event
        return process_event(session, consumer, event)
    return calls, handler

def test_transient_failure_succeeds_on_second_attempt(session_factory):
    event = events_for(payment(session_factory))[0]; calls, handler = failing_until(2)
    with session_factory.begin() as session: ingest_event(session, "ledger", event.to_dict(), NOW); drain_inbox(session, "ledger", NOW, backoff_seconds=(0, 0), handler=handler)
    with session_factory.begin() as session: assert drain_inbox(session, "ledger", NOW, backoff_seconds=(0, 0), handler=handler) == 1
    assert calls["count"] == 2

def test_transient_failure_succeeds_on_third_attempt(session_factory):
    event = events_for(payment(session_factory))[0]; calls, handler = failing_until(3)
    with session_factory.begin() as session: ingest_event(session, "ledger", event.to_dict(), NOW)
    for _ in range(3):
        with session_factory.begin() as session: drain_inbox(session, "ledger", NOW, backoff_seconds=(0, 0), handler=handler)
    assert calls["count"] == 3

def test_exhausted_failure_creates_consumer_specific_dlq(session_factory):
    event = events_for(payment(session_factory))[0]
    def fail(*_): raise RuntimeError("database timeout token=redacted")
    with session_factory.begin() as session: ingest_event(session, "notification", event.to_dict(), NOW)
    for _ in range(3):
        with session_factory.begin() as session: drain_inbox(session, "notification", NOW, backoff_seconds=(0, 0), handler=fail)
    with session_factory() as session:
        dlq = session.scalar(select(DlqOutbox))
        assert (dlq.consumer_name, dlq.payload["attempt_count"], dlq.payload["error_category"]) == ("notification", 3, "transient_exhausted")

@pytest.mark.parametrize("mutation", [lambda p: {**p, "schema_version": 2}, lambda p: {**p, "event_type": "unknown"}])
def test_non_retryable_event_goes_directly_to_dlq(session_factory, mutation):
    payload = mutation(PaymentCreated.new(uuid4(), 1, "RUB").to_dict())
    with session_factory.begin() as session: assert ingest_event(session, "ledger", payload, NOW) is False
    with session_factory() as session:
        dlq = session.scalar(select(DlqOutbox))
        assert dlq.payload["error_category"] == "non_retryable"
        assert dlq.payload["attempt_count"] == 1

def test_dlq_outbox_publishes_to_consumer_topic(session_factory):
    payload = {**PaymentCreated.new(uuid4(), 1, "RUB").to_dict(), "schema_version": 9}
    with session_factory.begin() as session: ingest_event(session, "reconciliation", payload, NOW)
    producer = FakeProducer()
    with session_factory.begin() as session: assert publish_dlq_pending(session, producer) == 1
    assert producer.messages[0][0] == "payments.reconciliation.dlq.v1"
    with session_factory() as session: assert session.scalar(select(DlqOutbox)).publication_status == "published"

def test_dlq_publish_failure_keeps_pending(session_factory):
    payload = {**PaymentCreated.new(uuid4(), 1, "RUB").to_dict(), "schema_version": 9}
    with session_factory.begin() as session: ingest_event(session, "ledger", payload, NOW)
    with pytest.raises(RuntimeError):
        with session_factory.begin() as session: publish_dlq_pending(session, FakeProducer(RuntimeError("broker down")))
    with session_factory() as session: assert session.scalar(select(DlqOutbox)).publication_status == "pending"
