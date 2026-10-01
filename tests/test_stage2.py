import json
from uuid import UUID, uuid4
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import inspect, select
from payments_lab.api import create_app
from payments_lab.consumers import process_event, reconcile_pending
from payments_lab.events import PaymentCreated
from payments_lab.health import create_worker_health_app
from payments_lab.models import LedgerEntry, NotificationDelivery, OutboxEvent, Payment, ProcessedEvent, ReconciliationResult
from payments_lab.relay import publish_pending
from payments_lab.service import IdempotencyConflict, PaymentInput, create_payment

class FakeProducer:
    def __init__(self, error=None):
        self.messages = []
        self.error = error
        self.callback = None
    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self.callback = on_delivery
    def flush(self, timeout):
        self.callback(self.error, None)

def stored_payment(session_factory, key="key-1"):
    with session_factory.begin() as session:
        return create_payment(session, PaymentInput(1250, "RUB"), key)

def event_for(payment):
    return PaymentCreated.new(payment.id, payment.amount_minor, payment.currency)

def test_migration_creates_all_required_tables(engine):
    expected = {"payments", "outbox_events", "ledger_entries", "notification_deliveries", "reconciliation_results", "processed_events"}
    assert expected <= set(inspect(engine).get_table_names())

def test_payment_and_outbox_are_created_atomically(session_factory):
    payment = stored_payment(session_factory)
    with session_factory() as session:
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment.id))
        assert event.payload["amount_minor"] == 1250
        assert event.publication_status == "pending"

def test_rollback_leaves_neither_payment_nor_event(session_factory):
    with pytest.raises(RuntimeError):
        with session_factory.begin() as session:
            create_payment(session, PaymentInput(500, "RUB"), "rollback")
            raise RuntimeError("forced rollback")
    with session_factory() as session:
        assert session.scalar(select(Payment).where(Payment.idempotency_key == "rollback")) is None
        assert session.scalar(select(OutboxEvent)) is None

def test_same_idempotency_key_and_payload_returns_original(session_factory):
    first = stored_payment(session_factory)
    with session_factory.begin() as session:
        second = create_payment(session, PaymentInput(1250, "RUB"), "key-1")
        assert second.id == first.id
    with session_factory() as session:
        assert len(list(session.scalars(select(Payment)))) == 1
        assert len(list(session.scalars(select(OutboxEvent)))) == 1

def test_same_idempotency_key_with_different_payload_conflicts(session_factory):
    stored_payment(session_factory)
    with pytest.raises(IdempotencyConflict):
        with session_factory.begin() as session:
            create_payment(session, PaymentInput(1251, "RUB"), "key-1")

def test_relay_publishes_envelope_and_marks_outbox(session_factory):
    payment = stored_payment(session_factory)
    producer = FakeProducer()
    with session_factory.begin() as session:
        assert publish_pending(session, producer, "payments.events.v1") == 1
    topic, key, raw = producer.messages[0]
    payload = json.loads(raw)
    assert topic == "payments.events.v1"
    assert key.decode() == str(payment.id)
    assert payload["event_type"] == "payment.created"
    assert payload["schema_version"] == 1
    with session_factory() as session:
        assert session.scalar(select(OutboxEvent)).publication_status == "published"

def test_relay_failure_does_not_mark_event_published(session_factory):
    stored_payment(session_factory)
    with pytest.raises(RuntimeError):
        with session_factory.begin() as session:
            publish_pending(session, FakeProducer(RuntimeError("broker down")), "payments.events.v1")
    with session_factory() as session:
        assert session.scalar(select(OutboxEvent)).publication_status == "pending"

@pytest.mark.parametrize("name,model", [("ledger", LedgerEntry), ("notification", NotificationDelivery)])
def test_each_consumer_creates_its_projection(session_factory, name, model):
    payment = stored_payment(session_factory)
    with session_factory.begin() as session:
        assert process_event(session, name, event_for(payment)) is True
    with session_factory() as session:
        assert session.scalar(select(model)) is not None
        assert session.scalar(select(ProcessedEvent).where(ProcessedEvent.consumer_name == name)) is not None

def test_consumer_redelivery_is_idempotent(session_factory):
    payment = stored_payment(session_factory)
    event = event_for(payment)
    with session_factory.begin() as session:
        assert process_event(session, "ledger", event) is True
    with session_factory.begin() as session:
        assert process_event(session, "ledger", event) is False
    with session_factory() as session:
        assert len(list(session.scalars(select(LedgerEntry)))) == 1

def test_pending_reconciliation_converges_after_ledger(session_factory):
    payment = stored_payment(session_factory)
    event = event_for(payment)
    with session_factory.begin() as session:
        process_event(session, "reconciliation", event)
    with session_factory.begin() as session:
        process_event(session, "ledger", event)
        assert reconcile_pending(session) == 1
    with session_factory() as session:
        result = session.scalar(select(ReconciliationResult))
        assert (result.status, result.actual_amount_minor) == ("matched", 1250)

def test_api_exposes_pending_then_consistent_state(engine, session_factory):
    client = TestClient(create_app(engine=engine))
    response = client.post("/payments", headers={"Idempotency-Key": "api-key"}, json={"amount_minor": 999, "currency": "RUB"})
    payment_id = response.json()["payment_id"]
    assert response.status_code == 202
    assert client.get(f"/payments/{payment_id}").json()["ledger"] == "pending"
    with session_factory() as session:
        payment = session.get(Payment, UUID(payment_id))
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.aggregate_id == payment.id))
        decoded = PaymentCreated.from_dict(event.payload)
    for name in ("ledger", "notification", "reconciliation"):
        with session_factory.begin() as session:
            process_event(session, name, decoded)
    body = client.get(f"/payments/{payment_id}").json()
    assert (body["ledger"], body["notification"], body["reconciliation"]) == ("applied", "recorded", "matched")

def test_api_returns_409_for_conflicting_reuse(engine):
    client = TestClient(create_app(engine=engine))
    headers = {"Idempotency-Key": "conflict"}
    assert client.post("/payments", headers=headers, json={"amount_minor": 1, "currency": "RUB"}).status_code == 202
    assert client.post("/payments", headers=headers, json={"amount_minor": 2, "currency": "RUB"}).status_code == 409

def test_unknown_payment_returns_404(engine):
    assert TestClient(create_app(engine=engine)).get(f"/payments/{uuid4()}").status_code == 404

def test_health_reports_database_readiness(engine):
    client = TestClient(create_app(engine=engine))
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").json() == {"status": "ready"}
    worker = TestClient(create_worker_health_app(engine, "unused", broker_check=lambda: None))
    assert worker.get("/health/live").status_code == 200
    assert worker.get("/health/ready").json() == {"status": "ready"}
    failed = TestClient(create_worker_health_app(engine, "unused", broker_check=lambda: (_ for _ in ()).throw(RuntimeError("broker down"))))
    assert failed.get("/health/ready").status_code == 503
