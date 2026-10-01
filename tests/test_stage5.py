import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from payments_lab.api import create_app
from payments_lab.consumers import drain_inbox, ingest_event, process_event
from payments_lab.events import PaymentCreated
from payments_lab.models import ConsumerInbox, LedgerEntry, OutboxEvent, Payment
from payments_lab.relay import publish_pending
from payments_lab.service import PaymentInput, create_payment


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class RecordingProducer:
    def __init__(self):
        self.messages = []
        self.callback = None

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self.callback = on_delivery

    def flush(self, _timeout):
        self.callback(None, None)


@pytest.mark.parametrize(
    "payload",
    [
        {"amount_minor": 0, "currency": "RUB"},
        {"amount_minor": -1, "currency": "RUB"},
        {"amount_minor": -10_000, "currency": "RUB"},
        {"amount_minor": 1.5, "currency": "RUB"},
        {"amount_minor": True, "currency": "RUB"},
        {"amount_minor": None, "currency": "RUB"},
        {"amount_minor": 9_223_372_036_854_775_808, "currency": "RUB"},
        {"amount_minor": 1, "currency": "rub"},
        {"amount_minor": 1, "currency": "RU"},
        {"amount_minor": 1, "currency": "RUBLE"},
        {"amount_minor": 1, "currency": None},
    ],
    ids=[
        "zero",
        "negative",
        "large-negative",
        "fractional",
        "boolean",
        "null-amount",
        "bigint-overflow",
        "lowercase-currency",
        "short-currency",
        "long-currency",
        "null-currency",
    ],
)
def test_invalid_money_command_is_rejected_without_writes(engine, session_factory, payload):
    response = TestClient(create_app(engine=engine)).post(
        "/payments", headers={"Idempotency-Key": "invalid"}, json=payload
    )

    assert response.status_code == 422
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Payment)) == 0
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.parametrize(
    "amount_minor,currency",
    [
        (1, "RUB"),
        (9_223_372_036_854_775_807, "RUB"),
        (100, "USD"),
        (100, "EUR"),
        (100, "JPY"),
    ],
    ids=["minimum", "bigint-maximum", "usd", "eur", "jpy"],
)
def test_valid_money_boundaries_are_stored_exactly(engine, session_factory, amount_minor, currency):
    response = TestClient(create_app(engine=engine)).post(
        "/payments",
        headers={"Idempotency-Key": f"valid-{currency}-{amount_minor}"},
        json={"amount_minor": amount_minor, "currency": currency},
    )

    assert response.status_code == 202
    with session_factory() as session:
        payment = session.get(Payment, UUID(response.json()["payment_id"]))
        assert (payment.amount_minor, payment.currency) == (amount_minor, currency)


def test_concurrent_identical_commands_return_one_payment(engine, session_factory):
    app = create_app(engine=engine)

    def submit():
        with TestClient(app) as client:
            return client.post(
                "/payments",
                headers={"Idempotency-Key": "parallel-same"},
                json={"amount_minor": 777, "currency": "RUB"},
            )

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(lambda _: submit(), range(4)))

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Payment)) == 1
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_concurrent_conflicting_commands_have_one_winner(engine, session_factory):
    app = create_app(engine=engine)

    def submit(amount):
        with TestClient(app) as client:
            return client.post(
                "/payments",
                headers={"Idempotency-Key": "parallel-conflict"},
                json={"amount_minor": amount, "currency": "RUB"},
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(submit, (100, 200)))

    assert sorted(response.status_code for response in responses) == [202, 409]
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Payment)) == 1
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_concurrent_authorization_keys_create_one_event(engine, session_factory):
    client = TestClient(create_app(engine=engine))
    payment_id = client.post(
        "/payments",
        headers={"Idempotency-Key": "auth-create"},
        json={"amount_minor": 900, "currency": "RUB"},
    ).json()["payment_id"]

    def authorize(key):
        with TestClient(client.app) as parallel_client:
            return parallel_client.post(
                f"/payments/{payment_id}/authorize", headers={"Idempotency-Key": key}
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(authorize, ("auth-a", "auth-b")))

    assert sorted(response.status_code for response in responses) == [202, 409]
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(OutboxEvent)) == 2


def test_publish_ack_followed_by_transaction_rollback_is_safe_on_redelivery(session_factory):
    with session_factory.begin() as session:
        payment = create_payment(session, PaymentInput(321, "RUB"), "relay-crash-window")
    producer = RecordingProducer()

    with pytest.raises(RuntimeError):
        with session_factory.begin() as session:
            assert publish_pending(session, producer, "payments.events.v1") == 1
            raise RuntimeError("process stopped before outbox commit")
    with session_factory.begin() as session:
        assert publish_pending(session, producer, "payments.events.v1") == 1

    assert len(producer.messages) == 2
    event = PaymentCreated.from_dict(json.loads(producer.messages[0][2]))
    with session_factory.begin() as session:
        assert process_event(session, "ledger", event) is True
    with session_factory.begin() as session:
        assert process_event(session, "ledger", event) is False
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(LedgerEntry)) == 1
        assert session.scalar(select(LedgerEntry)).payment_id == payment.id


def test_inbox_rollback_requires_broker_redelivery(session_factory):
    event = PaymentCreated.new(UUID("11111111-1111-1111-1111-111111111111"), 50, "RUB")

    with pytest.raises(RuntimeError):
        with session_factory.begin() as session:
            ingest_event(session, "ledger", event.to_dict(), NOW)
            raise RuntimeError("database connection lost before commit")
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ConsumerInbox)) == 0

    with session_factory.begin() as session:
        assert ingest_event(session, "ledger", event.to_dict(), NOW) is True
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ConsumerInbox)) == 1


def test_one_consumer_failure_does_not_block_another_projection(session_factory):
    with session_factory.begin() as session:
        payment = create_payment(session, PaymentInput(654, "RUB"), "consumer-isolation")
    event = PaymentCreated.new(payment.id, payment.amount_minor, payment.currency)
    with session_factory.begin() as session:
        ingest_event(session, "ledger", event.to_dict(), NOW)
        ingest_event(session, "notification", event.to_dict(), NOW)

    def fail_ledger(*_args):
        raise RuntimeError("ledger dependency unavailable")

    with session_factory.begin() as session:
        assert drain_inbox(session, "ledger", NOW, handler=fail_ledger) == 0
    with session_factory.begin() as session:
        assert drain_inbox(session, "notification", NOW) == 1

    with session_factory() as session:
        statuses = {
            row.consumer_name: row.status for row in session.scalars(select(ConsumerInbox))
        }
        assert statuses == {"ledger": "retry", "notification": "processed"}
