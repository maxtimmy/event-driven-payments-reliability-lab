import json
from pathlib import Path
from uuid import uuid4

import pytest

pact_module = pytest.importorskip("pact", reason="Pact V4 requires Python 3.10+")
from pact import Pact, Verifier, match

from payments_lab.events import PaymentCreated

PACTS = Path(__file__).parent.parent / "pacts"
CONSUMERS = ("ledger-consumer", "notification-consumer", "reconciliation-consumer")

def expected_body(event_type, aggregate_version):
    return {
        "event_id": match.uuid("11111111-1111-4111-8111-111111111111"),
        "event_type": event_type,
        "schema_version": 1,
        "payment_id": match.uuid("22222222-2222-4222-8222-222222222222"),
        "aggregate_version": aggregate_version,
        "amount_minor": match.integer(1250, min=1),
        "currency": match.regex("RUB", regex=r"^[A-Z]{3}$"),
        "correlation_id": match.uuid("33333333-3333-4333-8333-333333333333"),
        "causation_id": None,
        "occurred_at": match.regex("2026-01-01T00:00:00+00:00", regex=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$"),
    }

def make_pact(consumer):
    pact = Pact(consumer, "payment-events-producer").with_specification("V4")
    pact.upon_receiving("payment.created v1", "Async").with_body(expected_body("payment.created", 1), content_type="application/json")
    pact.upon_receiving("payment.authorized v1", "Async").with_body(expected_body("payment.authorized", 2), content_type="application/json")
    return pact

@pytest.mark.parametrize("consumer", CONSUMERS)
def test_consumer_contract_accepts_versioned_payment_events(consumer):
    pact = make_pact(consumer)
    seen = []
    def handler(contents, metadata):
        payload = json.loads(contents)
        seen.append(PaymentCreated.from_dict(payload).event_type)
    pact.verify(handler, "Async")
    PACTS.mkdir(exist_ok=True)
    pact.write_file(PACTS, overwrite=True)
    assert seen == ["payment.created", "payment.authorized"]

def test_provider_serializer_satisfies_all_consumer_contracts():
    PACTS.mkdir(exist_ok=True)
    for consumer in CONSUMERS:
        make_pact(consumer).write_file(PACTS, overwrite=True)
    payment_id = uuid4()
    handlers = {
        "payment.created v1": lambda: {"contents": PaymentCreated.new(payment_id, 1250, "RUB").to_json(), "content_type": "application/json"},
        "payment.authorized v1": lambda: {"contents": PaymentCreated.authorized(payment_id, 1250, "RUB").to_json(), "content_type": "application/json"},
    }
    Verifier("payment-events-producer").message_handler(handlers).add_source(str(PACTS)).verify()
