import json
import time
from uuid import uuid4
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from payments_lab.events import PaymentCreated

def test_redpanda_fixture_creates_main_and_consumer_dlq_topics(redpanda_bootstrap):
    topics = AdminClient({"bootstrap.servers": redpanda_bootstrap}).list_topics(timeout=5).topics
    assert {"payments.events.v1", "payments.ledger.dlq.v1", "payments.notification.dlq.v1", "payments.reconciliation.dlq.v1"} <= set(topics)

def test_real_broker_preserves_payment_key_and_envelope(redpanda_bootstrap):
    event = PaymentCreated.new(uuid4(), 780, "RUB")
    producer = Producer({"bootstrap.servers": redpanda_bootstrap})
    producer.produce("payments.events.v1", key=str(event.payment_id), value=event.to_json()); producer.flush(5)
    consumer = Consumer({"bootstrap.servers": redpanda_bootstrap, "group.id": f"test-{uuid4()}", "auto.offset.reset": "earliest"})
    consumer.subscribe(["payments.events.v1"])
    deadline, found = time.time() + 10, None
    while time.time() < deadline:
        message = consumer.poll(1)
        if message and not message.error():
            found = message; break
    consumer.close()
    assert found is not None
    assert found.key().decode() == str(event.payment_id)
    assert PaymentCreated.from_dict(json.loads(found.value())).amount_minor == 780

def test_independent_consumer_groups_each_receive_event(redpanda_bootstrap):
    event = PaymentCreated.new(uuid4(), 990, "RUB")
    producer = Producer({"bootstrap.servers": redpanda_bootstrap}); producer.produce("payments.events.v1", key=str(event.payment_id), value=event.to_json()); producer.flush(5)
    received = []
    for _ in range(2):
        consumer = Consumer({"bootstrap.servers": redpanda_bootstrap, "group.id": f"isolated-{uuid4()}", "auto.offset.reset": "earliest"})
        consumer.subscribe(["payments.events.v1"])
        deadline = time.time() + 10
        while time.time() < deadline:
            message = consumer.poll(1)
            if message and not message.error():
                received.append(message.value()); break
        consumer.close()
    assert len(received) == 2
