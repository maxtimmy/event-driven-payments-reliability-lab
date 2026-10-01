import hashlib
import json
from dataclasses import dataclass
from sqlalchemy import select
from sqlalchemy.orm import Session
from payments_lab.events import PaymentCreated
from payments_lab.models import OutboxEvent, Payment

class IdempotencyConflict(Exception):
    pass

class PaymentNotFound(Exception):
    pass

@dataclass(frozen=True)
class PaymentInput:
    amount_minor: int
    currency: str

    @property
    def request_hash(self) -> str:
        raw = json.dumps({"amount_minor": self.amount_minor, "currency": self.currency}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

def create_payment(session: Session, command: PaymentInput, idempotency_key: str) -> Payment:
    existing = session.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
    if existing:
        if existing.request_hash != command.request_hash:
            raise IdempotencyConflict
        return existing
    payment = Payment(idempotency_key=idempotency_key, request_hash=command.request_hash, amount_minor=command.amount_minor, currency=command.currency)
    session.add(payment)
    session.flush()
    event = PaymentCreated.new(payment.id, payment.amount_minor, payment.currency)
    session.add(OutboxEvent(id=event.event_id, aggregate_id=payment.id, event_type=event.event_type, schema_version=event.schema_version, payload=event.to_dict()))
    return payment

def authorize_payment(session: Session, payment_id, idempotency_key: str) -> Payment:
    payment = session.get(Payment, payment_id, with_for_update=True)
    if payment is None:
        raise PaymentNotFound
    if payment.status == "authorized":
        if payment.authorization_idempotency_key != idempotency_key:
            raise IdempotencyConflict
        return payment
    payment.status = "authorized"
    payment.aggregate_version = 2
    payment.authorization_idempotency_key = idempotency_key
    event = PaymentCreated.authorized(payment.id, payment.amount_minor, payment.currency)
    session.add(OutboxEvent(id=event.event_id, aggregate_id=payment.id, event_type=event.event_type, schema_version=event.schema_version, payload=event.to_dict()))
    return payment
