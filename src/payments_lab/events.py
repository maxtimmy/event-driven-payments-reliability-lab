from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Any, Optional
from uuid import UUID, uuid4

SUPPORTED_EVENT_TYPES = {"payment.created", "payment.authorized"}

class NonRetryableEventError(ValueError):
    pass

@dataclass(frozen=True)
class PaymentEvent:
    event_id: UUID
    event_type: str
    schema_version: int
    payment_id: UUID
    aggregate_version: int
    amount_minor: int
    currency: str
    correlation_id: UUID
    causation_id: Optional[UUID]
    occurred_at: datetime

    @classmethod
    def new(cls, payment_id: UUID, amount_minor: int, currency: str) -> "PaymentEvent":
        return cls(uuid4(), "payment.created", 1, payment_id, 1, amount_minor, currency, uuid4(), None, datetime.now(timezone.utc).replace(microsecond=0))

    @classmethod
    def authorized(cls, payment_id: UUID, amount_minor: int, currency: str, correlation_id: Optional[UUID] = None) -> "PaymentEvent":
        return cls(uuid4(), "payment.authorized", 1, payment_id, 2, amount_minor, currency, correlation_id or uuid4(), None, datetime.now(timezone.utc).replace(microsecond=0))

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("event_id", "payment_id", "correlation_id", "causation_id"):
            value[key] = str(value[key]) if value[key] is not None else None
        value["occurred_at"] = self.occurred_at.isoformat(timespec="seconds")
        return value

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True).encode()

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "PaymentEvent":
        try:
            event_type = value["event_type"]
            schema_version = int(value["schema_version"])
            if event_type not in SUPPORTED_EVENT_TYPES:
                raise NonRetryableEventError(f"unsupported event type: {event_type}")
            if schema_version != 1:
                raise NonRetryableEventError(f"unsupported schema version: {schema_version}")
            return cls(UUID(value["event_id"]), event_type, schema_version, UUID(value["payment_id"]), int(value["aggregate_version"]), int(value["amount_minor"]), value["currency"], UUID(value["correlation_id"]), UUID(value["causation_id"]) if value.get("causation_id") else None, datetime.fromisoformat(value["occurred_at"]))
        except NonRetryableEventError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise NonRetryableEventError(f"invalid event envelope: {error}") from error

PaymentCreated = PaymentEvent
