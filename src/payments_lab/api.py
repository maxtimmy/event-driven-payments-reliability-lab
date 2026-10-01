from __future__ import annotations

from uuid import UUID
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, StrictInt
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from payments_lab.config import Settings
from payments_lab.db import make_engine, make_session_factory
from payments_lab.models import LedgerEntry, NotificationDelivery, Payment, ReconciliationResult
from payments_lab.service import IdempotencyConflict, PaymentInput, PaymentNotFound, authorize_payment, create_payment

class PaymentCommand(BaseModel):
    amount_minor: StrictInt = Field(gt=0, le=9_223_372_036_854_775_807)
    currency: str = Field(pattern=r"^[A-Z]{3}$")

def create_app(settings: Settings | None = None, engine: Engine | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    engine = engine or make_engine(settings.database_url, settings.database_connect_timeout)
    factory = make_session_factory(engine)
    app = FastAPI(title="Event Driven Payments Reliability Lab")
    app.state.engine = engine

    @app.exception_handler(SQLAlchemyError)
    async def dependency_unavailable(_request, _error):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})

    def get_session():
        with factory() as session:
            yield session

    @app.get("/health/live")
    def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def readiness() -> dict[str, str]:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:
            raise HTTPException(status_code=503, detail="database unavailable")
        return {"status": "ready"}

    @app.post("/payments", status_code=202)
    def post_payment(command: PaymentCommand, idempotency_key: str = Header(min_length=1, max_length=128, alias="Idempotency-Key"), session: Session = Depends(get_session)) -> dict[str, str]:
        try:
            with session.begin():
                payment = create_payment(session, PaymentInput(command.amount_minor, command.currency), idempotency_key)
        except IntegrityError:
            session.rollback()
            payment = session.scalar(select(Payment).where(Payment.idempotency_key == idempotency_key))
            if payment is None or payment.request_hash != PaymentInput(command.amount_minor, command.currency).request_hash:
                raise HTTPException(status_code=409, detail="idempotency key reused with different payload")
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail="idempotency key reused with different payload")
        return {"payment_id": str(payment.id), "status": payment.status}

    @app.get("/payments/{payment_id}")
    def get_payment(payment_id: UUID, session: Session = Depends(get_session)) -> dict[str, object]:
        payment = session.get(Payment, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="payment not found")
        ledger = session.scalar(select(LedgerEntry).where(LedgerEntry.payment_id == payment_id))
        notification = session.scalar(select(NotificationDelivery).where(NotificationDelivery.payment_id == payment_id).order_by(NotificationDelivery.aggregate_version.desc()).limit(1))
        reconciliation = session.scalar(select(ReconciliationResult).where(ReconciliationResult.payment_id == payment_id).order_by(ReconciliationResult.aggregate_version.desc()).limit(1))
        return {"payment_id": str(payment.id), "amount_minor": payment.amount_minor, "currency": payment.currency, "status": payment.status, "aggregate_version": payment.aggregate_version, "ledger": "applied" if ledger else "pending", "notification": notification.status if notification else "pending", "reconciliation": reconciliation.status if reconciliation else "pending", "projection_versions": {"ledger": ledger.aggregate_version if ledger else 0, "notification": notification.aggregate_version if notification else 0, "reconciliation": reconciliation.aggregate_version if reconciliation else 0}}

    @app.post("/payments/{payment_id}/authorize", status_code=202)
    def authorize(payment_id: UUID, idempotency_key: str = Header(min_length=1, max_length=128, alias="Idempotency-Key"), session: Session = Depends(get_session)) -> dict[str, str]:
        try:
            with session.begin():
                payment = authorize_payment(session, payment_id, idempotency_key)
        except PaymentNotFound:
            raise HTTPException(status_code=404, detail="payment not found")
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail="authorization idempotency key conflict")
        return {"payment_id": str(payment.id), "status": payment.status}

    return app

app = create_app()
