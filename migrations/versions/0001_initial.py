"""Initial payment and projection tables."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table("payments", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("idempotency_key", sa.String(128), nullable=False, unique=True), sa.Column("request_hash", sa.String(64), nullable=False), sa.Column("amount_minor", sa.BigInteger(), nullable=False), sa.Column("currency", sa.String(3), nullable=False), sa.Column("status", sa.String(32), nullable=False), sa.Column("aggregate_version", sa.Integer(), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("outbox_events", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("event_type", sa.String(64), nullable=False), sa.Column("schema_version", sa.Integer(), nullable=False), sa.Column("payload", postgresql.JSONB(), nullable=False), sa.Column("publication_status", sa.String(16), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("published_at", sa.DateTime(timezone=True)), sa.Index("ix_outbox_events_aggregate_id", "aggregate_id"), sa.Index("ix_outbox_events_publication_status", "publication_status"))
    op.create_table("ledger_entries", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True), sa.Column("amount_minor", sa.BigInteger(), nullable=False), sa.Column("currency", sa.String(3), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("notification_deliveries", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True), sa.Column("status", sa.String(32), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("reconciliation_results", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True), sa.Column("expected_amount_minor", sa.BigInteger(), nullable=False), sa.Column("actual_amount_minor", sa.BigInteger()), sa.Column("status", sa.String(32), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("processed_events", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("consumer_name", sa.String(64), nullable=False), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False), sa.UniqueConstraint("consumer_name", "event_id", name="uq_processed_consumer_event"))

def downgrade() -> None:
    for table in ("processed_events", "reconciliation_results", "notification_deliveries", "ledger_entries", "outbox_events", "payments"):
        op.drop_table(table)
