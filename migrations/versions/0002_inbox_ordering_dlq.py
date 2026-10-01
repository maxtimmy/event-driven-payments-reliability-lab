"""Add authorization, consumer inbox, ordering state and DLQ outbox."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column("payments", sa.Column("authorization_idempotency_key", sa.String(128), nullable=True))
    op.create_unique_constraint("uq_payments_authorization_key", "payments", ["authorization_idempotency_key"])
    op.add_column("ledger_entries", sa.Column("aggregate_version", sa.Integer(), server_default="1", nullable=False))
    op.add_column("notification_deliveries", sa.Column("aggregate_version", sa.Integer(), server_default="1", nullable=False))
    op.add_column("reconciliation_results", sa.Column("aggregate_version", sa.Integer(), server_default="1", nullable=False))
    op.create_table("consumer_inbox", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("consumer_name", sa.String(64), nullable=False), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("aggregate_version", sa.Integer(), nullable=False), sa.Column("payload", postgresql.JSONB(), nullable=False), sa.Column("status", sa.String(32), nullable=False), sa.Column("attempt_count", sa.Integer(), nullable=False), sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False), sa.Column("last_error", sa.Text()), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("processed_at", sa.DateTime(timezone=True)), sa.UniqueConstraint("consumer_name", "event_id", name="uq_inbox_consumer_event"))
    op.create_index("ix_inbox_consumer", "consumer_inbox", ["consumer_name"])
    op.create_index("ix_inbox_payment", "consumer_inbox", ["payment_id"])
    op.create_index("ix_inbox_status", "consumer_inbox", ["status"])
    op.create_table("consumer_aggregate_versions", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("consumer_name", sa.String(64), nullable=False), sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("last_version", sa.Integer(), nullable=False), sa.UniqueConstraint("consumer_name", "payment_id", name="uq_consumer_aggregate"))
    op.create_table("dlq_outbox", sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True), sa.Column("consumer_name", sa.String(64), nullable=False), sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("payload", postgresql.JSONB(), nullable=False), sa.Column("publication_status", sa.String(16), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False), sa.Column("published_at", sa.DateTime(timezone=True)))
    op.create_index("ix_dlq_consumer", "dlq_outbox", ["consumer_name"])
    op.create_index("ix_dlq_status", "dlq_outbox", ["publication_status"])

def downgrade() -> None:
    op.drop_table("dlq_outbox")
    op.drop_table("consumer_aggregate_versions")
    op.drop_table("consumer_inbox")
    op.drop_column("ledger_entries", "aggregate_version")
    op.drop_column("notification_deliveries", "aggregate_version")
    op.drop_column("reconciliation_results", "aggregate_version")
    op.drop_constraint("uq_payments_authorization_key", "payments", type_="unique")
    op.drop_column("payments", "authorization_idempotency_key")
