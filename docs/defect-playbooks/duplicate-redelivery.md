# Duplicate and redelivery

1. Create a payment and capture its `payment.created` envelope.
2. Ingest the same `event_id` twice for each consumer, including a replay after simulated publication-transaction rollback.
3. Verify one inbox identity per consumer, one processed-event identity and exactly one ledger row.
4. Verify notification/reconciliation state is unchanged by the duplicate and reconciliation remains `matched`.

Failure signature: a uniqueness error escaping the worker, a second ledger entry, or projection state changing on replay.
