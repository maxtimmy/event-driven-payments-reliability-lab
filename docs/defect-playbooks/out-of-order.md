# Out-of-order delivery

1. Create matching version 1 and version 2 envelopes for one payment.
2. Ingest version 2 first for ledger, notification and reconciliation; it must remain `waiting` and produce no projection.
3. Ingest version 1 and drain the inbox.
4. Verify both versions apply in order, every aggregate state reaches version 2, ledger remains a single row and reconciliation becomes `matched` after ledger is visible.

Failure signature: version 2 applied before version 1, a stuck gap after version 1 arrives, or a second financial entry.
