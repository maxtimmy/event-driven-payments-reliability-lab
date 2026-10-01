# Redpanda outage

1. Start the stack and apply `broker-disconnect` through the Toxiproxy CLI.
2. Create and authorize a payment. The API may accept both because PostgreSQL remains available; projections must remain behind version 2.
3. Confirm pending outbox rows directly in PostgreSQL.
4. Reset Toxiproxy and poll `GET /payments/{id}` until all projection versions are 2 and reconciliation is `matched`.
5. Query `ledger_entries`; exactly one row must exist for the payment. Stop with `make compose-down`.

Failure signature: lost outbox work, worker restart required, duplicate ledger row or non-converging projection.
