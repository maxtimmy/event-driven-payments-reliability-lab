# Verification Evidence

Verified on 2026-10-01. This file records reproducible lab evidence, not a production reliability claim.

## Automated result

- 92 non-Pact pytest cases passed on local Python 3.9 with PostgreSQL, Redpanda and Toxiproxy Testcontainers.
- 4 Pact V4 cases cover three consumer contracts and provider verification.
- Full Python 3.12 run passed all 96 cases; minimum CI gate: 90.
- Statement coverage from the non-Pact local run: 87% (571 statements, 73 missed). Coverage is reported without a fail-under threshold.
- Static compilation and `docker compose config -q` passed.

Reproduce with:

```bash
python scripts/check_test_count.py
make verify
docker compose config -q
```

## Risk-to-evidence map

| Risk | Evidence |
|---|---|
| Lost payment/outbox half-write | transaction rollback and migration tests |
| Duplicate financial effect | duplicate/redelivery, relay crash-window and unique ledger tests |
| Concurrent idempotency race | parallel identical/conflicting create and authorization tests |
| Out-of-order delivery | version 2-before-1 cases for all three consumers |
| Poison event/retry exhaustion | deterministic retry and consumer-specific DLQ tests |
| Contract drift | three Pact asynchronous contracts plus real serializer verification |
| Dependency outage | Toxiproxy profiles, readiness/503 tests and Compose fault smoke |
| Precision/validation error | strict integer, bigint boundaries and currency-format cases |

## Compose evidence

The clean-stack smoke executed `created → authorized`, reached projection version 2 for ledger, notification and reconciliation, and ended with `matched` reconciliation.

During PostgreSQL disconnect, API liveness remained available while readiness and the command returned a controlled `503`; reset restored the API without a process restart. During Redpanda disconnect, commands remained durable and projections stayed pending; reset converged all projections. Direct PostgreSQL verification found exactly one ledger entry for the fault-test payment.

Local port 8000 was already occupied by an unrelated project, so the final local smoke exposed the lab API on port 18000. The checked-in GitHub Actions job runs the unchanged standard port 8000 on an isolated runner.

## Boundaries

No repository was created or published. CI has been validated through local compilation, Compose configuration and the exact scripts it invokes; its hosted execution will only exist after the user publishes the repository and enables GitHub Actions. There is no real-money processing, payment-card data, PCI DSS claim, production SLO claim, Pact Broker, cloud deployment or load/soak result.
