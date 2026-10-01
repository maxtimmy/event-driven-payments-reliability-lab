# Test Strategy

## Purpose and risk model

The lab demonstrates how a QA engineer can verify an event-driven payment workflow under duplicate, reordered and failed delivery. It does not process real money, claim PCI DSS compliance or demonstrate production reliability.

The highest risks are a duplicate ledger entry, a lost accepted command, processing aggregate versions out of order, an undetected reconciliation mismatch, contract drift and failure to recover after a dependency outage. Every collected test case must identify one of these risks or a boundary that can trigger it.

## Test levels and oracles

| Level | Dependency boundary | Primary oracle |
|---|---|---|
| API/service | FastAPI plus PostgreSQL | HTTP status and durable rows |
| Integration | PostgreSQL Testcontainer | transaction, uniqueness and projection state |
| Messaging | Redpanda GenericContainer | key, envelope and independent consumer groups |
| Contract | local Pact V4 files | consumer expectations against the real serializer |
| Resilience | Toxiproxy/Compose | observable degradation followed by convergence |

Money is asserted as exact integer minor units. The payment row is the command-side source of truth; ledger is an independent projection. Reconciliation must compare both and end as `matched` or explicitly expose `mismatch`.

## Asynchrony, retry and isolation

- Tests never use a fixed sleep as the success oracle. End-to-end checks poll a visible state with a bounded deadline; short sleeps are used only to prove that a state remains pending during an injected outage.
- Kafka offset safety is represented by durable inbox storage before commit. Inbox rollback requires redelivery; a committed duplicate is ignored by `(consumer_name, event_id)`.
- Aggregate versions are applied sequentially. A future version remains waiting until its gap closes.
- Transient processing gets three deterministic attempts using controlled time. Schema/type failures go directly to the consumer-specific DLQ.
- PostgreSQL, Redpanda and Toxiproxy Testcontainers isolate integration state. Per-test database tables are truncated after each test.

## Contract and fault policy

Pact contracts are local repository artifacts; there is no Pact Broker or publication. Provider verification calls the production event serializer for both event types.

Toxiproxy separates PostgreSQL and Redpanda failure domains. Required profiles cover latency, timeout, peer reset, disconnect and bandwidth reduction. Every manual fault scenario ends with `/reset`, and every CI Compose run ends with volume removal.

## Exit criteria

- At least 90 collected, risk-oriented pytest cases and no mandatory skips.
- All local contracts and provider verification pass on Python 3.12.
- Clean Compose lifecycle reaches aggregate/projection version 2 and `matched` reconciliation.
- PostgreSQL outage produces bounded `503`; broker outage leaves durable pending work; both recover without process restart.
- Exactly one ledger entry exists for a payment after duplicates, retries and recovery.
