# Plan

## 1. Foundation — complete

- Зафиксировать scope, инварианты и архитектуру.
- Создать FastAPI health endpoints и минимальный вертикальный срез.
- Добавить один сквозной happy-path test.

## 2. Real infrastructure — complete

- PostgreSQL schema и migrations; transactional outbox.
- Redpanda topics и реальные producer/consumers.
- Docker Compose и Testcontainers fixtures.

Реализованы PostgreSQL/Alembic, transactional outbox, Redpanda, отдельные relay/consumers, idempotency key и 15 тестов этапа.

## 3. Contracts and correctness — complete

- Pact contracts и schema compatibility.
- Idempotency, deduplication, ordering, retry и DLQ.
- Проверки ledger, reconciliation и eventual consistency.

Реализованы lifecycle created → authorized, Pact V4 message contracts, PostgreSQL inbox, восстановление порядка, три попытки обработки, consumer-specific DLQ и Redpanda Testcontainer.

## 4. Fault injection — complete

- Toxiproxy: latency, timeout, disconnect, recovery.
- Partial failures и воспроизводимые defect scenarios.

Реализованы отдельные PostgreSQL/Redpanda proxies, профили latency, timeout,
reset-peer, disconnect и bandwidth, а также восстановление API, relay и consumers.

## 5. Portfolio completion — complete

- Довести набор до 90+ риск-ориентированных тестов.
- CI, test strategy, отчёты и финальная документация.

Реализованы 96 pytest/Pact cases, GitHub Actions, coverage/JUnit artifacts,
автоматизированный lifecycle/fault smoke, test strategy, evidence и defect playbooks.
