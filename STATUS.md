# Status

## Current milestone

Этап 5 завершён: CI и портфельная финализация.

## Implemented

- PostgreSQL schema и Alembic migration для payments, outbox и трёх projections.
- Атомарное создание payment + outbox event.
- Redpanda topic `payments.events.v1`, relay и три независимые consumer groups.
- Idempotency-Key: безопасный повтор и `409` при конфликтующем payload.
- Consumer deduplication через `processed_events`.
- Eventual reconciliation payment ↔ ledger, включая промежуточный `pending`.
- Docker Compose для всего контура и PostgreSQL Testcontainer fixtures.
- Liveness/readiness endpoints для API, relay и consumers; worker readiness проверяет PostgreSQL и Redpanda.
- Lifecycle `payment.created` v1 → `payment.authorized` v2 с идемпотентной authorization command.
- PostgreSQL consumer inbox и отдельное состояние aggregate version для каждого consumer.
- Автоматическое восстановление out-of-order delivery и подавление duplicates/stale events.
- Три попытки transient processing и надёжный consumer-specific DLQ-outbox.
- Три локальных Pact V4 message contracts и provider verification реального serializer.
- Redpanda GenericContainer fixture с main и DLQ topics.
- Toxiproxy 2.12.0 в Compose с отдельными PostgreSQL и Redpanda proxies.
- Именованные fault-профили: latency, timeout, reset-peer, disconnect и bandwidth.
- Ограниченные database connect/pool timeouts и recovery loop для relay/consumers.
- Контролируемый API `503` при недоступной PostgreSQL при сохранении liveness.
- Изолированный Toxiproxy GenericContainer test.
- Конкурентная idempotency для create/authorize и точные money boundaries.
- GitHub Actions с test/coverage artifacts и отдельным Compose fault smoke job.
- Test strategy, evidence report и пять воспроизводимых defect playbooks.

## Verification

- 2026-10-01: 70 tests passed локально, включая PostgreSQL, Redpanda и Toxiproxy Testcontainers.
- 4 Pact V4 tests passed в Python 3.12 container; итого 74 tests.
- Docker Compose — migration `0002`, topic initialization и workers успешно запущены.
- HTTP smoke: payment `4242 RUB` прошёл created → authorized; все projection versions достигли 2, reconciliation — `matched`.
- PostgreSQL smoke подтвердил ровно одну ledger entry после двух lifecycle events.
- PostgreSQL disconnect: liveness остался `200`, readiness и command вернули `503`, после reset API восстановился.
- Redpanda disconnect: created/authorized остались pending, после reset все projections достигли version 2, reconciliation — `matched`, ledger entry — ровно одна.
- Compose и тестовые volumes корректно остановлены и удалены после проверки.
- 2026-10-01: 92 non-Pact cases passed локально; statement coverage — 87%.
- 2026-10-01: полный Python 3.12 прогон, включая 4 Pact V4 cases, — 96 passed.
- Финальный Compose smoke подтвердил lifecycle v2, PostgreSQL `503`/recovery, broker pending/recovery и одну ledger entry.

## Deliberate limitations

- Нет реальных денег, платёжных реквизитов, PCI DSS claims и production reliability claims.
- Нет Pact Broker, cloud deployment, external notification provider и длительных load/soak tests.
