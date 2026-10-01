# Architecture decisions

## ADR-001: Money representation

Деньги передаются как положительный integer `amount_minor`; `float` запрещён.

## ADR-002: Thin first vertical slice

Этап 1 использует синхронный in-memory broker и repositories в одном процессе. Это позволяет проверить границы и happy path до добавления инфраструктурной сложности. Адаптеры намеренно временные.

## ADR-003: Event identity

Каждое событие имеет UUID `event_id`, UUID `payment_id`, тип, версию aggregate и correlation ID. Эти поля станут основой deduplication и ordering.

## ADR-004: Source of truth

Payment state — источник истины команды; ledger — отдельная финансовая projection. Reconciliation сравнивает их, а не подменяет одно другим.

## ADR-005: Test taxonomy

Тест добавляется только с явно названным риском. Количество тестов не является заменой покрытию failure modes.

## ADR-006: Transactional outbox

Payment и событие фиксируются одной PostgreSQL-транзакцией. Kafka publication происходит отдельным relay; подтверждение брокера предшествует отметке `published`. Возможный повтор после сбоя между этими действиями нейтрализуется consumer deduplication.

## ADR-007: Kafka partitioning and consumer groups

Topic `payments.events.v1` использует `payment_id` как key. Ledger, notification и reconciliation имеют отдельные consumer groups, поэтому каждое событие обрабатывается всеми projections.

## ADR-008: Reconciliation ordering

Reconciliation сравнивает payment с ledger. Если ledger ещё не построен, результат остаётся `pending` и пересчитывается consumer-процессом; порядок доставки между consumer groups не предполагается.

## ADR-009: PostgreSQL consumer inbox

Kafka offset подтверждается после устойчивой записи delivery в inbox. Projection processor применяет события последовательно по aggregate version; будущая версия ждёт закрытия разрыва, stale и duplicate версии не меняют состояние.

## ADR-010: Retry and DLQ

Transient ошибка получает максимум три попытки с конфигурируемым backoff. Исчерпанное или non-retryable событие сохраняется в DLQ-outbox и публикуется в отдельный topic ledger, notification или reconciliation; сообщения об ошибках очищаются от очевидных token/password/secret значений.

## ADR-011: Local Pact V4 contracts

Каждый consumer владеет локальным asynchronous message contract. Producer verification вызывает реальный event serializer. Pact Broker и внешняя публикация не входят в лабораторный этап.

## ADR-012: Dependency-specific fault proxies

PostgreSQL и Redpanda имеют отдельные Toxiproxy endpoints. Это позволяет ломать одну зависимость, не маскируя поведение второй. Migrations и topic initialization намеренно не проходят через proxy, чтобы fault-сценарий проверял runtime, а не ломал подготовку стенда.

## ADR-013: Bounded failure and autonomous recovery

Database connection/pool wait ограничен, API преобразует недоступность PostgreSQL в `503`, а relay и consumers повторяют цикл с коротким конфигурируемым backoff. Liveness описывает жизнь процесса, readiness — доступность его обязательных зависимостей.

## ADR-014: Concurrent command serialization

Concurrent create requests resolve the unique idempotency-key race by reading the winning committed payment and comparing its request hash. Authorization locks the payment row, so competing keys produce one authorized event and one deterministic conflict rather than two events.

## ADR-015: CI evidence policy

GitHub Actions uses Python 3.12 and treats 90 collected cases as a minimum acceptance gate. Coverage is reported, not used as a vanity fail-under threshold. JUnit, XML/HTML coverage and Pact files are workflow artifacts; the workflow never commits generated evidence or publishes contracts externally.
