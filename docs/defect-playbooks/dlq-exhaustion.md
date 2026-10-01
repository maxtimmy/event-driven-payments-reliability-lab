# DLQ exhaustion

1. Persist a valid event in one consumer inbox and inject a handler that always raises a transient error.
2. Advance controlled time through exactly three attempts; no wall-clock backoff is required.
3. Verify the inbox becomes `dead_letter_pending` and one DLQ-outbox row targets only that consumer.
4. Fail DLQ publication once; the row must remain pending. Restore the producer and verify publication to `payments.<consumer>.dlq.v1`.
5. Inspect the envelope: original event, consumer, three attempts, category, bounded/redacted message and timestamp are required.

Failure signature: fewer/more attempts, lost event on DLQ failure, wrong consumer topic or exposed secret-like value.
