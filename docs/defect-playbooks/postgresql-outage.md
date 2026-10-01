# PostgreSQL outage

1. Start the clean stack with `make compose-up` and confirm `/health/ready` is `200`.
2. Apply `postgres-disconnect` through `python -m payments_lab.faults apply postgres-disconnect` with `TOXIPROXY_URL=http://localhost:8474`.
3. Verify liveness remains `200`, readiness becomes `503`, and a payment command returns `503` without a partial payment/outbox write.
4. Run `python -m payments_lab.faults reset`; readiness must recover without restarting API, relay or consumers.
5. Clean up with `make compose-down`.

Failure signature: process exits, an unbounded request, a `500`, or a payment without its outbox event.
