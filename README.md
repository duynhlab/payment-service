# payment-service

Payment service for the duynhlab platform (RFC-0010): Stripe-style
PaymentIntents behind mandatory idempotency keys, an auth/capture state
machine, and (from P2) a double-entry ledger, mockpay webhooks, and
reconciliation.

## Layout — 3-layer

```
internal/web/v1    HTTP handlers (Gin) — validation, auth, error translation
internal/logic/v1  Business rules — state machine, idempotent flows
internal/core      domain · repository (Postgres) · provider port · database
```

## Run

```bash
go run ./cmd migrate   # apply schema (golang-migrate via pkg/migratex)
go run ./cmd           # serve :8080 (REST) — /health /ready /metrics
go test -race ./...
```

Design doc: [RFC-0010](https://github.com/duynhlab/homelab/blob/main/docs/proposals/rfc/RFC-0010/README.md)
