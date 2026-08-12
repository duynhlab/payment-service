# payment-service

Authorises, captures, voids and refunds payments, and keeps the double-entry
ledger that explains every movement of money.

## Responsibilities

- **Owns:** payment state, refunds, the ledger, webhook event records,
  idempotency keys, the provider attempt log, and reconciliation runs. Nothing
  else writes payment state or the ledger.
- **Does not own:** orders, carts or checkout sessions; the saga itself — payment
  is a participant the order workflow calls over gRPC; token issuance; and the
  platform manifests, which live in homelab.

## Tech

| Area | Technology |
|------|------------|
| Runtime | Go 1.26 |
| Transports | HTTP (private, internal, and a public webhook) · gRPC (east-west) |
| Data | PostgreSQL |
| Platform libraries | `authmw`, `dbx`, `grpcx`, `httpx`, `idempotency`, `logger/zapx`, `migratex`, `obsx`, `proto` |

Alongside the two servers the process runs background jobs on their own tickers:
reconciliation, doubt resolution, hold expiry, retention reaping, and the outbox
relay.

## API

- **Canonical contract:** [`homelab/docs/api/payments.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/payments.md)
- **Shared conventions:** [`homelab/docs/api/api.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/api.md)
- **Surfaces:** JWT-protected HTTP for customer-facing reads and refunds, a
  cluster-only internal group, an anonymous public webhook authenticated by HMAC,
  and `payment.v1.PaymentService` east-west for the order saga. HTTP `:8080` also
  carries `/health` and `/ready`.

Routes, payloads, error codes and idempotency headers live in the contract, so
there is one place to change when they change.

## Run locally

Prefer the homelab **local-stack** — payment is only interesting with a checkout
in front of it and a provider behind it.

The same binary also runs the mock provider, as a second deployment:

```bash
go run cmd/main.go migrate   # apply schema migrations
go run cmd/main.go           # serve HTTP :8080 + gRPC :9090 + background jobs
go run cmd/main.go mockpay   # run the mock payment provider instead
```

There is no `seed` subcommand — payment ships no demo data.

## Verify

The commands CI runs, so a green local run means a green pipeline:

```bash
go build ./...
go test -race ./...
go test -tags=integration ./internal/core/repository/...   # needs Docker (testcontainers)
golangci-lint run
```

## Docs

- [Canonical contract](https://github.com/duynhlab/homelab/blob/main/docs/api/payments.md)
- [local-stack guide](https://github.com/duynhlab/homelab/blob/main/local-stack/README.md)

## License

MIT
