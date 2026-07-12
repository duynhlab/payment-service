# payment-service

Payment service for the duynhlab microservices platform: Stripe-style
PaymentIntents with mandatory idempotency keys, an auth/capture state machine,
partial refunds derived from data, and a deterministic mock provider for
reproducible failure testing.

## Features

- **PaymentIntents** — authorize now, capture later (`capture_method: manual`,
  the default) or auth+capture in one call (`automatic`); authorized holds
  carry a TTL and expire automatically
- **Idempotency keys** — every money-moving request requires an
  `Idempotency-Key` header; the first response is cached and replayed, the
  same key with a different body is rejected, concurrent duplicates are
  serialized, and crashed attempts are recovered from checkpointed progress
- **State machine** — transitions are a whitelist (`pending → authorized →
  captured`, `void ≠ refund`) enforced in the logic layer *and* as a
  database compare-and-swap, so concurrent transitions cannot both win
- **Refunds** — first-class objects, partial and repeatable;
  `partially_refunded` is derived from `SUM(refunds)` rather than stored
- **Deterministic failure triggers** — magic amount suffixes (`…02` generic
  decline, `…95` insufficient funds, `…19` transient error that succeeds on
  retry) make failure paths reproducible in tests and demos

## API Endpoints

| Method | Path | Audience | Description |
|--------|------|----------|-------------|
| POST | `/payment/v1/private/payments` | private (JWT) | Create a PaymentIntent — `Idempotency-Key` required |
| GET | `/payment/v1/private/payments/{id}` | private (JWT) | Fetch one payment (owner-only) |
| GET | `/payment/v1/private/payments` | private (JWT) | Paginated payment history |
| POST | `/payment/v1/internal/payments/{id}/refunds` | internal | Create a (partial) refund — `Idempotency-Key` required |
| POST | `/payment/v1/public/payments/webhooks/mockpay` | public | Provider webhook — the HMAC-signed body is the credential (`Mockpay-Signature: t=…,v1=…`, ±5 min tolerance, fail-closed on empty secret) |
| POST | `/payment/v1/internal/payments/reconciliation/runs` | internal | Trigger one reconciliation pass (single-flighted; 409 when one is running) |
| GET | `/payment/v1/internal/payments/reconciliation/runs/{id}` | internal | Reconciliation run report + discrepancies |
| GET | `/health`, `/ready`, `/metrics` | — | Probes + Prometheus metrics |

Amounts are always **integer minor units** (`2000` = $20.00) with an ISO-4217
currency; payment methods are opaque test tokens — PAN-like data is never
accepted, stored, or logged.

## gRPC (east-west, `:9090`)

The order saga's money transport — `payment.v1.PaymentService`, served
unconditionally on `GRPC_PORT` (reflection off in-cluster):

| RPC | Caller | Purpose |
|-----|--------|---------|
| `Authorize` | order-worker (saga) | Pre-pivot hold — a decline is a business response, not a gRPC error; idempotent by `order:<id>` |
| `Capture` | order-worker (saga) | Capture before the pivot; idempotent |
| `Void` | order-worker (compensation) | Release a pre-capture hold; idempotent |
| `Refund` | order-worker (compensation) | Post-pivot refund; idempotent by `refund:order:<id>` |
| `GetPayment` | order (API) | Payment snapshot by order id for order-details enrichment |

The same shared validators run on both the HTTP and gRPC paths, so the two
surfaces cannot drift.

## Architecture — 3-layer

```
internal/web/v1    Gin handlers: validation, JWT auth, error translation
internal/logic/v1  Business rules: state machine, idempotent flows (owns ports)
internal/core      domain · repository (Postgres/pgx) · provider port · database
```

Strict dependency direction: web → logic → core. The provider port ships with
an in-memory stub; **mockpay** (the deterministic mock provider) is a
subcommand of this binary (`go run ./cmd mockpay`), run as a second deployment.
Background loops: the **transactional-outbox relay** (10 s, single-writer) and
the **reconciliation ticker** (5 min, detect-only; `RECON_HEAL_ENABLED`
flag-gates the one auto-heal class) — both are why the service runs
**single-replica by design**.

## Tech Stack

Go 1.26 · Gin · PostgreSQL (pgx/v5, golang-migrate embedded) ·
OpenTelemetry (traces via shared middleware) · Prometheus RED metrics ·
Pyroscope profiling · shared `github.com/duynhlab/pkg` (authmw, httpx,
migratex, obsx)

## Configuration

| Env | Default | Description |
|-----|---------|-------------|
| `PORT` | `8080` | HTTP listen port |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | — | Postgres (pooled) |
| `DB_POOL_MAX_CONNECTIONS` | `30` | pgx pool cap |
| `AUTH_JWKS_URL` / `JWT_ISSUER` / `JWT_AUDIENCE` | auth defaults | JWT verification (authmw) |
| `AUTH_HOLD_TTL` | `168h` | Authorized-hold expiry |
| `IDEMPOTENCY_KEY_TTL` | `24h` | Key retention before reaping |
| `IDEMPOTENCY_LOCK_TAKEOVER` | `90s` | Stale in-flight lock takeover threshold |
| `MOCKPAY_URL` | `""` | External provider URL (empty = in-memory stub, reconciliation disabled) |
| `MOCKPAY_WEBHOOK_URL` / `MOCKPAY_WEBHOOK_SECRET` | `""` | Where mockpay POSTs signed webhooks + the shared HMAC secret |
| `GRPC_PORT` | `9090` | gRPC listen port (`PaymentService`) |
| `RECON_HEAL_ENABLED` | `false` | Flag-gated reconciliation auto-heal (ADR-012); default detect-only |
| `OTEL_*`, `TRACING_ENABLED`, `PROFILING_ENABLED` | platform defaults | Observability |

## Development

```bash
go build ./...                       # build
go test -race ./...                  # unit tests (no Docker)
golangci-lint run                    # lint (must pass before PR)
go test -tags=integration ./internal/core/repository/...  # repository tests (Docker)

go run ./cmd migrate                 # apply schema
go run ./cmd                         # serve :8080
```

### Pre-push checklist

`go build ./... && go vet ./... && golangci-lint run && go test -race ./...`,
integration tests green, and the local-stack e2e pass (see the platform's
`local-stack/README.md`).
