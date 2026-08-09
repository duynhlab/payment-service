# AGENTS.md

Agent-focused guide for `payment-service`. Keep changes minimal, verified against
the code, and consistent with existing patterns.

## Authority and scope

This repository implements the service. It does **not** define the contract.

- **Canonical contract:** [`homelab/docs/api/payments.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/payments.md)
- **Shared API rules:** [`homelab/docs/api/api.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/api.md)

Implement against those files. When this repository and the contract disagree,
**stop and classify the mismatch** using
[Resolving a mismatch](https://github.com/duynhlab/homelab/blob/main/docs/api/README.md#resolving-a-mismatch)
before changing either side. One class — an implementation that violates the
intended contract — **blocks the release tag**. In this repository that rule is
not paperwork: the contract is what other services rely on when they move money.

No route, RPC, payload or error inventory belongs in this file. Manifests,
gateway routing, NetworkPolicy and platform observability belong to
[duynhlab/homelab](https://github.com/duynhlab/homelab).

## Contribution workflow

- Never push to `main`. Branch, then open a PR.
- Branch names use conventional prefixes: `feat/`, `fix/`, `docs/`, `chore/`,
  `refactor/`, `test/`.
- One logical change per PR. PRs are merged with squash.
- Commit subject: imperative mood, capitalised, no trailing period, ≤ 50 chars.
- Commit body (only when non-trivial): explain *what* and *why*, wrap at 72 chars,
  one blank line after the subject.
- No attribution trailers (`Signed-off-by`, `Co-authored-by`, `Generated-by`, …).
- No issue references (`Fixes #123`) and no `@`-mentions in commit messages.

## Build, test, lint

These are the commands CI runs, so a green local run means a green pipeline.

```bash
go build ./...
go vet ./...
go test -race ./...
go test -tags=integration ./internal/core/repository/...   # needs Docker (testcontainers)
golangci-lint run
```

Local development against an unreleased `pkg`: `pkg` is one module per package,
so its root has no `go.mod` and a single `replace github.com/duynhlab/pkg` can no
longer resolve. Use one commented `replace` line per module — the trailer in
`go.mod` shows the shape, and
[`docs/api/pkg.md`](https://github.com/duynhlab/homelab/blob/main/docs/api/pkg.md)
explains why.

## Architecture boundaries

**3-layer, dependencies flow one way only: transport → logic → core.**

| Layer | Location | Role |
|-------|----------|------|
| Web | `internal/web/v1/` | HTTP transport; JWT on the private group, HMAC on the webhook |
| gRPC | `internal/grpc/v1/` | East-west money RPCs for the order saga |
| Logic | `internal/logic/v1/` | State machine, idempotency, reconciliation, outbox relay |
| Core | `internal/core/` | Domain, repositories, provider port and client |

The process also runs background jobs on their own tickers — reconciliation,
doubt resolution, hold expiry, retention reaping, and the outbox relay. They
share the pool with the request path, which is why shutdown order matters below.

Observability is wired once through `github.com/duynhlab/pkg/obsx`; the pool comes
from `github.com/duynhlab/pkg/dbx`; the gRPC server is built by
`github.com/duynhlab/pkg/grpcx`; HTTP responses use the shared
`github.com/duynhlab/pkg/httpx` envelope; the idempotency store is
`github.com/duynhlab/pkg/idempotency`.

## Invariants

This service moves money. Each rule below exists because breaking it charges
someone twice, loses a refund, or hides a discrepancy.

- **Money is integer minor units, with one hard cap defined in the logic layer**
  so HTTP and gRPC share a single definition and cannot drift apart. An
  over-limit amount is **rejected, never clamped**.
- **A card number must never be accepted, stored or echoed.** The payment method
  is an opaque token. The validator counts total digits rather than the longest
  run, precisely so a PAN cannot be smuggled through inside a token-shaped
  string.
- **The user id comes from JWT claims, never the body**, and reads are scoped by
  owner in SQL. An unscoped lookup is legitimate **only** on the cluster-only
  internal route; using it anywhere reachable from the edge is an IDOR.
- **The state machine is a whitelist *and* a database compare-and-swap.** The
  whitelist gives the good error message; the CAS gives the guarantee. Keep both
  — a lost CAS is a real concurrent writer, not a validation slip.
- **`processing` is doubt, not a verdict.** It is resolved only by learning what
  the provider actually did, never by guessing, and reaching it must never
  trigger the semantic opposite of the operation in doubt.
- **A parked intent must not re-charge.** The provider key is derived from the
  caller's key, so charging on that path would mint a fresh charge for every
  retry that arrives under a new `Idempotency-Key`.
- **Only a succeeded refund may be sealed under an idempotency key.** The caller
  cannot mint a different key for the same refund, so sealing a failure would
  make the money unsendable.
- **Releasing a key runs detached from the request context.** The canonical
  reason to be there is a provider timeout, so the request context is already
  dead; use a fresh context with its own small budget.
- **The webhook fails closed.** An empty signing secret is rejected outright,
  because HMAC with a zero key is publicly computable and would turn the endpoint
  into accept-anything. Comparison is constant-time and timestamp-bounded, and
  only signature, timestamp or infrastructure failures may answer non-2xx.
- **The attempt log is deliberately not transactional with the state change** —
  failing to record what happened must never block the thing that happened. Its
  open-doubt query is deliberately unwindowed, because doubt about money must not
  age out of view.
- **Double entry is enforced at write time:** at least two legs, debits equal
  credits, every amount positive. The ledger imbalance must always be zero.
- **HTTP and gRPC error mappings must agree.** A past bug had one transport
  answer an opaque 500 where its twin answered a precise status for the same
  condition. An unknown outcome answers **503**, not 500, because the same key is
  safe to retry.
- **Reconciliation is detect-only by default,** and auto-heal never calls the
  provider — it converges through the repository's own idempotent path.
- **Single-replica is a migration constraint, not a design one.** The lease makes
  the reconciler a single writer across processes, but one migration is not
  rolling-safe, and that is what pins the replica count.
- **Pooler-safe database settings live in `pkg/dbx`.** One DSN serves the app and
  migrations so both connect identically.
- **Shutdown order is load-bearing:** stop gRPC, stop the background jobs and wait
  for them, and only then close the pool — a tick landing after the pool closes
  acquires from a closed pool. `run()` owns the defers so the process can exit
  non-zero without skipping cleanup.

## Repository map

- `cmd/main.go` — wiring, subcommand dispatch, background jobs, observability, shutdown
- `config/config.go` — env config, `Validate()`, `BuildDSN()`
- `internal/web/v1/` — HTTP: router, webhook handler, reconciliation handler
- `internal/grpc/v1/` — the `PaymentService` implementation
- `internal/logic/v1/` — service, reconciliation, doubt resolution, healing, outbox relay, webhook, metrics, validation
- `internal/core/domain/` — payment, refund, attempt, outbox, lease, reconciliation, errors
- `internal/core/repository/` — payment, ledger, attempt, outbox, webhook, reconciliation, lease
- `internal/core/provider/` — the provider port, the HTTP client, and an in-memory stub
- `internal/mockpay/` — the mock provider served by the `mockpay` subcommand
- `internal/webhooksig/` — HMAC sign and verify
- `db/migrations/` — versioned golang-migrate SQL, embedded
- `middleware/` — tracing and logging only

## Gotchas

- Kyverno admission rejects bad images. The published image is
  `ghcr.io/duynhlab/payment-service/payment-service:<tag>` — the repository path
  repeats, and the tag carries no `v` prefix. `mockpay` is the **same image** under
  a different subcommand, so its tag must track this service's. Never `:latest`.
- Metrics leave over OTLP. There is no `/metrics` endpoint and nothing scrapes
  this service.
- There is **no `seed` subcommand** — payment ships no demo data. `db/seed/` and
  `internal/core/cache/` exist as empty directories and are untracked leftovers;
  do not read them as features.

## API change synchronization

An API change is not done when the code compiles.

- The contract in homelab and this repository move **together** — same change,
  and either the same PR pair or an immediate follow-up.
- Behaviour that is designed but not deployed is marked **`Planned`** in the
  contract; it is never described as current.
- A material mismatch between the contract and this implementation **blocks the
  release tag** until it is reconciled or explicitly accepted.
