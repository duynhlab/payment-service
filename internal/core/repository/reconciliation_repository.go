package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// ReconciliationRepository persists reconciliation runs + discrepancies and
// projects the internal payments the reconciler compares against the provider.
type ReconciliationRepository struct {
	pool *pgxpool.Pool
}

// NewReconciliationRepository wires the repository onto the pool.
func NewReconciliationRepository(pool *pgxpool.Pool) *ReconciliationRepository {
	return &ReconciliationRepository{pool: pool}
}

// ListReconcilable returns every payment that already has a provider_payment_id
// — the internal rows with a provider record to reconcile against, each with its
// applied refund total (to tell a benign partial refund from missed refund drift).
//
// The pass is bounded by a half-open window on created_at, matching the window
// the provider ledger is asked for. Both sides must use the SAME bounds: compare
// a narrow internal set against the provider's whole history and every older
// charge reads as missing on our side.
//
// A zero bound means unbounded on that side, which is how the very first pass
// (before any watermark exists) still covers everything once.
//
// Payments parked in `processing` are excluded. Their drift is not drift — it is a
// question the attempt log already owns, with a resolution path of its own. Left
// in, every parked row matches no provider status and re-reports the same
// status_mismatch on every single run, burying the real discrepancies in the one
// signal that is supposed to surface them.
func (r *ReconciliationRepository) ListReconcilable(ctx context.Context, window domain.ReconWindow) ([]domain.ReconRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT p.id, p.provider_payment_id, p.amount_minor, p.status,
		       COALESCE((SELECT SUM(rf.amount_minor) FROM refunds rf
		                 WHERE rf.payment_id = p.id AND rf.status IN ('pending', 'processing', 'succeeded')), 0) AS refunded_minor
		FROM payments p
		WHERE p.provider_payment_id IS NOT NULL AND p.provider_payment_id <> ''
		  AND p.status <> 'processing'
		  AND ($1::timestamptz IS NULL OR p.created_at >= $1)
		  AND ($2::timestamptz IS NULL OR p.created_at <  $2)`,
		nullTime(window.From), nullTime(window.Through))
	if err != nil {
		return nil, fmt.Errorf("list reconcilable payments: %w", err)
	}
	defer rows.Close()

	var out []domain.ReconRow
	for rows.Next() {
		var row domain.ReconRow
		var status string
		if err := rows.Scan(&row.ID, &row.ProviderPaymentID, &row.AmountMinor, &status, &row.RefundedMinor); err != nil {
			return nil, fmt.Errorf("scan reconcilable payment: %w", err)
		}
		row.Status = domain.Status(status)
		out = append(out, row)
	}
	return out, rows.Err()
}

// CreateRun opens a reconciliation run in the 'running' state and returns its id.
func (r *ReconciliationRepository) CreateRun(ctx context.Context) (int64, error) {
	var id int64
	if err := r.pool.QueryRow(ctx,
		`INSERT INTO reconciliation_runs (status) VALUES ('running') RETURNING id`).Scan(&id); err != nil {
		return 0, fmt.Errorf("create reconciliation run: %w", err)
	}
	return id, nil
}

// SaveDiscrepancies persists a run's discrepancies atomically: all rows commit
// together or none do, so a failure never leaves a run with a partial,
// misleading discrepancy set.
func (r *ReconciliationRepository) SaveDiscrepancies(ctx context.Context, runID int64, ds []domain.Discrepancy) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin discrepancies tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, d := range ds {
		if _, err := tx.Exec(ctx, `
			INSERT INTO reconciliation_discrepancies
				(run_id, provider_payment_id, class, internal_amount_minor,
				 provider_amount_minor, internal_status, provider_status, detail)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			runID, d.ProviderPaymentID, string(d.Class), d.InternalAmount,
			d.ProviderAmount, d.InternalStatus, d.ProviderStatus, d.Detail); err != nil {
			return fmt.Errorf("insert discrepancy: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// MarkResolved records what a heal pass did about one discrepancy, keyed by
// (run_id, provider_payment_id) — a run holds at most one discrepancy per charge.
// resolved_at is stamped only for a heal action (healed/failed/skipped), leaving
// the detect-only 'detected' rows without a resolution timestamp.
func (r *ReconciliationRepository) MarkResolved(ctx context.Context, runID int64, providerPaymentID string, res domain.Resolution) error {
	if _, err := r.pool.Exec(ctx, `
		UPDATE reconciliation_discrepancies
		SET resolution = $3, resolved_at = now()
		WHERE run_id = $1 AND provider_payment_id = $2`,
		runID, providerPaymentID, string(res)); err != nil {
		return fmt.Errorf("mark discrepancy resolved: %w", err)
	}
	return nil
}

// GetRun returns one reconciliation run, or domain.ErrNotFound.
func (r *ReconciliationRepository) GetRun(ctx context.Context, id int64) (*domain.ReconRun, error) {
	var run domain.ReconRun
	var status string
	err := r.pool.QueryRow(ctx, `
		SELECT id, status, transactions_scanned, discrepancies_found, started_at, finished_at
		FROM reconciliation_runs WHERE id = $1`, id).
		Scan(&run.ID, &status, &run.TransactionsScanned, &run.DiscrepanciesFound, &run.StartedAt, &run.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get reconciliation run %d: %w", id, err)
	}
	run.Status = domain.ReconRunStatus(status)
	return &run, nil
}

// ListDiscrepancies returns a page of a run's discrepancies in insertion order
// (limit/offset). A run's full count is available on the run row
// (discrepancies_found), so callers page against that total.
func (r *ReconciliationRepository) ListDiscrepancies(ctx context.Context, runID int64, limit, offset int) ([]domain.Discrepancy, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT provider_payment_id, class, internal_amount_minor, provider_amount_minor,
		       internal_status, provider_status, detail, resolution, resolved_at
		FROM reconciliation_discrepancies WHERE run_id = $1 ORDER BY id
		LIMIT $2 OFFSET $3`, runID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list discrepancies for run %d: %w", runID, err)
	}
	defer rows.Close()

	out := []domain.Discrepancy{}
	for rows.Next() {
		var d domain.Discrepancy
		var class, resolution string
		if err := rows.Scan(&d.ProviderPaymentID, &class, &d.InternalAmount, &d.ProviderAmount,
			&d.InternalStatus, &d.ProviderStatus, &d.Detail, &resolution, &d.ResolvedAt); err != nil {
			return nil, fmt.Errorf("scan discrepancy: %w", err)
		}
		d.Class = domain.DiscrepancyClass(class)
		d.Resolution = domain.Resolution(resolution)
		out = append(out, d)
	}
	return out, rows.Err()
}

// ReapRuns deletes reconciliation runs finished before the cutoff (ttl ago),
// bounding table growth; discrepancies cascade-delete via their run FK. Only
// finished runs are removed, so a run still in progress is never reaped. Returns
// the number of runs removed. Mirrors the outbox reaper.
func (r *ReconciliationRepository) ReapRuns(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM reconciliation_runs WHERE finished_at IS NOT NULL AND finished_at < $1`,
		time.Now().Add(-ttl))
	if err != nil {
		return 0, fmt.Errorf("reap reconciliation runs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// FinishRun closes a run with its terminal status and counts.
func (r *ReconciliationRepository) FinishRun(ctx context.Context, runID int64, scanned, found int, status domain.ReconRunStatus) error {
	if _, err := r.pool.Exec(ctx, `
		UPDATE reconciliation_runs
		SET status = $2, transactions_scanned = $3, discrepancies_found = $4, finished_at = now()
		WHERE id = $1`, runID, string(status), scanned, found); err != nil {
		return fmt.Errorf("finish reconciliation run %d: %w", runID, err)
	}
	return nil
}

// nullTime renders a zero time as SQL NULL, so an unbounded window side becomes
// an always-true predicate rather than a comparison against year zero.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// Watermark returns where the last completed pass finished, or the zero time when
// no pass has completed yet — which the caller reads as "cover everything".
func (r *ReconciliationRepository) Watermark(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := r.pool.QueryRow(ctx, `SELECT through_time FROM reconciliation_watermark WHERE id`).Scan(&t)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("read reconciliation watermark: %w", err)
	}
	return t, nil
}

// AdvanceWatermark records that everything before `through` has been compared.
// Monotonic by construction: GREATEST means a late-arriving pass over an older
// window can never pull the frontier backwards and cause the ground in between to
// be re-read forever.
func (r *ReconciliationRepository) AdvanceWatermark(ctx context.Context, through time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO reconciliation_watermark (id, through_time, updated_at)
		VALUES (TRUE, $1, now())
		ON CONFLICT (id) DO UPDATE
		   SET through_time = GREATEST(reconciliation_watermark.through_time, EXCLUDED.through_time),
		       updated_at   = now()`, through)
	if err != nil {
		return fmt.Errorf("advance reconciliation watermark: %w", err)
	}
	return nil
}
