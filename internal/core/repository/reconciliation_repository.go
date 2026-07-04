package repository

import (
	"context"
	"fmt"

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
// v1 does a full scan and the reconciler holds both this result AND the fully
// paged provider ledger in memory for one pass. That set grows monotonically for
// the life of the service, so before prod scale this must window by created_at /
// id (a rolling recent window + a slower full sweep), the same way the outbox
// relay documents its single-writer assumption.
func (r *ReconciliationRepository) ListReconcilable(ctx context.Context) ([]domain.ReconRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT p.provider_payment_id, p.amount_minor, p.status,
		       COALESCE((SELECT SUM(rf.amount_minor) FROM refunds rf
		                 WHERE rf.payment_id = p.id AND rf.status IN ('pending', 'succeeded')), 0) AS refunded_minor
		FROM payments p
		WHERE p.provider_payment_id IS NOT NULL AND p.provider_payment_id <> ''`)
	if err != nil {
		return nil, fmt.Errorf("list reconcilable payments: %w", err)
	}
	defer rows.Close()

	var out []domain.ReconRow
	for rows.Next() {
		var row domain.ReconRow
		var status string
		if err := rows.Scan(&row.ProviderPaymentID, &row.AmountMinor, &status, &row.RefundedMinor); err != nil {
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
