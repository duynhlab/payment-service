package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// Protected Backoffice readers (RFC-0023 slice A): the operator's
// cross-customer views. Everything here is read-only projection — the ledger
// and attempt log stay append-only, reconciliation stays detect-only.

// ListAll returns one cross-customer page of payments, newest first, plus
// the unpaged total; status narrows when set.
func (r *PaymentRepository) ListAll(ctx context.Context, status string, limit, offset int) ([]domain.Payment, int, error) {
	where := ""
	args := []any{}
	if status != "" {
		args = append(args, status)
		where = ` WHERE status = $1`
	}

	var total int
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM payments`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count payments: %w", err)
	}

	q := fmt.Sprintf(`SELECT `+paymentColumns+` FROM payments%s
		 ORDER BY created_at DESC, id DESC LIMIT $%d OFFSET $%d`, where, len(args)+1, len(args)+2)
	rows, err := r.pool.Query(ctx, q, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("list all payments: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Payment, 0)
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *p)
	}
	if rows.Err() != nil {
		return nil, 0, fmt.Errorf("list all payments: %w", rows.Err())
	}
	return items, total, nil
}

// ListForPayment returns EVERY attempt for one payment, oldest first — the
// operator's round-trip history, unlike ListOpenForPayment's worklist view.
func (r *AttemptRepository) ListForPayment(ctx context.Context, paymentID int64) ([]domain.Attempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+attemptColumns+`
		  FROM payment_attempts
		 WHERE payment_id = $1
		 ORDER BY created_at, id`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("list attempts for payment %d: %w", paymentID, err)
	}
	return scanAttempts(rows)
}

// LedgerTransactionView is one append-only ledger transaction with its
// balanced entry legs summarized for the operator.
type LedgerTransactionView struct {
	ID          int64     `json:"id"`
	Kind        string    `json:"kind"`
	ExternalRef string    `json:"external_ref"`
	AmountMinor int64     `json:"amount_minor"`
	CreatedAt   time.Time `json:"created_at"`
}

// TransactionsForPayment summarizes the payment's ledger lineage: one row per
// transaction, amount = the debit leg (entries are balanced by trigger).
func (r *LedgerRepository) TransactionsForPayment(ctx context.Context, paymentID int64) ([]LedgerTransactionView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT t.id, t.kind, COALESCE(t.external_ref, ''), t.created_at,
		       COALESCE((SELECT SUM(e.amount_minor) FROM ledger_entries e
		                  WHERE e.transaction_id = t.id AND e.direction = 'debit'), 0)
		  FROM ledger_transactions t
		 WHERE t.payment_id = $1
		 ORDER BY t.id`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("ledger transactions for payment %d: %w", paymentID, err)
	}
	defer rows.Close()

	out := make([]LedgerTransactionView, 0)
	for rows.Next() {
		var v LedgerTransactionView
		if err := rows.Scan(&v.ID, &v.Kind, &v.ExternalRef, &v.CreatedAt, &v.AmountMinor); err != nil {
			return nil, fmt.Errorf("scan ledger transaction: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ReconRunView is one reconciliation run header.
type ReconRunView struct {
	ID                  int64      `json:"id"`
	Status              string     `json:"status"`
	TransactionsScanned int        `json:"transactions_scanned"`
	DiscrepanciesFound  int        `json:"discrepancies_found"`
	StartedAt           time.Time  `json:"started_at"`
	FinishedAt          *time.Time `json:"finished_at"`
}

// ReconDiscrepancyView is one detected discrepancy — the triage row.
type ReconDiscrepancyView struct {
	ID                  int64     `json:"id"`
	RunID               int64     `json:"run_id"`
	ProviderPaymentID   string    `json:"provider_payment_id"`
	Class               string    `json:"class"`
	InternalAmountMinor int64     `json:"internal_amount_minor"`
	ProviderAmountMinor int64     `json:"provider_amount_minor"`
	InternalStatus      string    `json:"internal_status"`
	ProviderStatus      string    `json:"provider_status"`
	Detail              string    `json:"detail"`
	CreatedAt           time.Time `json:"created_at"`
}

// ReconReadRepository projects the detect-only reconciliation records
// (runs every 5 minutes; RFC-0021) for the operator — their first reader.
type ReconReadRepository struct {
	pool *pgxpool.Pool
}

// NewReconReadRepository creates the reconciliation reader.
func NewReconReadRepository(pool *pgxpool.Pool) *ReconReadRepository {
	return &ReconReadRepository{pool: pool}
}

// ListRuns returns one page of run headers, newest first, plus the total.
func (r *ReconReadRepository) ListRuns(ctx context.Context, limit, offset int) ([]ReconRunView, int, error) {
	var total int
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM reconciliation_runs`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count recon runs: %w", err)
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, status, transactions_scanned, discrepancies_found, started_at, finished_at
		  FROM reconciliation_runs
		 ORDER BY id DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list recon runs: %w", err)
	}
	defer rows.Close()

	out := make([]ReconRunView, 0)
	for rows.Next() {
		var v ReconRunView
		if err := rows.Scan(&v.ID, &v.Status, &v.TransactionsScanned, &v.DiscrepanciesFound, &v.StartedAt, &v.FinishedAt); err != nil {
			return nil, 0, fmt.Errorf("scan recon run: %w", err)
		}
		out = append(out, v)
	}
	return out, total, rows.Err()
}

// GetRun returns one run header with its discrepancies.
func (r *ReconReadRepository) GetRun(ctx context.Context, id int64) (*ReconRunView, []ReconDiscrepancyView, error) {
	var run ReconRunView
	err := r.pool.QueryRow(ctx, `
		SELECT id, status, transactions_scanned, discrepancies_found, started_at, finished_at
		  FROM reconciliation_runs WHERE id = $1`, id).
		Scan(&run.ID, &run.Status, &run.TransactionsScanned, &run.DiscrepanciesFound, &run.StartedAt, &run.FinishedAt)
	if err != nil {
		return nil, nil, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, run_id, provider_payment_id, class, internal_amount_minor,
		       provider_amount_minor, internal_status, provider_status, detail, created_at
		  FROM reconciliation_discrepancies
		 WHERE run_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, nil, fmt.Errorf("list discrepancies for run %d: %w", id, err)
	}
	defer rows.Close()

	discs := make([]ReconDiscrepancyView, 0)
	for rows.Next() {
		var d ReconDiscrepancyView
		if err := rows.Scan(&d.ID, &d.RunID, &d.ProviderPaymentID, &d.Class, &d.InternalAmountMinor,
			&d.ProviderAmountMinor, &d.InternalStatus, &d.ProviderStatus, &d.Detail, &d.CreatedAt); err != nil {
			return nil, nil, fmt.Errorf("scan discrepancy: %w", err)
		}
		discs = append(discs, d)
	}
	return &run, discs, rows.Err()
}
