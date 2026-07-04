package v1

import (
	"context"
	"testing"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

func TestClassify(t *testing.T) {
	internal := map[string]domain.ReconRow{
		"mp_ok":     {ProviderPaymentID: "mp_ok", AmountMinor: 1000, Status: domain.StatusCaptured},
		"mp_amt":    {ProviderPaymentID: "mp_amt", AmountMinor: 2000, Status: domain.StatusCaptured},
		"mp_status": {ProviderPaymentID: "mp_status", AmountMinor: 3000, Status: domain.StatusAuthorized},
		"mp_both":   {ProviderPaymentID: "mp_both", AmountMinor: 4000, Status: domain.StatusAuthorized},
		"mp_gone":   {ProviderPaymentID: "mp_gone", AmountMinor: 5000, Status: domain.StatusCaptured},
	}
	txns := []provider.Transaction{
		{ProviderPaymentID: "mp_ok", AmountMinor: 1000, Status: provider.TxnCaptured},     // exact match → no discrepancy
		{ProviderPaymentID: "mp_amt", AmountMinor: 2001, Status: provider.TxnCaptured},    // amount differs
		{ProviderPaymentID: "mp_status", AmountMinor: 3000, Status: provider.TxnCaptured}, // status differs (authorized vs captured)
		{ProviderPaymentID: "mp_both", AmountMinor: 4001, Status: provider.TxnCaptured},   // amount + status differ → amount wins
		{ProviderPaymentID: "mp_orphan", AmountMinor: 6000, Status: provider.TxnCaptured}, // provider-only → missing_internal
	}

	got := classify(internal, txns)

	byID := make(map[string]domain.Discrepancy, len(got))
	for _, d := range got {
		byID[d.ProviderPaymentID] = d
	}
	want := map[string]domain.DiscrepancyClass{
		"mp_amt":    domain.DiscrepancyAmountMismatch,
		"mp_status": domain.DiscrepancyStatusMismatch,
		"mp_both":   domain.DiscrepancyAmountMismatch, // amount precedence
		"mp_orphan": domain.DiscrepancyMissingInternal,
		"mp_gone":   domain.DiscrepancyMissingProvider,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d discrepancies, want %d: %+v", len(got), len(want), got)
	}
	for id, wc := range want {
		if byID[id].Class != wc {
			t.Errorf("%s: class = %q, want %q", id, byID[id].Class, wc)
		}
	}
	if _, ok := byID["mp_ok"]; ok {
		t.Errorf("mp_ok matched exactly — it must not be a discrepancy")
	}

	// Field fidelity: amount_mismatch carries both sides; missing_* carries only its side.
	if d := byID["mp_amt"]; d.InternalAmount != 2000 || d.ProviderAmount != 2001 {
		t.Errorf("amount_mismatch fields = internal %d / provider %d, want 2000/2001", d.InternalAmount, d.ProviderAmount)
	}
	if d := byID["mp_orphan"]; d.InternalAmount != 0 || d.ProviderAmount != 6000 || d.InternalStatus != "" {
		t.Errorf("missing_internal must carry only provider side, got %+v", d)
	}
	if d := byID["mp_gone"]; d.ProviderAmount != 0 || d.InternalAmount != 5000 || d.ProviderStatus != "" {
		t.Errorf("missing_provider must carry only internal side, got %+v", d)
	}
}

// fakeReconRepo records what the reconciler persists.
type fakeReconRepo struct {
	rows      []domain.ReconRow
	runID     int64
	saved     []domain.Discrepancy
	finished  domain.ReconRunStatus
	scanned   int
	found     int
	listErr   error
	createErr error
	saveErr   error
}

func (f *fakeReconRepo) ListReconcilable(context.Context) ([]domain.ReconRow, error) {
	return f.rows, f.listErr
}
func (f *fakeReconRepo) CreateRun(context.Context) (int64, error) { return f.runID, f.createErr }
func (f *fakeReconRepo) SaveDiscrepancies(_ context.Context, _ int64, ds []domain.Discrepancy) error {
	f.saved = ds
	return f.saveErr
}
func (f *fakeReconRepo) FinishRun(_ context.Context, _ int64, scanned, found int, status domain.ReconRunStatus) error {
	f.scanned, f.found, f.finished = scanned, found, status
	return nil
}

type fakeLedger struct {
	page *provider.TransactionsPage
	err  error
}

func (f *fakeLedger) GetTransactions(_ context.Context, page, _ int) (*provider.TransactionsPage, error) {
	if f.err != nil {
		return nil, f.err
	}
	if page == 1 {
		return f.page, nil
	}
	return &provider.TransactionsPage{}, nil
}

func TestReconciler_Run(t *testing.T) {
	repo := &fakeReconRepo{
		runID: 7,
		rows: []domain.ReconRow{
			{ProviderPaymentID: "mp_1", AmountMinor: 1000, Status: domain.StatusCaptured}, // match
			{ProviderPaymentID: "mp_2", AmountMinor: 2000, Status: domain.StatusCaptured}, // amount
		},
	}
	ledger := &fakeLedger{page: &provider.TransactionsPage{Total: 2, Transactions: []provider.Transaction{
		{ProviderPaymentID: "mp_1", AmountMinor: 1000, Status: provider.TxnCaptured},
		{ProviderPaymentID: "mp_2", AmountMinor: 2001, Status: provider.TxnCaptured},
	}}}

	runID, found, err := NewReconciler(repo, ledger).Run(context.Background(), 100)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if runID != 7 || found != 1 {
		t.Fatalf("run = id %d found %d, want 7/1", runID, found)
	}
	if repo.finished != domain.ReconRunCompleted || repo.scanned != 2 || repo.found != 1 {
		t.Fatalf("finish = {%s, scanned %d, found %d}, want completed/2/1", repo.finished, repo.scanned, repo.found)
	}
	if len(repo.saved) != 1 || repo.saved[0].Class != domain.DiscrepancyAmountMismatch {
		t.Fatalf("saved = %+v, want one amount_mismatch", repo.saved)
	}
}

func TestReconciler_Run_NoDiscrepancies(t *testing.T) {
	repo := &fakeReconRepo{runID: 1, rows: []domain.ReconRow{{ProviderPaymentID: "mp_1", AmountMinor: 100, Status: domain.StatusAuthorized}}}
	ledger := &fakeLedger{page: &provider.TransactionsPage{Total: 1, Transactions: []provider.Transaction{
		{ProviderPaymentID: "mp_1", AmountMinor: 100, Status: provider.TxnAuthorized},
	}}}
	_, found, err := NewReconciler(repo, ledger).Run(context.Background(), 100)
	if err != nil || found != 0 {
		t.Fatalf("run = found %d err %v, want 0/nil", found, err)
	}
	if repo.saved != nil {
		t.Errorf("no discrepancies → SaveDiscrepancies must not be called, got %+v", repo.saved)
	}
	if repo.finished != domain.ReconRunCompleted {
		t.Errorf("finished = %s, want completed", repo.finished)
	}
}

func TestReconciler_Run_DetectErrorMarksFailed(t *testing.T) {
	repo := &fakeReconRepo{runID: 3}
	ledger := &fakeLedger{err: errBoom}
	if _, _, err := NewReconciler(repo, ledger).Run(context.Background(), 100); err == nil {
		t.Fatal("want error when the ledger fails")
	}
	if repo.finished != domain.ReconRunFailed {
		t.Errorf("finished = %s, want failed", repo.finished)
	}
}

func TestReconciler_Run_Errors(t *testing.T) {
	ctx := context.Background()
	// A provider txn with no matching internal row → a missing_internal
	// discrepancy, so SaveDiscrepancies is reached.
	ledger := &fakeLedger{page: &provider.TransactionsPage{Total: 1, Transactions: []provider.Transaction{
		{ProviderPaymentID: "mp_x", AmountMinor: 999, Status: provider.TxnCaptured},
	}}}

	t.Run("create run fails", func(t *testing.T) {
		if _, _, err := NewReconciler(&fakeReconRepo{createErr: errBoom}, ledger).Run(ctx, 100); err == nil {
			t.Fatal("want error when CreateRun fails")
		}
	})
	t.Run("list fails → run marked failed", func(t *testing.T) {
		repo := &fakeReconRepo{runID: 1, listErr: errBoom}
		if _, _, err := NewReconciler(repo, ledger).Run(ctx, 100); err == nil {
			t.Fatal("want error when ListReconcilable fails")
		}
		if repo.finished != domain.ReconRunFailed {
			t.Errorf("finished = %s, want failed", repo.finished)
		}
	})
	t.Run("save fails → run marked failed", func(t *testing.T) {
		repo := &fakeReconRepo{runID: 1, saveErr: errBoom}
		if _, _, err := NewReconciler(repo, ledger).Run(ctx, 100); err == nil {
			t.Fatal("want error when SaveDiscrepancies fails")
		}
		if repo.finished != domain.ReconRunFailed {
			t.Errorf("finished = %s, want failed", repo.finished)
		}
	})
}

func TestClassify_StatusEquivalence(t *testing.T) {
	internal := map[string]domain.ReconRow{
		"mp_exp":     {ProviderPaymentID: "mp_exp", AmountMinor: 1000, Status: domain.StatusExpired},
		"mp_partial": {ProviderPaymentID: "mp_partial", AmountMinor: 2000, RefundedMinor: 500, Status: domain.StatusCaptured},
		"mp_lost":    {ProviderPaymentID: "mp_lost", AmountMinor: 3000, RefundedMinor: 0, Status: domain.StatusCaptured},
	}
	txns := []provider.Transaction{
		{ProviderPaymentID: "mp_exp", AmountMinor: 1000, Status: provider.TxnAuthorized},   // expired hold not voided at provider → benign
		{ProviderPaymentID: "mp_partial", AmountMinor: 2000, Status: provider.TxnRefunded}, // partial refund → benign
		{ProviderPaymentID: "mp_lost", AmountMinor: 3000, Status: provider.TxnRefunded},    // captured, no refund recorded, yet provider refunded → real drift
		{ProviderPaymentID: "", AmountMinor: 9, Status: provider.TxnCaptured},              // no id → skipped
	}
	got := classify(internal, txns)
	if len(got) != 1 || got[0].ProviderPaymentID != "mp_lost" || got[0].Class != domain.DiscrepancyStatusMismatch {
		t.Fatalf("want only mp_lost status_mismatch, got %+v", got)
	}
}

type pagedLedger struct{ pages []*provider.TransactionsPage }

func (p *pagedLedger) GetTransactions(_ context.Context, page, _ int) (*provider.TransactionsPage, error) {
	if page-1 < len(p.pages) {
		return p.pages[page-1], nil
	}
	return &provider.TransactionsPage{}, nil
}

func TestReconciler_Run_PagesPastStaleTotal(t *testing.T) {
	repo := &fakeReconRepo{runID: 1, rows: []domain.ReconRow{
		{ProviderPaymentID: "mp_1", AmountMinor: 100, Status: domain.StatusCaptured},
		{ProviderPaymentID: "mp_2", AmountMinor: 100, Status: domain.StatusCaptured},
		{ProviderPaymentID: "mp_3", AmountMinor: 100, Status: domain.StatusCaptured},
	}}
	// Total is understated (2) while there are really 3 rows across 2 pages. The
	// old page*pageSize>=Total termination would drop page 2 and falsely flag
	// mp_3 missing_provider; terminating on a short page reads it.
	ledger := &pagedLedger{pages: []*provider.TransactionsPage{
		{Total: 2, Transactions: []provider.Transaction{
			{ProviderPaymentID: "mp_1", AmountMinor: 100, Status: provider.TxnCaptured},
			{ProviderPaymentID: "mp_2", AmountMinor: 100, Status: provider.TxnCaptured},
		}},
		{Total: 2, Transactions: []provider.Transaction{
			{ProviderPaymentID: "mp_3", AmountMinor: 100, Status: provider.TxnCaptured},
		}},
	}}
	_, found, err := NewReconciler(repo, ledger).Run(context.Background(), 2)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if found != 0 {
		t.Fatalf("a stale Total must not drop page 2 (mp_3) → want 0 discrepancies, got %d: %+v", found, repo.saved)
	}
}

func TestClassify_NoDiscrepancies(t *testing.T) {
	internal := map[string]domain.ReconRow{
		"mp_1": {ProviderPaymentID: "mp_1", AmountMinor: 100, Status: domain.StatusAuthorized},
	}
	txns := []provider.Transaction{{ProviderPaymentID: "mp_1", AmountMinor: 100, Status: provider.TxnAuthorized}}
	if got := classify(internal, txns); len(got) != 0 {
		t.Fatalf("want no discrepancies, got %+v", got)
	}
}
