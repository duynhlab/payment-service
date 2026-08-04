package mockpay_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/provider"
)

func getTransactions(t *testing.T, url string) provider.TransactionsPage {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var page provider.TransactionsPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("decode transactions: %v (%s)", err, body)
	}
	return page
}

// TestServer_Transactions_Statuses drives a charge through each lifecycle state
// and asserts GET /transactions reports the right status + amount for the
// reconciliation job to match on.
func TestServer_Transactions_Statuses(t *testing.T) {
	url := newServer(t)

	charge := func(amount int64, autoCapture bool) string {
		_, body := post(t, url+"/charges", provider.ChargeRequest{
			AmountMinor: amount, Currency: "USD", PaymentMethod: "tok_visa",
			IdempotencyKey: "", AutoCapture: autoCapture,
		})
		return decodeCharge(t, body).ProviderPaymentID
	}

	authorizedID := charge(1000, false)
	capturedID := charge(2000, true)
	captureLaterID := charge(3000, false)
	if st, _ := post(t, url+"/charges/"+captureLaterID+"/capture", nil); st != http.StatusOK {
		t.Fatalf("capture: %d", st)
	}
	voidedID := charge(4000, false)
	if st, _ := post(t, url+"/charges/"+voidedID+"/void", nil); st != http.StatusOK {
		t.Fatalf("void: %d", st)
	}
	refundedID := charge(5000, true)
	if st, _ := post(t, url+"/refunds", provider.RefundRequest{ProviderPaymentID: refundedID, AmountMinor: 5000}); st != http.StatusOK {
		t.Fatalf("refund: %d", st)
	}

	page := getTransactions(t, url+"/transactions")
	if page.Total != 5 {
		t.Fatalf("total = %d, want 5", page.Total)
	}
	byID := map[string]provider.Transaction{}
	for _, tx := range page.Transactions {
		byID[tx.ProviderPaymentID] = tx
	}
	want := []struct {
		id     string
		status string
		amount int64
	}{
		{authorizedID, provider.TxnAuthorized, 1000},
		{capturedID, provider.TxnCaptured, 2000},
		{captureLaterID, provider.TxnCaptured, 3000},
		{voidedID, provider.TxnVoided, 4000},
		{refundedID, provider.TxnRefunded, 5000},
	}
	for _, w := range want {
		got, ok := byID[w.id]
		if !ok {
			t.Fatalf("txn %s missing", w.id)
		}
		if got.Status != w.status || got.AmountMinor != w.amount {
			t.Errorf("txn %s = {%s, %d}, want {%s, %d}", w.id, got.Status, got.AmountMinor, w.status, w.amount)
		}
	}
}

// TestServer_Transactions_LexicalOrder locks the real ordering semantics: ids
// are sorted lexically, not numerically, so mp_10 precedes mp_2. This crosses
// the boundary a 5-item test can't see, and guards the "stable sweep" contract
// the recon job relies on.
func TestServer_Transactions_LexicalOrder(t *testing.T) {
	url := newServer(t)
	for i := 0; i < 11; i++ {
		post(t, url+"/charges", provider.ChargeRequest{AmountMinor: 1000, Currency: "USD", PaymentMethod: "tok_visa"})
	}
	page := getTransactions(t, url+"/transactions?page_size=50")
	if page.Total != 11 {
		t.Fatalf("total = %d, want 11", page.Total)
	}
	for i := 1; i < len(page.Transactions); i++ {
		if page.Transactions[i-1].ProviderPaymentID >= page.Transactions[i].ProviderPaymentID {
			t.Fatalf("not lexically ordered at %d: %q >= %q", i,
				page.Transactions[i-1].ProviderPaymentID, page.Transactions[i].ProviderPaymentID)
		}
	}
	// The trap: mp_10 sorts right after mp_1, before mp_2.
	if page.Transactions[0].ProviderPaymentID != "mp_1" || page.Transactions[1].ProviderPaymentID != "mp_10" {
		t.Errorf("lexical head = %q,%q; want mp_1,mp_10",
			page.Transactions[0].ProviderPaymentID, page.Transactions[1].ProviderPaymentID)
	}
}

// TestServer_Transactions_Paging checks the page window + stable ordering.
func TestServer_Transactions_Paging(t *testing.T) {
	url := newServer(t)
	for i := int64(1); i <= 5; i++ {
		post(t, url+"/charges", provider.ChargeRequest{AmountMinor: i * 1000, Currency: "USD", PaymentMethod: "tok_visa"})
	}

	p1 := getTransactions(t, url+"/transactions?page=1&page_size=2")
	if p1.Total != 5 || len(p1.Transactions) != 2 || p1.PageSize != 2 {
		t.Fatalf("page1 = total %d, len %d, size %d; want 5/2/2", p1.Total, len(p1.Transactions), p1.PageSize)
	}
	p3 := getTransactions(t, url+"/transactions?page=3&page_size=2")
	if len(p3.Transactions) != 1 { // 5 items, page 3 of size 2 = the tail
		t.Fatalf("page3 len = %d, want 1", len(p3.Transactions))
	}
	// Stable ordering: page 1 ids sort before page 3's id.
	if p1.Transactions[0].ProviderPaymentID >= p3.Transactions[0].ProviderPaymentID {
		t.Errorf("pages not stably ordered: %q vs %q", p1.Transactions[0].ProviderPaymentID, p3.Transactions[0].ProviderPaymentID)
	}

	// Page past the end → empty window (still 200 OK).
	if beyond := getTransactions(t, url+"/transactions?page=99&page_size=2"); len(beyond.Transactions) != 0 {
		t.Errorf("page 99 len = %d, want 0", len(beyond.Transactions))
	}
	// Bad/oversized query params fall back to defaults / caps.
	if capped := getTransactions(t, url+"/transactions?page=abc&page_size=999"); capped.Page != 1 || capped.PageSize != 200 {
		t.Errorf("clamp: page=%d size=%d, want 1/200", capped.Page, capped.PageSize)
	}
	// Below-minimum values clamp up to the floor.
	if floor := getTransactions(t, url+"/transactions?page=0&page_size=0"); floor.Page != 1 || floor.PageSize != 1 {
		t.Errorf("floor clamp: page=%d size=%d, want 1/1", floor.Page, floor.PageSize)
	}
}

// TestServer_TransactionsWindow: the ledger has to be askable for a slice of
// history, because otherwise every reconciliation pass must read all of it and
// the cost of checking today's payments grows with every payment ever made.
func TestServer_TransactionsWindow(t *testing.T) {
	base := newServer(t)

	req := provider.ChargeRequest{IdempotencyKey: "win-1", AmountMinor: 1000, Currency: "USD"}
	if st, _ := post(t, base+"/charges", req); st != http.StatusOK {
		t.Fatalf("charge status %d", st)
	}

	t.Run("a window ending in the future includes it", func(t *testing.T) {
		page := getTransactions(t, base+"/transactions?to="+url.QueryEscape(time.Now().Add(time.Hour).Format(time.RFC3339)))
		if len(page.Transactions) != 1 {
			t.Fatalf("transactions = %d, want 1", len(page.Transactions))
		}
		if page.Transactions[0].CreatedAt.IsZero() {
			t.Error("created_at is zero: without it a caller cannot bound anything")
		}
	})

	t.Run("a window that closed before it existed excludes it", func(t *testing.T) {
		page := getTransactions(t, base+"/transactions?from="+url.QueryEscape(time.Now().Add(-48*time.Hour).Format(time.RFC3339))+
			"&to="+url.QueryEscape(time.Now().Add(-24*time.Hour).Format(time.RFC3339)))
		if len(page.Transactions) != 0 {
			t.Fatalf("transactions = %d, want 0", len(page.Transactions))
		}
	})

	// Refused, not ignored. Dropping an unparseable bound would widen the answer
	// to everything — and the caller would compare that against a narrow internal
	// set and report every older charge as missing on its side.
	t.Run("a malformed bound is refused", func(t *testing.T) {
		for _, q := range []string{"from=yesterday", "to=not-a-time", "from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z"} {
			resp, err := http.Get(base + "/transactions?" + q)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s → status %d, want 400", q, resp.StatusCode)
			}
		}
	})
}
