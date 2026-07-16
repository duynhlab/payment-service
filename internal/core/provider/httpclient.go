package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// maxRespBytes caps how much of a provider response we read. Responses are tiny
// JSON envelopes; the cap stops a misbehaving/MITM'd provider from OOMing the
// payment service with a giant body (the client timeout bounds time, not size).
const maxRespBytes = 1 << 20 // 1 MiB

// HTTPClient is the Provider implementation that talks to the mockpay HTTP
// service. It maps mockpay's status codes back onto the port's error contract:
// 402 → DeclinedError (→ 422 PAYMENT_DECLINED), 503 → ErrTransient (retryable),
// and any transport failure is likewise treated as transient by the caller.
type HTTPClient struct {
	baseURL string
	hc      *http.Client
}

// NewHTTPClient wires a mockpay client at baseURL (e.g. http://mockpay:8080).
// The transport is wrapped with otelhttp so each outbound call carries the W3C
// traceparent (the money-hop joins the caller's trace) and emits a client span.
// otelhttp injects only headers via the global propagator — it never touches
// the request body, so any body-level signing stays intact.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		hc: &http.Client{
			Timeout:   10 * time.Second,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// do issues the request and returns the status and raw body. A transport error
// is wrapped (the caller's default path treats it as transient/retryable).
func (c *HTTPClient) do(ctx context.Context, method, path string, reqBody any) (int, []byte, error) {
	var buf io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return 0, nil, fmt.Errorf("mockpay marshal %s: %w", path, err)
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, buf)
	if err != nil {
		return 0, nil, fmt.Errorf("mockpay request %s: %w", path, err)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("mockpay %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("mockpay read %s: %w", path, err)
	}
	return resp.StatusCode, body, nil
}

// decodeError pulls the error envelope from a non-2xx body (best-effort).
func decodeError(body []byte) ErrorResponse {
	var e ErrorResponse
	_ = json.Unmarshal(body, &e)
	return e
}

// Charge places (and optionally captures) a hold via POST /charges.
func (c *HTTPClient) Charge(ctx context.Context, req ChargeRequest) (*Charge, error) {
	start := time.Now()
	outcome := outcomeTransient // transport error / unexpected status default
	defer func() { recordProviderCall(ctx, opCharge, outcome, start) }()

	status, body, err := c.do(ctx, http.MethodPost, "/charges", req)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusOK:
		var charge Charge
		if err := json.Unmarshal(body, &charge); err != nil {
			return nil, fmt.Errorf("mockpay charge decode: %w", err)
		}
		outcome = outcomeOK
		return &charge, nil
	case http.StatusPaymentRequired:
		outcome = outcomeDeclined
		return nil, &DeclinedError{Code: decodeError(body).Code}
	case http.StatusServiceUnavailable:
		return nil, ErrTransient
	default:
		// Any other status is treated as retryable by the caller (driveCharge's
		// default path). That is the safe default: /charges only emits
		// 200/402/503, and a charge is idempotent per key, so re-driving an
		// unexpected status replays rather than double-charges.
		return nil, fmt.Errorf("mockpay charge: status %d: %s", status, decodeError(body).Error)
	}
}

// Capture captures a hold via POST /charges/{id}/capture.
func (c *HTTPClient) Capture(ctx context.Context, providerPaymentID string) error {
	return c.mutate(ctx, opCapture, "/charges/"+url.PathEscape(providerPaymentID)+"/capture")
}

// GetTransactions pages the provider's ledger (GET /transactions) — the food
// source for reconciliation. Returns one page; the caller pages to exhaustion.
func (c *HTTPClient) GetTransactions(ctx context.Context, page, pageSize int) (*TransactionsPage, error) {
	path := fmt.Sprintf("/transactions?page=%d&page_size=%d", page, pageSize)
	status, body, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("mockpay transactions: unexpected status %d", status)
	}
	var p TransactionsPage
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("mockpay decode transactions: %w", err)
	}
	return &p, nil
}

// Void releases a hold via POST /charges/{id}/void.
func (c *HTTPClient) Void(ctx context.Context, providerPaymentID string) error {
	return c.mutate(ctx, opVoid, "/charges/"+url.PathEscape(providerPaymentID)+"/void")
}

// mutate posts to a capture/void endpoint that returns no body on success. op is
// the bounded metric label ("capture"/"void").
func (c *HTTPClient) mutate(ctx context.Context, op, path string) error {
	start := time.Now()
	outcome := outcomeTransient
	defer func() { recordProviderCall(ctx, op, outcome, start) }()

	status, body, err := c.do(ctx, http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("mockpay %s: status %d: %s", path, status, decodeError(body).Error)
	}
	outcome = outcomeOK
	return nil
}

// Refund issues a refund via POST /refunds.
func (c *HTTPClient) Refund(ctx context.Context, providerPaymentID string, amountMinor int64, idempotencyKey string) (string, error) {
	start := time.Now()
	outcome := outcomeTransient
	defer func() { recordProviderCall(ctx, opRefund, outcome, start) }()

	status, body, err := c.do(ctx, http.MethodPost, "/refunds", RefundRequest{
		ProviderPaymentID: providerPaymentID,
		AmountMinor:       amountMinor,
		IdempotencyKey:    idempotencyKey,
	})
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("mockpay refund: status %d: %s", status, decodeError(body).Error)
	}
	var resp RefundResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("mockpay refund decode: %w", err)
	}
	outcome = outcomeOK
	return resp.ProviderRefundID, nil
}
