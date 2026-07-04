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
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{baseURL: baseURL, hc: &http.Client{Timeout: 10 * time.Second}}
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
		return &charge, nil
	case http.StatusPaymentRequired:
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
	return c.mutate(ctx, "/charges/"+url.PathEscape(providerPaymentID)+"/capture")
}

// Void releases a hold via POST /charges/{id}/void.
func (c *HTTPClient) Void(ctx context.Context, providerPaymentID string) error {
	return c.mutate(ctx, "/charges/"+url.PathEscape(providerPaymentID)+"/void")
}

// mutate posts to a capture/void endpoint that returns no body on success.
func (c *HTTPClient) mutate(ctx context.Context, path string) error {
	status, body, err := c.do(ctx, http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("mockpay %s: status %d: %s", path, status, decodeError(body).Error)
	}
	return nil
}

// Refund issues a refund via POST /refunds.
func (c *HTTPClient) Refund(ctx context.Context, providerPaymentID string, amountMinor int64, idempotencyKey string) (string, error) {
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
	return resp.ProviderRefundID, nil
}
