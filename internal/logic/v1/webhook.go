package v1

import "context"

// WebhookRepo is the webhook dedup port (implemented by
// repository.WebhookRepository).
type WebhookRepo interface {
	Record(ctx context.Context, eventID, eventType, providerPaymentID string) (status string, isNew bool, err error)
}

// WebhookResult is the outcome of processing one delivery. Duplicate marks a
// redelivered event that was already recorded; Status is processed|orphaned.
type WebhookResult struct {
	Duplicate bool
	Status    string
}

// WebhookProcessor records verified inbound webhooks. It is a peer of the
// payment Service (kept separate so the Service constructor stays lean), and
// deliberately does not drive state changes yet — reacting to events is the
// reconciliation phase's job; here we dedup, correlate, and durably record.
type WebhookProcessor struct {
	repo WebhookRepo
}

// NewWebhookProcessor wires the processor onto its port.
func NewWebhookProcessor(repo WebhookRepo) *WebhookProcessor {
	return &WebhookProcessor{repo: repo}
}

// Process records the event idempotently by event_id and reports whether it was
// a duplicate and how it correlated. Signature verification happens in the web
// layer before this is called; this method assumes an authentic event.
func (p *WebhookProcessor) Process(ctx context.Context, eventID, eventType, providerPaymentID string) (WebhookResult, error) {
	status, isNew, err := p.repo.Record(ctx, eventID, eventType, providerPaymentID)
	if err != nil {
		return WebhookResult{}, err
	}
	return WebhookResult{Duplicate: !isNew, Status: status}, nil
}
