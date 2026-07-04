package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// fakeOutbox is an in-memory OutboxRepo: unpublished events in FIFO order plus
// the set marked published.
type fakeOutbox struct {
	events    []domain.OutboxEvent
	published map[int64]bool
	markErr   error
	reapTTL   time.Duration // records the ttl passed to ReapPublished
}

func newFakeOutbox(ids ...int64) *fakeOutbox {
	f := &fakeOutbox{published: map[int64]bool{}}
	for _, id := range ids {
		f.events = append(f.events, domain.OutboxEvent{ID: id, EventType: domain.EventPaymentCaptured})
	}
	return f
}

func (f *fakeOutbox) FetchUnpublished(_ context.Context, limit int) ([]domain.OutboxEvent, error) {
	var out []domain.OutboxEvent
	for _, e := range f.events {
		if f.published[e.ID] {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, ids []int64) error {
	if f.markErr != nil {
		return f.markErr
	}
	for _, id := range ids {
		f.published[id] = true
	}
	return nil
}

func (f *fakeOutbox) ReapPublished(_ context.Context, ttl time.Duration) (int64, error) {
	f.reapTTL = ttl
	var n int64
	for _, e := range f.events {
		if f.published[e.ID] {
			n++
		}
	}
	return n, nil
}

// countingPublisher records delivered ids and can fail on the Nth delivery.
type countingPublisher struct {
	delivered []int64
	failOn    int64 // event id to fail on (0 = never)
	err       error
}

func (p *countingPublisher) Publish(_ context.Context, e domain.OutboxEvent) error {
	if p.failOn != 0 && e.ID == p.failOn {
		return p.err
	}
	p.delivered = append(p.delivered, e.ID)
	return nil
}

func TestOutboxRelay_EmptyIsNoop(t *testing.T) {
	pub := &countingPublisher{}
	n, err := NewOutboxRelay(newFakeOutbox(), pub).Relay(context.Background(), 10)
	if err != nil || n != 0 {
		t.Fatalf("empty relay: n=%d err=%v", n, err)
	}
	if len(pub.delivered) != 0 {
		t.Fatalf("nothing should be delivered, got %v", pub.delivered)
	}
}

func TestOutboxRelay_PublishesAndMarksAll(t *testing.T) {
	repo := newFakeOutbox(1, 2, 3)
	pub := &countingPublisher{}
	n, err := NewOutboxRelay(repo, pub).Relay(context.Background(), 10)
	if err != nil || n != 3 {
		t.Fatalf("relay all: n=%d err=%v", n, err)
	}
	if len(pub.delivered) != 3 {
		t.Fatalf("want 3 delivered, got %v", pub.delivered)
	}
	// A second pass finds nothing unpublished.
	n, _ = NewOutboxRelay(repo, pub).Relay(context.Background(), 10)
	if n != 0 {
		t.Fatalf("all should be marked published, got %d on second pass", n)
	}
}

func TestOutboxRelay_StopsAtFailureAndRedelivers(t *testing.T) {
	repo := newFakeOutbox(1, 2, 3)
	boom := errors.New("sink down")
	pub := &countingPublisher{failOn: 2, err: boom}
	relay := NewOutboxRelay(repo, pub)

	n, err := relay.Relay(context.Background(), 10)
	if !errors.Is(err, boom) || n != 1 {
		t.Fatalf("want 1 published + boom, got n=%d err=%v", n, err)
	}
	if len(pub.delivered) != 1 || pub.delivered[0] != 1 {
		t.Fatalf("only event 1 should be delivered, got %v", pub.delivered)
	}

	// Next tick with a healthy sink redelivers 2 and 3 (at-least-once, in order).
	pub.failOn = 0
	n, err = relay.Relay(context.Background(), 10)
	if err != nil || n != 2 {
		t.Fatalf("redelivery: n=%d err=%v", n, err)
	}
	if len(pub.delivered) != 3 {
		t.Fatalf("want 3 delivered total, got %v", pub.delivered)
	}
}

func TestOutboxRelay_MarkErrorPropagates(t *testing.T) {
	repo := newFakeOutbox(1, 2)
	repo.markErr = errors.New("db down")
	n, err := NewOutboxRelay(repo, &countingPublisher{}).Relay(context.Background(), 10)
	if err == nil {
		t.Fatalf("mark error must propagate, got n=%d", n)
	}
}

func TestOutboxRelay_ReapDelegates(t *testing.T) {
	repo := newFakeOutbox(1, 2, 3)
	relay := NewOutboxRelay(repo, &countingPublisher{})
	if _, err := relay.Relay(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	n, err := relay.ReapPublished(context.Background(), 48*time.Hour)
	if err != nil || n != 3 {
		t.Fatalf("reap should remove the 3 published events, got n=%d err=%v", n, err)
	}
	if repo.reapTTL != 48*time.Hour {
		t.Fatalf("ttl not passed through: %v", repo.reapTTL)
	}
}
