package v1

import (
	"context"
	"errors"
	"testing"
)

type fakeWebhookRepo struct {
	status string
	isNew  bool
	err    error
	calls  int
}

func (f *fakeWebhookRepo) Record(_ context.Context, _, _, _ string) (string, bool, error) {
	f.calls++
	return f.status, f.isNew, f.err
}

func TestWebhookProcessor_Process(t *testing.T) {
	tests := []struct {
		name      string
		repo      *fakeWebhookRepo
		wantDup   bool
		wantErr   bool
		wantState string
	}{
		{"new processed", &fakeWebhookRepo{status: "processed", isNew: true}, false, false, "processed"},
		{"new orphaned", &fakeWebhookRepo{status: "orphaned", isNew: true}, false, false, "orphaned"},
		{"duplicate carries stored status", &fakeWebhookRepo{status: "processed", isNew: false}, true, false, "processed"},
		{"repo error", &fakeWebhookRepo{err: errors.New("db down")}, false, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := NewWebhookProcessor(tt.repo).Process(context.Background(), "evt", "charge.captured", "mp_1")
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if res.Duplicate != tt.wantDup || res.Status != tt.wantState {
				t.Fatalf("got %+v", res)
			}
		})
	}
}
