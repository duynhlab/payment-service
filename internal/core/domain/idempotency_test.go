package domain

import "testing"

func TestIdempotencyKey_Finished(t *testing.T) {
	var unfinished IdempotencyKey
	if unfinished.Finished() {
		t.Fatal("a key with no cached response code must not be finished")
	}
	code := 201
	finished := IdempotencyKey{ResponseCode: &code}
	if !finished.Finished() {
		t.Fatal("a key with a cached response code must be finished")
	}
}
