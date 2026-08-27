package sidecar

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// TestForwardNonStreaming_RespectsContextDeadline guards against the bug
// that caused a stuck production consumer to hold 5 pending stream messages
// indefinitely with a climbing redelivery count: ForwardNonStreaming used to
// construct its own http.Client{} with no Timeout, so a request to a vLLM
// instance that stopped responding mid-request blocked forever, even though
// the request itself carried a ctx. With a bounded ctx now honored, the
// call must return promptly once the deadline passes -- not hang until the
// server (which never responds) eventually does something.
func TestForwardNonStreaming_RespectsContextDeadline(t *testing.T) {
	unblock := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a hung/stalled vLLM backend: never respond until the
		// test unblocks it, well after the client should have given up.
		<-unblock
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	job := queue.Job{JobID: "job-1", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{}`)}

	start := time.Now()
	_, _, _, err := ForwardNonStreaming(ctx, http.DefaultClient, job, srv.URL)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected an error once the context deadline passed, got nil")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected ctx.Err() to be DeadlineExceeded, got %v", ctx.Err())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("ForwardNonStreaming took %v to return after a 100ms deadline -- it did not actually respect the context", elapsed)
	}
}

// TestForwardNonStreaming_SucceedsWithinDeadline is the control case: a
// normal, promptly-responding backend must still work exactly as before.
func TestForwardNonStreaming_SucceedsWithinDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job := queue.Job{JobID: "job-2", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{}`)}

	status, _, body, err := ForwardNonStreaming(ctx, http.DefaultClient, job, srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("expected status 200, got %d", status)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("unexpected body: %q", body)
	}
}
