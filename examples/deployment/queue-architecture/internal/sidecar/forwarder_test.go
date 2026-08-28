package sidecar

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/model"
)

func TestForwardNonStreaming_RespectsContextDeadline(t *testing.T) {
	unblock := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	job := model.Job{JobID: "job-1", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{}`)}

	start := time.Now()
	_, _, _, err := ForwardNonStreaming(ctx, job, srv.URL)
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

func TestForwardNonStreaming_SucceedsWithinDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job := model.Job{JobID: "job-2", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{}`)}

	status, _, body, err := ForwardNonStreaming(ctx, job, srv.URL)
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
