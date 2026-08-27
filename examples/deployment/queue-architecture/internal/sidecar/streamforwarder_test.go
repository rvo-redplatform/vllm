package sidecar

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// testRedisAddr returns the address of a real Redis/Valkey instance to test
// against, skipping the test if none is reachable. ForwardStreaming's job is
// to relay onto a real Pub/Sub channel, so these tests need the real thing
// rather than a mock -- set REDIS_TEST_ADDR to override (default
// localhost:6379), e.g.:
//
//	docker run -d --rm -p 6379:6379 redis:7-alpine
//	go test ./internal/sidecar/...
func testRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Skipf("no Redis reachable at %s (set REDIS_TEST_ADDR or run `docker run -d --rm -p 6379:6379 redis:7-alpine`): %v", addr, err)
	}
	conn.Close()
	return redis.NewClient(&redis.Options{Addr: addr})
}

// TestForwardStreaming_RespectsContextDeadline guards against the same
// class of bug as TestForwardNonStreaming_RespectsContextDeadline, but for
// the streaming path: ForwardStreaming used to construct its own
// http.Client{} with no Timeout, so a vLLM backend that accepted the
// connection but never sent any SSE data (or stalled mid-stream) left the
// call blocked forever with the client's job never resolved.
func TestForwardStreaming_RespectsContextDeadline(t *testing.T) {
	rdb := testRedisClient(t)
	defer rdb.Close()

	unblock := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Simulate a stream that stalls forever after headers are sent.
		<-unblock
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	job := queue.Job{JobID: "stream-job-1", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{"stream":true}`), Stream: true}

	start := time.Now()
	err := ForwardStreaming(ctx, http.DefaultClient, rdb, job, srv.URL)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected an error once the context deadline passed, got nil")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("ForwardStreaming took %v to return after a 100ms deadline -- it did not actually respect the context", elapsed)
	}
}

// TestForwardStreaming_RelaysChunksAndDoneSentinel is the control case: a
// normal, promptly-responding SSE backend must still relay chunks and the
// [DONE] sentinel exactly as before.
func TestForwardStreaming_RelaysChunksAndDoneSentinel(t *testing.T) {
	rdb := testRedisClient(t)
	defer rdb.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		w.Write([]byte("data: {\"id\":\"chunk-1\"}\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
		w.Write([]byte("data: [DONE]\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job := queue.Job{JobID: "stream-job-2", Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{"stream":true}`), Stream: true}

	sub := rdb.Subscribe(ctx, "stream:stream-job-2")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- ForwardStreaming(ctx, http.DefaultClient, rdb, job, srv.URL)
	}()

	var messages []string
	for {
		select {
		case msg := <-sub.Channel():
			messages = append(messages, msg.Payload)
			var done map[string]bool
			if json.Unmarshal([]byte(msg.Payload), &done) == nil && done["__done"] {
				goto Done
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for messages, got so far: %v", messages)
		}
	}
Done:
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages (chunk + done), got %d: %v", len(messages), messages)
	}
}
