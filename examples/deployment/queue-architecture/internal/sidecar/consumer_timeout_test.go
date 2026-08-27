package sidecar

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// TestProcessJob_NonStreamingTimeout_WritesResultAndAcks is the end-to-end
// regression test for the production incident: a sidecar
// (vllm-deployment-vllm-7cdd8d9d9c-gm87n) held 5 unacked stream messages
// with climbing redelivery counts (idle time growing past 14 minutes)
// because a hung forward-to-vLLM call never timed out, so processJob never
// wrote a result or acked -- and ReclaimLoop kept handing the same stuck
// message right back to the same consumer every reclaim tick.
//
// With JOB_TIMEOUT enforced, processJob must, on a hung forward:
//  1. give up once jobTimeout elapses (not hang indefinitely)
//  2. write a 504 timeout result so the waiting proxy request unblocks
//  3. ack the message so it is removed from the pending entries list --
//     it must NOT be left there for ReclaimLoop to endlessly re-deliver.
func TestProcessJob_NonStreamingTimeout_WritesResultAndAcks(t *testing.T) {
	rdb := testRedisClient(t)
	defer rdb.Close()

	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock // simulate a hung vLLM backend, same as the incident
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	streamName := fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
	groupName := "test-group"
	consumerName := "test-consumer"
	ctx := t.Context()

	if err := queue.EnsureConsumerGroup(ctx, rdb, streamName, groupName); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	defer rdb.Del(ctx, streamName)

	job := queue.Job{JobID: fmt.Sprintf("job-%d", time.Now().UnixNano()), Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{}`)}
	if _, err := queue.Enqueue(ctx, rdb, streamName, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	defer rdb.Del(ctx, "result:"+job.JobID)

	readJob, entryID, err := queue.Read(ctx, rdb, streamName, groupName, consumerName)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	start := time.Now()
	err = processJob(ctx, http.DefaultClient, rdb, readJob, entryID, streamName, groupName, srv.URL, time.Hour, 100*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("processJob returned an infra error (expected nil, per-job errors are logged not returned): %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("processJob took %v to return for a job with a 100ms JOB_TIMEOUT -- it did not actually bound the hung forward", elapsed)
	}

	resultRaw, err := rdb.LRange(ctx, "result:"+job.JobID, 0, -1).Result()
	if err != nil || len(resultRaw) != 1 {
		t.Fatalf("expected exactly one result to be written, got %v (err=%v)", resultRaw, err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultRaw[0]), &result); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if status, _ := result["status"].(float64); int(status) != http.StatusGatewayTimeout {
		t.Fatalf("expected result status 504, got %v", result["status"])
	}

	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: streamName, Group: groupName, Start: "-", End: "+", Count: 10}).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected the timed-out job to be acked (0 pending entries), got %d pending: %+v -- a timed-out job left pending would be endlessly redelivered by ReclaimLoop, exactly like the production incident", len(pending), pending)
	}
}

// TestProcessJob_StreamingTimeout_PublishesErrorAndAcks mirrors the
// non-streaming case above for the SSE path: on a hung/stalled stream, the
// subscribed client must receive a timeout error frame followed by the done
// sentinel (so streamhandler.go's relay loop terminates instead of hanging
// until its own much longer STREAM_TIMEOUT), and the message must be acked.
func TestProcessJob_StreamingTimeout_PublishesErrorAndAcks(t *testing.T) {
	rdb := testRedisClient(t)
	defer rdb.Close()

	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-unblock // simulate a stream that stalls forever after headers
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	streamName := fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
	groupName := "test-group"
	consumerName := "test-consumer"
	ctx := t.Context()

	if err := queue.EnsureConsumerGroup(ctx, rdb, streamName, groupName); err != nil {
		t.Fatalf("EnsureConsumerGroup: %v", err)
	}
	defer rdb.Del(ctx, streamName)

	job := queue.Job{JobID: fmt.Sprintf("stream-job-%d", time.Now().UnixNano()), Method: "POST", Path: "/v1/chat/completions", Body: []byte(`{"stream":true}`), Stream: true}

	sub := rdb.Subscribe(ctx, "stream:"+job.JobID)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if _, err := queue.Enqueue(ctx, rdb, streamName, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	readJob, entryID, err := queue.Read(ctx, rdb, streamName, groupName, consumerName)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- processJob(ctx, http.DefaultClient, rdb, readJob, entryID, streamName, groupName, srv.URL, time.Hour, 100*time.Millisecond)
	}()

	var gotErrorFrame, gotDone bool
	timeout := time.After(3 * time.Second)
	for !gotDone {
		select {
		case msg := <-sub.Channel():
			var payload map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
				t.Fatalf("payload not valid JSON: %v", err)
			}
			if isErr, _ := payload["error"].(bool); isErr {
				gotErrorFrame = true
				if status, _ := payload["status"].(float64); int(status) != http.StatusGatewayTimeout {
					t.Fatalf("expected error status 504, got %v", payload["status"])
				}
			}
			if done, _ := payload["__done"].(bool); done {
				gotDone = true
			}
		case <-timeout:
			t.Fatalf("timed out waiting for timeout error + done sentinel (gotErrorFrame=%v, gotDone=%v)", gotErrorFrame, gotDone)
		}
	}
	if !gotErrorFrame {
		t.Fatalf("expected a timeout error frame before the done sentinel, got none")
	}

	if err := <-doneCh; err != nil {
		t.Fatalf("processJob returned an infra error: %v", err)
	}

	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: streamName, Group: groupName, Start: "-", End: "+", Count: 10}).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected the timed-out streaming job to be acked (0 pending entries), got %d pending: %+v", len(pending), pending)
	}
}
