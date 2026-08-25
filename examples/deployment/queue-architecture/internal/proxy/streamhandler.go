package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// HandleStreaming returns an HTTP handler that processes streaming requests.
// It builds a Job with Stream=true, enqueues it, subscribes to the job's Pub/Sub channel,
// and relays each chunk to the client as SSE until receiving the done sentinel.
func HandleStreaming(rdb *redis.Client, streamName string, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Generate a unique job ID
		jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

		// Read request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// Build the Job with Stream=true
		job := queue.Job{
			JobID:   jobID,
			Method:  r.Method,
			Path:    r.RequestURI,
			Headers: headerMapFromRequest(r),
			Body:    body,
			Stream:  true,
		}

		// Subscribe to the job's Pub/Sub channel BEFORE enqueueing
		channel := fmt.Sprintf("stream:%s", jobID)
		subscription := rdb.Subscribe(ctx, channel)
		defer subscription.Close()

		// Receive confirmation that the subscription is established
		// (go-redis sends SUBSCRIBE asynchronously; Receive blocks until confirmed)
		_, err = subscription.Receive(ctx)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to subscribe: %v", err), http.StatusInternalServerError)
			return
		}

		// Set up SSE headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		// Enqueue the job AFTER subscription is confirmed
		_, err = queue.Enqueue(ctx, rdb, streamName, job)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to enqueue job: %v", err), http.StatusInternalServerError)
			return
		}

		// Create a timeout context for the subscription
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Read from the subscription channel and relay to client
		for {
			select {
			case <-timeoutCtx.Done():
				// Timeout occurred
				fmt.Fprintf(w, "data: {\"error\": \"timeout\"}\n\n")
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				return
			case msg := <-subscription.Channel():
				if msg == nil {
					// Channel closed
					return
				}

				// Parse the message payload
				var payload map[string]interface{}
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
					fmt.Fprintf(w, "data: {\"error\": \"invalid payload\"}\n\n")
					if flusher, ok := w.(http.Flusher); ok {
						flusher.Flush()
					}
					continue
				}

				// frameRedisPayload translates the sidecar's internal Redis
				// transport encoding back into standard OpenAI SSE framing
				// - see its doc comment for why this translation exists.
				frame, terminal := frameRedisPayload(payload)
				fmt.Fprint(w, frame)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				if terminal {
					return
				}
			}
		}
	}
}

// frameRedisPayload decides the SSE frame to write for one parsed Redis
// Pub/Sub message, and whether the caller should stop after writing it.
//
// The sidecar (ForwardStreaming) translates the upstream's real "[DONE]"
// SSE sentinel into an internal {"__done": true} JSON marker on the way in,
// because Redis Pub/Sub can't carry a bare string payload. This function
// does the reverse translation on the way out, back into the standard
// OpenAI "[DONE]" sentinel real clients expect. Forwarding the raw internal
// marker verbatim breaks strict OpenAI-compatible SSE clients (e.g. the
// Vercel AI SDK used by opencode), which reject it outright: it matches
// neither the chat-completion-chunk schema (no "choices"/"object"/"id")
// nor an error object.
func frameRedisPayload(payload map[string]interface{}) (frame string, terminal bool) {
	if done, ok := payload["__done"].(bool); ok && done {
		return "data: [DONE]\n\n", true
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "data: {\"error\": \"marshal failed\"}\n\n", false
	}
	return fmt.Sprintf("data: %s\n\n", string(data)), false
}

// headerMapFromRequest extracts headers from the HTTP request.
func headerMapFromRequest(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return headers
}
