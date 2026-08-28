package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/apierror"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/model"
)

// HandleStreaming returns an HTTP handler that processes streaming requests.
// It builds a Job with Stream=true, subscribes on a NATS inbox before enqueue,
// and relays each inbox token to the client as SSE until receiving __done.
func HandleStreaming(prod Producer, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		job := model.Job{
			JobID:   ulid.Make().String(),
			Method:  r.Method,
			Path:    r.RequestURI,
			Headers: headerMapFromRequest(r),
			Body:    body,
			Stream:  true,
		}

		inbox, sub, err := prod.SubscribeSync()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to subscribe inbox: %v", err), http.StatusInternalServerError)
			return
		}
		defer sub.Unsubscribe()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		job.ReplyTo = inbox

		if err := prod.Enqueue(ctx, job); err != nil {
			status, msg := enqueueHTTPStatus(err)
			http.Error(w, msg, status)
			return
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		for {
			msg, err := sub.NextMsgWithContext(timeoutCtx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
					writeTimeoutSSE(w, timeout)
					return
				}
				fmt.Fprintf(w, "data: {\"error\": \"subscription error\"}\n\n")
				flushSSE(w)
				return
			}

			var payload map[string]interface{}
			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				fmt.Fprintf(w, "data: {\"error\": \"invalid payload\"}\n\n")
				flushSSE(w)
				continue
			}

			frame, terminal := frameInboxPayload(payload)
			fmt.Fprint(w, frame)
			flushSSE(w)
			if terminal {
				return
			}
		}
	}
}

// frameInboxPayload decides the SSE frame to write for one parsed inbox
// message, and whether the caller should stop after writing it.
//
// The sidecar translates upstream "[DONE]" into an internal
// {"__done": true} JSON marker because the inbox carries JSON payloads.
// This function does the reverse translation on the way out, back into
// the standard OpenAI "[DONE]" sentinel. Forwarding the raw internal
// marker breaks strict OpenAI-compatible SSE clients (e.g. the Vercel AI
// SDK), which reject it: it matches neither the chat-completion-chunk
// schema nor an error object.
func frameInboxPayload(payload map[string]interface{}) (frame string, terminal bool) {
	if done, ok := payload["__done"].(bool); ok && done {
		return "data: [DONE]\n\n", true
	}

	if isErr, ok := payload["error"].(bool); ok && isErr {
		return frameErrorPayload(payload), false
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "data: {\"error\": \"marshal failed\"}\n\n", false
	}
	return fmt.Sprintf("data: %s\n\n", string(data)), false
}

// frameErrorPayload translates the sidecar's internal error transport shape
// ({"error": true, "status": <code>, "body": "<raw upstream body>"}) into a
// standard OpenAI-compatible SSE error frame. vLLM's own error bodies are
// already OpenAI-style, so when the body parses as one it is forwarded
// as-is. Otherwise a minimal error object is synthesized.
func frameErrorPayload(payload map[string]interface{}) string {
	bodyStr, _ := payload["body"].(string)

	var upstream map[string]interface{}
	if bodyStr != "" && json.Unmarshal([]byte(bodyStr), &upstream) == nil {
		if _, hasErrorObj := upstream["error"].(map[string]interface{}); hasErrorObj {
			if data, err := json.Marshal(upstream); err == nil {
				return fmt.Sprintf("data: %s\n\n", string(data))
			}
		}
	}

	message := bodyStr
	if message == "" {
		message = "upstream error"
	}
	statusCode, _ := payload["status"].(float64) // json.Unmarshal decodes numbers as float64
	synthesized := map[string]interface{}{
		"error": map[string]interface{}{
			"message": message,
			"type":    "upstream_error",
			"code":    int(statusCode),
		},
	}
	data, err := json.Marshal(synthesized)
	if err != nil {
		return "data: {\"error\": {\"message\": \"upstream error\", \"type\": \"upstream_error\"}}\n\n"
	}
	return fmt.Sprintf("data: %s\n\n", string(data))
}

func headerMapFromRequest(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for key, values := range r.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return headers
}

func flushSSE(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeTimeoutSSE(w http.ResponseWriter, d time.Duration) {
	body, err := apierror.NewTimeoutError(d).Bytes()
	if err != nil {
		fmt.Printf("failed to marshal timeout error: %v\n", err)
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", body)
	flushSSE(w)
}
