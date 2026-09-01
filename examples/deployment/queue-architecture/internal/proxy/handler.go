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
	"github.com/nats-io/nats.go/jetstream"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/apierror"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/model"
)

// Result represents the inbox reply for a processed non-streaming job.
type Result struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// HandleNonStreaming returns an HTTP handler that processes non-streaming requests.
// It reads the request into a Job, subscribes on a NATS inbox, enqueues the job,
// waits for one inbox reply, and writes the response back to the client.
func HandleNonStreaming(prod Producer, timeout time.Duration, upstreamProcessing *prometheus.HistogramVec) http.HandlerFunc {
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
			Stream:  false,
		}

		inbox := nats.NewInbox()
		inbox, sub, err := prod.SubscribeSync()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to subscribe inbox: %v", err), http.StatusInternalServerError)
			return
		}
		defer sub.Unsubscribe()

		job.ReplyTo = inbox

		if err := prod.Enqueue(ctx, job); err != nil {
			status, msg := enqueueHTTPStatus(err)
			http.Error(w, msg, status)
			return
		}

		processingStart := time.Now()

		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		msg, err := sub.NextMsgWithContext(timeoutCtx)

		processingDuration := time.Since(processingStart)
		upstreamProcessing.WithLabelValues(classifyErrorType(err)).Observe(processingDuration.Seconds())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
				writeTimeoutJSON(w, timeout)
				return
			}
			http.Error(w, fmt.Sprintf("failed to wait for result: %v", err), http.StatusInternalServerError)
			return
		}

		var result Result
		if err := json.Unmarshal(msg.Data, &result); err != nil {
			http.Error(w, fmt.Sprintf("failed to unmarshal result: %v", err), http.StatusInternalServerError)
			return
		}

		for key, value := range result.Headers {
			w.Header().Set(key, value)
		}

		w.WriteHeader(result.Status)

		if _, err := w.Write([]byte(result.Body)); err != nil {
			fmt.Printf("failed to write response body: %v\n", err)
		}
	}
}

func enqueueHTTPStatus(err error) (int, string) {
	msg := fmt.Sprintf("failed to enqueue job: %v", err)
	if errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, jetstream.ErrMaxBytesExceeded) {
		return http.StatusRequestEntityTooLarge, msg
	}
	return http.StatusServiceUnavailable, msg
}

func classifyErrorType(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		return "timeout"
	}
	if errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, jetstream.ErrMaxBytesExceeded) {
		return "oversized"
	}
	return "user"
}

func writeTimeoutJSON(w http.ResponseWriter, d time.Duration) {
	body, err := apierror.NewTimeoutError(d).Bytes()
	if err != nil {
		fmt.Printf("failed to marshal timeout error: %v\n", err)
		return
	}
	w.Header().Set("Content-Type", apierror.JSONContentType)
	w.WriteHeader(apierror.TimeoutHTTPStatus)
	if _, err := w.Write(body); err != nil {
		fmt.Printf("failed to write response body: %v\n", err)
	}
}
