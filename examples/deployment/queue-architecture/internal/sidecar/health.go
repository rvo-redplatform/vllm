package sidecar

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// metricNumRequestsWaitingByReason is vLLM's own per-reason breakdown of
// requests it is holding in its internal admission queue rather than
// running right now. We only care about the "capacity" reason: it fires
// exactly when vLLM's continuous-batching scheduler cannot admit another
// sequence into its current KV-cache budget. This is vLLM's own real
// backpressure signal -- not an external, hand-picked concurrency ceiling.
//
// The "deferred" reason (LoRA budget, KV transfer, blocked status) is
// deliberately excluded: those are transient scheduling artifacts
// unrelated to whether this vLLM instance is actually out of room, and
// treating them as capacity pressure would cause the sidecar to back off
// for the wrong reason.
const metricNumRequestsWaitingByReason = "vllm:num_requests_waiting_by_reason"

// capacityReasonLabel is the label value on metricNumRequestsWaitingByReason
// that indicates genuine scheduler capacity pressure (as opposed to
// "deferred").
const capacityReasonLabel = `reason="capacity"`

// WaitForHealthy polls target's /health endpoint until it returns HTTP 200,
// or ctx is cancelled.
//
// Without this, the sidecar (a separate container in the same pod, with no
// startup-ordering guarantee relative to the vLLM container) would begin
// consuming from the queue and attempt to forward the very first job to a
// vLLM instance that is still mid multi-minute cold start (model load,
// cudagraph capture, etc.) -- getting connection-refused, burning the
// client-facing request timeout for no reason, even though the job
// eventually succeeds much later via reclaim.
func WaitForHealthy(ctx context.Context, client *http.Client, target string, pollInterval time.Duration) error {
	healthURL := target + "/health"
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		attempt++
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err == nil {
			resp, doErr := client.Do(req)
			if doErr == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					slog.InfoContext(ctx, "vLLM target is health",
						"target", target,
						"attempt", attempt,
					)
					return nil
				}
				slog.InfoContext(ctx, "waiting for vLLM target to become healthy",
					"target", target,
					"status", resp.StatusCode,
					"attempt", attempt,
				)
			} else {
				slog.ErrorContext(ctx, "error waiting on vLLM target",
					"target", target,
					"attempt", attempt,
					"err", doErr,
				)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// capacityPressure scrapes target's /metrics endpoint and returns vLLM's own
// vllm:num_requests_waiting_by_reason{reason="capacity"} gauge value --
// vLLM's real-time count of requests it cannot currently admit into its
// KV-cache budget. Scraped fresh on every call so the backoff decision
// reflects the engine's true current state, not an assumed/hardcoded
// ceiling.
//
// A return value of 0 means vLLM has room right now. Any value > 0 means
// vLLM's own scheduler is already holding back requests -- i.e. this
// instance is genuinely full, independent of however many requests the
// sidecar itself has in flight.
func capacityPressure(ctx context.Context, client *http.Client, target string) (float64, error) {
	metricsURL := target + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build metrics request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch metrics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metricNumRequestsWaitingByReason) {
			continue
		}
		// Prometheus text exposition format:
		// "<metric_name>{labels} <value>". Only the "capacity" reason
		// line matters here; the "deferred" reason line is skipped.
		if !strings.Contains(line, capacityReasonLabel) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, parseErr := strconv.ParseFloat(fields[len(fields)-1], 64)
		if parseErr != nil {
			continue
		}
		return val, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan metrics response: %w", err)
	}

	// Metric line not found at all (e.g. older vLLM without this gauge).
	// Treat as "no pressure reported" rather than erroring the whole
	// scrape -- callers that want strict enforcement should monitor for
	// this via logs/metrics rather than the gate wedging shut.
	return 0, nil
}
