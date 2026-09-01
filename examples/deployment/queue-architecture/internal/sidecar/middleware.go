package sidecar

import (
	"context"
	"time"

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// ProcessJobFunc is the shape of the core job-processing step: forward the
// fetched job to target and return its outcome. maxProcessTimeout is passed
// through for middleware that needs it (e.g. to log/observe against the
// configured ceiling); it is not itself used to bound processCtx here --
// that bound is already applied by the caller before invoking the chain.
type ProcessJobFunc func(ctx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) jobOutcome

// JobMiddleware wraps a ProcessJobFunc with cross-cutting behavior (metrics,
// logging, etc), mirroring the http.Handler middleware pattern used by the
// proxy (see internal/http/middleware.go and internal/proxy/metrics.go).
type JobMiddleware func(ProcessJobFunc) ProcessJobFunc

// ChainJobMiddleware composes mw around handler in the given order: the
// first middleware listed is outermost (runs first on the way in, last on
// the way out), matching inthttp.Chain's convention.
func ChainJobMiddleware(handler ProcessJobFunc, mw ...JobMiddleware) ProcessJobFunc {
	for i := len(mw) - 1; i >= 0; i-- {
		handler = mw[i](handler)
	}
	return handler
}

// classifyOutcome buckets a job's outcome into the sidecar's error_type
// label set. Unlike the proxy (which also sees an "oversized" case from
// NATS max-payload rejection at enqueue time), the sidecar only ever sees
// errors surfaced by runJob's forward to vLLM (ForwardNonStreaming /
// ForwardStreaming), so there is no oversized-payload case to classify here
// -- just success, timeout, or a generic user-facing error.
func classifyOutcome(processCtx context.Context, err error) string {
	switch {
	case err == nil:
		return "success"
	case isTimeout(processCtx, err):
		return "timeout"
	default:
		return "user"
	}
}

// WithInFlight tracks JobsInFlight around the full call, including any time
// spent waiting on other middleware in the chain. This is intended to be
// the outermost middleware so the gauge reflects the true in-flight window
// for the job, not just the inner processing time.
func WithInFlight(m *SidecarMetrics) JobMiddleware {
	return func(next ProcessJobFunc) ProcessJobFunc {
		return func(ctx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) jobOutcome {
			m.JobsInFlight.Inc()
			defer m.JobsInFlight.Dec()
			return next(ctx, fetched, target, maxProcessTimeout)
		}
	}
}

// WithProcessingDuration times the wrapped call and observes
// JobProcessingDuration labeled by the classified error_type. Intended to
// sit as close to the actual runJob call as possible so the observed
// duration reflects only forward/processing time, not other middleware
// overhead.
func WithProcessingDuration(m *SidecarMetrics) JobMiddleware {
	return func(next ProcessJobFunc) ProcessJobFunc {
		return func(ctx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) jobOutcome {
			start := time.Now()
			out := next(ctx, fetched, target, maxProcessTimeout)
			m.JobProcessingDuration.WithLabelValues(classifyOutcome(ctx, out.err)).Observe(time.Since(start).Seconds())
			return out
		}
	}
}

// WithJobCounters increments JobsProcessedTotal/JobsFailedTotal/
// JobErrorsTotal based on the classified outcome of the wrapped call.
// Intended to sit innermost, directly around the runJob call, so the
// classification reflects exactly what runJob returned.
func WithJobCounters(m *SidecarMetrics) JobMiddleware {
	return func(next ProcessJobFunc) ProcessJobFunc {
		return func(ctx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) jobOutcome {
			out := next(ctx, fetched, target, maxProcessTimeout)

			errType := classifyOutcome(ctx, out.err)
			m.JobOutcomesTotal.WithLabelValues(errType).Inc()
			if errType == "success" {
				m.JobsProcessedTotal.Inc()
			} else {
				m.JobsFailedTotal.Inc()
			}

			return out
		}
	}
}
