package sidecar

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/metrics"
)

// SidecarMetrics groups all Prometheus metric families owned by the sidecar,
// so they can be constructed once and injected into a Consumer rather than
// living as package-level globals. All fields are registered against the
// shared metrics.Registry (the same registry used by the proxy) at
// construction time via promauto.
type SidecarMetrics struct {
	// JobsProcessedTotal counts jobs processed successfully by the sidecar.
	JobsProcessedTotal prometheus.Counter

	// JobsFailedTotal counts jobs that failed (timeout or error) while
	// being processed by the sidecar.
	JobsFailedTotal prometheus.Counter

	// FetchSize is the current fetch batch size / queue depth signal,
	// set on every Fetch call in runWorker.
	FetchSize prometheus.Gauge

	// CapacityUsage is vLLM's running+waiting load, scraped by currentLoad.
	CapacityUsage prometheus.Gauge

	// JobsInFlight is the number of jobs currently being processed by this
	// sidecar (i.e. between fetch and Ack).
	JobsInFlight prometheus.Gauge

	// JobProcessingDuration is the time from job pickup to Ack, labeled by
	// error_type ("success", "timeout", "user").
	JobProcessingDuration *prometheus.HistogramVec

	// JobErrorsTotal counts job outcomes by classified error_type
	// ("success", "timeout", "user").
	JobOutcomesTotal *prometheus.CounterVec
}

// NewSidecarMetrics builds and registers all sidecar metric families against
// the shared metrics.Registry, returning the populated struct for injection
// into a Consumer. promauto.With registers each metric as it is constructed,
// so there is no separate MustRegister step needed here.
func NewSidecarMetrics() *SidecarMetrics {
	return &SidecarMetrics{
		JobsProcessedTotal: promauto.With(metrics.Registry).NewCounter(prometheus.CounterOpts{
			Namespace: "vllm_sidecar",
			Name:      "jobs_processed_total",
			Help:      "Total number of jobs processed by the sidecar.",
		}),
		JobsFailedTotal: promauto.With(metrics.Registry).NewCounter(prometheus.CounterOpts{
			Namespace: "vllm_sidecar",
			Name:      "jobs_failed_total",
			Help:      "Total number of failed jobs processed by the sidecar.",
		}),
		FetchSize: promauto.With(metrics.Registry).NewGauge(prometheus.GaugeOpts{
			Namespace: "vllm_sidecar",
			Name:      "queue_depth",
			Help:      "Current queue depth of the sidecar.",
		}),
		CapacityUsage: promauto.With(metrics.Registry).NewGauge(prometheus.GaugeOpts{
			Namespace: "vllm_sidecar",
			Name:      "capacity_usage",
			Help:      "Current capacity usage of the sidecar (0.0 to 1.0).",
		}),
		JobsInFlight: promauto.With(metrics.Registry).NewGauge(prometheus.GaugeOpts{
			Namespace: "vllm_sidecar",
			Name:      "jobs_in_flight",
			Help:      "Number of jobs currently being processed by this sidecar.",
		}),
		JobProcessingDuration: promauto.With(metrics.Registry).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "vllm_sidecar",
			Name:      "job_processing_duration_seconds",
			Help:      "Time from job pickup to Ack, in seconds.",
			// Aligned with AckWait=30s and max_process_timeout scenarios.
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
			[]string{"error_type"},
		),
		JobOutcomesTotal: promauto.With(metrics.Registry).NewCounterVec(prometheus.CounterOpts{
			Namespace: "vllm_sidecar",
			Name:      "job_outcomes_total",
			Help:      "Total number of job outcomes by classified error type.",
		},
			[]string{"error_type"},
		),
	}
}

// InitMetrics registers the sidecar's Go/process collectors with the shared
// registry. This should be called once during sidecar service startup,
// mirroring proxy.InitMetrics.
func InitMetrics() {
	metrics.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: "vllm_sidecar",
		}),
	)
}
