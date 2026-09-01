package proxy

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	inthttp "github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/http"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/metrics"
)

// ProxyMetrics groups all Prometheus metric families owned by the proxy,
// so they can be constructed once and injected into a ProxyServer rather than
// living as package-level globals. All families are registered against the
// shared metrics.Registry at construction time via promauto.
type ProxyMetrics struct {
	// RequestsTotal counts total requests received by the proxy, labeled by
	// response code and HTTP method.
	RequestsTotal *prometheus.CounterVec

	// RequestLatency records request latency in seconds.
	RequestLatency *prometheus.HistogramVec

	// Inflight is the number of requests currently being processed by the proxy.
	Inflight prometheus.Gauge

	// RequestSize records request size.
	RequestSize *prometheus.HistogramVec

	// ResponseSize records response size.
	ResponseSize *prometheus.HistogramVec

	// UpstreamProcessing records time spent waiting for sidecar to process request, labeled by error_type.
	UpstreamProcessing *prometheus.HistogramVec
}

// NewProxyMetrics builds and registers all proxy metric families against
// the shared metrics.Registry, returning the populated struct for injection
// into a ProxyServer. promauto.With registers each metric as it is constructed,
// so there is no separate MustRegister step needed here.
func NewProxyMetrics() *ProxyMetrics {
	return &ProxyMetrics{
		RequestsTotal: promauto.With(metrics.Registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "vllm_proxy",
				Name:      "requests_total",
				Help:      "Total number of requests received by the proxy.",
			},
			[]string{"code", "method"},
		),
		RequestLatency: promauto.With(metrics.Registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "vllm_proxy",
				Name:      "request_latency_seconds",
				Help:      "Request latency in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{},
		),
		Inflight: promauto.With(metrics.Registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: "vllm_proxy",
				Name:      "request_in_flight",
				Help:      "Number of requests currently being processed by the proxy.",
			},
		),
		RequestSize: promauto.With(metrics.Registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:                   "vllm_proxy",
				Name:                        "request_size",
				Help:                        "Request size.",
				NativeHistogramBucketFactor: 1.1,
			},
			[]string{},
		),
		ResponseSize: promauto.With(metrics.Registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:                   "vllm_proxy",
				Name:                        "response_size",
				Help:                        "Response size.",
				NativeHistogramBucketFactor: 1.1,
			},
			[]string{},
		),
		UpstreamProcessing: promauto.With(metrics.Registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:                   "vllm_proxy",
				Name:                        "upstream_processing_seconds",
				Help:                        "Time spent waiting for sidecar to process request.",
				NativeHistogramBucketFactor: 1.1,
			},
			[]string{"error_type"},
		),
	}
}

// InitMetrics registers the Go/process collectors with the shared registry.
// This should be called once during proxy service startup, mirroring the
// sidecar InitMetrics pattern.
func InitMetrics() {
	metrics.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			Namespace: "vllm_proxy",
		}),
	)
}

// wrapReqTotal returns a middleware that instruments request count metrics.
func wrapReqTotal(m *ProxyMetrics) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return promhttp.InstrumentHandlerCounter(m.RequestsTotal, h)
	}
}

// wrapReqLatency returns a middleware that instruments request latency metrics.
func wrapReqLatency(m *ProxyMetrics) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return promhttp.InstrumentHandlerDuration(m.RequestLatency, h)
	}
}

// wrapInflight returns a middleware that instruments in-flight request metrics.
func wrapInflight(m *ProxyMetrics) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return promhttp.InstrumentHandlerInFlight(m.Inflight, h)
	}
}

// wrapReqSize returns a middleware that instruments request size metrics.
func wrapReqSize(m *ProxyMetrics) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return promhttp.InstrumentHandlerRequestSize(m.RequestSize, h)
	}
}

// wrapResSize returns a middleware that instruments response size metrics.
func wrapResSize(m *ProxyMetrics) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return promhttp.InstrumentHandlerResponseSize(m.ResponseSize, h)
	}
}

// Instrument wraps an http.Handler with all proxy metrics instrumentation.
// The handler is instrumented in this order: count, latency, in-flight,
// request size, response size.
func (m *ProxyMetrics) Instrument(h http.Handler) http.Handler {
	return inthttp.Chain(h,
		wrapReqTotal(m),
		wrapReqLatency(m),
		wrapInflight(m),
		wrapReqSize(m),
		wrapResSize(m),
	)
}
