package sidecar

import (
	"context"

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/circuit"
)

// CircuitMetricsObserver maps admission circuit events to sidecar Prometheus
// metrics. Construct once and pass to circuit.WithObserver.
type CircuitMetricsObserver struct {
	metrics *SidecarMetrics
}

func NewCircuitMetricsObserver(metrics *SidecarMetrics) *CircuitMetricsObserver {
	o := &CircuitMetricsObserver{metrics: metrics}
	metrics.CircuitState.Set(float64(circuit.CircuitClosed))
	return o
}

func (o *CircuitMetricsObserver) OnProbe(_ context.Context, signal circuit.CapacitySignal, err error) {
	if err != nil {
		o.metrics.CircuitProbeErrorsTotal.Inc()
		return
	}
	o.metrics.CapacityUsage.Set(signal.Load)
}

func (o *CircuitMetricsObserver) OnTransition(
	_ context.Context,
	from, to circuit.CircuitState,
	reason circuit.TransitionReason,
) {
	o.metrics.CircuitState.Set(float64(to))
	o.metrics.CircuitTransitionsTotal.WithLabelValues(
		from.String(),
		to.String(),
		string(reason),
	).Inc()
}
