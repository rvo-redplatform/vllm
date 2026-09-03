package circuit

import "context"

// TransitionReason explains why the circuit changed state.
type TransitionReason string

const (
	ReasonSaturated    TransitionReason = "saturated"
	ReasonRecovered    TransitionReason = "recovered"
	ReasonProbeFailure TransitionReason = "probe_failure"
	ReasonRecovery     TransitionReason = "recovery"
	ReasonSafetyOpen   TransitionReason = "safety_open"
)

// Observer receives circuit telemetry. Implementations must be fast — they
// may be called while the circuit mutex is held during state transitions.
type Observer interface {
	// OnProbe is invoked after every probe attempt.
	OnProbe(ctx context.Context, signal CapacitySignal, err error)

	// OnTransition is invoked only when state actually changes.
	OnTransition(ctx context.Context, from, to CircuitState, reason TransitionReason)
}

type NoOpObserver struct{}

func (o NoOpObserver) OnProbe(_ context.Context, _ CapacitySignal, _ error) {}

func (o NoOpObserver) OnTransition(_ context.Context, _, _ CircuitState, _ TransitionReason) {}
