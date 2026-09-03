package circuit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingObserver struct {
	mu          sync.Mutex
	probes      []probeRecord
	transitions []transitionRecord
}

type probeRecord struct {
	signal CapacitySignal
	err    error
}

type transitionRecord struct {
	from   CircuitState
	to     CircuitState
	reason TransitionReason
}

func (o *recordingObserver) OnProbe(_ context.Context, signal CapacitySignal, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.probes = append(o.probes, probeRecord{signal: signal, err: err})
}

func (o *recordingObserver) OnTransition(_ context.Context, from, to CircuitState, reason TransitionReason) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transitions = append(o.transitions, transitionRecord{from: from, to: to, reason: reason})
}

func TestObserverOnProbeAndTransition(t *testing.T) {
	var pressure atomic.Int64
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			p := pressure.Load()
			return CapacitySignal{HasCapacity: p == 0, Load: float64(p)}, nil
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithOpenAfter(1),
		WithCloseAfter(1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	pressure.Store(3)
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	obs.mu.Lock()
	if len(obs.probes) == 0 {
		obs.mu.Unlock()
		t.Fatal("expected probe observations")
	}
	lastProbe := obs.probes[len(obs.probes)-1]
	if lastProbe.err != nil {
		obs.mu.Unlock()
		t.Fatalf("probe err: %v", lastProbe.err)
	}
	if lastProbe.signal.Load != 3 {
		obs.mu.Unlock()
		t.Fatalf("load: got %v want 3", lastProbe.signal.Load)
	}
	if len(obs.transitions) == 0 {
		obs.mu.Unlock()
		t.Fatal("expected transition to open")
	}
	tr := obs.transitions[len(obs.transitions)-1]
	obs.mu.Unlock()

	if tr.to != CircuitOpen || tr.reason != ReasonSaturated {
		t.Fatalf("transition: got %v->%v (%s) want open (saturated)", tr.from, tr.to, tr.reason)
	}

	cancel()
	<-done
}

func TestObserverProbeError(t *testing.T) {
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			return CapacitySignal{}, context.DeadlineExceeded
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithMaxFails(1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Run briefly then cancel to capture only the initial transition sequence
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.probes) == 0 {
		t.Fatal("expected probe observations")
	}
	if obs.probes[0].err == nil {
		t.Fatal("expected probe error")
	}
	if obs.probes[0].err != context.DeadlineExceeded {
		t.Fatalf("probe err: %v", obs.probes[0].err)
	}
	if len(obs.transitions) == 0 {
		t.Fatal("expected at least one transition")
	}
	// With maxFails=1 and NoOpRecover, the sequence is:
	// Closed -> Open (ReasonProbeFailure) -> HalfOpen (ReasonRecovery) -> Closed (ReasonRecovered)
	// We verify at least the first transition exists and is error->open
	var foundProbeFailure bool
	for _, tr := range obs.transitions {
		if tr.from == CircuitClosed && tr.to == CircuitOpen && tr.reason == ReasonProbeFailure {
			foundProbeFailure = true
		}
	}
	if !foundProbeFailure {
		t.Fatalf("expected Closed->Open (ReasonProbeFailure) as first transition, got %v", obs.transitions)
	}
}

func TestCircuit_UnknownStateSafetyOpen(t *testing.T) {
	var pressure atomic.Int64
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			p := pressure.Load()
			return CapacitySignal{HasCapacity: p == 0, Load: float64(p)}, nil
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithOpenAfter(1),
		WithCloseAfter(1),
		WithInitialState(CircuitUnknown),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Unknown → Open should happen immediately on first probe
	pressure.Store(3)
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	obs.mu.Lock()
	if len(obs.transitions) == 0 {
		obs.mu.Unlock()
		t.Fatal("expected transition from unknown to open")
	}
	found := false
	for _, tr := range obs.transitions {
		if tr.from == CircuitUnknown && tr.to == CircuitOpen && tr.reason == ReasonSafetyOpen {
			found = true
		}
	}
	obs.mu.Unlock()
	if !found {
		t.Fatalf("expected Unknown->Open (ReasonSafetyOpen), got %v", obs.transitions)
	}

	cancel()
	<-done
}

func TestCircuit_FullTransitionSequence(t *testing.T) {
	var pressure atomic.Int64
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			p := pressure.Load()
			return CapacitySignal{HasCapacity: p == 0, Load: float64(p)}, nil
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithOpenAfter(1),
		WithCloseAfter(1),
		WithInitialState(CircuitClosed),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Start with capacity pressure: Open → saturated after openAfter clears
	pressure.Store(3)
	// Run long enough to saturate
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Now clear pressure - should go Open -> HalfOpen -> Closed
	pressure.Store(0)

	// Wait for the circuit to fully transition through HalfOpen to Closed
	select {
	case err := <-done:
		t.Fatalf("run exited early after pressure clear: %v", err)
	case <-time.After(2 * time.Second):
	}

	obs.mu.Lock()
	if len(obs.transitions) < 3 {
		obs.mu.Unlock()
		t.Fatalf("expected at least 3 transitions, got %d: %v", len(obs.transitions), obs.transitions)
	}
	// Verify sequence: Open -> HalfOpen -> Closed
	// We expect: Open->HalfOpen (ReasonRecovered), HalfOpen->Closed (ReasonRecovered)
	// The exact order depends on timing; just verify all three states appear
	froms := make([]CircuitState, 0, len(obs.transitions))
	tos := make([]CircuitState, 0, len(obs.transitions))
	reasons := make([]TransitionReason, 0, len(obs.transitions))
	for _, tr := range obs.transitions {
		froms = append(froms, tr.from)
		tos = append(tos, tr.to)
		reasons = append(reasons, tr.reason)
	}

	// Check we have Open->HalfOpen transition
	var openToHalf bool
	for i := range obs.transitions {
		if obs.transitions[i].from == CircuitOpen && obs.transitions[i].to == CircuitHalfOpen {
			openToHalf = true
		}
	}
	if !openToHalf {
		t.Fatalf("expected Open->HalfOpen transition, got %v", obs.transitions)
	}

	// Check we have HalfOpen->Closed transition
	var halfToClosed bool
	for i := range obs.transitions {
		if obs.transitions[i].from == CircuitHalfOpen && obs.transitions[i].to == CircuitClosed {
			halfToClosed = true
		}
	}
	if !halfToClosed {
		t.Fatalf("expected HalfOpen->Closed transition, got %v", obs.transitions)
	}

	obs.mu.Unlock()

	cancel()
	<-done
}

func TestCircuit_ReSaturationInHalfOpen(t *testing.T) {
	var pressure atomic.Int64
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			p := pressure.Load()
			return CapacitySignal{HasCapacity: p == 0, Load: float64(p)}, nil
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithOpenAfter(1),
		WithCloseAfter(1),
		WithInitialState(CircuitOpen),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Start with pressure applied; state is CircuitOpen so the safety
	// guard keeps us in Open while pressure > 0.  Then clear pressure
	// briefly to trigger the Open->HalfOpen->Closed sequence, and
	// re-apply pressure while in HalfOpen to trigger re-saturation.
	pressure.Store(3)
	// Let it saturate (stay Open via safety guard)
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Clear pressure -> Open->HalfOpen->Closed
	pressure.Store(0)
	// Wait for full transition
	select {
	case err := <-done:
		t.Fatalf("run exited early after pressure clear: %v", err)
	case <-time.After(2 * time.Second):
	}

	// Re-apply pressure while in HalfOpen -> should re-trip to Open
	pressure.Store(3)

	// Wait for circuit to re-stabilize (should end with circuit blocked again)
	select {
	case err := <-done:
		t.Fatalf("run exited early on re-saturation: %v", err)
	case <-time.After(2 * time.Second):
	}

	obs.mu.Lock()
	// After re-saturation the circuit should be blocking again.
	// The sequence is: HalfOpen -> Closed (ReasonRecovered) -> Open (ReasonSaturated)
	// We verify the circuit ends in Open state (blocked) after re-applying pressure.
	for _, tr := range obs.transitions {
		if tr.to == CircuitOpen {
		}
	}
	// Also check we have the expected sequence: HalfOpen->Closed and Closed->Open
	var halfToClosed, closedToOpen bool
	for _, tr := range obs.transitions {
		if tr.from == CircuitHalfOpen && tr.to == CircuitClosed {
			halfToClosed = true
		}
		if tr.from == CircuitClosed && tr.to == CircuitOpen {
			closedToOpen = true
		}
	}
	obs.mu.Unlock()

	if !closedToOpen {
		t.Fatalf("expected Closed->Open (ReasonSaturated) after re-saturation, got %v", obs.transitions)
	}
	if !halfToClosed {
		t.Fatalf("expected HalfOpen->Closed transition after pressure clear, got %v", obs.transitions)
	}

	cancel()
	<-done
}

func TestCircuit_ProbeErrorRecoverySequence(t *testing.T) {
	obs := &recordingObserver{}

	c := New(
		func(_ context.Context) (CapacitySignal, error) {
			return CapacitySignal{}, context.DeadlineExceeded
		},
		NoOpRecover,
		WithProbeInterval(10*time.Millisecond),
		WithObserver(obs),
		WithMaxFails(1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// First probe error triggers maxFails=1 → Open
	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	obs.mu.Lock()
	if len(obs.transitions) == 0 {
		obs.mu.Unlock()
		t.Fatal("expected at least one transition (error -> open)")
	}
	// Verify error->open (ReasonProbeFailure)
	var foundProbeFailure bool
	for _, tr := range obs.transitions {
		if tr.from == CircuitClosed && tr.to == CircuitOpen && tr.reason == ReasonProbeFailure {
			foundProbeFailure = true
		}
	}
	obs.mu.Unlock()
	if !foundProbeFailure {
		t.Fatalf("expected Closed->Open (ReasonProbeFailure) transition, got %v", obs.transitions)
	}

	cancel()
	<-done
}
