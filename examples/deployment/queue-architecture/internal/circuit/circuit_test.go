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

	ctx := t.Context()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

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
}
