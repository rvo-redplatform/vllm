package circuit

import (
	"context"
	"sync"
	"time"
)

type CapacitySignal struct {
	HasCapacity bool
	Load        float64
}

type CapacityProbe func(ctx context.Context) (CapacitySignal, error)
type RecoveryWait func(ctx context.Context) error

func NoOpProbe(_ context.Context) (CapacitySignal, error) {
	return CapacitySignal{
		HasCapacity: true,
		Load:        0,
	}, nil
}

func NoOpRecover(_ context.Context) error {
	return nil
}

// Snapshot is a point-in-time view of circuit internals for debugging.
type Snapshot struct {
	State            CircuitState
	ProbesRemaining  uint
	ConsecutiveOpen  uint
	ConsecutiveClear uint
	ConsecutiveFail  uint
}

type Circuit struct {
	probe    CapacityProbe
	recover  RecoveryWait
	observer Observer
	interval time.Duration

	openAfter  uint
	closeAfter uint
	maxProbes  uint
	maxFails   uint

	mu               sync.RWMutex
	state            CircuitState
	probesRemaining  uint
	consecutiveOpen  uint
	consecutiveClear uint
	consecutiveFail  uint
	waitCh           chan struct{}
}

func New(
	probe CapacityProbe,
	recover RecoveryWait,
	opts ...Option,
) *Circuit {
	c := &Circuit{
		probe:      probe,
		recover:    recover,
		observer:   NoOpObserver{},
		state:      CircuitClosed,
		waitCh:     make(chan struct{}),
		interval:   defaultInterval,
		openAfter:  defaultOpenAfter,
		closeAfter: defaultCloseAfter,
		maxProbes:  defaultMaxProbes,
		maxFails:   defaultMaxFails,
	}

	if c.probe == nil {
		c.probe = NoOpProbe
	}
	if c.recover == nil {
		c.recover = NoOpRecover
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// State returns the current circuit state.
func (c *Circuit) State() CircuitState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// Snapshot returns a copy of circuit internals.
func (c *Circuit) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Snapshot{
		State:            c.state,
		ProbesRemaining:  c.probesRemaining,
		ConsecutiveOpen:  c.consecutiveOpen,
		ConsecutiveClear: c.consecutiveClear,
		ConsecutiveFail:  c.consecutiveFail,
	}
}

func (c *Circuit) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.state == CircuitUnknown {
		c.transitionLocked(ctx, CircuitOpen, ReasonSafetyOpen)
	}
	c.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		signal, err := c.probe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		go c.emitProbe(ctx, signal, err)

		if err != nil {
			if err := c.applyProbeError(ctx); err != nil {
				return err
			}
		} else {
			c.applySignal(ctx, signal)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.interval):
		}
	}
}

func (c *Circuit) applyProbeError(ctx context.Context) error {
	maxFails := c.maxFails
	if maxFails <= 0 {
		maxFails = defaultMaxFails
	}

	c.mu.Lock()
	c.consecutiveFail++
	if c.consecutiveFail < maxFails {
		c.mu.Unlock()
		return nil
	}
	c.consecutiveFail = 0
	c.transitionLocked(ctx, CircuitOpen, ReasonProbeFailure)
	c.mu.Unlock()

	if err := c.recover(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	c.transitionLocked(ctx, CircuitHalfOpen, ReasonRecovery)
	c.mu.Unlock()
	return nil
}

func (c *Circuit) applySignal(ctx context.Context, signal CapacitySignal) {
	openAfter := c.openAfter
	if openAfter <= 0 {
		openAfter = defaultOpenAfter
	}
	closeAfter := c.closeAfter
	if closeAfter <= 0 {
		closeAfter = defaultCloseAfter
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.consecutiveFail = 0

	if !signal.HasCapacity {
		c.consecutiveClear = 0
		c.consecutiveOpen++
		if c.state != CircuitOpen && c.consecutiveOpen >= openAfter {
			c.transitionLocked(ctx, CircuitOpen, ReasonSaturated)
		}
		return
	}

	c.consecutiveOpen = 0
	c.consecutiveClear++

	if c.state == CircuitOpen && c.consecutiveClear >= closeAfter {
		c.transitionLocked(ctx, CircuitHalfOpen, ReasonRecovered)
		return
	}
	if c.state == CircuitHalfOpen && c.consecutiveClear >= closeAfter {
		c.transitionLocked(ctx, CircuitClosed, ReasonRecovered)
	}
}

func (c *Circuit) transitionLocked(ctx context.Context, to CircuitState, reason TransitionReason) {
	from := c.state
	if from == to {
		return
	}

	switch to {
	case CircuitOpen:
		c.state = CircuitOpen
		c.probesRemaining = 0
		c.consecutiveClear = 0
	case CircuitHalfOpen:
		maxProbes := c.maxProbes
		if maxProbes <= 0 {
			maxProbes = defaultMaxProbes
		}
		c.state = CircuitHalfOpen
		c.probesRemaining = maxProbes
	case CircuitClosed:
		c.state = CircuitClosed
		c.consecutiveOpen = 0
	default:
		c.state = to
	}

	c.notifyLocked()

	if c.observer != nil {
		go c.observer.OnTransition(ctx, from, to, reason)
	}
}

func (c *Circuit) emitProbe(ctx context.Context, signal CapacitySignal, err error) {
	if c.observer != nil {
		c.observer.OnProbe(ctx, signal, err)
	}
}

func (c *Circuit) notifyLocked() {
	if c.waitCh == nil {
		c.waitCh = make(chan struct{})
		return
	}
	close(c.waitCh)
	c.waitCh = make(chan struct{})
}

func (c *Circuit) WaitReady(ctx context.Context) error {
	for {
		c.mu.Lock()
		switch c.state {
		case CircuitClosed:
			c.mu.Unlock()
			return nil
		case CircuitHalfOpen:
			if c.probesRemaining > 0 {
				c.probesRemaining--
				c.mu.Unlock()
				return nil
			}
		}

		waitCh := c.waitCh
		if waitCh == nil {
			waitCh = make(chan struct{})
			c.waitCh = waitCh
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitCh:
		}
	}
}
