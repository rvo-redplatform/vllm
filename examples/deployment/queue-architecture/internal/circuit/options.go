package circuit

import "time"

// Option configures a Circuit.
type Option func(*Circuit)

// WithObserver attaches a telemetry observer. Nil is a no-op.
func WithObserver(o Observer) Option {
	return func(c *Circuit) {
		if o != nil {
			c.observer = o
		}
	}
}

// WithOpenAfter sets consecutive saturated signals before opening.
func WithOpenAfter(n uint) Option {
	return func(c *Circuit) {
		c.openAfter = n
	}
}

// WithCloseAfter sets consecutive clear signals before leaving Open/HalfOpen.
func WithCloseAfter(n uint) Option {
	return func(c *Circuit) {
		c.closeAfter = n
	}
}

// WithMaxProbes sets HalfOpen admission slots per recovery.
func WithMaxProbes(n uint) Option {
	return func(c *Circuit) {
		c.maxProbes = n
	}
}

// WithMaxFails sets consecutive probe errors before recovery.
func WithMaxFails(n uint) Option {
	return func(c *Circuit) {
		c.maxFails = n
	}
}

// WithInitialState sets the starting state (default CircuitClosed).
func WithInitialState(state CircuitState) Option {
	return func(c *Circuit) {
		c.state = state
	}
}

func WithProbeInterval(i time.Duration) Option {
	return func(c *Circuit) {
		if i >= 0 {
			c.interval = i
		}
	}
}
