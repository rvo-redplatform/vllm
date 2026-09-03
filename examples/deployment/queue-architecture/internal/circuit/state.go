package circuit

import (
	"time"
)

type CircuitState int

const (
	CircuitUnknown CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
	CircuitClosed
)

func (s CircuitState) String() string {
	switch s {
	case CircuitUnknown:
		return "unknown"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	case CircuitClosed:
		return "closed"
	default:
		return "unknown"
	}
}

const (
	defaultOpenAfter  = 1
	defaultCloseAfter = 2
	defaultMaxProbes  = 1
	defaultMaxFails   = 3
	defaultInterval   = 2 * time.Second
)
