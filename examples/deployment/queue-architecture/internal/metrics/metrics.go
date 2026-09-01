package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Registry is the shared Prometheus registry used by both proxy and sidecar.
var Registry = prometheus.NewRegistry()

