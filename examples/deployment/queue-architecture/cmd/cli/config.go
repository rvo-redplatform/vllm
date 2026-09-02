package main

import "time"

const (
	// Shared (root persistent flags).
	natsURLKey       = "nats-url"
	streamNameKey    = "stream-name"
	streamSubjectKey = "stream-subject"

	// Proxy-only flags.
	portKey           = "port"
	metricsPortKey    = "metrics-port"
	maxBodyBytesKey   = "max-body-bytes"
	requestTimeoutKey = "request-timeout"
	streamTimeoutKey  = "stream-timeout"

	// Sidecar-only flags.
	consumerNameKey         = "consumer-name"
	vllmTargetKey           = "vllm-target"
	workerPoolSizeKey       = "worker-pool-size"
	disableCapacityGateKey  = "disable-capacity-gate"
	maxAckPendingKey        = "max-ack-pending"
	capacityPollIntervalKey = "capacity-poll-interval"
	healthCheckIntervalKey  = "health-check-interval"
	maxDrainTimeoutKey      = "max-drain-timeout"
	maxProcessTimeoutKey    = "max-process-timeout"

	defaultStreamName    = "vllm_requests"
	defaultStreamSubject = "vllm.requests"
	defaultConsumerName  = "vllm-sidecars"

	// defaultWorkerPoolSize just bounds how many jobs can be in flight to
	// vLLM at once before the capacity gate (see health.go waitForCapacity)
	// has a chance to trip -- it is NOT a safety ceiling. The gate is what
	// protects vLLM: every worker blocks the instant vLLM's own scheduler
	// reports capacity pressure, regardless of pool size. A generous
	// default is safe here; it only affects burst-fill speed and
	// redundant /metrics polling overhead while blocked, never whether
	// vLLM's real backpressure is respected.
	defaultWorkerPoolSize       = 32
	defaultDisableCapacityGate  = false
	defaultMaxAckPending        = -1
	defaultMaxBodyBytes         = 10 << 20 // 10 MiB
	defaultRequestTimeout       = time.Hour
	defaultStreamTimeout        = time.Hour
	defaultMetricsPort          = "9100"
	defaultCapacityPollInterval = 2 * time.Second
	defaultHealthCheckInterval  = 5 * time.Second
	defaultMaxDrainTimeout      = 660 * time.Second
	defaultMaxProcessTimeout    = 10 * time.Minute
)
