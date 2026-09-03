package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/metrics"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/sidecar"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func newSidecarCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sidecar",
		Short: "Starts the router sidecar",
		Long:  `Starts the router sidecar.`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return requireStrings(vllmTargetKey)
		},
		RunE: runSidecar,
	}

	flags := cmd.Flags()
	flags.String(vllmTargetKey, "", "vLLM upstream URL (env: RTR_VLLM_TARGET)")
	flags.String(consumerNameKey, defaultConsumerName, "JetStream consumer name (env: RTR_CONSUMER_NAME)")
	flags.String(metricsPortKey, defaultMetricsPort, "Metrics HTTP listen port (env: RTR_METRICS_PORT)")
	flags.Int(workerPoolSizeKey, defaultWorkerPoolSize, "Number of concurrent worker goroutines pulling/forwarding jobs. Bounds in-flight burst-fill speed only -- NOT a safety ceiling; vLLM's own capacity gate (see disable-capacity-gate) is what protects vLLM regardless of this value (env: RTR_WORKER_POOL_SIZE)")
	flags.Bool(disableCapacityGateKey, defaultDisableCapacityGate, "Disable the vLLM-capacity-driven backoff gate entirely (env: RTR_DISABLE_CAPACITY_GATE). Only for local/dev environments without a real vLLM /metrics endpoint.")
	flags.Int(maxAckPendingKey, defaultMaxAckPending, "JetStream max outstanding unacked messages; -1 is unlimited (env: RTR_MAX_ACK_PENDING)")
	flags.Duration(capacityPollIntervalKey, defaultCapacityPollInterval, "Capacity poll interval (env: RTR_CAPACITY_POLL_INTERVAL)")
	flags.Duration(healthCheckIntervalKey, defaultHealthCheckInterval, "vLLM health check interval (env: RTR_HEALTH_CHECK_INTERVAL)")
	flags.Duration(maxDrainTimeoutKey, defaultMaxDrainTimeout, "Shutdown drain timeout (env: RTR_MAX_DRAIN_TIMEOUT)")
	flags.Duration(maxProcessTimeoutKey, defaultMaxProcessTimeout, "Max per-job processing timeout (env: RTR_MAX_PROCESS_TIMEOUT)")

	if err := viper.BindPFlags(flags); err != nil {
		panic(fmt.Errorf("bind sidecar flags: %w", err))
	}

	return cmd
}

func runSidecar(cmd *cobra.Command, _ []string) error {
	natsURL := viper.GetString(natsURLKey)
	streamName := viper.GetString(streamNameKey)
	streamSubject := viper.GetString(streamSubjectKey)
	vllmTarget := viper.GetString(vllmTargetKey)
	consumerName := viper.GetString(consumerNameKey)
	metricsPort := viper.GetString(metricsPortKey)
	workerPoolSize := viper.GetInt(workerPoolSizeKey)
	disableCapacityGate := viper.GetBool(disableCapacityGateKey)
	maxAckPending := viper.GetInt(maxAckPendingKey)
	capacityPollInterval := viper.GetDuration(capacityPollIntervalKey)
	healthCheckInterval := viper.GetDuration(healthCheckIntervalKey)
	maxDrainTimeout := viper.GetDuration(maxDrainTimeoutKey)
	maxProcessTimeout := viper.GetDuration(maxProcessTimeoutKey)

	slog.InfoContext(cmd.Context(), "starting sidecar",
		"nats_url", natsURL,
		"stream_name", streamName,
		"stream_subject", streamSubject,
		"vllm_target", vllmTarget,
		"consumer_name", consumerName,
		"metrics_port", metricsPort,
		"worker_pool_size", workerPoolSize,
		"disable_capacity_gate", disableCapacityGate,
		"max_ack_pending", maxAckPending,
		"capacity_poll_interval", capacityPollInterval,
		"health_check_interval", healthCheckInterval,
		"max_drain_timeout", maxDrainTimeout,
		"max_process_timeout", maxProcessTimeout,
	)

	// Initialize metrics registry and register sidecar metric families
	sidecar.InitMetrics()

	shutdownCtx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workCtx := context.Background()

	qClient := queue.NewClient(
		cmd.Context(),
		queue.WithNatsURL(natsURL),
		queue.WithStreamName(streamName),
		queue.WithStreamSubject(streamSubject),
	)
	qConsumer := queue.NewConsumer(qClient, queue.WithMaxAckPending(maxAckPending))
	qConsumer.Connect(cmd.Context())
	sidecarMetrics := sidecar.NewSidecarMetrics()
	consumer := sidecar.NewConsumer(qConsumer, sidecarMetrics)
	defer qConsumer.Close()

	probeClient := &http.Client{Timeout: 5 * time.Second}

	slog.InfoContext(
		cmd.Context(),
		fmt.Sprintf("Waiting for VLLM_TARGET=%s to become healthy before consuming from the queue...", vllmTarget),
	)
	err := sidecar.WaitForHealthy(shutdownCtx, probeClient, vllmTarget, healthCheckInterval)
	if err != nil {
		return fmt.Errorf("gave up waiting for vllm to become healthy: %w", err)
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	metricsAddr := net.JoinHostPort("", metricsPort)
	metricsServer := &http.Server{Addr: metricsAddr, Handler: metricsMux}

	go func() {
		slog.InfoContext(cmd.Context(), "starting metrics server", "address", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(cmd.Context(), "metrics server error", "err", err)
		}
	}()

	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := consumer.ConsumerLoop(
			shutdownCtx,
			workCtx,
			vllmTarget,
			probeClient,
			workerPoolSize,
			disableCapacityGate,
			capacityPollInterval,
			healthCheckInterval,
			maxProcessTimeout,
		)
		if err != nil {
			errChan <- fmt.Errorf("consumer loop error: %w", err)
		}

	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("loop error: %w", err)
	case <-shutdownCtx.Done():
		slog.InfoContext(cmd.Context(), "shutdown sig received, draining in-flight work")
	}

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		slog.InfoContext(cmd.Context(), "all in-flight work has finished, exiting cleanly")
	case <-time.After(maxDrainTimeout):
		return fmt.Errorf(
			"WARNING: drain timeout (%v) exceeded with work still in flight -- exiting anyway. Any unfinished job was never acked, so JetStream may redeliver it (MaxDeliver=2).",
			maxDrainTimeout,
		)
	}

	return nil
}
