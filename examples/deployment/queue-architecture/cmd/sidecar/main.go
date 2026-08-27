package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/sidecar"
)

func main() {
	// Read configuration from environment variables
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		log.Fatal("REDIS_ADDR environment variable is required")
	}

	streamName := os.Getenv("STREAM_NAME")
	if streamName == "" {
		log.Fatal("STREAM_NAME environment variable is required")
	}

	consumerGroup := os.Getenv("CONSUMER_GROUP")
	if consumerGroup == "" {
		log.Fatal("CONSUMER_GROUP environment variable is required")
	}

	consumerName := os.Getenv("CONSUMER_NAME")
	if consumerName == "" {
		log.Fatal("CONSUMER_NAME environment variable is required")
	}

	vllmTarget := os.Getenv("VLLM_TARGET")
	if vllmTarget == "" {
		log.Fatal("VLLM_TARGET environment variable is required")
	}

	// Parse IDLE_THRESHOLD (default 10m)
	idleThresholdStr := os.Getenv("IDLE_THRESHOLD")
	if idleThresholdStr == "" {
		idleThresholdStr = "10m"
	}
	idleThreshold, err := time.ParseDuration(idleThresholdStr)
	if err != nil {
		log.Fatalf("Invalid IDLE_THRESHOLD: %v", err)
	}

	// Parse RECLAIM_INTERVAL (default 30s)
	reclaimIntervalStr := os.Getenv("RECLAIM_INTERVAL")
	if reclaimIntervalStr == "" {
		reclaimIntervalStr = "30s"
	}
	reclaimInterval, err := time.ParseDuration(reclaimIntervalStr)
	if err != nil {
		log.Fatalf("Invalid RECLAIM_INTERVAL: %v", err)
	}

	// Parse JOB_TIMEOUT (default 8m) -- the maximum time a single job's
	// forward-to-vLLM call is allowed to take before the sidecar gives up
	// on it, cancels the request (closing the connection so vLLM's own
	// with_cancellation aborts the in-progress generation and frees GPU
	// capacity), writes a 504 timeout result/SSE-error for the waiting
	// client, and acks the message.
	//
	// Without this bound, a hung or dropped connection to vLLM previously
	// blocked a worker goroutine forever: no timeout ever fired, no result
	// was ever written, and the message sat pending until ReclaimLoop's
	// IDLE_THRESHOLD -- at which point it was frequently reclaimed right
	// back onto the SAME stuck consumer, repeating forever with a growing
	// delivery count and no progress.
	//
	// Deliberately kept well BELOW IDLE_THRESHOLD (default 10m): every job
	// should resolve -- successfully or via this timeout -- on its own,
	// well before ReclaimLoop would otherwise need to notice it. Real
	// large-context / complex requests can legitimately take several
	// minutes; this is not a typical per-request HTTP timeout (see
	// cmd/proxy/main.go's REQUEST_TIMEOUT/STREAM_TIMEOUT for the client-facing
	// equivalent, which defaults much longer since a request may also
	// legitimately queue behind other jobs before a sidecar even starts it).
	jobTimeoutStr := os.Getenv("JOB_TIMEOUT")
	if jobTimeoutStr == "" {
		jobTimeoutStr = "8m"
	}
	jobTimeout, err := time.ParseDuration(jobTimeoutStr)
	if err != nil {
		log.Fatalf("Invalid JOB_TIMEOUT: %v", err)
	}
	if jobTimeout >= idleThreshold {
		log.Printf("WARNING: JOB_TIMEOUT=%v is >= IDLE_THRESHOLD=%v -- a hung job could still be reclaimed before it times out on its own, re-introducing the stuck-consumer symptom this setting exists to prevent. JOB_TIMEOUT should be set comfortably below IDLE_THRESHOLD.", jobTimeout, idleThreshold)
	}

	// Parse MAX_CONCURRENT_REQUESTS (default 2 -- matches the measured real
	// concurrency ceiling for large-context requests on a single GPU; see
	// internal/sidecar/health.go for why this gates claims against vLLM's
	// own real-time /metrics rather than being a purely static limit).
	// Set to 0 or a negative value to disable this gate entirely.
	maxConcurrentStr := os.Getenv("MAX_CONCURRENT_REQUESTS")
	maxConcurrent := 2
	if maxConcurrentStr != "" {
		parsed, err := strconv.Atoi(maxConcurrentStr)
		if err != nil {
			log.Fatalf("Invalid MAX_CONCURRENT_REQUESTS: %v", err)
		}
		maxConcurrent = parsed
	}

	// Parse CAPACITY_POLL_INTERVAL (default 2s) -- how often to re-check
	// vLLM's real-time load while waiting for capacity to free up.
	capacityPollIntervalStr := os.Getenv("CAPACITY_POLL_INTERVAL")
	if capacityPollIntervalStr == "" {
		capacityPollIntervalStr = "2s"
	}
	capacityPollInterval, err := time.ParseDuration(capacityPollIntervalStr)
	if err != nil {
		log.Fatalf("Invalid CAPACITY_POLL_INTERVAL: %v", err)
	}

	// Parse HEALTH_CHECK_INTERVAL (default 5s) -- how often to poll
	// VLLM_TARGET's /health endpoint at startup before consuming any jobs.
	healthCheckIntervalStr := os.Getenv("HEALTH_CHECK_INTERVAL")
	if healthCheckIntervalStr == "" {
		healthCheckIntervalStr = "5s"
	}
	healthCheckInterval, err := time.ParseDuration(healthCheckIntervalStr)
	if err != nil {
		log.Fatalf("Invalid HEALTH_CHECK_INTERVAL: %v", err)
	}

	// Parse MAX_DRAIN_TIMEOUT (default 660s / 11m) -- on shutdown, the
	// maximum time to wait for in-flight jobs to finish naturally before
	// exiting anyway. Should be set comfortably BELOW the pod's
	// terminationGracePeriodSeconds so this process can log a warning and
	// exit(1) voluntarily instead of being SIGKILLed with no chance to log
	// anything. A job still in flight when this timeout fires is not lost
	// -- it was never acked, so another replica's ReclaimLoop will pick it
	// up after IDLE_THRESHOLD, same as any other crash.
	maxDrainTimeoutStr := os.Getenv("MAX_DRAIN_TIMEOUT")
	if maxDrainTimeoutStr == "" {
		maxDrainTimeoutStr = "660s"
	}
	maxDrainTimeout, err := time.ParseDuration(maxDrainTimeoutStr)
	if err != nil {
		log.Fatalf("Invalid MAX_DRAIN_TIMEOUT: %v", err)
	}

	// Construct Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Lightweight HTTP client used only for the /health and /metrics probes
	// against VLLM_TARGET -- separate from the client used inside the
	// actual forwarders, since these probes should always be fast and
	// shouldn't ever block on a slow/hung backend.
	probeClient := &http.Client{Timeout: 5 * time.Second}

	// HTTP client used for the actual job forward to vLLM. Timeout is set
	// here as a defense-in-depth belt-and-suspenders measure -- the primary
	// bound on a forward's duration is the per-job context.WithTimeout
	// derived from JOB_TIMEOUT in processJob (internal/sidecar/consumer.go),
	// which is what actually cancels the request/closes the connection so
	// vLLM can abort the in-progress generation. This client-level Timeout
	// covers any request that somehow bypasses that context (e.g. a future
	// caller mistake), and is set comfortably above JOB_TIMEOUT so it never
	// fires first and masks the more precise, cancellation-aware timeout.
	forwardClient := &http.Client{Timeout: jobTimeout + 30*time.Second}

	// Set result expiry to 24 hours
	resultExpiry := 24 * time.Hour

	// Log startup configuration
	log.Printf("Starting sidecar consumer: REDIS_ADDR=%s, STREAM_NAME=%s, CONSUMER_GROUP=%s, CONSUMER_NAME=%s, VLLM_TARGET=%s, RESULT_EXPIRY=%v, IDLE_THRESHOLD=%v, RECLAIM_INTERVAL=%v, JOB_TIMEOUT=%v, MAX_CONCURRENT_REQUESTS=%d, CAPACITY_POLL_INTERVAL=%v, HEALTH_CHECK_INTERVAL=%v, MAX_DRAIN_TIMEOUT=%v",
		redisAddr, streamName, consumerGroup, consumerName, vllmTarget, resultExpiry, idleThreshold, reclaimInterval, jobTimeout, maxConcurrent, capacityPollInterval, healthCheckInterval, maxDrainTimeout)

	// shutdownCtx is cancelled on SIGINT/SIGTERM (e.g. KEDA scale-down,
	// node drain/consolidation). It gates every CLAIM decision in both
	// loops -- a hard stop on taking any NEW work.
	//
	// workCtx is deliberately NOT derived from shutdownCtx and is never
	// cancelled by the shutdown signal. It's used only once a job has
	// actually been claimed, so in-flight work (already consuming real
	// GPU time) always runs to completion instead of being aborted
	// mid-request when the pod is asked to terminate. See
	// internal/sidecar/consumer.go's ConsumerLoop/ReclaimLoop doc comments
	// for the full rationale.
	shutdownCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workCtx := context.Background()

	if err := queue.EnsureConsumerGroup(shutdownCtx, rdb, streamName, consumerGroup); err != nil {
		log.Fatalf("Failed to ensure consumer group: %v", err)
	}

	// Block here until vLLM's /health endpoint actually responds 200, before
	// touching the queue at all. vLLM's own multi-minute cold start (model
	// load, cudagraph capture) runs concurrently with this container
	// starting -- with no gate, the sidecar would otherwise start reading
	// jobs and failing to forward them for that entire window.
	log.Printf("Waiting for VLLM_TARGET=%s to become healthy before consuming from the queue...", vllmTarget)
	if err := sidecar.WaitForHealthy(shutdownCtx, probeClient, vllmTarget, healthCheckInterval); err != nil {
		log.Fatalf("Gave up waiting for vLLM to become healthy: %v", err)
	}

	// Create error channel to collect errors from both loops
	errChan := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	// Launch ConsumerLoop in a goroutine
	go func() {
		defer wg.Done()
		if err := sidecar.ConsumerLoop(shutdownCtx, workCtx, rdb, streamName, consumerGroup, consumerName, vllmTarget, resultExpiry, jobTimeout, probeClient, forwardClient, maxConcurrent, capacityPollInterval); err != nil {
			errChan <- fmt.Errorf("consumer loop: %w", err)
		}
	}()

	// Launch ReclaimLoop in a goroutine
	go func() {
		defer wg.Done()
		if err := sidecar.ReclaimLoop(shutdownCtx, workCtx, rdb, streamName, consumerGroup, consumerName, vllmTarget, resultExpiry, idleThreshold, reclaimInterval, jobTimeout, probeClient, forwardClient, maxConcurrent, capacityPollInterval); err != nil {
			errChan <- fmt.Errorf("reclaim loop: %w", err)
		}
	}()

	// Wait for either loop to return a fatal (non-shutdown) error, or for a
	// shutdown signal to arrive.
	select {
	case err := <-errChan:
		log.Fatalf("Loop error: %v", err)
	case <-shutdownCtx.Done():
		log.Printf("Shutdown signal received: no longer claiming new messages, draining in-flight work...")
	}

	// Block here until both loops have actually finished draining -- they
	// stop claiming new messages immediately (shutdownCtx is cancelled),
	// but anything already claimed keeps running on workCtx until it
	// genuinely completes. maxDrainTimeout is a safety net only: it should
	// be set below terminationGracePeriodSeconds so this process can log
	// and exit(1) voluntarily rather than being SIGKILLed silently. A job
	// still in flight when the timeout fires is not lost -- it's simply
	// unacked, and another replica's ReclaimLoop will pick it up later.
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		log.Printf("All in-flight work finished, exiting cleanly")
	case <-time.After(maxDrainTimeout):
		log.Printf("WARNING: drain timeout (%v) exceeded with work still in flight -- exiting anyway. Any unfinished job was never acked, so another replica will reclaim it after IDLE_THRESHOLD.", maxDrainTimeout)
		os.Exit(1)
	}
}
