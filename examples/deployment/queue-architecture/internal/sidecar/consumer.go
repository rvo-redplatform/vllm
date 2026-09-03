package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/apierror"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/circuit"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
	"golang.org/x/sync/errgroup"
)

const sidecarFetchBatchSize = 1

const inProgressInterval = 15 * time.Second // AckWait / 2

type ConsumerClient interface {
	Fetch(ctx context.Context, batchSize int) ([]queue.Message, error)
	Mail(recipient string, data []byte) error
}

type Consumer struct {
	client  ConsumerClient
	metrics *SidecarMetrics

	// runChain is the composed middleware chain wrapping the core runJob
	// call. Built once in NewConsumer so we don't re-allocate the chain on
	// every single job.
	runChain ProcessJobFunc
}

// NewConsumer wires a Consumer with its metrics and builds the job-processing
// middleware chain once. metrics is a required dependency -- no default or
// fallback is provided.
func NewConsumer(client ConsumerClient, metrics *SidecarMetrics) *Consumer {
	c := &Consumer{
		client:  client,
		metrics: metrics,
	}

	// core is the bare runJob call: ctx already carries the per-job
	// processing deadline applied by processJob before invoking the chain.
	core := func(ctx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) jobOutcome {
		return c.runJob(ctx, fetched, target)
	}

	// Composition order, outer to inner:
	//   WithInFlight            -- outermost: tracks the full call window,
	//                              including any middleware overhead below it.
	//   WithProcessingDuration  -- times/observes the call, closest to the
	//                              actual runJob work so latency reflects
	//                              forward time, not other middleware.
	//   WithJobCounters         -- innermost: increments the Processed/Failed/
	//                              ErrorsTotal counters based on exactly what
	//                              runJob returned, right next to the call.
	c.runChain = ChainJobMiddleware(core,
		WithInFlight(metrics),
		WithProcessingDuration(metrics),
		WithJobCounters(metrics),
	)

	return c
}

type jobOutcome struct {
	status  int
	headers map[string]string
	body    []byte
	err     error
}

// processJob forwards a pulled job to vLLM (via the metrics-instrumented
// middleware chain), publishes the reply on the proxy inbox, and ACKs the
// JetStream work message. Reply publish failures are logged but never block
// ACK (orphan replies are acceptable when the proxy is gone).
//
// workCtx must stay live through the whole call: processCtx is a bounded
// per-job deadline that hard-cancels the in-flight vLLM forward, but Ack is
// issued on workCtx itself so JetStream still receives it after a timeout.
func (c *Consumer) processJob(workCtx context.Context, fetched queue.Message, target string, maxProcessTimeout time.Duration) {
	processCtx, cancel := context.WithTimeout(workCtx, maxProcessTimeout)
	defer cancel()

	// this blocks until either the job:
	// 1. times out
	// 2. errors
	// 3. finishes
	// Metrics (in-flight gauge, processing duration, processed/failed/error
	// counters) are all recorded by the middleware chain around this call.
	out := c.runChain(processCtx, fetched, target, maxProcessTimeout)

	switch {
	case out.err == nil:
		// if the job ran sucessfully we only need
		// to respond to non-streaming requests
		// bc streaming requests were already streamed the response
		// chunks (follow up and confirm this)
		if !fetched.Job.Stream {
			c.publishNonStreamReply(fetched.ReplyTo(), out.status, out.headers, out.body)
		}
	case isTimeout(processCtx, out.err):
		// if we got a timeout error, vLLM took too
		// long to handle the request and we bail
		slog.ErrorContext(workCtx, "job timed out",
			"job_id", fetched.Job.JobID,
			"stream", fetched.Job.Stream,
			"max_process_timeout", maxProcessTimeout,
		)
		c.publishError(fetched, apierror.TimeoutHTTPStatus, apierror.NewTimeoutError(maxProcessTimeout))
	default:
		// if we get here, we got an actual error
		// and handle accordindly
		slog.ErrorContext(workCtx, "job failed",
			"job_id", fetched.Job.JobID,
			"stream", fetched.Job.Stream,
			"err", out.err,
		)
		c.publishError(fetched, apierror.ServerHTTPStatus, apierror.NewServerError(out.err.Error()))
	}

	// independeant of what happens we always ack
	// so that we fail fast on error and avoid poisen pill
	// messages hogging the pipeline.  The client can retry
	// the request after it is given the error
	if err := fetched.Ack(workCtx); err != nil {
		slog.ErrorContext(workCtx, "ack job",
			"job_id", fetched.Job.JobID,
			"err", err,
		)
	}
}

func (c *Consumer) runJob(ctx context.Context, fetched queue.Message, target string) jobOutcome {
	done := make(chan jobOutcome, 1)
	// start the job and send to the vLLM target
	// the ctx here controls the deadline, and our http clients
	// respect this, so if the process goes beyond it's processing
	// time, this returns and errors and breaks out
	go func() {
		var out jobOutcome
		if fetched.Job.Stream {
			out.err = ForwardStreaming(ctx, c.client, fetched.ReplyTo(), fetched.Job, target)
		} else {
			out.status, out.headers, out.body, out.err = ForwardNonStreaming(ctx, fetched.Job, target)
		}
		done <- out
	}()

	if fetched.Msg == nil {
		return <-done
	}

	// while we wait for vLLM to process our request,
	// periodically send heartbeats, until we are done
	tick := time.NewTicker(inProgressInterval)
	defer tick.Stop()
	for {
		select {
		case out := <-done:
			return out
		case <-tick.C:
			if err := fetched.InProgress(ctx); err != nil && ctx.Err() == nil {
				// not much we can do here
				slog.WarnContext(ctx, "in progress", "err", err)
			}
		}
	}
}

func isTimeout(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func (c *Consumer) publishError(fetched queue.Message, status int, body apierror.OpenAIErrorBody) {
	payload, err := body.Bytes()
	if err != nil {
		slog.Error("marshal openai error body",
			"job_id", fetched.Job.JobID,
			"err", err,
		)
		payload = []byte(body.Error.Message)
		status = apierror.ServerHTTPStatus
	}

	replyTo := fetched.ReplyTo()
	if fetched.Job.Stream {
		c.publishStreamError(replyTo, status, string(payload))
		return
	}
	c.publishNonStreamReply(
		replyTo,
		status,
		map[string]string{"Content-Type": apierror.JSONContentType},
		payload,
	)
}

func (c *Consumer) publishNonStreamReply(replyTo string, status int, headers map[string]string, body []byte) {
	if headers == nil {
		headers = map[string]string{}
	}
	result := map[string]any{
		"status":  status,
		"headers": headers,
		"body":    string(body),
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		slog.Error("marshal non-stream reply", "err", err)
		return
	}

	err = c.client.Mail(replyTo, resultJSON)
	if err != nil {
		slog.Warn("failed to mail result",
			"reply_to", replyTo,
			"err", err,
		)
	}
}

func (c *Consumer) publishStreamError(replyTo string, status int, body string) {
	errorMessage := map[string]any{
		"error":  true,
		"status": status,
		"body":   body,
	}
	errorJSON, err := json.Marshal(errorMessage)
	if err != nil {
		slog.Error("marshal stream error reply", "err", err)
		return
	}
	err = c.client.Mail(replyTo, errorJSON)
	if err != nil {
		slog.Warn("failed to mail error",
			"reply_to", replyTo,
			"err", err,
		)
	}
	err = c.client.Mail(replyTo, apierror.DoneEnvelope())
	if err != nil {
		slog.Warn("failed to mail done message",
			"reply_to", replyTo,
			"err", err,
		)
	}
}

// ConsumerLoop runs workerPoolSize independent worker goroutines, each
// looping: wait on the shared admission circuit, pull one job from JetStream,
// process it, repeat.
//
// workerPoolSize and the admission circuit are deliberately independent
// concerns:
//   - workerPoolSize bounds how many jobs this sidecar can have in flight
//     to vLLM at once before the circuit trips -- burst-fill speed only,
//     not a safety ceiling.
//   - The admission circuit (circuit.Run + WaitReady) is what protects
//     vLLM: one observer goroutine polls vLLM's /metrics; every worker
//     blocks on WaitReady when the circuit is Open.
//
// GRACEFUL SHUTDOWN / SCALE-DOWN: this takes two separate contexts, not one.
//   - shutdownCtx is cancelled on SIGTERM (pod being terminated -- scale-down,
//     node drain, etc). It gates every FETCH decision: checked explicitly
//     before each pull attempt, and threaded into WaitReady/queue.Fetch so
//     a worker blocked on the circuit or for a new message unblocks and
//     stops immediately when shutdown begins.
//   - workCtx is NOT cancelled by shutdown. Once a message has actually been
//     fetched, processJob runs on workCtx, so an in-flight job (already
//     consuming GPU time) always runs to completion -- forwarded, reply
//     published, acked -- regardless of a shutdown signal arriving mid-flight.
func (c *Consumer) ConsumerLoop(
	shutdownCtx, workCtx context.Context,
	target string,
	httpClient *http.Client,
	workerPoolSize int,
	capacityGateDisabled bool,
	capacityPollInterval time.Duration,
	healthCheckInterval time.Duration,
	maxProcessTimeout time.Duration,
) error {
	workers := workerPoolSize
	if workers <= 0 {
		workers = 1
	}

	capCircuit := c.newCapacityCircuit(
		target,
		httpClient,
		capacityGateDisabled,
		capacityPollInterval,
		healthCheckInterval,
	)

	slog.Info("starting consume loop",
		"workers", workers,
		"capacity_gate_disabled", capacityGateDisabled,
		"poll_interval", capacityPollInterval,
		"healthcheck_interval", healthCheckInterval,
	)

	g, claimCtx := errgroup.WithContext(shutdownCtx)

	if !capacityGateDisabled {
		g.Go(func() error {
			return capCircuit.Run(claimCtx)
		})
	}

	for i := 0; i < workers; i++ {
		workerID := i
		g.Go(func() error {
			return c.runWorker(claimCtx, workCtx, workerID, target, capCircuit, maxProcessTimeout)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return shutdownCtx.Err()
}

func (c *Consumer) newCapacityCircuit(
	target string,
	httpClient *http.Client,
	capacityGateDisabled bool,
	capacityPollInterval time.Duration,
	healthCheckInterval time.Duration,
) *circuit.Circuit {
	observer := NewCircuitMetricsObserver(c.metrics)

	if capacityGateDisabled {
		return circuit.New(
			circuit.NoOpProbe,
			circuit.NoOpRecover,
			circuit.WithObserver(observer),
			circuit.WithInitialState(circuit.CircuitClosed),
		)
	}

	probe := func(ctx context.Context) (circuit.CapacitySignal, error) {
		load, err := capacityPressure(ctx, httpClient, target)
		if err != nil {
			return circuit.CapacitySignal{}, err
		}
		return circuit.CapacitySignal{HasCapacity: load == 0, Load: load}, nil
	}
	recoverFn := func(ctx context.Context) error {
		return WaitForHealthy(ctx, httpClient, target, healthCheckInterval)
	}

	return circuit.New(
		probe,
		recoverFn,
		circuit.WithObserver(observer),
		circuit.WithProbeInterval(capacityPollInterval),
		circuit.WithInitialState(circuit.CircuitOpen),
	)
}

func (c *Consumer) runWorker(
	claimCtx, workCtx context.Context,
	workerID int,
	target string,
	capCircuit *circuit.Circuit,
	maxProcessTimeout time.Duration,
) error {
	for {
		select {
		case <-claimCtx.Done():
			return nil
		default:
		}

		if err := capCircuit.WaitReady(claimCtx); err != nil {
			slog.ErrorContext(claimCtx, "error waiting for capacity",
				"err", err,
			)
			continue
		}

		fetched, err := c.client.Fetch(claimCtx, sidecarFetchBatchSize)
		c.metrics.FetchSize.Set(float64(len(fetched)))
		if err != nil {
			if claimCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch job (worker %d): %w", workerID, err)
		}
		if len(fetched) == 0 {
			continue
		}

		c.processJob(workCtx, fetched[0], target, maxProcessTimeout)
	}
}
