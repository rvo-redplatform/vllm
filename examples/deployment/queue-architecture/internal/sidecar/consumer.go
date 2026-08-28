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
	client ConsumerClient
}

func NewConsumer(client ConsumerClient) *Consumer {
	return &Consumer{
		client: client,
	}
}

type jobOutcome struct {
	status  int
	headers map[string]string
	body    []byte
	err     error
}

// processJob forwards a pulled job to vLLM, publishes the reply on the proxy
// inbox, and ACKs the JetStream work message. Reply publish failures are logged
// but never block ACK (orphan replies are acceptable when the proxy is gone).
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
	out := c.runJob(processCtx, fetched, target)

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

// ConsumerLoop runs up to maxConcurrent independent worker goroutines, each
// looping: gate on vLLM's real-time load, pull one job from JetStream,
// process it, repeat.
//
// GRACEFUL SHUTDOWN / SCALE-DOWN: this takes two separate contexts, not one.
//   - shutdownCtx is cancelled on SIGTERM (pod being terminated -- scale-down,
//     node drain, etc). It gates every FETCH decision: checked explicitly
//     before each pull attempt, and threaded into waitForCapacity/queue.Fetch
//     so a worker blocked waiting for capacity or for a new message unblocks
//     and stops immediately when shutdown begins.
//   - workCtx is NOT cancelled by shutdown. Once a message has actually been
//     fetched, processJob runs on workCtx, so an in-flight job (already
//     consuming GPU time) always runs to completion -- forwarded, reply
//     published, acked -- regardless of a shutdown signal arriving mid-flight.
func (c *Consumer) ConsumerLoop(
	shutdownCtx, workCtx context.Context,
	target string,
	httpClient *http.Client,
	maxConcurrent int,
	capacityPollInterval time.Duration,
	maxProcessTimeout time.Duration,
) error {
	workers := maxConcurrent
	if workers <= 0 {
		workers = 1
	}

	g, claimCtx := errgroup.WithContext(shutdownCtx)
	for i := 0; i < workers; i++ {
		workerID := i
		g.Go(func() error {
			return c.runWorker(claimCtx, workCtx, workerID, target, httpClient, maxConcurrent, capacityPollInterval, maxProcessTimeout)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return shutdownCtx.Err()
}

func (c *Consumer) runWorker(
	claimCtx, workCtx context.Context,
	workerID int,
	target string,
	httpClient *http.Client,
	maxConcurrent int,
	capacityPollInterval time.Duration,
	maxProcessTimeout time.Duration,
) error {
	for {
		select {
		case <-claimCtx.Done():
			return nil
		default:
		}

		if err := waitForCapacity(claimCtx, httpClient, target, maxConcurrent, capacityPollInterval); err != nil {
			if claimCtx.Err() != nil {
				return nil
			}
			return err
		}

		fetched, err := c.client.Fetch(claimCtx, sidecarFetchBatchSize)
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
