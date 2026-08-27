package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

// writeNonStreamingResult marshals and writes a non-streaming result to
// Redis (with resultExpiry TTL), using ctx -- which must NOT be a context
// that may already be expired (e.g. a per-job timeout context after it has
// fired), or the write itself would fail. Callers processing a timeout
// must pass the outer, still-live context here, not the expired one.
func writeNonStreamingResult(ctx context.Context, rdb *redis.Client, jobID string, status int, headers map[string]string, body string, resultExpiry time.Duration) error {
	result := map[string]interface{}{
		"status":  status,
		"headers": headers,
		"body":    body,
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	resultKey := "result:" + jobID
	if err := rdb.RPush(ctx, resultKey, resultJSON).Err(); err != nil {
		return fmt.Errorf("rpush result: %w", err)
	}
	if err := rdb.Expire(ctx, resultKey, resultExpiry).Err(); err != nil {
		return fmt.Errorf("expire result: %w", err)
	}
	return nil
}

// publishStreamTimeout publishes a sidecar-internal error frame followed by
// the __done sentinel to a streaming job's Pub/Sub channel, using the same
// {"error": true, "status", "body"} shape ForwardStreaming itself uses for
// upstream errors -- internal/proxy/streamhandler.go's frameErrorPayload
// already knows how to translate this into a standard OpenAI-compatible SSE
// error frame for the client. ctx must be a still-live context (see
// writeNonStreamingResult's doc comment for why).
func publishStreamTimeout(ctx context.Context, rdb *redis.Client, jobID string, message string) error {
	channel := fmt.Sprintf("stream:%s", jobID)
	errorJSON, err := json.Marshal(map[string]interface{}{
		"error":  true,
		"status": http.StatusGatewayTimeout,
		"body":   message,
	})
	if err != nil {
		return fmt.Errorf("marshal timeout error: %w", err)
	}
	if err := rdb.Publish(ctx, channel, string(errorJSON)).Err(); err != nil {
		return fmt.Errorf("publish timeout error: %w", err)
	}
	doneJSON, err := json.Marshal(map[string]bool{"__done": true})
	if err != nil {
		return fmt.Errorf("marshal done message: %w", err)
	}
	if err := rdb.Publish(ctx, channel, string(doneJSON)).Err(); err != nil {
		return fmt.Errorf("publish done message: %w", err)
	}
	return nil
}

// processJob handles the per-job processing logic: idempotency check, forwarding,
// result writing, and acking. This shared helper is used by both ConsumerLoop and
// ReclaimLoop to ensure identical semantics.
//
// Order matters for crash protection: write the result BEFORE acking -- so a crash
// between those two steps causes at most wasted redundant work, never a lost answer.
//
// jobTimeout bounds the actual forward-to-vLLM call: a fresh context.WithTimeout
// derived from ctx is created for the forward only, NOT for the idempotency check,
// result write, or ack (those use ctx directly, so they can still complete even
// after the forward's deadline has passed).
//
// This closes two related failure modes that previously let a single hung
// forward wedge a job (and its consumer) forever:
//   - Without a bounded context, a hung/dropped connection to vLLM (or vLLM
//     itself stalling mid-generation) left the forward call -- and the
//     worker goroutine running it -- blocked indefinitely. Nothing timed
//     out, no result was ever written, and the message just sat pending.
//   - Cancelling the forward's context here also closes the underlying HTTP
//     connection to vLLM, which is what allows vLLM's own with_cancellation
//     disconnect-detection (see vllm/entrypoints/serve/utils/api_utils.py)
//     to actually abort the in-progress generation and free GPU capacity,
//     instead of continuing to compute for a caller that gave up.
//
// On timeout, a real timeout result/SSE-error is written and the job is
// acked here -- rather than left unacked for ReclaimLoop to pick up -- so
// the waiting proxy request unblocks immediately with a clear 504 instead
// of hanging until its own (much longer) REQUEST_TIMEOUT/STREAM_TIMEOUT,
// and so the message can't be redelivered into an endless reclaim loop by
// a consumer that will only hit the same timeout again.
//
// Parameters:
//   - ctx: context for cancellation (idempotency check, result write, ack)
//   - httpClient: HTTP client used for the forward call
//   - rdb: Redis client
//   - job: the job to process
//   - entryID: the Redis stream entry ID (for acking)
//   - streamName: name of the Redis stream
//   - groupName: consumer group name
//   - target: base URL of the vLLM instance
//   - resultExpiry: TTL for result keys in Redis
//   - jobTimeout: max time allowed for the forward-to-vLLM call
//
// Returns error only for infrastructure failures; per-job errors are logged and
// the function returns nil so the caller can continue processing other jobs.
func processJob(ctx context.Context, httpClient *http.Client, rdb *redis.Client, job queue.Job, entryID, streamName, groupName, target string, resultExpiry, jobTimeout time.Duration) error {
	// Check if result already exists (idempotency)
	exists, err := ResultExists(ctx, rdb, job.JobID)
	if err != nil {
		log.Printf("ERROR: check result exists for job %s: %v", job.JobID, err)
		return nil // Per-job error, don't fail the loop
	}

	if exists {
		// Result already exists, just ack and continue
		if err := queue.Ack(ctx, rdb, streamName, groupName, entryID); err != nil {
			log.Printf("ERROR: ack job %s: %v", job.JobID, err)
			return nil // Per-job error, don't fail the loop
		}
		return nil
	}

	// jobCtx bounds only the forward call -- see doc comment above. Derived
	// from ctx so an outer cancellation (e.g. a real infra error on a
	// sibling worker) still propagates, but ctx itself is otherwise
	// long-lived (workCtx, per ConsumerLoop/ReclaimLoop) and must remain
	// usable after jobCtx expires so the timeout result can still be
	// written and the job still acked.
	jobCtx, cancelJob := context.WithTimeout(ctx, jobTimeout)
	defer cancelJob()

	// Dispatch to the appropriate forwarder based on job.Stream
	if job.Stream {
		// Streaming job: forward to ForwardStreaming
		if err := ForwardStreaming(jobCtx, httpClient, rdb, job, target); err != nil {
			if jobCtx.Err() != nil {
				log.Printf("TIMEOUT: streaming job %s exceeded JOB_TIMEOUT=%v, aborting forward and notifying client", job.JobID, jobTimeout)
				if pubErr := publishStreamTimeout(ctx, rdb, job.JobID, fmt.Sprintf("request timed out after %v", jobTimeout)); pubErr != nil {
					log.Printf("ERROR: publish stream timeout for job %s: %v", job.JobID, pubErr)
					return nil // Per-job error, don't fail the loop
				}
				// Fall through to ack below: the client has been notified,
				// don't leave this message pending for reclaim.
			} else {
				log.Printf("ERROR: forward streaming job %s: %v", job.JobID, err)
				return nil // Per-job error, don't fail the loop -- leave unacked for reclaim/retry
			}
		}
	} else {
		// Non-streaming job: forward and write result
		status, headers, body, err := ForwardNonStreaming(jobCtx, httpClient, job, target)
		if err != nil {
			if jobCtx.Err() != nil {
				log.Printf("TIMEOUT: job %s exceeded JOB_TIMEOUT=%v, aborting forward and notifying client", job.JobID, jobTimeout)
				timeoutBody := fmt.Sprintf(`{"error":{"message":"request timed out after %v","type":"upstream_timeout","code":504}}`, jobTimeout)
				if writeErr := writeNonStreamingResult(ctx, rdb, job.JobID, http.StatusGatewayTimeout, map[string]string{"Content-Type": "application/json"}, timeoutBody, resultExpiry); writeErr != nil {
					log.Printf("ERROR: write timeout result for job %s: %v", job.JobID, writeErr)
					return nil // Per-job error, don't fail the loop
				}
				// Fall through to ack below: the client has been notified,
				// don't leave this message pending for reclaim.
			} else {
				log.Printf("ERROR: forward non-streaming job %s: %v", job.JobID, err)
				return nil // Per-job error, don't fail the loop -- leave unacked for reclaim/retry
			}
		} else {
			// Write result to Redis with expiry (BEFORE acking for crash protection)
			if err := writeNonStreamingResult(ctx, rdb, job.JobID, status, headers, string(body), resultExpiry); err != nil {
				log.Printf("ERROR: write result for job %s: %v", job.JobID, err)
				return nil // Per-job error, don't fail the loop
			}
		}
	}

	// Ack the job (after writing result for non-streaming, or notifying the
	// client of a timeout for either job type).
	if err := queue.Ack(ctx, rdb, streamName, groupName, entryID); err != nil {
		log.Printf("ERROR: ack job %s: %v", job.JobID, err)
		return nil // Per-job error, don't fail the loop
	}

	return nil
}

// ConsumerLoop runs up to maxConcurrent independent worker goroutines, each
// looping: gate on vLLM's real-time load, read one job from the queue,
// process it, repeat. Running multiple workers concurrently (rather than a
// single sequential loop) is what actually lets the sidecar have more than
// one in-flight request to vLLM at a time -- the capacity gate alone only
// decides *whether* to claim the next message; it's the worker pool that
// determines *how many* can be claimed and forwarded concurrently.
//
// Multiple goroutines issuing concurrent XReadGroup calls under the same
// consumer name is safe: Redis Streams hands each blocking reader its own
// distinct message as new ones arrive, there's no risk of two workers
// getting the same message.
//
// Order matters for crash protection: write the result BEFORE acking -- so a crash
// between those two steps causes at most wasted redundant work, never a lost answer.
//
// Each worker gates on vLLM's real-time load (see waitForCapacity in
// health.go) before claiming its next message: if vLLM already reports
// being at or above maxConcurrent (running+waiting requests), that worker
// holds off rather than claiming work it can't make timely progress on.
// This makes the sidecar's aggregate concurrency ceiling self-adjusting to
// vLLM's actual measured capacity instead of a blind assumption, and
// leaves un-claimed backlog visible to KEDA's stream-lag scaling metric
// instead of hidden behind claimed-but-blocked workers.
//
// GRACEFUL SHUTDOWN / SCALE-DOWN: this takes two separate contexts, not one.
//   - shutdownCtx is cancelled on SIGTERM (pod being terminated -- scale-down,
//     node drain, etc). It gates every CLAIM decision: checked explicitly
//     before each read attempt, and threaded into waitForCapacity/queue.Read
//     so a worker blocked waiting for capacity or for a new message unblocks
//     and stops immediately when shutdown begins. This is a hard gate --
//     once shutdownCtx is cancelled, no worker will claim another message.
//   - workCtx is NOT cancelled by shutdown. Once a message has actually been
//     claimed (queue.Read succeeded), processJob runs on workCtx, so an
//     in-flight job (already consuming GPU time) always runs to completion
//     -- forwarded, result written, acked -- regardless of a shutdown
//     signal arriving mid-flight. Only after processJob returns does the
//     worker loop back and check the shutdown gate again.
//   - ConsumerLoop itself does not return until every worker has both (a)
//     stopped claiming new work and (b) finished whatever it had already
//     claimed. Callers (main.go) should wait for this function to return
//     before exiting the process, so in-flight work is never killed by the
//     process dying out from under it.
//
// Error handling:
//   - Infrastructure errors (queue.Read failures) cancel all sibling workers'
//     claiming (not their in-flight work) and are returned from the loop.
//   - Per-job errors (idempotency check, forwarding, marshaling, result write, ack) are
//     logged with the job ID by processJob, which returns nil so that worker continues to
//     the next job. Failed jobs remain un-acked and stay in the Pending Entries List for
//     later reclaim/retry.
//
// Parameters:
//   - shutdownCtx: cancelled on shutdown signal; gates claiming new work (hard gate)
//   - workCtx: NOT cancelled by shutdown; used for in-flight job processing so it always
//     runs to completion
//   - rdb: Redis client
//   - streamName: name of the Redis stream to read from
//   - groupName: consumer group name
//   - consumerName: consumer name within the group
//   - target: base URL of the vLLM instance (e.g., "http://localhost:8000")
//   - resultExpiry: TTL for result keys in Redis
//   - jobTimeout: max time allowed for a single job's forward-to-vLLM call
//     before it's treated as timed out (see processJob's doc comment)
//   - probeClient: HTTP client used for the lightweight /health and /metrics probes
//   - forwardClient: HTTP client used for the actual job forward to vLLM
//   - maxConcurrent: number of concurrent worker goroutines, and the vLLM running+waiting
//     threshold each worker gates on before claiming (<=0 is treated as 1 -- always at
//     least one worker; the capacity gate itself is separately disabled by health.go's
//     own <=0 check on this same value)
//   - capacityPollInterval: how often to re-check vLLM's load while waiting for capacity
func ConsumerLoop(shutdownCtx, workCtx context.Context, rdb *redis.Client, streamName, groupName, consumerName, target string, resultExpiry, jobTimeout time.Duration, probeClient, forwardClient *http.Client, maxConcurrent int, capacityPollInterval time.Duration) error {
	workers := maxConcurrent
	if workers <= 0 {
		workers = 1
	}

	// Derived from shutdownCtx: a real infra error (not a shutdown) also
	// stops sibling workers from claiming further work, same as before.
	claimCtx, cancelClaim := context.WithCancel(shutdownCtx)
	defer cancelClaim()

	var wg sync.WaitGroup
	var firstErrOnce sync.Once
	var firstErr error

	recordFatal := func(err error) {
		if err == nil {
			return
		}
		firstErrOnce.Do(func() {
			firstErr = err
			cancelClaim() // stop sibling workers from claiming on a real infrastructure error
		})
	}

	runWorker := func(workerID int) {
		defer wg.Done()
		for {
			// Hard gate: stop claiming new work the instant shutdown
			// begins (or a sibling hits a fatal error). Checked BEFORE
			// every claim attempt.
			select {
			case <-claimCtx.Done():
				return
			default:
			}

			// Gate on vLLM's real-time load before claiming the next
			// message. Uses claimCtx so this wait is itself interruptible
			// by shutdown -- no point checking capacity if we're about to
			// stop claiming anyway.
			if err := waitForCapacity(claimCtx, probeClient, target, maxConcurrent, capacityPollInterval); err != nil {
				if shutdownCtx.Err() == nil {
					recordFatal(err)
				}
				return
			}

			// Read a job from the queue. Also uses claimCtx: if no
			// message is available yet and shutdown arrives while
			// blocked here, this unblocks immediately -- nothing has
			// been claimed yet, so there is nothing to drain.
			job, entryID, err := queue.Read(claimCtx, rdb, streamName, groupName, consumerName)
			if err != nil {
				if shutdownCtx.Err() == nil {
					recordFatal(fmt.Errorf("read job (worker %d): %w", workerID, err))
				}
				return
			}

			// The job is now claimed. From here on we use workCtx, NOT
			// claimCtx/shutdownCtx -- a shutdown signal arriving now must
			// NOT abort in-flight work. The job runs to completion
			// (forward to vLLM, write result, ack) regardless of
			// shutdown, so already-spent GPU compute is never wasted.
			// Only after this call returns does the worker loop back and
			// check the shutdown gate again.
			if err := processJob(workCtx, forwardClient, rdb, job, entryID, streamName, groupName, target, resultExpiry, jobTimeout); err != nil {
				recordFatal(err)
				return
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go runWorker(i)
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	return shutdownCtx.Err()
}

// ReclaimLoop periodically claims idle jobs from the consumer group and processes them.
// It runs on a ticker with interval reclaimInterval, calling queue.Claim to reclaim
// jobs that have been idle longer than idleThreshold. This implements the crash-protection
// redelivery mechanism: if a sidecar dies mid-job, another sidecar will eventually reclaim
// and complete that job.
//
// The idempotency check in processJob ensures that if the crashed sidecar had already
// written the result before dying, the reclaiming sidecar will see the result exists
// and just ack the job without re-executing it.
//
// Each tick's claimed batch is processed by up to maxConcurrent worker
// goroutines pulling from a shared channel (the same bounded-concurrency
// model as ConsumerLoop -- see health.go's waitForCapacity), so a burst of
// reclaimed jobs after a crash is drained with the same real-time
// vLLM-capacity awareness as fresh reads, not one at a time.
//
// GRACEFUL SHUTDOWN / SCALE-DOWN: same shutdownCtx/workCtx split as
// ConsumerLoop (see its doc comment for the full rationale). shutdownCtx
// gates whether a new reclaim tick even attempts to claim more idle jobs
// (hard gate, checked before every queue.Claim call); workCtx is used once
// a job is actually claimed, so an in-flight reclaimed job also always
// runs to completion regardless of a shutdown signal arriving mid-flight.
//
// Parameters:
//   - shutdownCtx: cancelled on shutdown signal; gates claiming new work (hard gate)
//   - workCtx: NOT cancelled by shutdown; used for in-flight job processing so it always
//     runs to completion
//   - rdb: Redis client
//   - streamName: name of the Redis stream
//   - groupName: consumer group name
//   - consumerName: consumer name within the group
//   - target: base URL of the vLLM instance
//   - resultExpiry: TTL for result keys in Redis
//   - idleThreshold: minimum idle time before a job is eligible for reclaim
//   - reclaimInterval: how often to run the reclaim check
//   - jobTimeout: max time allowed for a single job's forward-to-vLLM call
//     before it's treated as timed out (see processJob's doc comment)
//   - probeClient: HTTP client used for the lightweight /health and /metrics probes
//   - forwardClient: HTTP client used for the actual job forward to vLLM
//   - maxConcurrent: number of concurrent worker goroutines per batch, and the vLLM
//     running+waiting threshold each worker gates on (<=0 is treated as 1 worker)
//   - capacityPollInterval: how often to re-check vLLM's load while waiting for capacity
func ReclaimLoop(shutdownCtx, workCtx context.Context, rdb *redis.Client, streamName, groupName, consumerName, target string, resultExpiry, idleThreshold, reclaimInterval, jobTimeout time.Duration, probeClient, forwardClient *http.Client, maxConcurrent int, capacityPollInterval time.Duration) error {
	ticker := time.NewTicker(reclaimInterval)
	defer ticker.Stop()

	workers := maxConcurrent
	if workers <= 0 {
		workers = 1
	}

	for {
		select {
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		case <-ticker.C:
			// Hard gate: don't even attempt to claim more idle jobs once
			// shutdown has begun.
			select {
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			default:
			}

			// Claim idle jobs
			claimedJobs, err := queue.Claim(shutdownCtx, rdb, streamName, groupName, consumerName, idleThreshold)
			if err != nil {
				log.Printf("ERROR: claim idle jobs: %v", err)
				// Don't return; continue trying on the next tick
				continue
			}
			if len(claimedJobs) == 0 {
				continue
			}

			jobCh := make(chan queue.ClaimedJob, len(claimedJobs))
			for _, cj := range claimedJobs {
				jobCh <- cj
			}
			close(jobCh)

			claimCtx, cancelClaim := context.WithCancel(shutdownCtx)
			var wg sync.WaitGroup
			var firstErrOnce sync.Once
			var firstErr error

			recordFatal := func(e error) {
				if e == nil {
					return
				}
				firstErrOnce.Do(func() {
					firstErr = e
					cancelClaim()
				})
			}

			wg.Add(workers)
			for i := 0; i < workers; i++ {
				go func() {
					defer wg.Done()
					for cj := range jobCh {
						select {
						case <-claimCtx.Done():
							return
						default:
						}
						if err := waitForCapacity(claimCtx, probeClient, target, maxConcurrent, capacityPollInterval); err != nil {
							if shutdownCtx.Err() == nil {
								recordFatal(err)
							}
							return
						}
						// Claimed -- process on workCtx, NOT
						// claimCtx/shutdownCtx, so shutdown never aborts
						// in-flight reclaimed work either.
						if err := processJob(workCtx, forwardClient, rdb, cj.Job, cj.EntryID, streamName, groupName, target, resultExpiry, jobTimeout); err != nil {
							recordFatal(err)
							return
						}
					}
				}()
			}
			wg.Wait()
			cancelClaim()

			if firstErr != nil {
				return firstErr
			}
		}
	}
}
