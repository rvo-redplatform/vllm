package sidecar

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testMetrics is shared across all tests in this package because
// NewSidecarMetrics registers each metric family against the shared
// metrics.Registry via promauto, and that registration is NOT idempotent --
// calling it more than once panics with "duplicate metrics collector
// registration attempted" (see the identical note on TestProcessJob_Timeout
// in consumer_test.go).
var (
	testMetricsOnce sync.Once
	testMetricsVal  *SidecarMetrics
)

func testMetrics() *SidecarMetrics {
	testMetricsOnce.Do(func() {
		testMetricsVal = NewSidecarMetrics()
	})
	return testMetricsVal
}

// metricsPage renders a minimal vLLM-shaped /metrics page with the given
// capacity/deferred waiting-reason values, mirroring the real exposition
// format enough for capacityPressure's line-scanning to parse correctly.
func metricsPage(capacity, deferred float64) string {
	return fmt.Sprintf(`# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="test"} 1.0
# HELP vllm:num_requests_waiting_by_reason Number of waiting requests by reason.
# TYPE vllm:num_requests_waiting_by_reason gauge
vllm:num_requests_waiting_by_reason{engine="0",model_name="test",reason="capacity"} %v
vllm:num_requests_waiting_by_reason{engine="0",model_name="test",reason="deferred"} %v
`, capacity, deferred)
}

func TestCapacityPressure_ParsesCapacityReasonOnly(t *testing.T) {
	tests := []struct {
		name         string
		capacity     float64
		deferred     float64
		wantPressure float64
	}{
		{name: "no pressure", capacity: 0, deferred: 0, wantPressure: 0},
		{name: "capacity pressure only", capacity: 3, deferred: 0, wantPressure: 3},
		// deferred pressure alone must NOT count as capacity pressure --
		// it's a different, unrelated scheduling reason.
		{name: "deferred pressure ignored", capacity: 0, deferred: 5, wantPressure: 0},
		{name: "both present, only capacity counted", capacity: 2, deferred: 7, wantPressure: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(metricsPage(tt.capacity, tt.deferred)))
			}))
			defer srv.Close()

			got, err := capacityPressure(context.Background(), srv.Client(), srv.URL)
			if err != nil {
				t.Fatalf("capacityPressure: unexpected error: %v", err)
			}
			if got != tt.wantPressure {
				t.Errorf("capacityPressure: got %v want %v", got, tt.wantPressure)
			}
		})
	}
}

func TestCapacityPressure_MissingMetricTreatedAsNoPressure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# no relevant metrics here\n"))
	}))
	defer srv.Close()

	got, err := capacityPressure(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("capacityPressure: unexpected error: %v", err)
	}
	if got != 0 {
		t.Errorf("capacityPressure: got %v want 0 when metric is absent", got)
	}
}

// TestWaitForCapacity_BlocksThenUnblocksOnPressureClear is the core
// regression test for the binary gate: it must block for as long as vLLM
// reports capacity pressure, and return as soon as pressure clears -- with
// no dependency on any external concurrency count.
func TestWaitForCapacity_BlocksThenUnblocksOnPressureClear(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(1) // start under pressure

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsPage(float64(pressure.Load()), 0)))
	}))
	defer srv.Close()

	done := make(chan error, 1)
	go func() {
		done <- waitForCapacity(context.Background(), srv.Client(), testMetrics(), srv.URL, false, 20*time.Millisecond)
	}()

	// Should still be blocked shortly after starting -- vLLM is under
	// pressure.
	select {
	case err := <-done:
		t.Fatalf("waitForCapacity returned early (err=%v) while vLLM was still under capacity pressure", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Clear pressure; waitForCapacity must unblock promptly.
	pressure.Store(0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForCapacity: unexpected error after pressure cleared: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForCapacity did not unblock after vLLM capacity pressure cleared")
	}
}

func TestWaitForCapacity_DisabledGateReturnsImmediately(t *testing.T) {
	// No server at all -- if the gate were not actually disabled, this
	// would hang/error trying to scrape an unreachable target.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := waitForCapacity(ctx, http.DefaultClient, testMetrics(), "http://127.0.0.1:1", true, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForCapacity with capacityGateDisabled=true: unexpected error: %v", err)
	}
}
