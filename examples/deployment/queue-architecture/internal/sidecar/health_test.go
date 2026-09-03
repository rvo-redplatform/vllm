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

	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/circuit"
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

// TestCapacityCircuit_WaitReadyBlocksThenUnblocks is the core regression
// test for admission gating: WaitReady must block while vLLM reports
// capacity pressure and return once pressure clears.
func TestCapacityCircuit_WaitReadyBlocksThenUnblocks(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metricsPage(float64(pressure.Load()), 0)))
	}))
	defer srv.Close()

	probe := func(ctx context.Context) (circuit.CapacitySignal, error) {
		load, err := capacityPressure(ctx, srv.Client(), srv.URL)
		if err != nil {
			return circuit.CapacitySignal{}, err
		}
		return circuit.CapacitySignal{HasCapacity: load == 0, Load: load}, nil
	}

	capCircuit := circuit.New(
		probe,
		circuit.NoOpRecover,
		circuit.WithProbeInterval(20*time.Millisecond),
		circuit.WithOpenAfter(1),
		circuit.WithCloseAfter(1),
		circuit.WithInitialState(circuit.CircuitOpen),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = capCircuit.Run(ctx) }()

	done := make(chan error, 1)
	go func() {
		done <- capCircuit.WaitReady(context.Background())
	}()

	select {
	case err := <-done:
		t.Fatalf("WaitReady returned early (err=%v) while vLLM was still under capacity pressure", err)
	case <-time.After(100 * time.Millisecond):
	}

	pressure.Store(0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitReady: unexpected error after pressure cleared: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReady did not unblock after vLLM capacity pressure cleared")
	}
}

func TestCapacityCircuit_DisabledGateWaitReadyImmediate(t *testing.T) {
	capCircuit := circuit.New(
		circuit.NoOpProbe,
		circuit.NoOpRecover,
		circuit.WithInitialState(circuit.CircuitClosed),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := capCircuit.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady with disabled gate: unexpected error: %v", err)
	}
}

func TestNewConsumer_DisabledCapacityCircuitWaitReady(t *testing.T) {
	c := NewConsumer(&fakeClient{&jobProbe{t: t, workCtx: context.Background()}}, testMetrics())

	capCircuit := c.newCapacityCircuit(
		"http://127.0.0.1:1",
		http.DefaultClient,
		true,
		10*time.Millisecond,
		10*time.Millisecond,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := capCircuit.WaitReady(ctx); err != nil {
		t.Fatalf("disabled capacity circuit WaitReady: unexpected error: %v", err)
	}
}
