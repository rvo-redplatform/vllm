package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/apierror"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/model"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/queue"
)

var _ jetstream.Msg = (*fakeJSMsg)(nil)
var _ ConsumerClient = (*fakeClient)(nil)

// jobProbe records Mail/Ack/Nak and fails Ack if the ACK context is cancelled.
type jobProbe struct {
	t       *testing.T
	workCtx context.Context

	mu     sync.Mutex
	events []string
	mails  []mailed
	acks   int
	naks   int
}

type mailed struct {
	recipient string
	data      []byte
}

func (p *jobProbe) add(event string) {
	p.events = append(p.events, event)
}

type fakeClient struct{ *jobProbe }

func (c *fakeClient) Fetch(context.Context, int) ([]queue.Message, error) {
	return nil, nil
}

func (c *fakeClient) Mail(recipient string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.add("mail")
	c.mails = append(c.mails, mailed{
		recipient: recipient,
		data:      bytes.Clone(data),
	})
	return nil
}

type fakeJSMsg struct{ *jobProbe }

func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *fakeJSMsg) Data() []byte                              { return nil }
func (m *fakeJSMsg) Headers() nats.Header                      { return nil }
func (m *fakeJSMsg) Subject() string                           { return "" }
func (m *fakeJSMsg) Reply() string                             { return "" }
func (m *fakeJSMsg) DoubleAck(context.Context) error           { return nil }
func (m *fakeJSMsg) NakWithDelay(time.Duration) error          { return m.Nak() }
func (m *fakeJSMsg) InProgress() error                         { return nil }
func (m *fakeJSMsg) Term() error                               { return nil }
func (m *fakeJSMsg) TermWithReason(string) error               { return nil }

func (m *fakeJSMsg) Ack() error {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.workCtx.Err(); err != nil {
		m.t.Errorf("Ack: ctx.Err()=%v, want nil", err)
		return err
	}
	m.add("ack")
	m.acks++
	return nil
}

func (m *fakeJSMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.add("nak")
	m.naks++
	return nil
}

func TestProcessJob_Timeout(t *testing.T) {
	tests := []struct {
		name   string
		stream bool
	}{
		{name: "non-stream", stream: false},
		{name: "stream", stream: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCancelled := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
				close(handlerCancelled)
			}))
			defer srv.Close()

			workCtx := context.Background()
			probe := &jobProbe{t: t, workCtx: workCtx}
			timeout := 50 * time.Millisecond
			job := model.Job{
				JobID:   "job-timeout",
				Method:  http.MethodPost,
				Path:    "/v1/completions",
				Body:    []byte(`{}`),
				Stream:  tt.stream,
				ReplyTo: "inbox.test",
			}
			fetched := queue.Message{
				Job: job,
				Msg: &fakeJSMsg{probe},
			}

			c := NewConsumer(&fakeClient{probe})
			runProcessJob(t, c, workCtx, fetched, srv.URL, timeout)

			select {
			case <-handlerCancelled:
			case <-time.After(2 * time.Second):
				t.Fatal("upstream HTTP handler did not observe cancellation")
			}

			probe.mu.Lock()
			defer probe.mu.Unlock()
			if probe.acks != 1 {
				t.Errorf("Ack calls: got %d want 1", probe.acks)
			}
			if probe.naks != 0 {
				t.Errorf("Nak calls: got %d want 0", probe.naks)
			}
			if workCtx.Err() != nil {
				t.Errorf("workCtx.Err()=%v, want nil at Ack", workCtx.Err())
			}

			if tt.stream {
				assertStreamTimeoutMail(t, probe, job.ReplyTo)
				want := []string{"mail", "mail", "ack"}
				if !slices.Equal(probe.events, want) {
					t.Errorf("events: got %v want %v", probe.events, want)
				}
				return
			}
			assertNonStreamTimeoutMail(t, probe, job.ReplyTo)
			want := []string{"mail", "ack"}
			if !slices.Equal(probe.events, want) {
				t.Errorf("events: got %v want %v", probe.events, want)
			}
		})
	}
}

func runProcessJob(
	t *testing.T,
	c *Consumer,
	workCtx context.Context,
	msg queue.Message,
	target string,
	timeout time.Duration,
) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.processJob(workCtx, msg, target, timeout)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processJob did not return")
	}
}

func assertNonStreamTimeoutMail(t *testing.T, p *jobProbe, replyTo string) {
	t.Helper()
	if len(p.mails) != 1 {
		t.Fatalf("mails: got %d want 1", len(p.mails))
	}
	if p.mails[0].recipient != replyTo {
		t.Errorf("recipient: got %q want %q", p.mails[0].recipient, replyTo)
	}
	var reply map[string]any
	if err := json.Unmarshal(p.mails[0].data, &reply); err != nil {
		t.Fatalf("non-stream reply: %v\n%s", err, p.mails[0].data)
	}
	status, _ := reply["status"].(float64)
	if int(status) != apierror.TimeoutHTTPStatus {
		t.Errorf("status: got %v want %d", reply["status"], apierror.TimeoutHTTPStatus)
	}
	headers, _ := reply["headers"].(map[string]any)
	if headers["Content-Type"] != apierror.JSONContentType {
		t.Errorf("Content-Type: got %v want %s", headers["Content-Type"], apierror.JSONContentType)
	}
	body, _ := reply["body"].(string)
	assertTimeoutOpenAIBody(t, []byte(body))
}

func assertStreamTimeoutMail(t *testing.T, p *jobProbe, replyTo string) {
	t.Helper()
	if len(p.mails) != 2 {
		t.Fatalf("mails: got %d want 2 (error then __done)", len(p.mails))
	}
	for i, m := range p.mails {
		if m.recipient != replyTo {
			t.Errorf("mail[%d] recipient: got %q want %q", i, m.recipient, replyTo)
		}
	}
	var env map[string]any
	if err := json.Unmarshal(p.mails[0].data, &env); err != nil {
		t.Fatalf("stream error envelope: %v\n%s", err, p.mails[0].data)
	}
	if env["error"] != true {
		t.Errorf("error: got %v want true", env["error"])
	}
	status, _ := env["status"].(float64)
	if int(status) != apierror.TimeoutHTTPStatus {
		t.Errorf("status: got %v want %d", env["status"], apierror.TimeoutHTTPStatus)
	}
	body, _ := env["body"].(string)
	assertTimeoutOpenAIBody(t, []byte(body))
	if !bytes.Equal(p.mails[1].data, apierror.DoneEnvelope()) {
		t.Errorf("second mail: got %s want %s", p.mails[1].data, apierror.DoneEnvelope())
	}
}

func assertTimeoutOpenAIBody(t *testing.T, raw []byte) {
	t.Helper()
	var body apierror.OpenAIErrorBody
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("openai body: %v\n%s", err, raw)
	}
	if body.Error.Type != "timeout_error" {
		t.Errorf("type: got %q want timeout_error", body.Error.Type)
	}
	if body.Error.Code != "timeout" {
		t.Errorf("code: got %q want timeout", body.Error.Code)
	}
}
