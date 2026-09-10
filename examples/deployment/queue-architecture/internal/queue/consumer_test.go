package queue

import (
	"context"
	"strings"
	"testing"
)

// TestNewConsumer_ConsumerName verifies that the configured consumer name is
// threaded into the consumer config (which becomes the JetStream durable name
// in Connect), and that the package default is preserved when no name is
// supplied.
func TestNewConsumer_ConsumerName(t *testing.T) {
	tests := []struct {
		name string
		opts []ConsumerOpts
		want string
	}{
		{
			name: "explicit consumer name",
			opts: []ConsumerOpts{WithConsumerName("vllm-sidecars-qwen")},
			want: "vllm-sidecars-qwen",
		},
		{
			name: "default preserved when unset",
			opts: nil,
			want: defaultConsumerName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewConsumer(nil, tt.opts...)
			if got := c.Config().ConsumerName; got != tt.want {
				t.Errorf("ConsumerName: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestConsumerConnect_PropagatesDialFailure(t *testing.T) {
	c := NewConsumer(NewClient(context.Background()))

	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() error = nil, want missing NATS URL error")
	}
	if !strings.Contains(err.Error(), "NATS_URL is required") {
		t.Errorf("Connect() error = %q, want missing NATS URL error", err)
	}
}
