package main

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestRunSidecar_ReturnsConsumerConnectError(t *testing.T) {
	previousNATSURL := viper.Get(natsURLKey)
	viper.Set(natsURLKey, "")
	t.Cleanup(func() {
		viper.Set(natsURLKey, previousNATSURL)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)

	err := runSidecar(cmd, nil)
	if err == nil {
		t.Fatal("runSidecar() error = nil, want consumer connection error")
	}
	if !strings.Contains(err.Error(), "connect queue consumer") {
		t.Errorf("runSidecar() error = %q, want consumer connection error", err)
	}
}
