package proxy

import "testing"

func TestFrameRedisPayload_DoneMarkerTranslatesToStandardSentinel(t *testing.T) {
	// The bug this guards against: forwarding the sidecar's internal Redis
	// transport marker verbatim instead of translating it back to the
	// standard OpenAI "[DONE]" sentinel breaks strict OpenAI-compatible SSE
	// clients (e.g. opencode's AI SDK), which reject {"__done":true} - it
	// matches neither the chat-completion-chunk schema nor an error object.
	frame, terminal := frameRedisPayload(map[string]interface{}{"__done": true})

	if !terminal {
		t.Fatalf("expected terminal=true for a done marker, got false")
	}
	if frame != "data: [DONE]\n\n" {
		t.Fatalf("expected the standard OpenAI done sentinel, got %q", frame)
	}
}

func TestFrameRedisPayload_DoneMarkerWithExtraFields(t *testing.T) {
	// Confirmed live against the real deployed stack: Switchyard relabels
	// the sidecar's bare {"__done": true} with an extra "model" field as it
	// relays through - the proxy must still recognize it as the done
	// marker regardless of what other fields are present.
	frame, terminal := frameRedisPayload(map[string]interface{}{
		"__done": true,
		"model":  "nemotron",
	})

	if !terminal {
		t.Fatalf("expected terminal=true for a done marker with extra fields, got false")
	}
	if frame != "data: [DONE]\n\n" {
		t.Fatalf("expected the standard OpenAI done sentinel, got %q", frame)
	}
}

func TestFrameRedisPayload_OrdinaryChunkIsNotTerminal(t *testing.T) {
	frame, terminal := frameRedisPayload(map[string]interface{}{
		"id":      "chatcmpl-123",
		"object":  "chat.completion.chunk",
		"choices": []interface{}{},
	})

	if terminal {
		t.Fatalf("expected terminal=false for an ordinary chunk, got true")
	}
	if frame == "data: [DONE]\n\n" {
		t.Fatalf("ordinary chunk must not be mistaken for the done sentinel")
	}
	if frame != "data: {\"choices\":[],\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\"}\n\n" {
		t.Fatalf("unexpected frame for ordinary chunk: %q", frame)
	}
}

func TestFrameRedisPayload_DoneFieldPresentButFalse(t *testing.T) {
	// A payload that happens to have a "__done" key set to false (or any
	// non-bool-true value) must be treated as an ordinary chunk, not the
	// terminal marker.
	frame, terminal := frameRedisPayload(map[string]interface{}{"__done": false})

	if terminal {
		t.Fatalf("expected terminal=false when __done is false, got true")
	}
	if frame != "data: {\"__done\":false}\n\n" {
		t.Fatalf("unexpected frame: %q", frame)
	}
}
