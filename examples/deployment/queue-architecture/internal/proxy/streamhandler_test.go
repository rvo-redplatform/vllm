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

func TestFrameRedisPayload_UpstreamErrorForwardsVLLMErrorObjectAsIs(t *testing.T) {
	// Confirmed live against the real deployed stack: when vLLM rejects a
	// request before producing any SSE data (e.g. tool_choice="auto"
	// without --enable-auto-tool-choice), ForwardStreaming publishes
	// {"error": true, "status": <code>, "body": "<raw vLLM error JSON>"}.
	// The bug this guards against: forwarding that internal transport shape
	// verbatim breaks strict OpenAI-compatible SSE clients (e.g. opencode's
	// AI SDK) - a bare boolean "error" field matches neither the
	// chat-completion-chunk schema nor a valid {"error": <object>} response.
	// vLLM's own error body is already an OpenAI-style error object, so it
	// should be forwarded as-is rather than re-wrapped.
	frame, terminal := frameRedisPayload(map[string]interface{}{
		"error":  true,
		"status": float64(400),
		"body":   `{"error":{"message":"\"auto\" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set","type":"BadRequestError","param":null,"code":400}}`,
		"model":  "nemotron", // Switchyard adds this as it relays through, same as the done marker
	})

	if terminal {
		t.Fatalf("expected terminal=false for an error frame (done sentinel follows separately), got true")
	}
	want := "data: {\"error\":{\"code\":400,\"message\":\"\\\"auto\\\" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set\",\"param\":null,\"type\":\"BadRequestError\"}}\n\n"
	if frame != want {
		t.Fatalf("unexpected frame:\ngot:  %q\nwant: %q", frame, want)
	}
}

func TestFrameRedisPayload_UpstreamErrorSynthesizesObjectFromPlainBody(t *testing.T) {
	// If the upstream error body isn't a parseable OpenAI-style error
	// object (e.g. a plain-text or unexpected-shape body), a minimal valid
	// error object must still be synthesized so the client never receives
	// the raw internal transport shape.
	frame, terminal := frameRedisPayload(map[string]interface{}{
		"error":  true,
		"status": float64(502),
		"body":   "upstream connection reset",
	})

	if terminal {
		t.Fatalf("expected terminal=false for an error frame, got true")
	}
	want := "data: {\"error\":{\"code\":502,\"message\":\"upstream connection reset\",\"type\":\"upstream_error\"}}\n\n"
	if frame != want {
		t.Fatalf("unexpected frame:\ngot:  %q\nwant: %q", frame, want)
	}
}
