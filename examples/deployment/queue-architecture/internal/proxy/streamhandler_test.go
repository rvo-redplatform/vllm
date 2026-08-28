package proxy

import "testing"

func TestFrameInboxPayload_DoneMarkerTranslatesToStandardSentinel(t *testing.T) {
	// Forwarding the sidecar's internal inbox marker verbatim instead of
	// translating it back to the standard OpenAI "[DONE]" sentinel breaks
	// strict OpenAI-compatible SSE clients, which reject {"__done":true}.
	frame, terminal := frameInboxPayload(map[string]interface{}{"__done": true})

	if !terminal {
		t.Fatalf("expected terminal=true for a done marker, got false")
	}
	if frame != "data: [DONE]\n\n" {
		t.Fatalf("expected the standard OpenAI done sentinel, got %q", frame)
	}
}

func TestFrameInboxPayload_DoneMarkerWithExtraFields(t *testing.T) {
	// Relays may add fields (e.g. "model") onto the sidecar's
	// {"__done": true}. The proxy must still recognize it as the done marker.
	frame, terminal := frameInboxPayload(map[string]interface{}{
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

func TestFrameInboxPayload_OrdinaryChunkIsNotTerminal(t *testing.T) {
	frame, terminal := frameInboxPayload(map[string]interface{}{
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

func TestFrameInboxPayload_DoneFieldPresentButFalse(t *testing.T) {
	frame, terminal := frameInboxPayload(map[string]interface{}{"__done": false})

	if terminal {
		t.Fatalf("expected terminal=false when __done is false, got true")
	}
	if frame != "data: {\"__done\":false}\n\n" {
		t.Fatalf("unexpected frame: %q", frame)
	}
}

func TestFrameInboxPayload_UpstreamErrorForwardsVLLMErrorObjectAsIs(t *testing.T) {
	// When vLLM rejects a request before producing any SSE data,
	// ForwardStreaming publishes
	// {"error": true, "status": <code>, "body": "<raw vLLM error JSON>"}.
	// Forwarding that internal shape verbatim breaks strict OpenAI clients.
	// vLLM's own error body is already OpenAI-style, so it is forwarded as-is.
	frame, terminal := frameInboxPayload(map[string]interface{}{
		"error":  true,
		"status": float64(400),
		"body":   `{"error":{"message":"\"auto\" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set","type":"BadRequestError","param":null,"code":400}}`,
		"model":  "nemotron",
	})

	if terminal {
		t.Fatalf("expected terminal=false for an error frame (done sentinel follows separately), got true")
	}
	want := "data: {\"error\":{\"code\":400,\"message\":\"\\\"auto\\\" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set\",\"param\":null,\"type\":\"BadRequestError\"}}\n\n"
	if frame != want {
		t.Fatalf("unexpected frame:\ngot:  %q\nwant: %q", frame, want)
	}
}

func TestFrameInboxPayload_UpstreamErrorSynthesizesObjectFromPlainBody(t *testing.T) {
	frame, terminal := frameInboxPayload(map[string]interface{}{
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
