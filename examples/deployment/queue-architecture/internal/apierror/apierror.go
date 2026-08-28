// Package apierror builds the canonical OpenAI-compatible error JSON body
// shared by the proxy's raw HTTP error path and the sidecar's stream inbox
// envelope. It is intentionally dependency-free: no NATS/JetStream import,
// no HTTP server/client behavior, no side effects. Just types and pure
// functions that construct byte payloads and structs, so both the sidecar
// and proxy binaries can import it without pulling in unrelated concerns.
package apierror

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TimeoutHTTPStatus is the HTTP status code returned for timeout errors.
const TimeoutHTTPStatus = http.StatusGatewayTimeout

// ServerHTTPStatus is the HTTP status code returned for non-timeout
// sidecar/proxy failures (vLLM unreachable, forward errors, etc).
const ServerHTTPStatus = http.StatusBadGateway

// JSONContentType is the Content-Type header value paired with an
// OpenAIErrorBody written as a raw HTTP response body.
const JSONContentType = "application/json"

// OpenAIError is the inner "error" object of an OpenAI-compatible error
// response body.
type OpenAIError struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code"`
}

// OpenAIErrorBody is the canonical OpenAI-compatible error response body:
//
//	{"error": {"message": "...", "type": "...", "param": null, "code": "..."}}
type OpenAIErrorBody struct {
	Error OpenAIError `json:"error"`
}

// Bytes marshals the error body to JSON.
func (b OpenAIErrorBody) Bytes() ([]byte, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("marshal openai error body: %w", err)
	}
	return data, nil
}

// NewTimeoutError builds an OpenAIErrorBody for a request that exceeded the
// configured max processing time d. The message includes d's duration
// string, e.g. "Request exceeded max processing time of 10m0s".
func NewTimeoutError(d time.Duration) OpenAIErrorBody {
	return OpenAIErrorBody{
		Error: OpenAIError{
			Message: fmt.Sprintf("Request exceeded max processing time of %s", d),
			Type:    "timeout_error",
			Param:   nil,
			Code:    "timeout",
		},
	}
}

// NewServerError builds a generic OpenAIErrorBody for non-timeout server
// failures, using msg as the error message.
func NewServerError(msg string) OpenAIErrorBody {
	return OpenAIErrorBody{
		Error: OpenAIError{
			Message: msg,
			Type:    "server_error",
			Param:   nil,
			Code:    "internal_error",
		},
	}
}

// TimeoutBody returns the marshaled OpenAI timeout error body for d, ready
// to be written as an HTTP response body alongside TimeoutHTTPStatus and
// JSONContentType.
func TimeoutBody(d time.Duration) ([]byte, error) {
	return NewTimeoutError(d).Bytes()
}

// TimeoutStreamEnvelope builds the existing sidecar stream inbox error
// envelope (as used by consumer.go's publishStreamError:
// {"error": true, "status": N, "body": "<string>"}), with body set to the
// marshaled OpenAI timeout error JSON and status set to TimeoutHTTPStatus.
func TimeoutStreamEnvelope(d time.Duration) ([]byte, error) {
	body, err := TimeoutBody(d)
	if err != nil {
		return nil, err
	}
	envelope := map[string]any{
		"error":  true,
		"status": TimeoutHTTPStatus,
		"body":   string(body),
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal timeout stream envelope: %w", err)
	}
	return data, nil
}

// DoneEnvelope returns the marshaled stream "done" sentinel message
// ({"__done": true}) used to terminate a sidecar stream inbox.
func DoneEnvelope() []byte {
	return []byte(`{"__done":true}`)
}
