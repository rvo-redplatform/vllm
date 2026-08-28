package apierror

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestNewTimeoutErrorOpenAIShape(t *testing.T) {
	d := 10 * time.Millisecond
	raw, err := NewTimeoutError(d).Bytes()
	if err != nil {
		t.Fatalf("Bytes: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	errObj, ok := got["error"].(map[string]any)
	if !ok {
		t.Fatalf("want error object, got %s", raw)
	}
	if errObj["type"] != "timeout_error" {
		t.Errorf("type: got %v want timeout_error", errObj["type"])
	}
	if errObj["code"] != "timeout" {
		t.Errorf("code: got %v want timeout", errObj["code"])
	}
	if errObj["param"] != nil {
		t.Errorf("param: got %v want nil", errObj["param"])
	}
	wantMsg := fmt.Sprintf("Request exceeded max processing time of %s", d)
	if errObj["message"] != wantMsg {
		t.Errorf("message: got %v want %q", errObj["message"], wantMsg)
	}
	if TimeoutHTTPStatus != http.StatusGatewayTimeout {
		t.Errorf("TimeoutHTTPStatus: got %d want 504", TimeoutHTTPStatus)
	}
}

func TestDoneEnvelope(t *testing.T) {
	var got map[string]any
	if err := json.Unmarshal(DoneEnvelope(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["__done"] != true {
		t.Errorf("__done: got %v want true", got["__done"])
	}
}
