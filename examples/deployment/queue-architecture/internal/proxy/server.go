package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rvo-redplatform/vllm/examples/deployment/queue-architecture/internal/model"
)

type Producer interface {
	Enqueue(ctx context.Context, job model.Job) error
	SubscribeSync() (string, *nats.Subscription, error)
}

// statusWriter wraps http.ResponseWriter to capture the HTTP status code
// written via WriteHeader, so we can track failed request metrics at the mux level.
type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.statusCode = code
	sw.ResponseWriter.WriteHeader(code)
}

// ProxyServer handles proxy HTTP requests via NATS.
type ProxyServer struct {
	prod           Producer
	maxBodyBytes   int64
	requestTimeout time.Duration
	streamTimeout  time.Duration
	metrics        *ProxyMetrics
	server         *http.Server
}

// NewProxyServer creates a new ProxyServer with the given producer, body limit,
// timeouts, and proxy metrics.
func NewProxyServer(
	prod Producer,
	maxBodyBytes int64,
	requestTimeout time.Duration,
	streamTimeout time.Duration,
	metrics *ProxyMetrics,
) *ProxyServer {

	return &ProxyServer{
		prod:           prod,
		maxBodyBytes:   maxBodyBytes,
		requestTimeout: requestTimeout,
		streamTimeout:  streamTimeout,
		metrics:        metrics,
	}
}

func (s *ProxyServer) Register(addr string) {
	mux := http.NewServeMux()

	mux.Handle("/", s.metrics.Instrument(http.HandlerFunc(s.proxy)))

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}

func (s *ProxyServer) Serve() error {
	return s.server.ListenAndServe()
}

func (s *ProxyServer) proxy(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w}

	r.Body = http.MaxBytesReader(sw, r.Body, s.maxBodyBytes)

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		if err.Error() == "http: request body too large" {
			http.Error(sw, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(sw, "failed to read body", http.StatusBadRequest)
		return
	}

	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	isStreaming := peekStreamFlag(bodyBytes)

	var handler http.HandlerFunc
	if isStreaming {
		handler = HandleStreaming(s.prod, s.streamTimeout)
	} else {
		handler = HandleNonStreaming(s.prod, s.requestTimeout, s.metrics.UpstreamProcessing)
	}

	handler(sw, r)
}

// peekStreamFlag performs a cheap JSON peek to check if "stream":true is present.
// It returns true if the stream flag is found and set to true, false otherwise.
func peekStreamFlag(bodyBytes []byte) bool {
	if len(bodyBytes) == 0 {
		return false
	}

	var data map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return false
	}

	if stream, ok := data["stream"]; ok {
		if streamBool, ok := stream.(bool); ok {
			return streamBool
		}
	}

	return false
}
