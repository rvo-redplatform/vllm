# Queue Architecture Local Development Guide

This directory contains a local development stack for the vLLM queue-based architecture, which demonstrates request queuing and async processing through a proxy.

## Architecture Overview

The stack consists of two isolated applications that share NATS:

- **nats**: JetStream-enabled NATS broker (see [WIRE-CONTRACT.md](WIRE-CONTRACT.md))
- **mockvllm**, **sidecar**, **proxy**: Default application using stream `vllm_requests`, subject `vllm.requests`, and durable `vllm-sidecars`
- **mockvllm-qwen**, **sidecar-qwen**, **proxy-qwen**: Second application using stream `vllm_requests_qwen`, subject `vllm.requests.qwen`, and durable `vllm-sidecars-qwen`

The distinct stream/subject is required. JetStream work-queue retention permits
only one consumer per subject, so consumer names alone cannot isolate two vLLM
applications that use the same subject.

## Running the Stack

### Prerequisites

- Docker and Docker Compose installed
- Ports 18001/18000 (default proxy/mockvllm), 28101/28000 (qwen proxy/mockvllm), 4222 (NATS client), and optionally 8222 (NATS monitoring) available on your machine

### Start the Stack

From this directory, run:

```bash
docker compose up --build
```

This command will:
1. Build application services from their Dockerfiles
2. Start NATS with JetStream and health checks
3. Start the mock vLLM server
4. Start both sidecar consumers
5. Start both proxy API gateways

Wait for all services to be healthy (you should see logs indicating successful startup).

### NATS endpoints

| Port | Purpose |
|---|---|
| `4222` | NATS client connections (`nats://localhost:4222` from the host; `nats://nats:4222` inside compose) |
| `8222` | HTTP monitoring (`/healthz`, `/varz`, etc.) |

Server `max_payload` is set to 10 MiB in `nats-server.conf` (NATS default is 1 MiB).

## Testing the Stack

The proxy listens on `http://localhost:18001` and exposes an OpenAI-compatible API endpoint at `/v1/chat/completions`.
The second application exposes the same endpoint at `http://localhost:28101`.

### Verify Two Consumer Groups

Start from a clean NATS state so stale durable consumers cannot affect the
result:

```bash
docker compose down -v
docker compose up -d --build
docker compose ps
docker compose logs sidecar sidecar-qwen
```

Both sidecar logs must reach `starting consume loop` and must not contain
`consumer not initialized; call Connect`. Confirm the two distinct durables
through the NATS monitoring endpoint:

```bash
curl -s "http://localhost:8222/jsz?streams=1&consumers=1"
```

The output must contain both pairs:

```text
vllm_requests       -> vllm-sidecars
vllm_requests_qwen  -> vllm-sidecars-qwen
```

Send one request through each proxy. Each must return HTTP 200:

```bash
curl -s -o /dev/null -w "default: %{http_code}\n" \
  -X POST http://localhost:18001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-model","messages":[{"role":"user","content":"default"}],"stream":false}'

curl -s -o /dev/null -w "qwen: %{http_code}\n" \
  -X POST http://localhost:28101/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-model-qwen","messages":[{"role":"user","content":"qwen"}],"stream":false}'
```

### Non-Streaming Request

Send a non-streaming request (the request is queued, processed, and the full response is returned):

```bash
curl -X POST http://localhost:18001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mock-model",
    "messages": [
      {
        "role": "user",
        "content": "Hello, how are you?"
      }
    ],
    "stream": false
  }'
```

Expected response: A JSON object containing the model's completion.

### Streaming Request

Send a streaming request (the response is streamed back as Server-Sent Events):

```bash
curl -X POST http://localhost:18001/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mock-model",
    "messages": [
      {
        "role": "user",
        "content": "Hello, how are you?"
      }
    ],
    "stream": true
  }'
```

Expected response: A stream of JSON objects (one per line), each containing a partial completion chunk.

## Stopping the Stack

To stop all services:

```bash
docker compose down
```

To stop and remove all volumes (including NATS JetStream data):

```bash
docker compose down -v
```

## Troubleshooting

- **Port already in use**: If port 18001, 18000, 4222, or 8222 is already in use, either stop the conflicting service or modify the port mappings in `docker-compose.yaml`.
- **Services not starting**: Check the logs with `docker compose logs <service-name>` (e.g., `docker compose logs proxy`).
- **Requests timing out**: Ensure all services are healthy by running `docker compose ps` and checking the STATUS column.
