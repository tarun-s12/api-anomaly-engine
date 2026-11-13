# Automated API Performance & Log Anomaly Engine

An intelligent observability backend that ingests raw application logs and API telemetry at high throughput, batches them into **PostgreSQL / TimescaleDB**, detects latency spikes, error bursts, and memory leaks via **Scikit-Learn (Isolation Forest) + Rolling Z-Score**, and dispatches surrounding log windows and slow SQL `EXPLAIN ANALYZE` plans to an **AI Root-Cause Agent** for structured incident reports and automated SQL fixes.

Features an embedded **Developer Observability Dashboard** (`http://localhost:3000`) with zero external frontend dependencies, real-time buffer metrics, 1-click simulation, and interactive incident resolution.

---

## Tech Stack

- **Ingestion API**: Node.js 20, Express, TypeScript (strict), Zod schema validation, in-memory ring-buffer micro-batcher (`UNNEST` bulk inserts).
- **Observability Dashboard**: Embedded dark-mode UI (Tailwind CSS CDN) served directly by Express at `http://localhost:3000`.
- **Time-Series Storage**: PostgreSQL 16 + TimescaleDB hypertable (partitioned by timestamp) with zero-dependency `jsonl` mode for instant local testing.
- **Anomaly Detection Worker**: Python 3.11, Pandas, NumPy, Scikit-Learn (`IsolationForest` + rolling Z-score & memory slope tracking).
- **AI Root-Cause Agent**: Pydantic structured outputs supporting **Google Gemini API**, **OpenAI**, local **Ollama** (e.g. Llama 3.2), or a built-in **Deterministic Expert Fallback** requiring zero API keys.

---

## Architecture

<img width="972" height="687" alt="image" src="https://github.com/user-attachments/assets/46129deb-e1ce-42c1-bbe3-4f61a74b414e" />

---

## Key Capabilities

1. **High-Throughput Micro-Batching**: Enqueues telemetry into an in-memory ring buffer and writes batches via PostgreSQL parameterized `UNNEST`, eliminating DB connection contention and reducing round-trips by >95%.
2. **Dual-Layer Anomaly Detection**:
   - Fast rolling window **Z-score** for sudden baseline latency shifts.
   - Multivariate **Scikit-Learn Isolation Forest** to catch complex regressions across latency, error rate, and RSS memory slope simultaneously.
3. **Context-Aware AI Root Cause Analysis**:
   - Correlates surrounding trace windows and PostgreSQL `EXPLAIN ANALYZE` query plans.
   - Automatically pinpoints unindexed sequential scans, downstream 3rd-party gateway timeouts, memory leaks, and connection pool saturations.
   - Generates exact `CREATE INDEX CONCURRENTLY` DDL remediation commands.
4. **Zero-API-Key Deterministic Mode**: Fully functional offline or in air-gapped VPCs using the built-in deterministic expert engine.
5. **Interactive Developer Console**: 1-click simulation across diverse microservices, live buffer metrics, and incident lifecycle management (`OPEN` $\rightarrow$ `RESOLVED`).

---

## Quick Start

### Option 1: Docker Compose (Recommended)

1. **Initialize environment and start services**:
   ```bash
   cp api/.env.example api/.env
   docker compose up --build
   ```

2. **Open the Observability Dashboard**:
   Navigate to **[http://localhost:3000](http://localhost:3000)** in your browser.

3. **Run Anomaly Simulation**:
   - Click the **"Run Anomaly Simulation"** button directly on the dashboard, OR
   - Run via CLI in another terminal:
     ```bash
     docker compose exec api node dist/simulator.js
     ```

4. **View Incident Reports**:
   Within 15 seconds, the worker flags anomalies and displays the AI diagnosis, evidence, and `CREATE INDEX` fix on the dashboard. Click **"Resolve"** to mark incidents resolved.

---

### Option 2: Local Mode (Zero Docker Required)

1. **Start Ingestion API** (`jsonl` storage mode):
   ```bash
   cd api
   npm install
   STORAGE_MODE=jsonl npm run dev
   ```

2. **Generate Baseline & Anomaly Traffic**:
   ```bash
   cd api
   npm run simulate
   ```

3. **Run the Python Anomaly Worker**:
   ```bash
   cd worker
   pip install -r requirements.txt
   STORAGE_MODE=jsonl python main.py --once
   ```
   *(Add `--watch` to poll continuously every 15 seconds).*

---

## Connecting Existing Applications

The engine exposes a non-blocking ingestion endpoint at `POST http://localhost:3000/api/v1/logs` that validates incoming telemetry and immediately returns `202 Accepted` (<5ms response time).

### Telemetry Payload Contract

```json
{
  "service": "payment-gateway",
  "endpoint": "/api/v1/payments/charge",
  "method": "POST",
  "status_code": 200,
  "latency_ms": 48.2,
  "memory_mb": 210.5,
  "trace_id": "tr_991823a",
  "error_message": null,
  "query_plan": null
}
```
*(Accepts a single log object or an array of up to 5,000 logs in a single batch).*

### Node.js / Express Middleware

```typescript
import { Request, Response, NextFunction } from "express";

export function anomalyTelemetry(engineUrl = "http://localhost:3000/api/v1/logs") {
  return (req: Request, res: Response, next: NextFunction) => {
    const startTime = process.hrtime.bigint();

    res.on("finish", () => {
      const elapsedNs = process.hrtime.bigint() - startTime;
      const logPayload = {
        service: "order-service",
        endpoint: req.route?.path || req.path,
        method: req.method,
        status_code: res.statusCode,
        latency_ms: Number(elapsedNs) / 1_000_000,
        memory_mb: process.memoryUsage().rss / (1024 * 1024),
        trace_id: req.headers["x-request-id"] || null,
        error_message: res.statusCode >= 500 ? res.statusMessage : null,
        query_plan: (res.locals as any)?.queryPlan || null,
      };

      // Non-blocking fire-and-forget
      fetch(engineUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(logPayload),
      }).catch(() => {});
    });

    next();
  };
}

// Usage in app.ts
app.use(anomalyTelemetry());
```

### Python / FastAPI Middleware

```python
import time, psutil, httpx
from fastapi import Request

ANOMALY_ENGINE_URL = "http://localhost:3000/api/v1/logs"

@app.middleware("http")
async def anomaly_telemetry_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    latency_ms = (time.perf_counter() - start_time) * 1000.0

    payload = {
        "service": "catalog-service",
        "endpoint": request.url.path,
        "method": request.method,
        "status_code": response.status_code,
        "latency_ms": round(latency_ms, 2),
        "memory_mb": round(psutil.Process().memory_info().rss / (1024 * 1024), 2),
        "trace_id": request.headers.get("x-request-id"),
    }

    try:
        async with httpx.AsyncClient(timeout=0.5) as client:
            await client.post(ANOMALY_ENGINE_URL, json=payload)
    except Exception:
        pass

    return response
```

---

## API & Dashboard Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Real-time dark-mode developer observability dashboard. |
| `POST` | `/api/v1/logs` | Ingest a single log object or an array of up to 5,000 logs (`202 Accepted`). |
| `GET` | `/api/v1/incidents` | List detected anomalies, metric statistics, and AI root-cause reports. |
| `PATCH`| `/api/v1/incidents/:id` | Update incident status (`OPEN`, `ACKNOWLEDGED`, `RESOLVED`). |
| `GET` | `/api/v1/metrics` | Ingestion throughput, buffer depth, and batch flush statistics. |
| `POST` | `/api/v1/simulate` | Trigger baseline and multi-service anomaly simulation via HTTP. |
| `POST` | `/api/v1/clear` | Clear all telemetry logs and incident records from storage. |
| `GET` | `/api/v1/health` | System health and database connectivity check. |

---

## Configuration

| Variable | Service | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` | API | `3000` | Port for Express ingestion server and dashboard. |
| `STORAGE_MODE` | API / Worker | `postgres` | Storage backend (`postgres` or `jsonl`). |
| `DATABASE_URL` | API / Worker | `postgres://postgres:postgres@localhost:5432/observability` | PostgreSQL / TimescaleDB connection URI. |
| `BATCH_SIZE` | API | `100` | Maximum logs accumulated before triggering an immediate flush. |
| `FLUSH_INTERVAL_MS`| API | `2000` | Maximum time buffer waits before flushing to database. |
| `POLL_INTERVAL_SEC`| Worker | `15` | Interval in seconds between worker analysis cycles. |
| `LLM_PROVIDER` | Worker | `fallback` | RCA Provider: `fallback` (deterministic, 0 keys), `gemini`, `openai`, or `ollama`. |
| `LLM_API_KEY` | Worker | `""` | API key for Gemini or OpenAI (leave empty for deterministic fallback). |
| `LLM_MODEL` | Worker | `gpt-4o-mini` | LLM model name (e.g. `gemini-2.0-flash`, `gpt-4o-mini`, `llama3.2`). |
| `OLLAMA_URL` | Worker | `http://host.docker.internal:11434` | Endpoint when using local Ollama models. |

---

## Simulated Microservice Scenarios

When running the simulator (`npm run simulate` or clicking **"Run Anomaly Simulation"** on the dashboard), traffic is injected across 6 realistic services:

1. **`payment-gateway` (`/api/v1/payments/charge`)**: Downstream banking partner SLA timeout breach $\rightarrow$ Latency spikes to 5,000ms + 504 timeouts $\rightarrow$ Diagnosed with circuit breaker & socket timeout recommendations.
2. **`order-service` (`/api/v1/orders/checkout`)**: Missing index sequential scan on `orders` table filtering 1.94M rows $\rightarrow$ Diagnosed with exact `CREATE INDEX CONCURRENTLY` DDL fix.
3. **`inventory-service` (`/api/v1/inventory/sync`)**: Unbounded memory buffering climbing +36MB/req up to 980MB $\rightarrow$ Worker `OOMKilled` (exit 137) $\rightarrow$ Diagnosed with streaming cursor recommendations.
4. **`auth-service` (`/api/v1/oauth/token`)**: High-concurrency request surge exhausting Redis connection pool $\rightarrow$ Diagnosed with connection pool tuning & token-bucket rate limiting.
5. **`catalog-service` & `user-service`**: Healthy baseline traffic demonstrating 0 false-positive alerts.
