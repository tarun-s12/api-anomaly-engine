import crypto from "crypto";
import { LogEntry } from "./types";

const API_URL = process.env.API_URL ?? "http://localhost:3000/api/v1/logs";

function randBetween(min: number, max: number): number {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

function buildSlowQueryPlan(): Record<string, unknown> {
  return {
    "Node Type": "Seq Scan",
    "Relation Name": "orders",
    Alias: "o",
    "Startup Cost": 0.0,
    "Total Cost": 142890.0,
    "Plan Rows": 1940000,
    "Actual Startup Time": 14.2,
    "Actual Total Time": 1920.4,
    "Actual Rows": 38,
    Filter: "(customer_id = 'cust_9821' AND status = 'PENDING')",
    "Rows Removed by Filter": 1939962,
    "Shared Hit Blocks": 380,
    "Shared Read Blocks": 91200,
  };
}

export function generateSimulationLogs(): LogEntry[] {
  const now = Date.now();
  const logs: LogEntry[] = [];

  // Baseline healthy traffic across diverse microservices (180 logs)
  const baselineServices = [
    { service: "order-service", endpoint: "/api/v1/orders/checkout", method: "POST" as const, minLat: 35, maxLat: 75 },
    { service: "payment-gateway", endpoint: "/api/v1/payments/charge", method: "POST" as const, minLat: 40, maxLat: 95 },
    { service: "catalog-service", endpoint: "/api/v1/products/search", method: "GET" as const, minLat: 18, maxLat: 50 },
    { service: "auth-service", endpoint: "/api/v1/oauth/token", method: "POST" as const, minLat: 15, maxLat: 40 },
    { service: "inventory-service", endpoint: "/api/v1/inventory/sync", method: "POST" as const, minLat: 25, maxLat: 55 },
    { service: "user-service", endpoint: "/api/v1/users/profile", method: "GET" as const, minLat: 12, maxLat: 38 },
  ];

  for (let i = 0; i < 180; i++) {
    const target = baselineServices[i % baselineServices.length];
    const ts = new Date(now - (180 - i) * 1000).toISOString();
    logs.push({
      timestamp: ts,
      service: target.service,
      endpoint: target.endpoint,
      method: target.method,
      status_code: Math.random() < 0.005 ? 500 : 200,
      latency_ms: randBetween(target.minLat, target.maxLat),
      memory_mb: randBetween(180, 220),
      trace_id: `tr_${crypto.randomBytes(6).toString("hex")}`,
      error_message: null,
      query_plan: null,
    });
  }

  // Incident A: order-service unindexed SQL Seq Scan (spiked P95 latency + 504 timeouts)
  for (let i = 0; i < 25; i++) {
    const ts = new Date(now - (45 - i) * 1000).toISOString();
    logs.push({
      timestamp: ts,
      service: "order-service",
      endpoint: "/api/v1/orders/checkout",
      method: "POST",
      status_code: i % 4 === 0 ? 504 : 200,
      latency_ms: randBetween(1850, 2900),
      memory_mb: randBetween(230, 260),
      trace_id: `tr_${crypto.randomBytes(6).toString("hex")}`,
      error_message: i % 4 === 0 ? "QueryTimeoutError: statement timeout after 2500ms" : null,
      query_plan: buildSlowQueryPlan(),
    });
  }

  // Incident B: payment-gateway 3rd-party banking partner timeout (latency escalation to 5,000ms)
  for (let i = 0; i < 25; i++) {
    const ts = new Date(now - (35 - i) * 1000).toISOString();
    logs.push({
      timestamp: ts,
      service: "payment-gateway",
      endpoint: "/api/v1/payments/charge",
      method: "POST",
      status_code: i % 3 === 0 ? 504 : 200,
      latency_ms: randBetween(3900, 5200),
      memory_mb: randBetween(210, 240),
      trace_id: `tr_${crypto.randomBytes(6).toString("hex")}`,
      error_message: i % 3 === 0 ? "UpstreamTimeout: visa_direct_connector statement timeout after 4000ms" : null,
      query_plan: null,
    });
  }

  // Incident C: inventory-service memory leak (+35MB/req leading to OOMKilled)
  for (let i = 0; i < 20; i++) {
    const ts = new Date(now - (25 - i) * 1000).toISOString();
    logs.push({
      timestamp: ts,
      service: "inventory-service",
      endpoint: "/api/v1/inventory/sync",
      method: "POST",
      status_code: i % 2 === 0 ? 502 : 200,
      latency_ms: randBetween(340, 680),
      memory_mb: 290 + i * 36,
      trace_id: `tr_${crypto.randomBytes(6).toString("hex")}`,
      error_message: i % 2 === 0 ? "BadGateway: worker process OOMKilled (exit 137)" : null,
      query_plan: null,
    });
  }

  // Incident D: auth-service session connection pool exhaustion & error burst
  for (let i = 0; i < 25; i++) {
    const ts = new Date(now - (20 - i) * 1000).toISOString();
    logs.push({
      timestamp: ts,
      service: "auth-service",
      endpoint: "/api/v1/oauth/token",
      method: "POST",
      status_code: i % 2 === 0 ? 503 : 200,
      latency_ms: randBetween(95, 340),
      memory_mb: randBetween(160, 190),
      trace_id: `tr_${crypto.randomBytes(6).toString("hex")}`,
      error_message: i % 2 === 0 ? "ConnectionPoolExhausted: Redis cluster timeout on session token verification" : null,
      query_plan: null,
    });
  }

  return logs;
}

async function sendBatch(logs: LogEntry[], targetUrl: string): Promise<void> {
  const res = await fetch(targetUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(logs),
  });
  if (!res.ok) {
    throw new Error(`Ingest failed: ${res.status} ${await res.text()}`);
  }
}

export async function runSimulation(targetUrl = API_URL): Promise<{ count: number }> {
  console.log(`[Simulator] Generating diverse microservice telemetry to ${targetUrl}`);
  const logs = generateSimulationLogs();
  await sendBatch(logs, targetUrl);
  console.log(`[Simulator] Sent ${logs.length} telemetry events successfully across 6 microservices.`);
  return { count: logs.length };
}

if (require.main === module) {
  runSimulation().catch((err) => {
    console.error("[Simulator] Error:", err);
    process.exit(1);
  });
}
