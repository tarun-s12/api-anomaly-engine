import fs from "fs/promises";
import path from "path";
import { Pool } from "pg";
import { config } from "./config";
import { LogEntry } from "./types";

export interface StorageAdapter {
  init(): Promise<void>;
  insertBatch(logs: LogEntry[]): Promise<void>;
  getIncidents(limit?: number): Promise<unknown[]>;
  updateIncidentStatus(id: number, status: string): Promise<boolean>;
  clearAll(): Promise<void>;
  healthCheck(): Promise<boolean>;
  close(): Promise<void>;
}

class PostgresStorage implements StorageAdapter {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });
  }

  async init(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async insertBatch(logs: LogEntry[]): Promise<void> {
    if (logs.length === 0) return;

    const timestamps: string[] = [];
    const services: string[] = [];
    const endpoints: string[] = [];
    const methods: string[] = [];
    const statusCodes: number[] = [];
    const latencies: number[] = [];
    const memories: number[] = [];
    const traceIds: (string | null)[] = [];
    const errorMessages: (string | null)[] = [];
    const queryPlans: (string | null)[] = [];

    for (const log of logs) {
      timestamps.push(log.timestamp);
      services.push(log.service);
      endpoints.push(log.endpoint);
      methods.push(log.method);
      statusCodes.push(log.status_code);
      latencies.push(log.latency_ms);
      memories.push(log.memory_mb);
      traceIds.push(log.trace_id ?? null);
      errorMessages.push(log.error_message ?? null);
      queryPlans.push(log.query_plan ? JSON.stringify(log.query_plan) : null);
    }

    const sql = `
      INSERT INTO api_logs (
        timestamp, service, endpoint, method, status_code,
        latency_ms, memory_mb, trace_id, error_message, query_plan
      )
      SELECT * FROM UNNEST(
        $1::timestamptz[],
        $2::text[],
        $3::text[],
        $4::text[],
        $5::int4[],
        $6::float8[],
        $7::float8[],
        $8::text[],
        $9::text[],
        $10::jsonb[]
      )
    `;

    await this.pool.query(sql, [
      timestamps,
      services,
      endpoints,
      methods,
      statusCodes,
      latencies,
      memories,
      traceIds,
      errorMessages,
      queryPlans,
    ]);
  }

  async getIncidents(limit = 50): Promise<unknown[]> {
    const { rows } = await this.pool.query(
      `SELECT * FROM anomaly_incidents ORDER BY detected_at DESC LIMIT $1`,
      [limit]
    );
    return rows;
  }

  async updateIncidentStatus(id: number, status: string): Promise<boolean> {
    const res = await this.pool.query(
      `UPDATE anomaly_incidents SET status = $1 WHERE id = $2`,
      [status, id]
    );
    return (res.rowCount ?? 0) > 0;
  }

  async clearAll(): Promise<void> {
    await this.pool.query("TRUNCATE api_logs, anomaly_incidents RESTART IDENTITY");
  }

  async healthCheck(): Promise<boolean> {
    try {
      await this.pool.query("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

class JsonlStorage implements StorageAdapter {
  private logsFile: string;
  private incidentsFile: string;

  constructor(private dataDir: string) {
    this.logsFile = path.join(dataDir, "api_logs.jsonl");
    this.incidentsFile = path.join(dataDir, "anomaly_incidents.jsonl");
  }

  async init(): Promise<void> {
    await fs.mkdir(this.dataDir, { recursive: true });
  }

  async insertBatch(logs: LogEntry[]): Promise<void> {
    if (logs.length === 0) return;
    const lines = logs.map((l) => JSON.stringify(l)).join("\n") + "\n";
    await fs.appendFile(this.logsFile, lines, "utf8");
  }

  async getIncidents(limit = 50): Promise<unknown[]> {
    try {
      const raw = await fs.readFile(this.incidentsFile, "utf8");
      const items = raw
        .split("\n")
        .filter((line) => line.trim().length > 0)
        .map((line) => JSON.parse(line));
      return items.reverse().slice(0, limit);
    } catch {
      return [];
    }
  }

  async updateIncidentStatus(id: number, status: string): Promise<boolean> {
    try {
      const raw = await fs.readFile(this.incidentsFile, "utf8");
      const lines = raw.split("\n").filter((l) => l.trim().length > 0);
      let found = false;
      const updated = lines.map((l, idx) => {
        const item = JSON.parse(l);
        if (item.id === id || idx + 1 === id) {
          item.status = status;
          found = true;
        }
        return JSON.stringify(item);
      });
      await fs.writeFile(this.incidentsFile, updated.join("\n") + "\n", "utf8");
      return found;
    } catch {
      return false;
    }
  }

  async clearAll(): Promise<void> {
    try {
      await fs.writeFile(this.logsFile, "");
      await fs.writeFile(this.incidentsFile, "");
    } catch {}
  }

  async healthCheck(): Promise<boolean> {
    return true;
  }

  async close(): Promise<void> {}
}

export const storage: StorageAdapter =
  config.STORAGE_MODE === "postgres"
    ? new PostgresStorage(config.DATABASE_URL)
    : new JsonlStorage(config.LOCAL_DATA_DIR);
