import { z } from "zod";

export const LogEntrySchema = z.object({
  timestamp: z
    .string()
    .datetime({ offset: true })
    .optional()
    .transform((val) => val ?? new Date().toISOString()),
  service: z.string().min(1).max(120),
  endpoint: z.string().min(1).max(255),
  method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]).default("GET"),
  status_code: z.number().int().min(100).max(599),
  latency_ms: z.number().nonnegative(),
  memory_mb: z.number().nonnegative().default(0),
  trace_id: z.string().max(128).nullable().optional().default(null),
  error_message: z.string().max(2000).nullable().optional().default(null),
  query_plan: z.record(z.unknown()).nullable().optional().default(null),
});

export type LogEntry = z.infer<typeof LogEntrySchema>;

export const IngestPayloadSchema = z.union([
  LogEntrySchema,
  z.array(LogEntrySchema).min(1).max(5000),
]);

export interface BatcherMetrics {
  totalReceived: number;
  totalFlushed: number;
  flushCount: number;
  flushErrors: number;
  bufferSize: number;
  lastFlushAt: string | null;
}
