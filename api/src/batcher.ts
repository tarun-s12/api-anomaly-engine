import { StorageAdapter } from "./db";
import { BatcherMetrics, LogEntry } from "./types";

export class LogBatcher {
  private buffer: LogEntry[] = [];
  private timer: NodeJS.Timeout | null = null;
  private isFlushing = false;

  private metrics: Omit<BatcherMetrics, "bufferSize"> = {
    totalReceived: 0,
    totalFlushed: 0,
    flushCount: 0,
    flushErrors: 0,
    lastFlushAt: null,
  };

  constructor(
    private readonly storage: StorageAdapter,
    private readonly batchSize: number,
    private readonly flushIntervalMs: number
  ) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.flush();
    }, this.flushIntervalMs);
    this.timer.unref();
  }

  enqueue(entries: LogEntry[]): void {
    this.buffer.push(...entries);
    this.metrics.totalReceived += entries.length;

    if (this.buffer.length >= this.batchSize) {
      void this.flush();
    }
  }

  async flush(): Promise<void> {
    if (this.isFlushing || this.buffer.length === 0) return;

    this.isFlushing = true;
    const batch = this.buffer.splice(0, this.batchSize);

    try {
      await this.storage.insertBatch(batch);
      this.metrics.totalFlushed += batch.length;
      this.metrics.flushCount += 1;
      this.metrics.lastFlushAt = new Date().toISOString();
    } catch (err) {
      this.metrics.flushErrors += 1;
      // Re-queue failed batch at front to avoid silent data loss (capped at 10k)
      if (this.buffer.length + batch.length <= 10000) {
        this.buffer.unshift(...batch);
      }
      console.error("[LogBatcher] Failed to flush batch:", err);
    } finally {
      this.isFlushing = false;
      if (this.buffer.length >= this.batchSize) {
        void this.flush();
      }
    }
  }

  async shutdown(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    while (this.buffer.length > 0) {
      await this.flush();
      if (this.metrics.flushErrors > 0 && this.buffer.length > 0) {
        break;
      }
    }
  }

  resetMetrics(): void {
    this.buffer = [];
    this.metrics = {
      totalReceived: 0,
      totalFlushed: 0,
      flushCount: 0,
      flushErrors: 0,
      lastFlushAt: null,
    };
  }

  getMetrics(): BatcherMetrics {
    return {
      ...this.metrics,
      bufferSize: this.buffer.length,
    };
  }
}
