import { Request, Response, Router } from "express";
import { LogBatcher } from "../batcher";
import { config } from "../config";
import { StorageAdapter } from "../db";
import { generateSimulationLogs } from "../simulator";
import { IngestPayloadSchema } from "../types";

export function createRouter(
  batcher: LogBatcher,
  storage: StorageAdapter
): Router {
  const router = Router();

  router.post("/logs", (req: Request, res: Response) => {
    const parsed = IngestPayloadSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({
        error: "Invalid log payload",
        details: parsed.error.flatten(),
      });
      return;
    }

    const entries = Array.isArray(parsed.data) ? parsed.data : [parsed.data];
    batcher.enqueue(entries);

    res.status(202).json({
      accepted: entries.length,
      buffer_size: batcher.getMetrics().bufferSize,
    });
  });

  router.post("/simulate", (_req: Request, res: Response) => {
    try {
      const logs = generateSimulationLogs();
      batcher.enqueue(logs);
      res.status(202).json({
        success: true,
        accepted: logs.length,
        message: `Injected ${logs.length} baseline and anomalous telemetry logs into the batcher.`,
        buffer_size: batcher.getMetrics().bufferSize,
      });
    } catch (err) {
      console.error("[POST /simulate] Error:", err);
      res.status(500).json({ error: "Failed to generate simulation logs" });
    }
  });

  router.post("/clear", async (_req: Request, res: Response) => {
    try {
      await storage.clearAll();
      batcher.resetMetrics();
      res.json({
        success: true,
        message: "Database telemetry and incident records cleared successfully.",
      });
    } catch (err) {
      console.error("[POST /clear] Error:", err);
      res.status(500).json({ error: "Failed to clear database" });
    }
  });

  router.get("/health", async (_req: Request, res: Response) => {
    const dbHealthy = await storage.healthCheck();
    res.status(dbHealthy ? 200 : 503).json({
      status: dbHealthy ? "ok" : "degraded",
      storage_mode: config.STORAGE_MODE,
      timestamp: new Date().toISOString(),
    });
  });

  router.get("/metrics", (_req: Request, res: Response) => {
    res.json({
      storage_mode: config.STORAGE_MODE,
      batch_size: config.BATCH_SIZE,
      flush_interval_ms: config.FLUSH_INTERVAL_MS,
      ...batcher.getMetrics(),
    });
  });

  router.get("/incidents", async (req: Request, res: Response) => {
    try {
      const limit = Math.min(Number(req.query.limit) || 50, 200);
      const incidents = await storage.getIncidents(limit);
      res.json({ count: incidents.length, incidents });
    } catch (err) {
      console.error("[GET /incidents] Error:", err);
      res.status(500).json({ error: "Failed to fetch incidents" });
    }
  });

  router.patch("/incidents/:id", async (req: Request, res: Response) => {
    try {
      const id = Number(req.params.id);
      const { status } = req.body;
      const validStatuses = ["OPEN", "ACKNOWLEDGED", "RESOLVED"];
      if (!validStatuses.includes(status)) {
        res.status(400).json({
          error: `Invalid status '${status}'. Must be one of: ${validStatuses.join(", ")}`,
        });
        return;
      }
      const updated = await storage.updateIncidentStatus(id, status);
      if (!updated) {
        res.status(404).json({ error: `Incident #${id} not found` });
        return;
      }
      res.json({ success: true, id, status });
    } catch (err) {
      console.error("[PATCH /incidents/:id] Error:", err);
      res.status(500).json({ error: "Failed to update incident status" });
    }
  });

  return router;
}
