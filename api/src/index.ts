import cors from "cors";
import express, { NextFunction, Request, Response } from "express";
import helmet from "helmet";
import path from "path";
import { LogBatcher } from "./batcher";
import { config } from "./config";
import { storage } from "./db";
import { createRouter } from "./routes/ingest";

async function bootstrap(): Promise<void> {
  await storage.init();

  const batcher = new LogBatcher(
    storage,
    config.BATCH_SIZE,
    config.FLUSH_INTERVAL_MS
  );
  batcher.start();

  const app = express();
  app.use(
    helmet({
      contentSecurityPolicy: false,
    })
  );
  app.use(cors());
  app.use(express.json({ limit: "5mb" }));

  const publicDir = path.join(__dirname, "../public");
  app.use(express.static(publicDir));
  app.use("/api/v1", createRouter(batcher, storage));

  app.get("/", (_req: Request, res: Response) => {
    res.sendFile(path.join(publicDir, "index.html"));
  });

  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error("[UnhandledError]", err);
    res.status(500).json({ error: "Internal server error" });
  });

  const server = app.listen(config.PORT, () => {
    console.log(
      `[IngestionAPI] Listening on port ${config.PORT} (mode=${config.STORAGE_MODE})`
    );
    console.log(`[Dashboard] Available at http://localhost:${config.PORT}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`[IngestionAPI] Received ${signal}, flushing remaining logs...`);
    server.close();
    await batcher.shutdown();
    await storage.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

bootstrap().catch((err) => {
  console.error("[IngestionAPI] Fatal startup error:", err);
  process.exit(1);
});
