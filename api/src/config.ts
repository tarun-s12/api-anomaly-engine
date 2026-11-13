import dotenv from "dotenv";
import path from "path";
import { z } from "zod";

dotenv.config();

const ConfigSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  STORAGE_MODE: z.enum(["postgres", "jsonl"]).default("postgres"),
  DATABASE_URL: z
    .string()
    .default("postgres://postgres:postgres@localhost:5432/observability"),
  BATCH_SIZE: z.coerce.number().int().min(1).max(5000).default(100),
  FLUSH_INTERVAL_MS: z.coerce.number().int().min(100).max(60000).default(2000),
  LOCAL_DATA_DIR: z.string().default(path.resolve(__dirname, "../../data")),
});

const parsed = ConfigSchema.safeParse(process.env);
if (!parsed.success) {
  console.error("Invalid environment configuration:", parsed.error.format());
  process.exit(1);
}

export const config = parsed.data;
