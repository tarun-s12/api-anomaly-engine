from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time
import pandas as pd
from dotenv import load_dotenv

from ai_agent import diagnose_anomaly
from detector import DetectedAnomaly, detect_anomalies

load_dotenv()

STORAGE_MODE = os.getenv("STORAGE_MODE", "postgres")
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgres://postgres:postgres@localhost:5432/observability"
)
LOCAL_DATA_DIR = Path(
    os.getenv("LOCAL_DATA_DIR", str(Path(__file__).resolve().parent.parent / "data"))
)
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "15"))


def load_recent_logs() -> pd.DataFrame:
    if STORAGE_MODE == "jsonl":
        logs_path = LOCAL_DATA_DIR / "api_logs.jsonl"
        if not logs_path.exists():
            return pd.DataFrame()
        return pd.read_json(logs_path, lines=True)

    import psycopg2

    with psycopg2.connect(DATABASE_URL) as conn:
        query = """
            SELECT timestamp, service, endpoint, method, status_code,
                   latency_ms, memory_mb, trace_id, error_message, query_plan
            FROM api_logs
            WHERE timestamp >= NOW() - INTERVAL '15 minutes'
            ORDER BY timestamp ASC
        """
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)


def save_incident(anomaly: DetectedAnomaly, diagnosis_dict: dict) -> None:
    record = {
        "detected_at": pd.Timestamp.now("UTC").isoformat(),
        "window_start": anomaly.window_start,
        "window_end": anomaly.window_end,
        "service": anomaly.service,
        "endpoint": anomaly.endpoint,
        "anomaly_type": anomaly.anomaly_type,
        "severity": anomaly.severity,
        "z_score": anomaly.z_score,
        "isolation_score": anomaly.isolation_score,
        "p95_latency_ms": anomaly.p95_latency_ms,
        "error_rate": anomaly.error_rate,
        "metrics_summary": anomaly.metrics_summary,
        "llm_diagnosis": diagnosis_dict,
        "status": "OPEN",
    }

    if STORAGE_MODE == "jsonl":
        LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        incidents_path = LOCAL_DATA_DIR / "anomaly_incidents.jsonl"
        with incidents_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        return

    import psycopg2

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO anomaly_incidents (
                    window_start, window_end, service, endpoint,
                    anomaly_type, severity, z_score, isolation_score,
                    p95_latency_ms, error_rate, metrics_summary, llm_diagnosis
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    anomaly.window_start,
                    anomaly.window_end,
                    anomaly.service,
                    anomaly.endpoint,
                    anomaly.anomaly_type,
                    anomaly.severity,
                    anomaly.z_score,
                    anomaly.isolation_score,
                    anomaly.p95_latency_ms,
                    anomaly.error_rate,
                    json.dumps(anomaly.metrics_summary),
                    json.dumps(diagnosis_dict),
                ),
            )
        conn.commit()


def run_once() -> int:
    df = load_recent_logs()
    if df.empty:
        print("[Worker] No logs available yet.")
        return 0

    anomalies = detect_anomalies(df)
    if not anomalies:
        print(f"[Worker] Analyzed {len(df)} logs — all endpoints healthy.")
        return 0

    print(f"[Worker] Detected {len(anomalies)} anomalies across {len(df)} logs:")
    for anomaly in anomalies:
        diagnosis = diagnose_anomaly(anomaly)
        diag_dict = (
            diagnosis.model_dump()
            if hasattr(diagnosis, "model_dump")
            else diagnosis.dict()
        )
        save_incident(anomaly, diag_dict)
        print(
            f"  -> [{diagnosis.severity}] {anomaly.service}{anomaly.endpoint} "
            f"({anomaly.anomaly_type}): {diagnosis.root_cause}"
        )
    return len(anomalies)


def main() -> None:
    parser = argparse.ArgumentParser(description="API Anomaly Detection & AI RCA Worker")
    parser.add_argument("--once", action="store_true", help="Run a single analysis pass")
    parser.add_argument("--watch", action="store_true", help="Poll continuously")
    args = parser.parse_args()

    if args.watch and not args.once:
        print(f"[Worker] Starting continuous polling every {POLL_INTERVAL_SEC}s...")
        while True:
            try:
                run_once()
            except Exception as exc:
                print(f"[Worker] Error during cycle: {exc}")
            time.sleep(POLL_INTERVAL_SEC)
    else:
        run_once()


if __name__ == "__main__":
    main()
