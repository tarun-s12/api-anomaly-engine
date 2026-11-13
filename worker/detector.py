from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import IsolationForest
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


@dataclass
class DetectedAnomaly:
    service: str
    endpoint: str
    anomaly_type: str
    severity: str
    z_score: float
    isolation_score: float
    p95_latency_ms: float
    error_rate: float
    window_start: str
    window_end: str
    metrics_summary: dict[str, Any]
    surrounding_logs: list[dict[str, Any]]
    slow_query_plans: list[dict[str, Any]]


def _compute_isolation_scores(features: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Runs Scikit-Learn IsolationForest on telemetry feature vectors.
    Falls back to robust MAD-based outlier scoring if scikit-learn is unavailable.
    Returns (predictions [-1 anomaly, 1 normal], decision_scores [lower = more anomalous]).
    """
    if HAS_SKLEARN and len(features) >= 10:
        model = IsolationForest(
            n_estimators=100,
            contamination=0.12,
            random_state=42,
        )
        preds = model.fit_predict(features)
        scores = model.decision_function(features)
        return preds, scores

    # Robust Median Absolute Deviation (MAD) fallback
    medians = np.median(features, axis=0)
    mad = np.median(np.abs(features - medians), axis=0)
    mad = np.where(mad == 0, 1.0, mad)
    mod_z = 0.6745 * np.abs(features - medians) / mad
    max_z = np.max(mod_z, axis=1)
    preds = np.where(max_z > 3.5, -1, 1)
    scores = 0.5 - (max_z / 10.0)
    return preds, scores


def detect_anomalies(df: pd.DataFrame) -> list[DetectedAnomaly]:
    """
    Analyzes API telemetry DataFrame per (service, endpoint) using:
    1. Rolling window Z-score on latency
    2. P95 latency & error rate thresholds
    3. Memory slope detection (memory leak creep)
    4. Scikit-Learn IsolationForest multivariate outlier detection
    """
    if df.empty or len(df) < 10:
        return []

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["is_error"] = (df["status_code"] >= 500).astype(float)
    df["memory_mb"] = pd.to_numeric(df.get("memory_mb", 0), errors="coerce").fillna(0.0)

    anomalies: list[DetectedAnomaly] = []

    for (service, endpoint), group in df.groupby(["service", "endpoint"]):
        if len(group) < 12:
            continue

        group = group.sort_values("timestamp").reset_index(drop=True)

        # Split into baseline (first 70%) and recent evaluation window (last 30%, min 5 rows)
        split_idx = max(5, int(len(group) * 0.7))
        baseline = group.iloc[:split_idx]
        recent = group.iloc[split_idx:]

        if len(recent) < 3:
            continue

        base_mean = float(baseline["latency_ms"].mean())
        base_std = float(baseline["latency_ms"].std(ddof=1)) or 1.0
        recent_mean = float(recent["latency_ms"].mean())
        recent_p50 = float(recent["latency_ms"].quantile(0.50))
        recent_p95 = float(recent["latency_ms"].quantile(0.95))
        recent_p99 = float(recent["latency_ms"].quantile(0.99))
        base_p95 = float(baseline["latency_ms"].quantile(0.95))

        z_score = (recent_mean - base_mean) / max(base_std, 5.0)
        error_rate = float(recent["is_error"].mean())

        # Memory leak slope across recent window
        mem_values = recent["memory_mb"].to_numpy()
        mem_slope = 0.0
        if len(mem_values) >= 4 and np.ptp(mem_values) > 0:
            x = np.arange(len(mem_values), dtype=float)
            mem_slope = float(np.polyfit(x, mem_values, 1)[0])

        # Multivariate Isolation Forest across the full endpoint series
        feature_matrix = group[["latency_ms", "is_error", "memory_mb"]].to_numpy(dtype=float)
        preds, iso_scores = _compute_isolation_scores(feature_matrix)
        recent_preds = preds[split_idx:]
        recent_iso_scores = iso_scores[split_idx:]
        outlier_ratio = float(np.mean(recent_preds == -1))
        min_iso_score = float(np.min(recent_iso_scores))

        # Determine anomaly classification
        anomaly_type: str | None = None
        if mem_slope >= 8.0 and float(mem_values[-1]) > 450.0:
            anomaly_type = "MEMORY_LEAK"
        elif error_rate >= 0.15:
            anomaly_type = "ERROR_BURST"
        elif z_score >= 2.5 or (recent_p95 > 500.0 and recent_p95 > base_p95 * 2.5):
            anomaly_type = "LATENCY_SPIKE"
        elif outlier_ratio >= 0.35:
            anomaly_type = "MULTIVARIATE_OUTLIER"

        if anomaly_type is None:
            continue

        # Severity scoring
        if error_rate >= 0.30 or recent_p95 >= 1500.0 or mem_slope >= 20.0:
            severity = "CRITICAL"
        elif error_rate >= 0.15 or z_score >= 4.0 or recent_p95 >= 800.0:
            severity = "HIGH"
        else:
            severity = "MEDIUM"

        # Extract surrounding logs (up to 15 most relevant rows from recent window)
        anomalous_rows = recent[
            (recent["status_code"] >= 500)
            | (recent["latency_ms"] >= recent_p95 * 0.7)
            | (recent["memory_mb"] >= float(np.percentile(mem_values, 75)))
        ].tail(15)
        if anomalous_rows.empty:
            anomalous_rows = recent.tail(10)

        surrounding_logs = []
        slow_query_plans = []
        for _, row in anomalous_rows.iterrows():
            surrounding_logs.append(
                {
                    "timestamp": row["timestamp"].isoformat(),
                    "method": row["method"],
                    "status_code": int(row["status_code"]),
                    "latency_ms": round(float(row["latency_ms"]), 2),
                    "memory_mb": round(float(row["memory_mb"]), 2),
                    "trace_id": row.get("trace_id"),
                    "error_message": row.get("error_message") if pd.notna(row.get("error_message")) else None,
                }
            )
            qp = row.get("query_plan")
            if isinstance(qp, dict) and qp not in slow_query_plans:
                slow_query_plans.append(qp)

        metrics_summary = {
            "sample_count_total": int(len(group)),
            "sample_count_recent": int(len(recent)),
            "baseline_p95_ms": round(base_p95, 2),
            "recent_p50_ms": round(recent_p50, 2),
            "recent_p95_ms": round(recent_p95, 2),
            "recent_p99_ms": round(recent_p99, 2),
            "error_rate_pct": round(error_rate * 100, 2),
            "memory_slope_mb_per_req": round(mem_slope, 2),
            "peak_memory_mb": round(float(np.max(mem_values)), 2),
            "isolation_outlier_ratio": round(outlier_ratio, 3),
            "detector_engine": "IsolationForest" if HAS_SKLEARN else "MAD-Fallback",
        }

        anomalies.append(
            DetectedAnomaly(
                service=str(service),
                endpoint=str(endpoint),
                anomaly_type=anomaly_type,
                severity=severity,
                z_score=round(z_score, 3),
                isolation_score=round(min_iso_score, 4),
                p95_latency_ms=round(recent_p95, 2),
                error_rate=round(error_rate, 4),
                window_start=recent["timestamp"].min().isoformat(),
                window_end=recent["timestamp"].max().isoformat(),
                metrics_summary=metrics_summary,
                surrounding_logs=surrounding_logs,
                slow_query_plans=slow_query_plans[:3],
            )
        )

    return anomalies
