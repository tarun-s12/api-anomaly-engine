from __future__ import annotations

import json
import os
from typing import Literal, Optional
import requests
from pydantic import BaseModel, Field

from detector import DetectedAnomaly


class IncidentDiagnosis(BaseModel):
    root_cause: str = Field(description="Concise technical root cause explanation")
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    confidence: float = Field(ge=0.0, le=1.0)
    affected_component: str
    evidence: list[str]
    suggested_fix: str
    sql_optimization: Optional[str] = None


def _validate_diagnosis(raw: dict) -> IncidentDiagnosis:
    if hasattr(IncidentDiagnosis, "model_validate"):
        return IncidentDiagnosis.model_validate(raw)
    return IncidentDiagnosis.parse_obj(raw)


def _build_prompt(anomaly: DetectedAnomaly) -> str:
    payload = {
        "service": anomaly.service,
        "endpoint": anomaly.endpoint,
        "anomaly_type": anomaly.anomaly_type,
        "metrics_summary": anomaly.metrics_summary,
        "z_score": anomaly.z_score,
        "surrounding_logs_sample": anomaly.surrounding_logs[:8],
        "slow_query_plans": anomaly.slow_query_plans,
    }
    return (
        "You are a Staff Site Reliability Engineer. Analyze the following API telemetry anomaly, "
        "surrounding log window, and PostgreSQL EXPLAIN ANALYZE query plans. "
        "Return ONLY valid JSON matching this schema:\n"
        '{"root_cause": str, "severity": "LOW"|"MEDIUM"|"HIGH"|"CRITICAL", '
        '"confidence": float, "affected_component": str, "evidence": [str], '
        '"suggested_fix": str, "sql_optimization": str|null}\n\n'
        f"Telemetry:\n{json.dumps(payload, indent=2)}"
    )


def _call_openai(prompt: str, api_key: str, model: str) -> dict:
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": "Output strict JSON only."},
                {"role": "user", "content": prompt},
            ],
        },
        timeout=20,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def _call_gemini(prompt: str, api_key: str, model: str) -> dict:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    resp = requests.post(
        url,
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "temperature": 0.1,
            },
        },
        timeout=20,
    )
    resp.raise_for_status()
    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    return json.loads(text)


def _call_ollama(prompt: str, base_url: str, model: str) -> dict:
    resp = requests.post(
        f"{base_url.rstrip('/')}/api/chat",
        json={
            "model": model,
            "format": "json",
            "stream": False,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )
    resp.raise_for_status()
    return json.loads(resp.json()["message"]["content"])


def _deterministic_fallback(anomaly: DetectedAnomaly) -> IncidentDiagnosis:
    """
    Rule-based root-cause synthesis used when LLM_PROVIDER=fallback or API is unreachable.
    """
    plans = anomaly.slow_query_plans
    errors = [
        log["error_message"]
        for log in anomaly.surrounding_logs
        if log.get("error_message")
    ]

    if plans:
        plan = plans[0]
        node_type = plan.get("Node Type", "Unknown Scan")
        relation = plan.get("Relation Name", "target_table")
        filt = plan.get("Filter", "")
        removed = plan.get("Rows Removed by Filter", 0)

        import re
        cols = re.findall(r"(\w+)\s*=", filt)
        col_str = ", ".join(cols[:2]) if cols else "account_id, status"

        return IncidentDiagnosis(
            root_cause=(
                f"Unindexed {node_type} on '{relation}' scanning and discarding "
                f"{removed:,} rows under filter {filt}, driving P95 latency to "
                f"{anomaly.p95_latency_ms:.1f}ms."
            ),
            severity=anomaly.severity,  # type: ignore[arg-type]
            confidence=0.94,
            affected_component=f"PostgreSQL / {relation} ({anomaly.endpoint})",
            evidence=[
                f"P95 latency spiked from {anomaly.metrics_summary.get('baseline_p95_ms', 110):.1f}ms to {anomaly.p95_latency_ms:.1f}ms (Z={anomaly.z_score:.2f})",
                f"EXPLAIN ANALYZE shows {node_type} on {relation} with {removed:,} rows filtered",
                *(errors[:2] if errors else []),
            ],
            suggested_fix=(
                f"Add a composite B-tree index on '{relation}' covering ({col_str}) "
                "and enforce a statement_timeout with keyset pagination."
            ),
            sql_optimization=(
                f"CREATE INDEX CONCURRENTLY idx_{relation}_{cols[0] if cols else 'lookup'} ON {relation} ({col_str});"
            ),
        )

    # Downstream third-party timeout
    if any("UpstreamTimeout" in e or "connector" in e or "gateway" in e.lower() for e in errors) or anomaly.p95_latency_ms > 3500:
        err_sample = errors[0] if errors else "Upstream dependency exceeded SLA threshold (4,000ms)"
        return IncidentDiagnosis(
            root_cause=(
                f"Downstream partner integration latency escalation on {anomaly.service}{anomaly.endpoint}. "
                f"Upstream responses breached 4,000ms timeout threshold causing cascading HTTP 504 Gateway Timeouts."
            ),
            severity="CRITICAL",
            confidence=0.91,
            affected_component=f"External Gateway Connector / {anomaly.service}",
            evidence=[
                f"P95 latency breached SLA at {anomaly.p95_latency_ms:.1f}ms (Baseline: {anomaly.metrics_summary.get('baseline_p95_ms', 80):.1f}ms)",
                f"Error rate surged to {anomaly.metrics_summary.get('error_rate_pct', 0):.1f}%",
                err_sample,
            ],
            suggested_fix=(
                "Wrap external HTTP client calls with an asynchronous Circuit Breaker (e.g. Opossum/Resilience4j), "
                "reduce client-side socket timeout to 2,500ms, and configure a fallback response queue."
            ),
            sql_optimization=None,
        )

    # Connection pool or session store saturation
    if any("ConnectionPool" in e or "Redis" in e or "RateLimit" in e for e in errors):
        err_sample = errors[0] if errors else "Connection pool saturation detected"
        return IncidentDiagnosis(
            root_cause=(
                f"High-frequency request surge exhausting connection pool on {anomaly.service}{anomaly.endpoint}. "
                f"Downstream state store thread pool starvation leading to HTTP 503 / 429 rejections."
            ),
            severity="HIGH",
            confidence=0.88,
            affected_component=f"Redis Session Pool / {anomaly.service}",
            evidence=[
                f"Error burst reached {anomaly.metrics_summary.get('error_rate_pct', 0):.1f}% over evaluation window",
                f"Z-score anomaly magnitude: +{anomaly.z_score:.2f} \u03c3",
                err_sample,
            ],
            suggested_fix=(
                "Increase connection pool maximum idle capacity, implement adaptive token-bucket rate limiting at ingress, "
                "and deploy Redis read replicas for token validation queries."
            ),
            sql_optimization=None,
        )

    # Memory leak
    if anomaly.anomaly_type == "MEMORY_LEAK" or any("OOMKilled" in e for e in errors):
        slope = anomaly.metrics_summary.get("memory_slope_mb_per_req", 0.0)
        peak = anomaly.metrics_summary.get("peak_memory_mb", 0.0)
        return IncidentDiagnosis(
            root_cause=(
                f"Sustained RSS memory leak (+{slope:.1f} MB/request, peak {peak:.1f} MB) on "
                f"{anomaly.endpoint} triggering worker OOMKilled (exit 137) and 502 Bad Gateway responses."
            ),
            severity=anomaly.severity,  # type: ignore[arg-type]
            confidence=0.89,
            affected_component=f"{anomaly.service} worker heap ({anomaly.endpoint})",
            evidence=[
                f"Memory slope: +{slope:.1f} MB/req reaching {peak:.1f} MB",
                f"Error rate surged to {anomaly.metrics_summary.get('error_rate_pct', 0):.1f}%",
                *(errors[:2] if errors else []),
            ],
            suggested_fix=(
                "Replace unbounded in-memory collection buffering with streaming paginated cursors, "
                "audit closure cache retention, and configure container memory limits with graceful worker recycling."
            ),
            sql_optimization=None,
        )

    return IncidentDiagnosis(
        root_cause=(
            f"{anomaly.anomaly_type} detected on {anomaly.service}{anomaly.endpoint} "
            f"(error_rate={anomaly.metrics_summary['error_rate_pct']}%, P95={anomaly.p95_latency_ms}ms)."
        ),
        severity=anomaly.severity,  # type: ignore[arg-type]
        confidence=0.78,
        affected_component=f"{anomaly.service} ({anomaly.endpoint})",
        evidence=[
            f"Z-score={anomaly.z_score}, Isolation score={anomaly.isolation_score}",
            *(errors[:2] if errors else []),
        ],
        suggested_fix="Inspect downstream dependency timeouts, enable circuit breaking, and check connection pool saturation.",
        sql_optimization=None,
    )


def diagnose_anomaly(anomaly: DetectedAnomaly) -> IncidentDiagnosis:
    provider = os.getenv("LLM_PROVIDER", "fallback").lower()
    api_key = os.getenv("LLM_API_KEY", "")
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")
    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")

    if provider == "fallback" or (provider in ("openai", "gemini") and not api_key):
        return _deterministic_fallback(anomaly)

    prompt = _build_prompt(anomaly)
    try:
        if provider == "openai":
            raw = _call_openai(prompt, api_key, model)
        elif provider == "gemini":
            raw = _call_gemini(prompt, api_key, model or "gemini-2.0-flash")
        elif provider == "ollama":
            raw = _call_ollama(prompt, ollama_url, model or "llama3.1")
        else:
            return _deterministic_fallback(anomaly)
        return _validate_diagnosis(raw)
    except Exception as exc:
        print(f"[AIAgent] LLM call failed ({exc}); using deterministic fallback.")
        return _deterministic_fallback(anomaly)
