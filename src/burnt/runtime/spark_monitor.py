"""Spark monitoring REST API client for runtime metric enrichment.

Attaches to the active SparkSession by resolving its monitoring REST endpoint,
then fetches per-stage metrics at check() time. No JAR install, no JVM listener,
works on every Spark deployment including UC shared clusters and Serverless.

Endpoint resolution priority:
1. Databricks: driver-proxy-api via dbruntime context (Connect-safe)
2. Generic Spark: spark.sparkContext.uiWebUrl (local, EMR, Dataproc, Glue)
3. Not available: SessionState(active=False), check() runs static-only
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import warnings
from typing import Any


class SessionState:
    """Holds REST session configuration and collected stage metrics."""

    def __init__(self) -> None:
        self.active: bool = False
        self._rest_url: str | None = None
        self._app_id: str | None = None
        self._auth_header: str | None = None
        self.collected: list[dict[str, Any]] = []

    def __repr__(self) -> str:
        return (
            f"SessionState(active={self.active}, "
            f"app_id={self._app_id!r}, "
            f"stages={len(self.collected)})"
        )


def start() -> SessionState:
    """Resolve the Spark monitoring REST endpoint and return a SessionState.

    Returns SessionState(active=False) silently if no Spark session is found
    or the REST endpoint is unreachable — check() then runs static-only.
    """
    state = SessionState()

    spark = _get_spark_session()
    if spark is None:
        return state

    app_id = _resolve_app_id(spark)
    if not app_id:
        return state

    rest_url, auth_header = _resolve_rest_endpoint(spark)
    if not rest_url:
        return state

    state.active = True
    state._rest_url = rest_url
    state._app_id = app_id
    state._auth_header = auth_header
    return state


def collect(state: SessionState) -> None:
    """Fetch stage metrics from REST API and populate state.collected.

    Called at check() time. Populates state.collected with normalised per-stage
    dicts. No-op when state.active is False.
    """
    if not state.active or not state._rest_url or not state._app_id:
        return

    base = f"{state._rest_url.rstrip('/')}/applications/{state._app_id}"
    headers: dict[str, str] = {}
    if state._auth_header:
        headers["Authorization"] = state._auth_header

    stages_raw = _http_get(f"{base}/stages", headers)
    if stages_raw is None:
        warnings.warn(
            "burnt: Could not reach Spark monitoring REST API — "
            "runtime enrichment skipped. "
            f"(tried {base}/stages)",
            RuntimeWarning,
            stacklevel=4,
        )
        state.active = False
        return

    state.collected = [_normalise_stage(s) for s in stages_raw if isinstance(s, dict)]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_spark_session() -> Any:
    """Return the active SparkSession or None."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


def _resolve_app_id(spark: Any) -> str | None:
    """Get spark.app.id safely (works on Connect / UC shared / Serverless).

    Never uses spark.sparkContext.applicationId — that raises
    JVM_ATTRIBUTE_NOT_SUPPORTED on UC shared and Serverless clusters.
    Falls back to a single warmup query if the app hasn't registered yet.
    """
    try:
        app_id = spark.conf.get("spark.app.id", "")
    except Exception:
        return None

    if not app_id:
        # Warmup: trigger the first Spark action so the app registers.
        try:
            spark.sql("SELECT 1").collect()
            app_id = spark.conf.get("spark.app.id", "")
        except Exception:
            return None

    return app_id or None


def _resolve_rest_endpoint(spark: Any) -> tuple[str | None, str | None]:
    """Return (rest_base_url, auth_header) for the current Spark environment.

    Returns (None, None) if no endpoint can be resolved.
    """
    # ── 1. Databricks: driver-proxy-api (Connect-safe, no sparkContext needed) ──
    try:
        from dbruntime.databricks_repl_context import get_context  # type: ignore[import]

        ctx = get_context()
        if ctx and getattr(ctx, "browserHostName", None) and getattr(ctx, "clusterId", None):
            url = (
                f"https://{ctx.browserHostName}"
                f"/driver-proxy-api/o/{ctx.workspaceId}/{ctx.clusterId}/40001/api/v1"
            )
            auth = f"Bearer {ctx.apiToken}" if getattr(ctx, "apiToken", None) else None
            return url, auth
    except (ImportError, Exception):
        pass

    # ── 2. Generic Spark UI (local, EMR, Dataproc, Glue, on-prem) ──
    try:
        ui_url: str = spark.sparkContext.uiWebUrl  # safe on non-Connect clusters
        if ui_url:
            return f"{ui_url.rstrip('/')}/api/v1", None
    except Exception:
        pass

    return None, None


def _http_get(url: str, headers: dict[str, str]) -> list | None:
    """HTTP GET returning parsed JSON list, or None on any failure.

    Uses `requests` if available (installed via [databricks] extra) for
    connection pooling and better error messages; falls back to stdlib
    urllib so the core install stays zero-dep.
    """
    try:
        import requests  # type: ignore[import]

        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return None
    except ImportError:
        pass
    except Exception:
        return None

    # stdlib fallback
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, ValueError):
        return None


def _normalise_stage(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalise a raw REST /stages item to burnt's internal schema."""
    return {
        "stage_id": raw.get("stageId", 0),
        "name": raw.get("name", ""),
        "executor_run_time_ms": raw.get("executorRunTime", 0),
        "shuffle_read_bytes": raw.get("shuffleReadBytes", 0),
        "shuffle_write_bytes": raw.get("shuffleWriteBytes", 0),
        "memory_bytes_spilled": raw.get("memoryBytesSpilled", 0),
        "disk_bytes_spilled": raw.get("diskBytesSpilled", 0),
        "input_bytes": raw.get("inputBytes", 0),
    }
