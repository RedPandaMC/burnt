"""Session management — Rust-backed REST client with Python endpoint discovery."""

from __future__ import annotations

import warnings
from typing import Any

from burnt._engine import SessionState, session_collect, session_start

__all__ = ["SessionState", "collect", "start"]


def start() -> SessionState:
    """Resolve the Spark monitoring REST endpoint and return a SessionState.

    Returns SessionState(active=False) silently if no Spark session is found
    or the REST endpoint is unreachable — check() then runs static-only.
    """
    spark = _get_spark_session()
    if spark is None:
        return SessionState()

    app_id = _resolve_app_id(spark)
    if not app_id:
        return SessionState()

    rest_url, auth_header = _resolve_rest_endpoint(spark)
    if not rest_url:
        return SessionState()

    state = session_start(rest_url, app_id)
    state.auth_header = auth_header
    return state


def collect(state: SessionState) -> None:
    """Fetch stage / job / sql / executor metrics via the Rust HTTP client.

    Called at check() time. Populates state.collected with normalised dicts.
    No-op when state.active is False.
    """
    if not state.active or not state.rest_url or not state.app_id:
        return

    was_active = state.active
    session_collect(state)

    if was_active and not state.active:
        warnings.warn(
            "burnt: Could not reach Spark monitoring REST API — "
            "runtime enrichment skipped. "
            f"(tried {state.rest_url}/applications/{state.app_id}/stages)",
            RuntimeWarning,
            stacklevel=4,
        )


# ---------------------------------------------------------------------------
# Private helpers — endpoint discovery (Python-only, needs pyspark/dbruntime)
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
        from dbruntime.databricks_repl_context import (
            get_context,  # type: ignore[import]
        )

        ctx = get_context()
        if (
            ctx
            and getattr(ctx, "browserHostName", None)
            and getattr(ctx, "clusterId", None)
        ):
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
