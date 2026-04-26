"""Session listener for capturing Spark runtime metrics."""

from __future__ import annotations

import contextlib
from typing import Any


class SessionState:
    """Holds captured session metrics."""

    def __init__(self) -> None:
        self.stages: list[dict[str, Any]] = []
        self.sql_executions: list[dict[str, Any]] = []
        self.cells: list[dict[str, Any]] = []
        self.active: bool = True


def start(
    *,
    capture_sql: bool = True,
    capture_stages: bool = True,
    capture_cells: bool = True,
) -> SessionState:
    """Start listening to the active Spark session.

    Args:
        capture_sql: Capture SQL query text and duration.
        capture_stages: Capture stage-level metrics.
        capture_cells: Capture cell execution times.

    Returns:
        SessionState object that will be populated as Spark events occur.
    """
    state = SessionState()

    with contextlib.suppress(Exception):
        _register_listener(state, capture_sql, capture_stages)

    return state


def _register_listener(
    state: SessionState,
    capture_sql: bool,
    capture_stages: bool,
) -> None:
    """Try to register a SparkListener."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return

    spark = SparkSession.getActiveSession()
    if spark is None:
        return

    sc = spark.sparkContext

    listener = _BurntSparkListener(state, capture_sql, capture_stages)
    sc._jsc.sc().addSparkListener(listener)


class _BurntSparkListener:
    """Lightweight SparkListener that captures key metrics."""

    def __init__(
        self,
        state: SessionState,
        capture_sql: bool,
        capture_stages: bool,
    ) -> None:
        self._state = state
        self._capture_sql = capture_sql
        self._capture_stages = capture_stages

    def onStageCompleted(self, stage_completed: Any) -> None:
        if not self._capture_stages:
            return
        stage_info = stage_completed.stageInfo()
        metrics = stage_info.taskMetrics() if stage_info.taskMetrics() else None
        self._state.stages.append(
            {
                "stage_id": stage_info.stageId(),
                "attempt_id": stage_info.attemptNumber(),
                "name": stage_info.name(),
                "num_tasks": stage_info.numTasks(),
                "shuffle_read_bytes": (
                    metrics.shuffleReadMetrics().totalBytesRead()
                    if metrics and metrics.shuffleReadMetrics()
                    else 0
                ),
                "shuffle_write_bytes": (
                    metrics.shuffleWriteMetrics().bytesWritten()
                    if metrics and metrics.shuffleWriteMetrics()
                    else 0
                ),
                "input_bytes": (
                    metrics.inputMetrics().bytesRead()
                    if metrics and metrics.inputMetrics()
                    else 0
                ),
                "output_bytes": (
                    metrics.outputMetrics().bytesWritten()
                    if metrics and metrics.outputMetrics()
                    else 0
                ),
                "executor_cpu_time_ms": (metrics.executorCpuTime() if metrics else 0),
            }
        )

    def onOtherEvent(self, event: Any) -> None:
        if not self._capture_sql:
            return
        # Spark SQL execution events
        name = event.name() if hasattr(event, "name") else ""
        if "sqlExecution" in name.lower():
            self._state.sql_executions.append(
                {
                    "execution_id": getattr(event, "executionId", lambda: None)(),
                    "description": getattr(event, "description", lambda: "")(),
                    "duration_ms": getattr(event, "duration", lambda: 0)(),
                }
            )
