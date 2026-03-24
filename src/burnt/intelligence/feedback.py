"""Feedback loop for calibrating cost estimates."""

from __future__ import annotations

from pydantic import BaseModel


class CalibrationResult(BaseModel):
    """Result of calibrating coefficients."""

    job_id: str | None = None
    pipeline_id: str | None = None
    coefficients_updated: dict[str, float]
    improvement_pct: float | None = None


def calibrate(
    job_id: int | None = None,
    run_id: int | None = None,
    pipeline_id: str | None = None,
    update_id: int | None = None,
    *,
    store: str = "local",
) -> CalibrationResult:
    """Calibrate cost coefficients from actual billing data.

    Args:
        job_id: Job ID for job-based calibration.
        run_id: Run ID for specific run.
        pipeline_id: Pipeline ID for DLT calibration.
        update_id: DLT pipeline update ID.
        store: Where to store calibration ("local" or "delta:catalog.schema.table").

    Returns:
        Calibration result with updated coefficients.
    """
    raise NotImplementedError(
        "Calibration requires burnt-engine. Install with: pip install burnt[engine]"
    )
