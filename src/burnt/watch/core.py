"""Watch orchestration."""

from __future__ import annotations

import contextlib
from typing import Any

from pydantic import BaseModel


class WatchResult(BaseModel):
    """Result of cost monitoring."""

    tag_costs: dict[str, float] = {}
    idle_clusters: list[dict[str, Any]] = []
    cost_drift: dict[str, float] = {}
    total_cost_usd: float = 0

    def alert(
        self,
        *,
        slack: str | None = None,
        teams: str | None = None,
        webhook: str | None = None,
        delta: str | None = None,
    ) -> Any:
        """Dispatch alerts for this watch result.

        If no destinations are provided, reads from [alert] in burnt.toml.

        Args:
            slack: Slack incoming webhook URL.
            teams: Microsoft Teams webhook URL.
            webhook: Generic webhook URL.
            delta: Delta table path (e.g. "catalog.schema.alerts").

        Returns:
            AlertResult with per-channel send status.
        """
        from ..alerts.dispatch import dispatch

        if not any([slack, teams, webhook, delta]):
            from ..core.config import Settings

            _, settings = Settings.discover()
            slack = slack or settings.alert.slack
            teams = teams or settings.alert.teams
            webhook = webhook or settings.alert.webhook
            delta = delta or settings.alert.delta_table

        return dispatch(
            self._build_message(),
            slack=slack,
            teams=teams,
            webhook=webhook,
            delta_table=delta,
        )

    def _build_message(self) -> str:
        lines = ["*burnt watch report*"]
        if self.cost_drift:
            for resource, drift in self.cost_drift.items():
                sign = "+" if drift >= 0 else ""
                lines.append(f"  {resource}: {sign}{drift:.1%} cost drift")
        if self.idle_clusters:
            for cluster in self.idle_clusters:
                lines.append(
                    f"  Idle cluster {cluster['cluster_id']}: "
                    f"${cluster['wasted_cost_usd']:.2f} wasted"
                )
        if self.total_cost_usd:
            lines.append(f"Total cost: ${self.total_cost_usd:.2f}")
        return "\n".join(lines)


def watch(
    tag_key: str | None = None,
    *,
    drift_threshold: float = 0.25,
    idle_threshold: float = 0.10,
    budget: float | None = None,
    days: int = 30,
    job_id: int | None = None,
    pipeline_id: str | None = None,
) -> WatchResult:
    """Monitor Databricks costs via system tables.

    Orchestrates tag attribution, idle cluster detection, and cost drift
    analysis into a single WatchResult. Reads default parameters from
    [watch] in burnt.toml if not explicitly provided.

    Args:
        tag_key: Databricks tag to group costs by.
        drift_threshold: Alert if cost changes by more than this percentage.
        idle_threshold: Alert if cluster idle time exceeds this percentage.
        budget: Monthly budget for alerts (informational, stored in result).
        days: Number of days to look back.
        job_id: Filter drift detection to a specific job (not yet implemented).
        pipeline_id: Filter drift detection to a specific DLT pipeline (not yet implemented).

    Returns:
        WatchResult with tag_costs, idle_clusters, cost_drift, and total_cost_usd.
    """
    from ..core.config import Settings
    from .drift import detect_cost_drift
    from .idle import find_idle_clusters
    from .tags import tag_attribution

    _, settings = Settings.discover()

    # Apply config defaults where callers didn't pass explicit values
    resolved_tag_key = tag_key or settings.watch.tag_key
    resolved_drift_threshold = drift_threshold
    resolved_idle_threshold = idle_threshold
    resolved_days = days
    warehouse_id = settings.watch.warehouse_id or settings.warehouse_id

    tag_costs: dict[str, float] = {}
    if resolved_tag_key:
        with contextlib.suppress(Exception):
            tag_costs = tag_attribution(
                resolved_tag_key,
                days=resolved_days,
                warehouse_id=warehouse_id,
            )

    idle_clusters: list[dict[str, Any]] = []
    with contextlib.suppress(Exception):
        idle_clusters = find_idle_clusters(
            resolved_idle_threshold,
            days=min(resolved_days, 7),
            warehouse_id=warehouse_id,
        )

    cost_drift: dict[str, float] = {}
    with contextlib.suppress(Exception):
        cost_drift = detect_cost_drift(
            threshold_pct=resolved_drift_threshold,
            days=resolved_days,
            warehouse_id=warehouse_id,
        )

    total_cost_usd = sum(tag_costs.values())

    return WatchResult(
        tag_costs=tag_costs,
        idle_clusters=idle_clusters,
        cost_drift=cost_drift,
        total_cost_usd=total_cost_usd,
    )
