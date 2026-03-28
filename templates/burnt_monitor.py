# Databricks notebook source
# MAGIC %md
# MAGIC # burnt Cost Monitor
# MAGIC
# MAGIC Automated cost monitoring for Databricks workloads.
# MAGIC Run this notebook on a schedule (e.g. daily) to get cost reports and alerts.
# MAGIC
# MAGIC **Requirements:** `pip install burnt[alerts]` on the cluster.

# COMMAND ----------

# MAGIC %pip install burnt[alerts] --quiet

# COMMAND ----------

# Widget: SQL Warehouse ID (required for system table queries)
dbutils.widgets.text(
    "warehouse_id",
    defaultValue="",
    label="SQL Warehouse ID",
)

# Widget: Tag key to group costs by
dbutils.widgets.text(
    "tag_key",
    defaultValue="team",
    label="Tag Key",
)

# Widget: Cost drift threshold (fraction)
dbutils.widgets.text(
    "drift_threshold",
    defaultValue="0.25",
    label="Drift Threshold (e.g. 0.25 = 25%)",
)

# Widget: Idle cluster threshold (fraction)
dbutils.widgets.text(
    "idle_threshold",
    defaultValue="0.10",
    label="Idle CPU Threshold (e.g. 0.10 = 10%)",
)

# Widget: Look-back window in days
dbutils.widgets.text(
    "days",
    defaultValue="30",
    label="Days to Look Back",
)

# Widget: Slack webhook URL (optional)
dbutils.widgets.text(
    "slack_webhook",
    defaultValue="",
    label="Slack Webhook URL (optional)",
)

# Widget: Teams webhook URL (optional)
dbutils.widgets.text(
    "teams_webhook",
    defaultValue="",
    label="Microsoft Teams Webhook URL (optional)",
)

# Widget: Generic webhook URL (optional)
dbutils.widgets.text(
    "webhook",
    defaultValue="",
    label="Generic Webhook URL (optional)",
)

# Widget: Delta alert table (optional)
dbutils.widgets.text(
    "alert_delta_table",
    defaultValue="",
    label="Delta Alert Table (optional, e.g. catalog.schema.burnt_alerts)",
)

# COMMAND ----------

# Read widget values
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
tag_key = dbutils.widgets.get("tag_key").strip() or None
drift_threshold = float(dbutils.widgets.get("drift_threshold") or "0.25")
idle_threshold = float(dbutils.widgets.get("idle_threshold") or "0.10")
days = int(dbutils.widgets.get("days") or "30")
slack_webhook = dbutils.widgets.get("slack_webhook").strip() or None
teams_webhook = dbutils.widgets.get("teams_webhook").strip() or None
webhook = dbutils.widgets.get("webhook").strip() or None
alert_delta_table = dbutils.widgets.get("alert_delta_table").strip() or None

if not warehouse_id:
    raise ValueError("warehouse_id widget is required. Set it to your SQL Warehouse ID.")

print(f"Running burnt monitor | days={days} | tag_key={tag_key} | warehouse={warehouse_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run burnt.watch()

# COMMAND ----------

import burnt

result = burnt.watch(
    tag_key=tag_key,
    drift_threshold=drift_threshold,
    idle_threshold=idle_threshold,
    days=days,
)

print(f"Total cost (last {days} days): ${result.total_cost_usd:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tag Cost Breakdown

# COMMAND ----------

if result.tag_costs:
    import pandas as pd

    df = pd.DataFrame(
        [{"tag_value": k, "cost_usd": v} for k, v in result.tag_costs.items()]
    ).sort_values("cost_usd", ascending=False)
    display(df)
else:
    print("No tag cost data (set tag_key widget or check connectivity).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idle Clusters

# COMMAND ----------

if result.idle_clusters:
    import pandas as pd

    df = pd.DataFrame(result.idle_clusters)
    display(df)
    total_wasted = sum(c["wasted_cost_usd"] for c in result.idle_clusters)
    print(f"Total estimated wasted cost: ${total_wasted:.2f}")
else:
    print("No idle clusters detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Drift

# COMMAND ----------

if result.cost_drift:
    import pandas as pd

    df = pd.DataFrame(
        [{"resource": k, "drift_pct": f"{v:+.1%}"} for k, v in result.cost_drift.items()]
    )
    display(df)
else:
    print("No significant cost drift detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send Alerts

# COMMAND ----------

if any([slack_webhook, teams_webhook, webhook, alert_delta_table]):
    alert_result = result.alert(
        slack=slack_webhook,
        teams=teams_webhook,
        webhook=webhook,
        delta=alert_delta_table,
    )
    print(f"Alerts sent: slack={alert_result.slack_sent}, "
          f"teams={alert_result.teams_sent}, "
          f"webhook={alert_result.webhook_sent}, "
          f"delta={alert_result.delta_written}")
    if alert_result.errors:
        for err in alert_result.errors:
            print(f"  WARNING: {err}")
else:
    print("No alert destinations configured. Set a webhook widget to enable alerts.")
