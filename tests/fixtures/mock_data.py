"""Mock data loader for benchmark testing.

Generates synthetic data matching the distribution from system_tables_masked.xlsx:
- 250 total billing records
- 5 SKU types (JOBS_COMPUTE dominant at ~70%)
- DBU range: 0.025 - 13.5, mean 2.16
- Hourly time granularity
- AZURE cloud only
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3

# SKU distribution from xlsx analysis
SKU_DISTRIBUTION = {
    "PREMIUM_JOBS_COMPUTE": 0.70,
    "PREMIUM_SQL_PRO_COMPUTE_EU_NORTH": 0.15,
    "PREMIUM_ALL_PURPOSE_COMPUTE": 0.10,
    "PREMIUM_JOBS_COMPUTE_(PHOTON)": 0.04,
    "PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH": 0.01,
}

# Product type mapping
SKU_TO_PRODUCT = {
    "PREMIUM_JOBS_COMPUTE": "JOBS",
    "PREMIUM_SQL_PRO_COMPUTE_EU_NORTH": "SQL",
    "PREMIUM_ALL_PURPOSE_COMPUTE": "ALL_PURPOSE",
    "PREMIUM_JOBS_COMPUTE_(PHOTON)": "JOBS",
    "PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH": "SQL",
}

# DBU rates by SKU (Azure Premium, US East)
SKU_DBU_RATES = {
    "PREMIUM_JOBS_COMPUTE": 0.30,
    "PREMIUM_SQL_PRO_COMPUTE_EU_NORTH": 0.70,
    "PREMIUM_ALL_PURPOSE_COMPUTE": 0.55,
    "PREMIUM_JOBS_COMPUTE_(PHOTON)": 0.30,
    "PREMIUM_SERVERLESS_SQL_COMPUTE_EU_NORTH": 0.70,
}

# DBU distribution parameters from xlsx
DBU_MIN = 0.025
DBU_MAX = 13.5
DBU_MEAN = 2.16
DBU_MEDIAN = 0.50  # Estimated from distribution

# Total records to generate
DEFAULT_BILLING_RECORDS = 100
DEFAULT_QUERIES_PER_HOUR = 3


def _generate_dbu() -> float:
    """Generate a DBU value matching xlsx distribution.

    Uses log-normal distribution to match the observed data
    where most values are low but with a long tail.
    """
    # Log-normal parameters approximated from xlsx
    # mean of log(x) ≈ -0.5, std of log(x) ≈ 1.5
    import math

    log_mean = -0.5
    log_std = 1.5

    log_value = random.gauss(log_mean, log_std)
    dbu = math.exp(log_value)

    # Clamp to observed range
    return max(DBU_MIN, min(DBU_MAX, dbu))


def _select_sku() -> str:
    """Select a SKU based on distribution from xlsx."""
    r = random.random()
    cumulative = 0.0
    for sku, prob in SKU_DISTRIBUTION.items():
        cumulative += prob
        if r <= cumulative:
            return sku
    return list(SKU_DISTRIBUTION.keys())[-1]


def _generate_timestamp_range(days: int = 30) -> list[datetime]:
    """Generate hourly timestamps for the past N days."""
    end = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)

    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(hours=1)

    return timestamps


def load_billing_data(
    conn: sqlite3.Connection,
    xlsx_path: str | None = None,
    num_records: int = DEFAULT_BILLING_RECORDS,
) -> list[dict[str, Any]]:
    """Load billing data into SQLite.

    If xlsx_path is provided and exists, loads from real data.
    Otherwise generates synthetic data matching xlsx distribution.

    Args:
        conn: SQLite connection
        xlsx_path: Path to xlsx file (optional)
        num_records: Number of records to generate if using synthetic data

    Returns:
        List of billing records
    """
    records = []

    if xlsx_path and Path(xlsx_path).exists():
        # Load from real xlsx
        records = _load_from_xlsx(conn, xlsx_path)
    else:
        # Generate synthetic data
        records = _generate_synthetic_billing(conn, num_records)

    return records


def _load_from_xlsx(conn: sqlite3.Connection, xlsx_path: str) -> list[dict[str, Any]]:
    """Load billing data from xlsx file."""
    try:
        import openpyxl
    except ImportError:
        # Fall back to synthetic
        return _generate_synthetic_billing(conn, DEFAULT_BILLING_RECORDS)

    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    # Skip header row
    rows = list(ws.iter_rows(values_only=True))[1:]

    records = []
    cursor = conn.cursor()

    for row in rows:
        if len(row) < 10:
            continue

        record = {
            "account_id": str(row[0]) if row[0] else "ACCT_0001",
            "workspace_id": str(row[1]) if row[1] else "WS_0001",
            "record_id": str(row[2]) if row[2] else f"REC_{len(records):04d}",
            "sku_name": str(row[3]) if row[3] else _select_sku(),
            "cloud": str(row[4]) if row[4] else "AZURE",
            "usage_start_time": str(row[5]) if row[5] else None,
            "usage_end_time": str(row[6]) if row[6] else None,
            "usage_date": str(row[7]) if row[7] else None,
            "custom_tags": str(row[8]) if row[8] else "REDACTED",
            "usage_unit": str(row[9]) if row[9] else "DBU",
            "usage_quantity": float(row[10]) if row[10] else _generate_dbu(),
            "usage_metadata": str(row[11]) if row[11] else "REDACTED",
            "identity_metadata": str(row[12]) if row[12] else "REDACTED",
            "record_type": str(row[13]) if row[13] else "ORIGINAL",
            "ingestion_date": str(row[14]) if row[14] else None,
            "billing_origin_product": str(row[15]) if row[15] else None,
            "product_features": str(row[16]) if row[16] else "REDACTED",
            "usage_type": str(row[17]) if row[17] else "COMPUTE_TIME",
        }

        records.append(record)

        # Insert into database
        cursor.execute(
            """
            INSERT OR REPLACE INTO system_billing_usage
            (account_id, workspace_id, record_id, sku_name, cloud,
             usage_start_time, usage_end_time, usage_date, custom_tags,
             usage_unit, usage_quantity, usage_metadata, identity_metadata,
             record_type, ingestion_date, billing_origin_product,
             product_features, usage_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(record.values()),
        )

    conn.commit()
    return records


def _generate_synthetic_billing(
    conn: sqlite3.Connection, num_records: int
) -> list[dict[str, Any]]:
    """Generate synthetic billing data matching xlsx distribution."""
    records = []
    cursor = conn.cursor()

    timestamps = _generate_timestamp_range(30)

    for i in range(num_records):
        ts_idx = i % len(timestamps)
        start_time = timestamps[ts_idx]
        end_time = start_time + timedelta(hours=1)

        sku = _select_sku()
        dbu = _generate_dbu()

        record = {
            "account_id": "ACCT_0001",
            "workspace_id": "WS_0001",
            "record_id": f"REC_{i:04d}",
            "sku_name": sku,
            "cloud": "AZURE",
            "usage_start_time": start_time.isoformat(),
            "usage_end_time": end_time.isoformat(),
            "usage_date": start_time.date().isoformat(),
            "custom_tags": "REDACTED",
            "usage_unit": "DBU",
            "usage_quantity": dbu,
            "usage_metadata": "REDACTED",
            "identity_metadata": "REDACTED",
            "record_type": "ORIGINAL",
            "ingestion_date": start_time.date().isoformat(),
            "billing_origin_product": SKU_TO_PRODUCT.get(sku, "JOBS"),
            "product_features": "REDACTED",
            "usage_type": "COMPUTE_TIME",
        }

        records.append(record)

        cursor.execute(
            """
            INSERT OR REPLACE INTO system_billing_usage
            (account_id, workspace_id, record_id, sku_name, cloud,
             usage_start_time, usage_end_time, usage_date, custom_tags,
             usage_unit, usage_quantity, usage_metadata, identity_metadata,
             record_type, ingestion_date, billing_origin_product,
             product_features, usage_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(record.values()),
        )

    conn.commit()
    return records


def generate_query_history(
    conn: sqlite3.Connection,
    billing_records: list[dict[str, Any]],
    queries_per_hour: int = DEFAULT_QUERIES_PER_HOUR,
) -> list[dict[str, Any]]:
    """Generate synthetic query history correlated with billing data.

    For each billing record, generates multiple queries with
    execution duration correlated to DBU consumption.

    Args:
        conn: SQLite connection
        billing_records: List of billing records
        queries_per_hour: Number of queries to generate per billing hour

    Returns:
        List of query records
    """
    records = []
    cursor = conn.cursor()

    statement_id = 0

    for billing in billing_records:
        sku = billing["sku_name"]
        dbu_rate = SKU_DBU_RATES.get(sku, 0.30)
        billing_dbu = billing["usage_quantity"]

        # Calculate approximate duration from DBU
        # DBU = (duration_ms / 3_600_000) * dbu_rate
        # duration_ms = (DBU / dbu_rate) * 3_600_000
        base_duration_ms = int((billing_dbu / dbu_rate) * 3_600_000 / queries_per_hour)

        for _ in range(queries_per_hour):
            # Add some variation to duration
            variation = random.uniform(0.5, 2.0)
            duration_ms = int(base_duration_ms * variation)
            duration_ms = max(1000, min(3_600_000, duration_ms))  # Clamp 1s to 1hr

            # Calculate read bytes (correlated with duration)
            read_bytes = int(
                duration_ms * random.uniform(100, 1000)
            )  # 100-1000 bytes/ms

            start_time = billing["usage_start_time"]
            if isinstance(start_time, str):
                start_dt = datetime.fromisoformat(start_time)
            else:
                start_dt = datetime.now()

            end_dt = start_dt + timedelta(milliseconds=duration_ms)

            # Determine query type based on SKU
            if "SQL" in sku:
                statement_type = random.choice(["SELECT", "INSERT", "MERGE"])
            else:
                statement_type = random.choice(["SELECT", "INSERT", "UPDATE", "DELETE"])

            record = {
                "statement_id": f"stmt_{statement_id:06d}",
                "statement_text": f"-- {statement_type} query for benchmarking",
                "statement_type": statement_type,
                "start_time": start_dt.isoformat(),
                "end_time": end_dt.isoformat(),
                "execution_duration_ms": duration_ms,
                "compilation_duration_ms": int(duration_ms * 0.1),  # ~10% of execution
                "read_bytes": read_bytes,
                "read_rows": int(read_bytes / 100),  # Assume ~100 bytes per row
                "produced_rows": int(read_bytes / 200),
                "written_bytes": 0
                if statement_type == "SELECT"
                else int(read_bytes * 0.1),
                "total_task_duration_ms": duration_ms,
                "warehouse_id": "WH_001" if "SQL" in sku else None,
                "cluster_id": "CL_001" if "JOBS" in sku else None,
                "status": "FINISHED",
                "error_message": None,
            }

            records.append(record)
            statement_id += 1

            cursor.execute(
                """
                INSERT OR REPLACE INTO system_query_history
                (statement_id, statement_text, statement_type, start_time, end_time,
                 execution_duration_ms, compilation_duration_ms, read_bytes, read_rows,
                 produced_rows, written_bytes, total_task_duration_ms, warehouse_id,
                 cluster_id, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(record.values()),
            )

    conn.commit()
    return records


def load_pricing_data(conn: sqlite3.Connection) -> None:
    """Load pricing data into SQLite."""
    cursor = conn.cursor()

    for sku, rate in SKU_DBU_RATES.items():
        cursor.execute(
            """
            INSERT OR REPLACE INTO system_billing_list_prices
            (sku_name, cloud, currency_code, price_usd, price_start_time, price_end_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sku, "AZURE", "USD", rate, "2023-01-01T00:00:00Z", None),
        )

    conn.commit()


def load_compute_node_types(conn: sqlite3.Connection) -> None:
    """Load compute node type metadata into SQLite."""
    cursor = conn.cursor()

    node_types = [
        ("Standard_DS3_v2", 4, 14, "General", 0.75, 1.875),
        ("Standard_DS4_v2", 8, 28, "General", 1.50, 3.75),
        ("Standard_DS5_v2", 16, 56, "General", 3.00, 7.50),
        ("Standard_E8s_v3", 8, 64, "Memory", 1.50, 3.75),
        ("Standard_E16s_v3", 16, 128, "Memory", 3.00, 7.50),
        ("Standard_F8s_v2", 8, 16, "Compute", 1.50, 3.75),
        ("Standard_F16s_v2", 16, 32, "Compute", 3.00, 7.50),
    ]

    for node_type, vcpus, memory, category, dbu, photon_dbu in node_types:
        cursor.execute(
            """
            INSERT OR REPLACE INTO system_compute_node_types
            (instance_type, vcpus, memory_gb, category, dbu_per_hour, photon_dbu_per_hour)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (node_type, vcpus, memory, category, dbu, photon_dbu),
        )

    conn.commit()


def load_benchmark_tables(conn: sqlite3.Connection, scale_factor: int = 100) -> None:
    """Load sample data into benchmark tables.

    Creates tables with sizes suitable for benchmark queries:
    - table_a: scale_factor * 100 rows
    - table_b: scale_factor * 500 rows
    - table_c: scale_factor * 1000 rows
    - table_d: scale_factor * 2000 rows
    - table_e: scale_factor * 4000 rows

    Args:
        conn: SQLite connection
        scale_factor: Multiplier for table sizes
    """
    cursor = conn.cursor()

    # Categories for filtering
    categories = ["A", "B", "C", "D", "E"]
    statuses = ["active", "pending", "completed", "failed"]
    descriptions = ["test", "prod", "staging", "dev"]
    labels = ["high", "medium", "low", "critical"]

    # Generate table_a
    table_a_size = scale_factor * 100
    for i in range(table_a_size):
        cursor.execute(
            """
            INSERT INTO benchmark_table_a (id, category, value, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                i,
                categories[i % len(categories)],
                random.uniform(0, 1000),
                (datetime.now() - timedelta(days=i % 365)).isoformat(),
            ),
        )

    # Generate table_b (FK to table_a)
    table_b_size = scale_factor * 500
    for i in range(table_b_size):
        cursor.execute(
            """
            INSERT INTO benchmark_table_b (id, table_a_id, amount, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                i,
                i % table_a_size,
                random.uniform(0, 10000),
                statuses[i % len(statuses)],
            ),
        )

    # Generate table_c (FK to table_b)
    table_c_size = scale_factor * 1000
    for i in range(table_c_size):
        cursor.execute(
            """
            INSERT INTO benchmark_table_c (id, table_b_id, description)
            VALUES (?, ?, ?)
            """,
            (
                i,
                i % table_b_size,
                descriptions[i % len(descriptions)],
            ),
        )

    # Generate table_d (FK to table_c)
    table_d_size = scale_factor * 2000
    for i in range(table_d_size):
        cursor.execute(
            """
            INSERT INTO benchmark_table_d (id, table_c_id, score)
            VALUES (?, ?, ?)
            """,
            (
                i,
                i % table_c_size,
                random.uniform(0, 100),
            ),
        )

    # Generate table_e (FK to table_d)
    table_e_size = scale_factor * 4000
    for i in range(table_e_size):
        cursor.execute(
            """
            INSERT INTO benchmark_table_e (id, table_d_id, label)
            VALUES (?, ?, ?)
            """,
            (
                i,
                i % table_d_size,
                labels[i % len(labels)],
            ),
        )

    conn.commit()


def init_mock_database(
    conn: sqlite3.Connection,
    xlsx_path: str | None = None,
    scale_factor: int = 10,
) -> dict[str, Any]:
    """Initialize the mock database with all data.

    Args:
        conn: SQLite connection
        xlsx_path: Path to xlsx file (optional)
        scale_factor: Multiplier for benchmark table sizes

    Returns:
        Dictionary with statistics about loaded data
    """
    # Load schema
    schema_path = Path(__file__).parent / "mock_schema.sql"
    if schema_path.exists():
        with open(schema_path) as f:
            conn.executescript(f.read())

    # Load billing data (from xlsx or synthetic)
    billing_records = load_billing_data(conn, xlsx_path)

    # Generate query history
    query_records = generate_query_history(conn, billing_records)

    # Load pricing data
    load_pricing_data(conn)

    # Load compute node types
    load_compute_node_types(conn)

    # Load benchmark tables
    load_benchmark_tables(conn, scale_factor)

    return {
        "billing_records": len(billing_records),
        "query_records": len(query_records),
        "table_sizes": {
            "table_a": scale_factor * 100,
            "table_b": scale_factor * 500,
            "table_c": scale_factor * 1000,
            "table_d": scale_factor * 2000,
            "table_e": scale_factor * 4000,
        },
    }
