"""GCP Databricks DBU rates and SKU mapping."""

DBU_RATES: dict[str, float] = {
    "ALL_PURPOSE": 0.50,
    "JOBS_COMPUTE": 0.28,
    "SERVERLESS_JOBS": 0.42,
    "SERVERLESS_NOTEBOOKS": 0.90,
    "SQL_CLASSIC": 0.21,
    "SQL_PRO": 0.52,
    "SQL_SERVERLESS": 0.68,
    "DLT_CORE": 0.28,
    "DLT_PRO": 0.36,
    "DLT_ADVANCED": 0.52,
}

PHOTON_MULTIPLIER = 2.5


def infer_dbu_rate(instance_type: str, vcpus: int) -> float:
    if "n2" in instance_type or "e2" in instance_type:
        return vcpus * 0.19
    if "c2" in instance_type:
        return vcpus * 0.17
    return vcpus * 0.18
