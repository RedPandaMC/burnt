"""AWS Databricks DBU rates and SKU mapping."""

DBU_RATES: dict[str, float] = {
    "ALL_PURPOSE": 0.48,
    "JOBS_COMPUTE": 0.26,
    "SERVERLESS_JOBS": 0.40,
    "SERVERLESS_NOTEBOOKS": 0.88,
    "SQL_CLASSIC": 0.20,
    "SQL_PRO": 0.50,
    "SQL_SERVERLESS": 0.65,
    "DLT_CORE": 0.26,
    "DLT_PRO": 0.35,
    "DLT_ADVANCED": 0.50,
}

PHOTON_MULTIPLIER = 2.5


def infer_dbu_rate(instance_type: str, vcpus: int) -> float:
    if "_inf" in instance_type:
        return vcpus * 0.22
    if "r5" in instance_type or "r6" in instance_type:
        return vcpus * 0.20
    if "m5" in instance_type or "m6" in instance_type:
        return vcpus * 0.18
    if "c5" in instance_type or "c6" in instance_type:
        return vcpus * 0.16
    if "i3" in instance_type or "i4" in instance_type:
        return vcpus * 0.25
    return vcpus * 0.18
