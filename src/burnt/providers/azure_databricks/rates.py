"""Azure Databricks DBU rates and SKU mapping."""

DBU_RATES: dict[str, float] = {
    "ALL_PURPOSE": 0.55,
    "JOBS_COMPUTE": 0.30,
    "SERVERLESS_JOBS": 0.45,
    "SERVERLESS_NOTEBOOKS": 0.95,
    "SQL_CLASSIC": 0.22,
    "SQL_PRO": 0.55,
    "SQL_SERVERLESS": 0.70,
    "DLT_CORE": 0.30,
    "DLT_PRO": 0.38,
    "DLT_ADVANCED": 0.54,
}

PHOTON_MULTIPLIER = 2.5


def infer_dbu_rate(instance_type: str, vcpus: int) -> float:
    if "F-" in instance_type or "FX" in instance_type:
        return vcpus * 0.15
    if "E-" in instance_type or "Eav" in instance_type:
        return vcpus * 0.20
    if "L-" in instance_type:
        return vcpus * 0.25
    if "Dsv3" in instance_type or "Dds" in instance_type:
        return vcpus * 0.18
    if "Dsv2" in instance_type or "DS" in instance_type:
        return vcpus * 0.19
    return vcpus * 0.20
