"""Delta Lake _delta_log metadata reader."""

import json
from pathlib import Path

from ..core.exceptions import ParseError
from ..core.models import DeltaTableInfo


def read_delta_log(log_dir: Path) -> DeltaTableInfo:
    """Read Delta Lake transaction log and return table statistics.

    Parses all JSON files in the _delta_log directory to compute current
    file set (adds minus removes) and aggregate statistics.

    Raises ParseError if log_dir does not exist or contains no JSON files.
    """
    if not log_dir.exists():
        raise ParseError(f"Delta log directory not found: {log_dir}")

    json_files = sorted(log_dir.glob("*.json"))
    if not json_files:
        raise ParseError(f"No JSON files found in Delta log: {log_dir}")

    # Track current file set: path -> {size, num_records}
    files: dict[str, dict] = {}

    for json_file in json_files:
        with open(json_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    action = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if "add" in action:
                    add = action["add"]
                    path = add["path"]
                    size = int(add.get("size", 0))
                    num_records = None
                    stats_str = add.get("stats")
                    if stats_str:
                        try:
                            stats = json.loads(stats_str)
                            num_records = stats.get("numRecords")
                        except (json.JSONDecodeError, TypeError):
                            pass
                    files[path] = {"size": size, "num_records": num_records}

                elif "remove" in action:
                    path = action["remove"]["path"]
                    files.pop(path, None)

    total_size = sum(f["size"] for f in files.values())
    num_files = len(files)

    # Sum num_records only if all files have it
    record_counts = [f["num_records"] for f in files.values()]
    if record_counts and all(r is not None for r in record_counts):
        num_records: int | None = sum(record_counts)
    else:
        num_records = None

    return DeltaTableInfo(
        location=str(log_dir.parent),
        total_size_bytes=total_size,
        num_files=num_files,
        num_records=num_records,
        partition_columns=[],
    )


def parse_describe_detail(rows: list[dict]) -> DeltaTableInfo:
    """Parse output of DESCRIBE DETAIL SQL command into DeltaTableInfo.

    Raises ParseError if rows is empty.
    """
    if not rows:
        raise ParseError("DESCRIBE DETAIL returned no rows")

    row = rows[0]
    return DeltaTableInfo(
        location=str(row.get("location", "")),
        total_size_bytes=int(row.get("sizeInBytes", 0)),
        num_files=int(row.get("numFiles", 0)),
        num_records=None,
        partition_columns=_parse_partition_columns(row.get("partitionColumns")),
    )


def _parse_partition_columns(value: object) -> list[str]:
    """Parse partitionColumns field which may be a list, string, or None."""
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str) and value:
        return [value]
    return []
