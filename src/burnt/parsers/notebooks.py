"""Notebook parsing for Jupyter and DBC formats."""

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class NotebookCell:
    """A cell from a notebook."""

    language: Literal["sql", "python", "scala", "markdown"]
    source: str
    cell_index: int


def parse_notebook(path: Path) -> list[NotebookCell]:
    """Parse a Jupyter notebook (.ipynb) file."""
    with open(path) as f:
        nb = json.load(f)

    cells = []
    for i, cell in enumerate(nb.get("cells", [])):
        source = "".join(cell.get("source", []))
        cell_type = cell.get("cell_type", "code")

        if cell_type == "markdown":
            continue

        language = _detect_language(cell.get("metadata", {}), source)

        cells.append(
            NotebookCell(
                language=language,
                source=source,
                cell_index=i,
            )
        )

    return cells


def parse_dbc(path: Path) -> list[NotebookCell]:
    """Parse a Databricks archive (.dbc) file."""
    cells = []

    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue

            with zf.open(name) as f:
                data = json.load(f)

            for i, cmd in enumerate(data.get("commands", [])):
                source = cmd.get("commandText", "")
                language = _detect_language_from_dbc(cmd)

                cells.append(
                    NotebookCell(
                        language=language,
                        source=source,
                        cell_index=i,
                    )
                )

    return cells


def _detect_language(metadata: dict, source: str) -> str:
    """Detect language from metadata or magic commands."""
    if source.lstrip().startswith("%sql"):
        return "sql"
    elif source.lstrip().startswith("%python"):
        return "python"
    elif source.lstrip().startswith("%scala"):
        return "scala"

    kernel = metadata.get("kernelspec", {}).get("name", "")
    if "python" in kernel.lower():
        return "python"
    elif "scala" in kernel.lower():
        return "scala"

    return "python"


def _detect_language_from_dbc(cmd: dict) -> str:
    """Detect language from DBC command."""
    language = cmd.get("language", "").lower()
    if language == "sql":
        return "sql"
    elif language == "scala":
        return "scala"
    return "python"
