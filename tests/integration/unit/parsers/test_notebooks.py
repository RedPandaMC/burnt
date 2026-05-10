import json
import tempfile
from pathlib import Path

from burnt.parsers.notebooks import NotebookCell, parse_notebook


class TestParseNotebook:
    def test_parse_notebook_code_cell(self):
        nb = {
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["print('hello')"],
                    "metadata": {"kernelspec": {"name": "python3"}},
                }
            ]
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            json.dump(nb, f)
            f.flush()
            path = Path(f.name)

        try:
            cells = parse_notebook(path)
            assert len(cells) == 1
            assert cells[0].language == "python"
            assert "print('hello')" in cells[0].source
        finally:
            path.unlink()

    def test_parse_notebook_sql_magic(self):
        nb = {
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["%sql SELECT * FROM users"],
                    "metadata": {},
                }
            ]
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            json.dump(nb, f)
            f.flush()
            path = Path(f.name)

        try:
            cells = parse_notebook(path)
            assert len(cells) == 1
            assert cells[0].language == "sql"
        finally:
            path.unlink()

    def test_parse_notebook_markdown_skipped(self):
        nb = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "source": ["# Title"],
                    "metadata": {},
                }
            ]
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            json.dump(nb, f)
            f.flush()
            path = Path(f.name)

        try:
            cells = parse_notebook(path)
            assert len(cells) == 0
        finally:
            path.unlink()

    def test_parse_notebook_python_magic(self):
        nb = {
            "cells": [
                {
                    "cell_type": "code",
                    "source": ["%python\nimport pandas as pd"],
                    "metadata": {},
                }
            ]
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            json.dump(nb, f)
            f.flush()
            path = Path(f.name)

        try:
            cells = parse_notebook(path)
            assert len(cells) == 1
            assert cells[0].language == "python"
        finally:
            path.unlink()


class TestNotebookCell:
    def test_notebook_cell_creation(self):
        cell = NotebookCell(
            language="sql",
            source="SELECT * FROM users",
            cell_index=0,
        )
        assert cell.language == "sql"
        assert cell.source == "SELECT * FROM users"
        assert cell.cell_index == 0
