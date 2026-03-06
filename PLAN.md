# Implementation Plan: `dburnrate` — Databricks Pre-Execution Cost Estimation

## Overview

**Goal**: Build a Python package that estimates Databricks costs *before* execution by combining static code analysis (SQL/PySpark) with historical billing data, pricing lookups, and complexity scoring. No existing open-source tool does this.

**Package name**: `dburnrate`  
**License**: GPL-3.0 (already set)  
**Python**: `>=3.12`  
**Build**: uv + hatchling  
**Scope**: MVP first — core models, SQL parser, static estimator, CLI. System tables, SDP/DLT, and cluster right-sizing come in post-MVP phases.

---

## Project Setup (Phase 0)

### 0.1 Initialize Project Structure

```
dburnrate/
├── src/
│   └── dburnrate/
│       ├── __init__.py
│       ├── py.typed
│       ├── _compat.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── models.py
│       │   ├── config.py
│       │   ├── pricing.py
│       │   ├── protocols.py
│       │   └── exceptions.py
│       ├── parsers/
│       │   ├── __init__.py
│       │   ├── sql.py
│       │   ├── pyspark.py
│       │   ├── notebooks.py
│       │   └── antipatterns.py
│       ├── estimators/
│       │   ├── __init__.py
│       │   ├── static.py
│       │   └── whatif.py
│       ├── forecast/
│       │   ├── __init__.py
│       │   └── prophet.py
│       └── cli/
│           ├── __init__.py
│           └── main.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── conftest.py
│   │   ├── core/
│   │   ├── parsers/
│   │   └── estimators/
│   ├── integration/
│   │   └── conftest.py
│   └── data/
│       ├── sql_samples/
│       └── fixtures/
├── pyproject.toml
├── README.md
├── LICENSE
├── RESEARCH.md
├── CONCEPT.md
└── .gitignore
```

### 0.2 `pyproject.toml`

```toml
[project]
name = "dburnrate"
version = "0.1.0"
description = "Pre-execution cost estimation for Databricks workloads"
readme = "README.md"
license = "GPL-3.0-or-later"
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2.0,<3",
    "pydantic-settings>=2.0",
    "typer>=0.15",
    "rich>=13.0",
]

[project.optional-dependencies]
sql = ["sqlglot>=26.0"]
forecasting = ["prophet>=1.1"]
ml = ["scikit-learn>=1.3"]
all = ["dburnrate[sql,forecasting,ml]"]

[project.scripts]
dburnrate = "dburnrate.cli.main:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dburnrate"]

# Dev Dependencies (PEP 735)
[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-cov>=6.0",
    "pytest-xdist>=3.5",
    "pytest-randomly>=4.0",
    "pytest-timeout>=2.3",
    "pytest-mock>=3.14",
    "hypothesis>=6.130",
]
lint = [
    "ruff>=0.8",
    "bandit[toml]>=1.9",
    "xenon>=0.9",
    "vulture>=2.14",
    "pip-audit>=2.9",
    "interrogate>=1.7",
]

# uv config
[tool.uv]
default-groups = ["dev"]

# pytest config
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = [
    "--strict-markers",
    "--strict-config",
    "-ra",
    "--import-mode=importlib",
]
markers = [
    "unit: fast isolated tests with no external dependencies",
    "integration: tests requiring Databricks connectivity",
    "slow: tests taking >5s",
]
timeout = 30
timeout_method = "signal"
xfail_strict = true
filterwarnings = ["error"]

# coverage config
[tool.coverage.run]
source_pkgs = ["dburnrate"]
branch = true
parallel = true

[tool.coverage.paths]
source = ["src/dburnrate", "*/site-packages/dburnrate"]

[tool.coverage.report]
show_missing = true
skip_covered = true
precision = 1
fail_under = 80
exclude_also = [
    "def __repr__",
    "if TYPE_CHECKING:",
    "class .*\\bProtocol\\):",
    "@(abc\\.)?abstractmethod",
    "raise NotImplementedError",
    "\\.\\.\\.",
]

# ruff config
[tool.ruff]
target-version = "py312"
line-length = 88
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "W", "F", "I", "B", "C4", "UP", "SIM", "TCH", "RUF"]
ignore = ["E501"]

[tool.ruff.lint.isort]
known-first-party = ["dburnrate"]

[tool.ruff.lint.per-file-ignores]
"__init__.py" = ["F401"]
"tests/**/*" = ["S101"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
docstring-code-format = true

# bandit config
[tool.bandit]
exclude_dirs = ["tests", "venv", ".venv"]

[tool.bandit.assert_used]
skips = ["*/test_*.py", "*/tests/*"]

# interrogate config
[tool.interrogate]
ignore-init-method = true
ignore-init-module = true
ignore-magic = true
ignore-semiprivate = true
ignore-private = true
ignore-property-decorators = true
ignore-nested-functions = true
ignore-nested-classes = true
fail-under = 80
exclude = ["tests", "docs"]
verbose = 1
```

### 0.3 Setup Commands

```bash
uv sync
uv sync --extra sql
uv run pytest
uv run ruff check src/
uv run ruff format src/ tests/
```

---

## Phase 1: Core Models & Configuration

### 1.1 Custom Exceptions (`src/dburnrate/core/exceptions.py`)

```python
class DburnrateError(Exception): ...

class ParseError(DburnrateError): ...
class ConfigError(DburnrateError): ...
class PricingError(DburnrateError): ...
class EstimationError(DburnrateError): ...
```

### 1.2 Protocol Classes (`src/dburnrate/core/protocols.py`)

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Estimator(Protocol):
    def estimate(self, query: str, **kwargs) -> "CostEstimate": ...

@runtime_checkable
class Parser(Protocol):
    def parse(self, source: str) -> "ParseResult": ...

@runtime_checkable
class ExchangeRateProvider(Protocol):
    def get_rate(self, date: date, from_currency: str, to_currency: str) -> Decimal: ...
```

### 1.3 Pydantic Models (`src/dburnrate/core/models.py`)

```python
from pydantic import BaseModel, ConfigDict
from typing import Literal

class OperationInfo(BaseModel):
    name: str      # Join, Merge, Window, etc.
    kind: str      # CROSS, LEFT, INNER, etc.
    weight: float  # From complexity weight table

class QueryProfile(BaseModel):
    sql: str
    dialect: str = "databricks"
    operations: list[OperationInfo]
    tables: list[str]
    complexity_score: float

class ClusterConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False

class PricingInfo(BaseModel):
    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"

class CostEstimate(BaseModel):
    estimated_dbu: float
    estimated_cost_usd: float | None = None
    estimated_cost_eur: float | None = None
    confidence: Literal["low", "medium", "high"] = "low"
    breakdown: dict[str, float] = {}
    warnings: list[str] = []

class ClusterRecommendation(BaseModel):
    current_config: ClusterConfig
    recommended_config: ClusterConfig
    bottleneck: list[str]  # cpu_bound, memory_bound, io_bound, etc.
    estimated_savings_pct: float
    confidence: Literal["low", "medium", "high"]
    reason: str
```

### 1.4 Configuration (`src/dburnrate/core/config.py`)

Supports both TOML file and programmatic class:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pathlib import Path
import tomli

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DBURNRATE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
    workspace_url: str | None = None
    token: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "embedded"  # "embedded" | "live"
    
    @classmethod
    def from_toml(cls, path: Path) -> "Settings":
        with open(path, "rb") as f:
            data = tomli.load(f)
        return cls(**data.get("dburnrate", {}))

# Alternative: programmatic config class
@dataclass(frozen=True)
class Config:
    workspace_url: str | None = None
    token: str | None = None
    target_currency: str = "USD"
    pricing_source: str = "embedded"
    
    def to_settings(self) -> Settings:
        return Settings(
            workspace_url=self.workspace_url,
            token=self.token,
            target_currency=self.target_currency,
            pricing_source=self.pricing_source,
        )
```

### 1.5 Pricing (`src/dburnrate/core/pricing.py`)

```python
from decimal import Decimal
from dataclasses import dataclass

# Embedded Azure Premium pricing (USD)
AZURE_DBU_RATES = {
    "JOBS_COMPUTE": Decimal("0.30"),
    "ALL_PURPOSE": Decimal("0.55"),
    "SERVERLESS_JOBS": Decimal("0.45"),
    "SERVERLESS_NOTEBOOKS": Decimal("0.95"),
    "SQL_CLASSIC": Decimal("0.22"),
    "SQL_PRO": Decimal("0.55"),
    "SQL_SERVERLESS": Decimal("0.70"),
    "DLT_CORE": Decimal("0.30"),
    "DLT_PRO": Decimal("0.38"),
    "DLT_ADVANCED": Decimal("0.54"),
}

# DBU per hour per instance type (Azure)
AZURE_INSTANCE_DBU = {
    "Standard_DS3_v2": 0.75,
    "Standard_DS4_v2": 1.50,
    "Standard_D8s_v3": 2.00,
    "Standard_D16s_v3": 4.00,
    "Standard_D32s_v3": 8.00,
    "Standard_D64s_v3": 12.00,
}

PHOTON_MULTIPLIER_AZURE = Decimal("2.5")

def get_dbu_rate(sku_name: str) -> Decimal:
    """Get DBU rate for a given SKU."""
    rate = AZURE_DBU_RATES.get(sku_name.upper())
    if rate is None:
        raise PricingError(f"Unknown SKU: {sku_name}")
    return rate

def compute_cost_usd(dbu: float, sku_name: str) -> Decimal:
    """Compute USD cost from DBU count and SKU."""
    rate = get_dbu_rate(sku_name)
    return Decimal(str(dbu)) * rate

def apply_photon(dbu: Decimal, enabled: bool) -> Decimal:
    """Apply Photon multiplier."""
    if not enabled:
        return dbu
    return dbu * PHOTON_MULTIPLIER_AZURE

# EUR conversion
def usd_to_eur(usd_amount: Decimal, rate: Decimal = Decimal("0.92")) -> Decimal:
    """Convert USD to EUR using provided exchange rate."""
    return usd_amount * rate
```

### 1.6 Exchange Rate Provider (`src/dburnrate/core/exchange.py`)

```python
import requests
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
from .exceptions import PricingError
from .protocols import ExchangeRateProvider

class FrankfurterProvider:
    """Free, no-API-key exchange rate provider using ECB data."""
    
    BASE_URL = "https://api.frankfurter.dev/v1"
    
    @lru_cache(maxsize=30)
    def get_rate(self, target_date: date, from_curr: str, to_curr: str) -> Decimal:
        if from_curr == to_curr:
            return Decimal("1")
        
        # ECB doesn't publish on weekends; use last available
        if target_date.weekday() >= 5:
            target_date = target_date - timedelta(days=target_date.weekday() - 4)
        
        try:
            resp = requests.get(
                f"{self.BASE_URL}/{target_date.isoformat()}",
                params={"base": from_curr, "symbols": to_curr},
                timeout=10,
            )
            resp.raise_for_status()
            rates = resp.json()["rates"]
            return Decimal(str(rates[to_curr]))
        except Exception as e:
            raise PricingError(f"Failed to get exchange rate: {e}") from e
    
    def get_rate_for_amount(self, amount: Decimal, target_date: date, 
                           from_curr: str = "USD", to_curr: str = "EUR") -> Decimal:
        """Convert an amount from one currency to another."""
        rate = self.get_rate(target_date, from_curr, to_curr)
        return amount * rate

class FixedRateProvider:
    """User-supplied fixed exchange rate."""
    
    def __init__(self, rate: Decimal):
        self._rate = rate
    
    def get_rate(self, target_date: date, from_curr: str, to_curr: str) -> Decimal:
        if from_curr == to_curr:
            return Decimal("1")
        return self._rate
```

---

## Phase 2: SQL Parser

### 2.1 `src/dburnrate/parsers/sql.py`

```python
from sqlglot import parse_one, exp
from sqlglot.dialects.dialect import Dialect
from collections import Counter
from ...core.models import OperationInfo
from ...core.exceptions import ParseError
from ...core._compat import require

# Weight table from CONCEPT.md
OPERATION_WEIGHTS = {
    "MERGE": 20,
    "CROSS_JOIN": 50,
    "SHUFFLE_JOIN": 10,
    "GROUP_BY": 8,
    "WINDOW": 8,
    "COLLECT": 25,
    "PYTHON_UDF": 15,
    "PANDAS_UDF": 5,
    "ORDER_BY": 7,
    "DISTINCT": 6,
    "SUBQUERY": 3,
    "CTE": 2,
}

def parse_sql(sql: str, dialect: str = "databricks") -> exp.Expression:
    """Parse SQL string into AST."""
    if not sql or not sql.strip():
        raise ParseError("Empty SQL string")
    
    try:
        return parse_one(sql, dialect=dialect)
    except Exception as e:
        raise ParseError(f"Failed to parse SQL: {e}") from e

def extract_tables(sql: str, dialect: str = "databricks") -> list[str]:
    """Extract all table references from SQL."""
    ast = parse_sql(sql, dialect)
    tables = []
    for table in ast.find_all(exp.Table):
        # Handle 3-level names (catalog.schema.table)
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        parts.append(table.name)
        tables.append(".".join(parts))
    return list(dict.fromkeys(tables))  # unique, preserve order

def detect_operations(sql: str, dialect: str = "databricks") -> list[OperationInfo]:
    """Detect cost-affecting operations in SQL."""
    ast = parse_sql(sql, dialect)
    operations = []
    
    for node in ast.walk():
        if isinstance(node, exp.Merge):
            operations.append(OperationInfo(name="Merge", kind="", weight=20))
        
        elif isinstance(node, exp.Join):
            kind = node.args.get("kind", "")
            if kind.upper() == "CROSS":
                operations.append(OperationInfo(name="Join", kind="CROSS", weight=50))
            else:
                operations.append(OperationInfo(name="Join", kind=kind or "INNER", weight=10))
        
        elif isinstance(node, exp.Group):
            operations.append(OperationInfo(name="GroupBy", kind="", weight=8))
        
        elif isinstance(node, exp.Window):
            operations.append(OperationInfo(name="Window", kind="", weight=8))
        
        elif isinstance(node, exp.Order):
            operations.append(OperationInfo(name="OrderBy", kind="", weight=7))
        
        elif isinstance(node, exp.Distinct):
            operations.append(OperationInfo(name="Distinct", kind="", weight=6))
        
        elif isinstance(node, exp.Subquery):
            operations.append(OperationInfo(name="Subquery", kind="", weight=3))
        
        elif isinstance(node, exp.CTE):
            operations.append(OperationInfo(name="CTE", kind="", weight=2))
    
    return operations

def compute_complexity(sql: str, dialect: str = "databricks") -> float:
    """Compute total complexity score from operations."""
    ops = detect_operations(sql, dialect)
    return sum(op.weight for op in ops)

def analyze_query(sql: str, dialect: str = "databricks") -> QueryProfile:
    """Full query analysis."""
    ops = detect_operations(sql, dialect)
    tables = extract_tables(sql, dialect)
    complexity = compute_complexity(sql, dialect)
    
    return QueryProfile(
        sql=sql,
        dialect=dialect,
        operations=ops,
        tables=tables,
        complexity_score=complexity,
    )
```

### 2.2 `src/dburnrate/parsers/pyspark.py`

```python
import ast
from ...core.models import OperationInfo
from ...core.exceptions import ParseError

# PySpark method weights
PYSPARK_WEIGHTS = {
    "groupBy": 8,
    "groupby": 8,
    "join": 10,
    "crossJoin": 50,
    "collect": 25,
    "toPandas": 25,
    "repartition": 5,
    "repartition(1)": 15,  # Anti-pattern
    "write": 3,
    "writeStream": 8,
}

# Decorator weights
DECORATOR_WEIGHTS = {
    "udf": 15,
    "pandas_udf": 5,
    "pandas_udf(pandas_udf_type())": 5,
}

def analyze_pyspark(source: str) -> list[OperationInfo]:
    """Analyze PySpark code for cost-affecting operations."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise ParseError(f"Failed to parse PySpark: {e}") from e
    
    operations = []
    visitor = PySparkVisitor()
    visitor.visit(tree)
    return visitor.operations

class PySparkVisitor(ast.NodeVisitor):
    def __init__(self):
        self.operations: list[OperationInfo] = []
        self._in_udf = False
        self._udf_type = None
    
    def visit_Call(self, node: ast.Call):
        # Method calls
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            
            if method_name in PYSPARK_WEIGHTS:
                weight = PYSPARK_WEIGHTS[method_name]
                # Check for repartition(1) anti-pattern
                if method_name == "repartition" and node.args:
                    if isinstance(node.args[0], ast.Constant):
                        if node.args[0].value == 1:
                            weight = 15
                self.operations.append(OperationInfo(
                    name=method_name,
                    kind="",
                    weight=weight,
                ))
            
            # spark.sql() - extract embedded SQL
            if method_name == "sql" and isinstance(node.func.value, ast.Name):
                if node.func.value.id == "spark":
                    # Would route to SQL parser
                    pass
        
        self.generic_visit(node)
    
    def visit_FunctionDef(self, node: ast.FunctionDef):
        # Check decorators for UDF type
        for decorator in node.decorator_list:
            dec_name = self._get_decorator_name(decorator)
            if dec_name in DECORATOR_WEIGHTS:
                self.operations.append(OperationInfo(
                    name=f"@{dec_name}",
                    kind="",
                    weight=DECORATOR_WEIGHTS[dec_name],
                ))
        self.generic_visit(node)
    
    def _get_decorator_name(self, decorator) -> str:
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Call):
            return self._get_decorator_name(decorator.func)
        elif isinstance(decorator, ast.Attribute):
            return decorator.attr
        return ""
```

### 2.3 `src/dburnrate/parsers/antipatterns.py`

```python
from dataclasses import dataclass
from enum import Enum
from typing import Literal

class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

@dataclass
class AntiPattern:
    name: str
    severity: Severity
    description: str
    suggestion: str
    line_number: int | None = None

# Anti-pattern detection rules
def detect_antipatterns(source: str, language: str = "sql") -> list[AntiPattern]:
    if language == "sql":
        return _detect_sql_antipatterns(source)
    elif language == "pyspark":
        return _detect_pyspark_antipatterns(source)
    return []

def _detect_sql_antipatterns(sql: str) -> list[AntiPattern]:
    patterns = []
    
    if "CROSS JOIN" in sql.upper():
        patterns.append(AntiPattern(
            name="cross_join",
            severity=Severity.WARNING,
            description="CROSS JOIN creates O(n*m) rows",
            suggestion="Use INNER JOIN with explicit ON clause",
        ))
    
    if "SELECT *" in sql.upper() and "LIMIT" not in sql.upper():
        patterns.append(AntiPattern(
            name="select_star_no_limit",
            severity=Severity.INFO,
            description="SELECT * without LIMIT may return large result sets",
            suggestion="Add LIMIT clause or select specific columns",
        ))
    
    if "ORDER BY" in sql.upper() and "LIMIT" not in sql.upper():
        patterns.append(AntiPattern(
            name="order_by_no_limit",
            severity=Severity.WARNING,
            description="ORDER BY without LIMIT forces global sort",
            suggestion="Add LIMIT or remove ORDER BY if not needed",
        ))
    
    return patterns

def _detect_pyspark_antipatterns(source: str) -> list[AntiPattern]:
    patterns = []
    
    if ".collect()" in source and ".limit(" not in source:
        patterns.append(AntiPattern(
            name="collect_without_limit",
            severity=Severity.ERROR,
            description="collect() without limit() can OOM the driver",
            suggestion="Add .limit(n).collect() or use .take()",
        ))
    
    if "@udf" in source and "@pandas_udf" not in source:
        patterns.append(AntiPattern(
            name="python_udf",
            severity=Severity.WARNING,
            description="Python UDF has 10-100x overhead vs Pandas UDF",
            suggestion="Use @pandas_udf for vectorized operations",
        ))
    
    if ".repartition(1)" in source:
        patterns.append(AntiPattern(
            name="repartition_one",
            severity=Severity.WARNING,
            description="repartition(1) causes single partition bottleneck",
            suggestion="Use larger partition count or remove",
        ))
    
    if ".toPandas()" in source:
        patterns.append(AntiPattern(
            name="toPandas",
            severity=Severity.WARNING,
            description="toPandas() brings all data to driver",
            suggestion="Use Koalas/Pandas API on Spark or filter first",
        ))
    
    return patterns
```

### 2.4 `src/dburnrate/parsers/notebooks.py`

```python
from pathlib import Path
import json
import zipfile
from dataclasses import dataclass
from typing import Literal

@dataclass
class NotebookCell:
    language: Literal["sql", "python", "scala", "markdown"]
    source: str
    cell_index: int

def parse_notebook(path: Path) -> list[NotebookCell]:
    """Parse .ipynb Jupyter notebook."""
    with open(path) as f:
        nb = json.load(f)
    
    cells = []
    for i, cell in enumerate(nb.get("cells", [])):
        source = "".join(cell.get("source", []))
        cell_type = cell.get("cell_type", "code")
        
        if cell_type == "markdown":
            continue
        
        # Detect language from metadata or magic commands
        language = _detect_language(cell.get("metadata", {}), source)
        
        cells.append(NotebookCell(
            language=language,
            source=source,
            cell_index=i,
        ))
    
    return cells

def parse_dbc(path: Path) -> list[NotebookCell]:
    """Parse .dbc Databricks archive."""
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
                
                cells.append(NotebookCell(
                    language=language,
                    source=source,
                    cell_index=i,
                ))
    
    return cells

def _detect_language(metadata: dict, source: str) -> str:
    # Check magic commands
    if source.lstrip().startswith("%sql"):
        return "sql"
    elif source.lstrip().startswith("%python"):
        return "python"
    elif source.lstrip().startswith("%scala"):
        return "scala"
    
    # Check metadata
    kernel = metadata.get("kernelspec", {}).get("name", "")
    if "python" in kernel.lower():
        return "python"
    elif "scala" in kernel.lower():
        return "scala"
    
    return "python"  # default

def _detect_language_from_dbc(cmd: dict) -> str:
    # DBC format language info
    language = cmd.get("language", "").lower()
    if language == "sql":
        return "sql"
    elif language == "scala":
        return "scala"
    return "python"
```

---

## Phase 3: Static Cost Estimator

### 3.1 `src/dburnrate/estimators/static.py`

```python
from decimal import Decimal
from ...core.models import CostEstimate, ClusterConfig, QueryProfile
from ...core.pricing import get_dbu_rate, apply_photon, usd_to_eur
from ...core.exchange import FrankfurterProvider
from ...parsers.sql import analyze_query
from ...parsers.pyspark import analyze_pyspark
from datetime import date

class CostEstimator:
    def __init__(
        self,
        cluster: ClusterConfig | None = None,
        target_currency: str = "USD",
        exchange_rate_provider: FrankfurterProvider | None = None,
    ):
        self.cluster = cluster or ClusterConfig()
        self.target_currency = target_currency
        self.exchange_rate = exchange_rate_provider or FrankfurterProvider()
    
    def estimate(
        self,
        query: str,
        language: str = "sql",
        cluster: ClusterConfig | None = None,
    ) -> CostEstimate:
        """Estimate cost for a query."""
        cluster = cluster or self.cluster
        
        # Parse and analyze
        if language == "sql":
            profile = analyze_query(query)
            complexity = profile.complexity_score
        else:
            ops = analyze_pyspark(query)
            complexity = sum(op.weight for op in ops)
        
        # Estimate DBU (simplified formula)
        # Base: complexity * cluster_factor * time_estimate
        cluster_factor = cluster.num_workers * cluster.dbu_per_hour
        time_estimate = complexity / 100  # rough proxy: higher complexity = longer
        
        estimated_dbu = complexity * cluster_factor * time_estimate
        
        # Apply Photon
        if cluster.photon_enabled:
            # Photon is 2.5x DBU but 2.7x faster on average
            estimated_dbu = estimated_dbu * 2.5 / 2.7
        
        # Compute cost
        sku = self._infer_sku(cluster)
        estimated_cost_usd = float(Decimal(str(estimated_dbu)) * get_dbu_rate(sku))
        
        # Convert to target currency
        estimated_cost_eur = None
        if self.target_currency != "USD":
            estimated_cost_eur = self.exchange_rate.get_rate_for_amount(
                Decimal(str(estimated_cost_usd)),
                date.today(),
                "USD",
                self.target_currency,
            )
        
        return CostEstimate(
            estimated_dbu=round(estimated_dbu, 2),
            estimated_cost_usd=round(estimated_cost_usd, 4),
            estimated_cost_eur=round(float(estimated_cost_eur), 4) if estimated_cost_eur else None,
            confidence=self._compute_confidence(profile),
            breakdown={"complexity": complexity, "cluster_factor": cluster_factor},
            warnings=[],
        )
    
    def _infer_sku(self, cluster: ClusterConfig) -> str:
        # Simple inference based on instance type
        if "Standard_D" in cluster.instance_type:
            return "ALL_PURPOSE"
        return "JOBS_COMPUTE"
    
    def _compute_confidence(self, profile: QueryProfile) -> str:
        if not profile.tables:
            return "low"
        if profile.complexity_score > 50:
            return "medium"
        return "high"

def estimate_cost(
    query: str,
    cluster: ClusterConfig | None = None,
    language: str = "sql",
    target_currency: str = "USD",
) -> CostEstimate:
    """Convenience function."""
    estimator = CostEstimator(cluster=cluster, target_currency=target_currency)
    return estimator.estimate(query, language=language)
```

### 3.2 `src/dburnrate/estimators/whatif.py`

```python
from decimal import Decimal
from ...core.models import CostEstimate, ClusterConfig

# Speedup factors from benchmarks
SPEEDUP_FACTORS = {
    "complex_join": 2.7,
    "aggregation": 4.0,
    "window": 2.5,
    "simple_insert": 1.0,  # No speedup
}

PHOTON_COST_MULTIPLIER = Decimal("2.5")

def apply_photon_scenario(
    estimate: CostEstimate,
    query_type: str = "complex_join",
) -> CostEstimate:
    """Model cost impact of enabling Photon."""
    speedup = SPEEDUP_FACTORS.get(query_type, 2.0)
    
    # Photon costs 2.5x more DBU but is faster
    new_dbu = estimate.estimated_dbu * float(PHOTON_COST_MULTIPLIER) / speedup
    new_cost = estimate.estimated_cost_usd * float(PHOTON_COST_MULTIPLIER) / speedup
    
    savings_pct = (estimate.estimated_cost_usd - new_cost) / estimate.estimated_cost_usd * 100
    
    warnings = estimate.warnings.copy()
    if savings_pct < 0:
        warnings.append(f"Photon increases cost by {-savings_pct:.1f}% for {query_type}")
    
    return CostEstimate(
        estimated_dbu=round(new_dbu, 2),
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "photon": True, "speedup": speedup},
        warnings=warnings,
    )

def apply_cluster_resize(
    estimate: CostEstimate,
    current_cluster: ClusterConfig,
    new_cluster: ClusterConfig,
) -> CostEstimate:
    """Model cost impact of changing cluster size."""
    current_factor = current_cluster.num_workers * current_cluster.dbu_per_hour
    new_factor = new_cluster.num_workers * new_cluster.dbu_per_hour
    
    ratio = new_factor / current_factor
    new_cost = estimate.estimated_cost_usd * ratio
    
    savings_pct = (estimate.estimated_cost_usd - new_cost) / estimate.estimated_cost_usd * 100
    
    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "cluster_resize_ratio": ratio},
        warnings=[f"Estimated savings: {savings_pct:.1f}%"],
    )

def apply_serverless_migration(
    estimate: CostEstimate,
    current_sku: str = "ALL_PURPOSE",
    utilization_pct: float = 50.0,
) -> CostEstimate:
    """Compare classic vs serverless costs."""
    # Serverless rates (Azure)
    serverless_rates = {
        "ALL_PURPOSE": 0.95,
        "JOBS_COMPUTE": 0.45,
        "SQL_PRO": 0.70,
    }
    
    classic_rates = {
        "ALL_PURPOSE": 0.55,
        "JOBS_COMPUTE": 0.30,
        "SQL_PRO": 0.55,
    }
    
    serverless_rate = serverless_rates.get(current_sku, 0.70)
    classic_rate = classic_rates.get(current_sku, 0.55)
    
    # If utilization < 30%, serverless is cheaper (no idle cost)
    if utilization_pct < 30:
        # Serverless: higher rate but no idle
        ratio = serverless_rate / classic_rate
    else:
        # Classic: lower rate but may have idle time
        effective_classic = classic_rate * (utilization_pct / 100)
        ratio = serverless_rate / effective_classic
    
    new_cost = estimate.estimated_cost_usd * ratio
    
    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="low",
        breakdown={**estimate.breakdown, "serverless": True, "utilization": utilization_pct},
        warnings=[f"Serverless is {'cheaper' if ratio < 1 else 'more expensive'} at {utilization_pct}% utilization"],
    )
```

---

## Phase 4: CLI

### 4.1 `src/dburnrate/cli/main.py`

```python
import typer
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from pathlib import Path
from datetime import date

from ..core.config import Settings, Config
from ..core.models import ClusterConfig
from ..core.pricing import AZURE_INSTANCE_DBU
from ..estimators.static import CostEstimator
from ..estimators.whatif import apply_photon_scenario, apply_serverless_migration
from ..parsers.notebooks import parse_notebook, parse_dbc

app = typer.Typer(help="dburnrate - Pre-execution cost estimation for Databricks")
console = Console()

@app.command()
def estimate(
    query: str = typer.Argument(..., help="SQL query or path to .sql/.ipynb/.dbc file"),
    cluster_type: str = typer.Option("Standard_DS3_v2", "--cluster", "-c", help="Instance type"),
    workers: int = typer.Option(2, "--workers", "-w", help="Number of workers"),
    photon: bool = typer.Option(False, "--photon", help="Enable Photon"),
    currency: str = typer.Option("USD", "--currency", help="Output currency (USD/EUR)"),
    output: str = typer.Option("table", "--output", "-o", help="Output format: table, json, text"),
):
    """Estimate cost for a SQL query or notebook."""
    
    # Load query from file if path
    query_path = Path(query)
    if query_path.exists():
        if query_path.suffix == ".sql":
            query = query_path.read_text()
        elif query_path.suffix == ".ipynb":
            cells = parse_notebook(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")
        elif query_path.suffix == ".dbc":
            cells = parse_dbc(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")
    
    # Build cluster config
    dbu_rate = AZURE_INSTANCE_DBU.get(cluster_type, 0.75)
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=dbu_rate,
        photon_enabled=photon,
    )
    
    # Estimate
    estimator = CostEstimator(cluster=cluster, target_currency=currency)
    result = estimator.estimate(query)
    
    # Output
    if output == "json":
        import json
        console.print(json.dumps(result.model_dump(), indent=2))
    elif output == "text":
        console.print(f"Estimated DBU: {result.estimated_dbu}")
        console.print(f"Estimated Cost ({currency}): ${getattr(result, f'estimated_cost_{currency.lower()}') or result.estimated_cost_usd}")
        console.print(f"Confidence: {result.confidence}")
    else:
        table = Table(title="Cost Estimate")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Estimated DBU", str(result.estimated_dbu))
        cost_col = f"estimated_cost_{currency.lower()}"
        cost_val = getattr(result, cost_col, None) or result.estimated_cost_usd
        table.add_row(f"Estimated Cost ({currency})", f"${cost_val}")
        table.add_row("Confidence", result.confidence)
        console.print(table)

@app.command()
def whatif(
    query: str = typer.Argument(..., help="SQL query to model scenarios for"),
    scenario: str = typer.Option(..., "--scenario", "-s", 
                                  help="Scenario: photon, serverless, resize"),
    cluster_type: str = typer.Option("Standard_DS3_v2", "--cluster", "-c"),
    workers: int = typer.Option(2, "--workers", "-w"),
    utilization: float = typer.Option(50.0, "--utilization", help="Cluster utilization %"),
):
    """Run what-if scenario modeling."""
    
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=AZURE_INSTANCE_DBU.get(cluster_type, 0.75),
    )
    
    estimator = CostEstimator(cluster=cluster)
    base = estimator.estimate(query)
    
    if scenario == "photon":
        result = apply_photon_scenario(base, "complex_join")
    elif scenario == "serverless":
        result = apply_serverless_migration(base, "ALL_PURPOSE", utilization)
    else:
        console.print(f"[red]Unknown scenario: {scenario}[/red]")
        raise typer.Exit(1)
    
    console.print(f"[bold]Base Cost:[/bold] ${base.estimated_cost_usd:.4f}")
    console.print(f"[bold]With {scenario}:[/bold] ${result.estimated_cost_usd:.4f}")
    
    if result.warnings:
        for w in result.warnings:
            console.print(f"[yellow]Warning:[/yellow] {w}")

@app.command()
def version():
    """Print version info."""
    from .. import __version__
    console.print(f"dburnrate v{__version__}")

if __name__ == "__main__":
    app()
```

---

## Phase 5: System Tables (Post-MVP)

### 5.1 `src/dburnrate/tables/billing.py`

```python
# Query system.billing.usage and system.billing.list_prices
# Requires live Databricks connectivity

def get_usage_by_pipeline(
    workspace_id: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Get DLT/DLT pipeline costs from billing tables."""
    query = f"""
    SELECT
        usage_date,
        sku_name,
        SUM(usage_quantity) as total_dbus,
        product_features.dlt_tier,
        usage_metadata.dlt_pipeline_id,
        usage_metadata.dlt_update_id
    FROM system.billing.usage
    WHERE billing_origin_product = 'DLT'
      AND workspace_id = '{workspace_id}'
      AND usage_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY ALL
    """
    return _execute_query(query)

def join_with_prices(usage_df) -> "DataFrame":
    """Join usage with list_prices for cost calculation."""
    # Implementation using system.billing.list_prices
    pass
```

### 5.2 `src/dburnrate/tables/cluster_rightsizing.py`

```python
# Query system.compute.node_timeline and system.compute.clusters
# Classification and recommendation logic

from dataclasses import dataclass

@dataclass
class ClusterMetrics:
    cluster_id: str
    avg_cpu: float
    p95_cpu: float
    avg_mem: float
    max_mem: float
    max_swap: float
    avg_io_wait: float

def get_cluster_metrics(cluster_id: str, days: int = 30) -> ClusterMetrics:
    """Query node_timeline for cluster utilization metrics."""
    query = f"""
    SELECT
        cluster_id,
        AVG(cpu_user_percent + cpu_system_percent) as avg_cpu,
        PERCENTILE(cpu_user_percent + cpu_system_percent, 0.95) as p95_cpu,
        AVG(mem_used_percent) as avg_mem,
        MAX(mem_used_percent) as max_mem,
        MAX(mem_swap_percent) as max_swap,
        AVG(cpu_wait_percent) as avg_io_wait
    FROM system.compute.node_timeline
    WHERE cluster_id = '{cluster_id}'
      AND start_time >= current_date() - INTERVAL {days} DAYS
    GROUP BY cluster_id
    """
    return _execute_query(query)

def classify_bottleneck(metrics: ClusterMetrics) -> list[str]:
    """Classify cluster bottleneck from metrics."""
    bottlenecks = []
    
    if metrics.p95_cpu > 85:
        bottlenecks.append("cpu_bound")
    if metrics.max_swap > 0.1 or metrics.avg_mem > 85:
        bottlenecks.append("memory_bound")
    if metrics.avg_io_wait > 15:
        bottlenecks.append("io_bound")
    if metrics.avg_cpu < 20 and metrics.avg_mem < 40:
        bottlenecks.append("over_provisioned")
    
    return bottlenecks or ["balanced"]

def recommend_instance_family(bottlenecks: list[str], cloud: str = "AZURE") -> str:
    """Map bottleneck to instance family."""
    if cloud == "AZURE":
        if "cpu_bound" in bottlenecks:
            return "Standard_Fs_v2"  # Compute-optimized
        if "memory_bound" in bottlenecks:
            return "Standard_Es_v5"  # Memory-optimized
        if "io_bound" in bottlenecks:
            return "Standard_Ls_v3"  # Storage-optimized
    
    return "Standard_Ds_v2"  # General purpose default
```

---

## Phase 6: Forecasting (Post-MVP)

### 6.1 `src/dburnrate/forecast/prophet.py`

```python
# Optional dburnrate[forecasting] extra
# Uses Prophet for time-series cost forecasting

from prophet import Prophet
import pandas as pd

def forecast_costs(
    usage_df: pd.DataFrame,
    periods: int = 30,
    freq: str = "D",
) -> pd.DataFrame:
    """Forecast daily costs using Prophet."""
    
    df = usage_df.rename(columns={"usage_date": "ds", "total_cost": "y"})
    
    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True,
    )
    model.fit(df)
    
    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)
    
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
```

---

## Testing Strategy

### Test Organization

```
tests/
├── conftest.py              # Root fixtures, marker auto-application
├── unit/
│   ├── core/
│   │   ├── test_models.py   # Pydantic validation
│   │   ├── test_config.py   # Config loading
│   │   ├── test_pricing.py  # Pricing lookup
│   │   └── test_exchange.py # Exchange rate provider
│   ├── parsers/
│   │   ├── test_sql.py      # SQL parsing, operation detection
│   │   ├── test_pyspark.py  # PySpark analysis
│   │   ├── test_antipatterns.py
│   │   └── test_notebooks.py
│   └── estimators/
│       ├── test_static.py   # Cost estimation
│       └── test_whatif.py   # Scenario modeling
├── integration/
│   └── (requires live Databricks)
└── data/
    ├── sql_samples/
    │   ├── simple_select.sql
    │   ├── cross_join.sql
    │   ├── merge_into.sql
    │   └── window_functions.sql
    └── fixtures/
        └── pricing_snapshot.json
```

### Running Tests

```bash
# All unit tests
uv run pytest -m unit

# With coverage
uv run pytest --cov --cov-report=term-missing

# Parallel execution
uv run pytest -n auto

# Lint
uv run ruff check src/ tests/
uv run bandit -c pyproject.toml -r src/
uv run xenon --max-absolute B --max-modules B --max-average A src/

# Docstring coverage
uv run interrogate src/

# Security audit
uv run pip-audit
```

---

## Execution Order Summary

| Phase | Module | Tests First? | Key Output |
|-------|--------|:---:|------------|
| 0 | Scaffolding + pyproject.toml | N/A | Working project |
| 1 | `core/exceptions.py` | Yes | Custom exception hierarchy |
| 1 | `core/models.py` | Yes | Pydantic models |
| 1 | `core/protocols.py` | Yes | Protocol contracts |
| 1 | `core/config.py` | Yes | Config from TOML + class |
| 1 | `core/pricing.py` | Yes | Pricing lookup + Photon |
| 1 | `core/exchange.py` | Yes | EUR conversion |
| 2 | `parsers/sql.py` | Yes | SQL → AST → operations |
| 2 | `parsers/pyspark.py` | Yes | PySpark analysis |
| 2 | `parsers/antipatterns.py` | Yes | Anti-pattern detection |
| 2 | `parsers/notebooks.py` | Yes | Notebook parsing |
| 3 | `estimators/static.py` | Yes | Core cost estimation |
| 3 | `estimators/whatif.py` | Yes | Scenario modeling |
| 4 | `cli/main.py` | Yes | CLI interface |
| 5 | `tables/*` (Post-MVP) | — | System table queries |
| 6 | `forecast/*` (Post-MVP) | — | Prophet forecasting |

---

## Key Design Decisions

1. **Config via class + TOML file**: Programmatic `Config` class takes precedence over `.env`/env vars. TOML file support via `Settings.from_toml()`.

2. **Currency**: All internal calculations in USD. EUR conversion via Frankfurter API (free, ECB-backed). Target currency set at estimator creation time.

3. **SDP/DLT**: Post-MVP. Would add parser for `@dp.materialized_view`, `@dp.table`, `CREATE STREAMING TABLE`, and tier detection from code.

4. **Cluster right-sizing**: Post-MVP. Requires live system table access. Would use `system.compute.node_timeline` for utilization metrics and `system.compute.clusters` for current config.

5. **MVP scope**: Core SQL/PySpark parsing → complexity scoring → CLI with what-if scenarios. System tables in Phase 5.

---

## Verification Commands

After each phase completes:

```bash
uv run pytest -m unit -v
uv run ruff check src/
uv run ruff format --check src/
uv run bandit -c pyproject.toml -r src/
uv run interrogate src/ -v
```