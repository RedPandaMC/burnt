"""Graph models and helpers consumed by the ``burnt._check`` orchestrator.

After the resolved-graph refactor, ``estimate`` and ``enrich_dlt`` are
the only Python-callable helpers exposed by this package. The previous
``enrich_graph`` is gone — its logic moved into Rust behind
``burnt._engine._resolve_graph`` and is reachable only through
``_check`` per the architectural firewall.
"""

from .enrich import enrich_dlt
from .estimate import estimate
from .model import PipelineGraph, PyGraph, PyNode
from .scaling import ScalingFunction, linear, quadratic, step_failure

__all__ = [
    "PipelineGraph",
    "PyGraph",
    "PyNode",
    "ScalingFunction",
    "enrich_dlt",
    "estimate",
    "linear",
    "quadratic",
    "step_failure",
]
