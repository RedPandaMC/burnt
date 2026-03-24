"""Scaling functions for cost estimation."""

from __future__ import annotations

from typing import Callable

ScalingFunction = Callable[[float], float]


def linear(input_bytes: float, coefficient: float = 1.0) -> float:
    """Linear scaling: cost proportional to input bytes.

    Args:
        input_bytes: Number of input bytes.
        coefficient: Multiplier for the linear function.

    Returns:
        Estimated operation cost in DBU.
    """
    return input_bytes * coefficient


def linear_with_cliff(
    input_bytes: float,
    cliff_threshold: float = 1e9,
    before_coefficient: float = 1.0,
    after_coefficient: float = 3.0,
) -> float:
    """Linear with cliff: becomes 3x after threshold is exceeded.

    Used to model memory spill behavior.

    Args:
        input_bytes: Number of input bytes.
        cliff_threshold: Bytes at which cliff occurs.
        before_coefficient: Coefficient before threshold.
        after_coefficient: Coefficient after threshold.

    Returns:
        Estimated operation cost in DBU.
    """
    if input_bytes < cliff_threshold:
        return input_bytes * before_coefficient
    return input_bytes * after_coefficient


def quadratic(
    left_bytes: float, right_bytes: float, coefficient: float = 1e-12
) -> float:
    """Quadratic scaling: cost proportional to left * right.

    Used for cartesian joins.

    Args:
        left_bytes: Size of left input.
        right_bytes: Size of right input.
        coefficient: Multiplier for the quadratic function.

    Returns:
        Estimated operation cost in DBU.
    """
    return left_bytes * right_bytes * coefficient


def step_failure(
    threshold: float, coefficient: float = 1.0
) -> Callable[[float], float]:
    """Step failure: works until threshold, then fails.

    Returns a function that returns cost up to threshold,
    then raises an error.

    Args:
        threshold: Maximum input bytes before failure.
        coefficient: Multiplier for the function.

    Returns:
        A scaling function.
    """

    def _step(input_bytes: float) -> float:
        if input_bytes > threshold:
            raise MemoryError(f"Input size {input_bytes} exceeds threshold {threshold}")
        return input_bytes * coefficient

    return _step


def maintenance(
    input_bytes: float, file_count: int, coefficient: float = 1e-8
) -> float:
    """Maintenance scaling: proportional to size + file count.

    Used for OPTIMIZE, VACUUM operations.

    Args:
        input_bytes: Total size in bytes.
        file_count: Number of files in the table.
        coefficient: Multiplier for the function.

    Returns:
        Estimated operation cost in DBU.
    """
    return (input_bytes + file_count * 1e6) * coefficient
