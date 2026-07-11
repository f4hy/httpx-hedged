"""Unit tests for httpx_hedged.sketch._ddsketch.DDSketch."""

from __future__ import annotations

import math

import pytest

from httpx_hedged.sketch._ddsketch import DDSketch


def test_empty_sketch_quantile_is_nan() -> None:
    sketch = DDSketch()
    assert math.isnan(sketch.quantile(0.5))
    assert sketch.count == 0


def test_relative_accuracy_must_be_in_open_unit_interval() -> None:
    with pytest.raises(ValueError):
        DDSketch(relative_accuracy=0.0)
    with pytest.raises(ValueError):
        DDSketch(relative_accuracy=1.0)


def test_quantile_within_relative_accuracy_on_linear_distribution() -> None:
    accuracy = 0.01
    sketch = DDSketch(relative_accuracy=accuracy)
    values = [float(i) for i in range(1, 1001)]
    for v in values:
        sketch.add(v)

    for q in (0.1, 0.5, 0.9, 0.99):
        rank = math.ceil(q * len(values))
        true_value = values[rank - 1]
        estimate = sketch.quantile(q)
        assert abs(estimate - true_value) / true_value <= accuracy + 1e-9


def test_quantile_boundaries_return_exact_min_max() -> None:
    sketch = DDSketch()
    for v in (5.0, 1.0, 9.0, 3.0):
        sketch.add(v)
    assert sketch.quantile(0.0) == 1.0
    assert sketch.quantile(1.0) == 9.0


def test_negative_and_zero_values() -> None:
    sketch = DDSketch()
    for v in (-10.0, -1.0, 0.0, 0.0, 1.0, 10.0):
        sketch.add(v)
    assert sketch.count == 6
    assert sketch.quantile(0.0) == -10.0
    assert sketch.quantile(1.0) == 10.0
    # median should land near zero given the symmetric distribution
    median = sketch.quantile(0.5)
    assert -1.5 <= median <= 1.5


def test_nan_and_inf_are_ignored() -> None:
    sketch = DDSketch()
    sketch.add(math.nan)
    sketch.add(math.inf)
    sketch.add(-math.inf)
    assert sketch.count == 0


def test_merge_is_exact() -> None:
    a = DDSketch()
    b = DDSketch()
    for v in range(1, 51):
        a.add(float(v))
    for v in range(51, 101):
        b.add(float(v))

    combined = DDSketch()
    for v in range(1, 101):
        combined.add(float(v))

    a.merge(b)
    assert a.count == combined.count == 100
    assert a.quantile(0.5) == combined.quantile(0.5)
    assert a.quantile(0.99) == combined.quantile(0.99)


def test_reset_clears_state() -> None:
    sketch = DDSketch()
    for v in (1.0, 2.0, 3.0):
        sketch.add(v)
    sketch.reset()
    assert sketch.count == 0
    assert math.isnan(sketch.quantile(0.5))
