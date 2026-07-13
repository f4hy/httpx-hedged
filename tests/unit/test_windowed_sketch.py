"""Unit tests for httpx_hedged.sketch._windowed.WindowedSketch.

Rotation is lazy/pull-based (see httpx_hedged._rotation) rather than driven
by a background thread or asyncio task, so a service tracking many
endpoints doesn't spin one rotation task per endpoint. These tests exercise
that behavior directly via a monkeypatched clock.
"""

from __future__ import annotations

import math
import threading
from collections.abc import Callable

import pytest

from httpx_hedged.sketch._windowed import WindowedSketch


def test_no_background_thread_or_task_is_created(
    fake_clock: Callable[[float], None],
) -> None:
    before = threading.active_count()
    sketches = [WindowedSketch(window_duration=1.0) for _ in range(50)]
    for sketch in sketches:
        sketch.add(0.1)
        sketch.quantile(0.5)
    assert threading.active_count() == before
    # No lifecycle methods exist to spin up background rotation.
    assert not hasattr(sketches[0], "start")
    assert not hasattr(sketches[0], "start_async")
    assert not hasattr(sketches[0], "stop")


def test_quantile_reflects_data_within_window(
    fake_clock: Callable[[float], None],
) -> None:
    sketch = WindowedSketch(window_duration=30.0)
    for v in range(1, 51):
        sketch.add(float(v))
    fake_clock(15.0)  # still within the first window, no rotation
    assert sketch.quantile(0.0) == 1.0
    assert sketch.quantile(1.0) == 50.0


def test_rotate_keeps_previous_window_data_visible(
    fake_clock: Callable[[float], None],
) -> None:
    sketch = WindowedSketch(window_duration=30.0)
    sketch.add(10.0)
    fake_clock(31.0)  # rotate: old data moves to "previous"
    # quantile still sees the rotated-out sample merged with the (empty)
    # current, within DDSketch's relative-error guarantee (~1%)
    assert sketch.quantile(0.5) == pytest.approx(10.0, rel=0.02)
    sketch.add(20.0)
    q = sketch.quantile(1.0)
    assert q == pytest.approx(20.0, rel=0.02)


def test_idle_beyond_two_windows_hard_resets(
    fake_clock: Callable[[float], None],
) -> None:
    sketch = WindowedSketch(window_duration=10.0)
    sketch.add(5.0)
    fake_clock(25.0)  # more than 2x window_duration has elapsed: reset, not rotate
    assert math.isnan(sketch.quantile(0.5))


def test_add_after_reset_starts_fresh_window(
    fake_clock: Callable[[float], None],
) -> None:
    sketch = WindowedSketch(window_duration=10.0)
    sketch.add(5.0)
    fake_clock(25.0)
    sketch.add(99.0)
    assert sketch.quantile(1.0) == 99.0
