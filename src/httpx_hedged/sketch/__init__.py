"""Sliding-window latency quantile tracking, built on the ``ddsketch`` package."""

from __future__ import annotations

from httpx_hedged.sketch._windowed import WindowedSketch

__all__ = ["WindowedSketch"]
