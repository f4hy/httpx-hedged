"""Sliding-window latency quantile tracking, built on ``ddsketch``/``rddsketch``."""

from __future__ import annotations

from httpx_hedged.sketch._windowed import WindowedSketch

__all__ = ["WindowedSketch"]
