"""Unit tests for httpx_hedged._rotation.next_action."""

from __future__ import annotations

from httpx_hedged._rotation import RotateAction, next_action


def test_no_rotation_before_window_elapses() -> None:
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=10.0)
        is RotateAction.NONE
    )
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=29.999)
        is RotateAction.NONE
    )


def test_rotate_between_one_and_two_windows() -> None:
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=30.0)
        is RotateAction.ROTATE
    )
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=59.999)
        is RotateAction.ROTATE
    )


def test_reset_at_or_beyond_two_windows() -> None:
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=60.0)
        is RotateAction.RESET
    )
    assert (
        next_action(window_start=0.0, window_duration=30.0, now=1000.0)
        is RotateAction.RESET
    )
