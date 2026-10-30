"""
Test-only crash-point and fault-injection hooks — Module 8.

Controlled by settings.ENABLE_TEST_HOOKS (or ENABLE_TEST_HOOKS environment variable).
In production / non-test environments, hooks are disabled and compile down to no-ops.

Supported named crash points:
- before_wal_append
- after_primary_fsync
- after_follower_fsync_before_ack
- after_follower_ack_before_client_ack
- after_client_ack
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Registry of active hook callbacks: name -> callback(kwargs)
_ACTIVE_HOOKS: dict[str, Callable[..., Any]] = {}
_FORCE_TEST_HOOKS: bool | None = None


class CrashPointError(RuntimeError):
    """Raised when a simulated crash point is triggered."""


def set_test_hooks_enabled(enabled: bool | None) -> None:
    """Explicitly enable or disable test hooks programmatically."""
    global _FORCE_TEST_HOOKS
    _FORCE_TEST_HOOKS = enabled


def is_test_hooks_enabled() -> bool:
    """Return True if test hooks are explicitly enabled via environment or config."""
    if _FORCE_TEST_HOOKS is not None:
        return _FORCE_TEST_HOOKS
    return os.environ.get("ENABLE_TEST_HOOKS", "false").lower() in ("true", "1", "yes")


def register_crash_point(name: str, action: Callable[..., Any] | None = None) -> None:
    """Register a callback or simulated crash at a named crash point."""
    if not is_test_hooks_enabled():
        logger.warning(
            "Attempted to register test hook %r with ENABLE_TEST_HOOKS=false; ignored.",
            name,
        )
        return

    if action is None:

        def _default_crash(**kwargs: Any) -> None:
            raise CrashPointError(f"Simulated crash at hook: {name}")

        _ACTIVE_HOOKS[name] = _default_crash
    else:
        _ACTIVE_HOOKS[name] = action


def clear_crash_points() -> None:
    """Clear all registered crash point hooks."""
    _ACTIVE_HOOKS.clear()


def trigger_hook(name: str, enabled: bool = False, **kwargs: Any) -> Any:
    """Trigger a named hook if test hooks are enabled and the hook is registered."""
    if not enabled and not is_test_hooks_enabled():
        return None

    action = _ACTIVE_HOOKS.get(name)
    if action is not None:
        logger.info("Executing test hook: %s (kwargs=%s)", name, kwargs)
        return action(**kwargs)
    return None
