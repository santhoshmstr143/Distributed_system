"""
Coordinator module for Sharded-Docstore — Module 6.
"""

from __future__ import annotations

from docstore.coordinator.app import build_coordinator_app
from docstore.coordinator.coordinator import CoordinatorService

__all__ = ["CoordinatorService", "build_coordinator_app"]
