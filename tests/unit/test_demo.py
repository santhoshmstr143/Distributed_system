"""Unit test to verify that scripts/demo.py functions correctly."""

from __future__ import annotations

import pytest
from scripts.demo import run_demo


@pytest.mark.asyncio
async def test_demo_execution() -> None:
    """Run the complete end-to-end demo and verify clean execution."""
    await run_demo(fast=True)
