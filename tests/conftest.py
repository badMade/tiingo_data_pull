"""Test configuration shared across the suite."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="module", params=["asyncio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """Force AnyIO tests to run only on the asyncio backend."""

    return request.param
