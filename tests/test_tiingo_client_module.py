"""Tests for the lightweight ``tiingo_client`` module."""
from __future__ import annotations

from datetime import date
from typing import List

import pytest

import tiingo_client


class DummyResponse:
    def __init__(self, payload: List[dict]) -> None:
        self._payload = payload
        self.ok = True

    def json(self) -> List[dict]:
        return self._payload


class DummySession:
    def __init__(self, payload: List[dict]) -> None:
        self._payload = payload
        self.calls = []

    def get(self, url: str, *, params: dict, timeout: int):
        self.calls.append((url, params, timeout))
        return DummyResponse(self._payload)


def test_fetch_prices_uses_env_token(monkeypatch) -> None:
    session = DummySession([{"close": 1}])
    monkeypatch.setenv("TIINGO_API_KEY", "demo-key")
    data = tiingo_client.fetch_prices(
        "AAPL",
        date(2024, 1, 1),
        date(2024, 1, 2),
        session=session,
    )
    assert data == [{"close": 1}]
    assert session.calls[0][1]["token"] == "demo-key"


def test_fetch_prices_requires_key(monkeypatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        tiingo_client.fetch_prices(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=DummySession([]),
        )
