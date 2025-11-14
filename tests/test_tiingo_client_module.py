"""Tests for the lightweight ``tiingo_client`` module."""
from __future__ import annotations

from datetime import date
from typing import List

import pytest

import tiingo_client


class DummyResponse:
    def __init__(self, payload: List[dict] | dict | str, ok: bool = True, status_code: int = 200) -> None:
        self._payload = payload
        self.ok = ok
        self.status_code = status_code
        self.text = str(payload) if not ok else ""

    def json(self) -> List[dict] | dict:
        return self._payload


class DummySession:
    def __init__(self, payload: List[dict] | dict | str, ok: bool = True, status_code: int = 200) -> None:
        self._payload = payload
        self._ok = ok
        self._status_code = status_code
        self.calls = []

    def get(self, url: str, *, params: dict, headers: dict = None, timeout: int):
        self.calls.append((url, params, headers, timeout))
        return DummyResponse(self._payload, self._ok, self._status_code)


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
    assert session.calls[0][2]["Authorization"] == "Token demo-key"


def test_fetch_prices_requires_key(monkeypatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        tiingo_client.fetch_prices(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=DummySession([]),
        )


def test_fetch_prices_raises_on_401_unauthorized(monkeypatch) -> None:
    session = DummySession("Unauthorized", ok=False, status_code=401)
    monkeypatch.setenv("TIINGO_API_KEY", "invalid-key")
    with pytest.raises(tiingo_client.TiingoApiError) as exc_info:
        tiingo_client.fetch_prices(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=session,
        )
    assert "401" in str(exc_info.value)
    assert "AAPL" in str(exc_info.value)


def test_fetch_prices_raises_on_404_not_found(monkeypatch) -> None:
    session = DummySession("Not Found", ok=False, status_code=404)
    monkeypatch.setenv("TIINGO_API_KEY", "demo-key")
    with pytest.raises(tiingo_client.TiingoApiError) as exc_info:
        tiingo_client.fetch_prices(
            "INVALID",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=session,
        )
    assert "404" in str(exc_info.value)
    assert "INVALID" in str(exc_info.value)


def test_fetch_prices_raises_on_500_server_error(monkeypatch) -> None:
    session = DummySession("Internal Server Error", ok=False, status_code=500)
    monkeypatch.setenv("TIINGO_API_KEY", "demo-key")
    with pytest.raises(tiingo_client.TiingoApiError) as exc_info:
        tiingo_client.fetch_prices(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=session,
        )
    assert "500" in str(exc_info.value)
    assert "AAPL" in str(exc_info.value)


def test_fetch_prices_raises_on_non_list_response(monkeypatch) -> None:
    session = DummySession({"error": "unexpected format"})
    monkeypatch.setenv("TIINGO_API_KEY", "demo-key")
    with pytest.raises(tiingo_client.TiingoApiError) as exc_info:
        tiingo_client.fetch_prices(
            "AAPL",
            date(2024, 1, 1),
            date(2024, 1, 2),
            session=session,
        )
    assert "Unexpected" in str(exc_info.value)
    assert "list" in str(exc_info.value)
