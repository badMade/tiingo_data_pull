"""Minimal Tiingo REST API wrapper used by the lightweight batch pipeline."""
from __future__ import annotations

from datetime import date
import os
from typing import List, Optional

import requests

API_BASE_URL = "https://api.tiingo.com/tiingo/daily"


class TiingoApiError(RuntimeError):
    """Raised when Tiingo returns a non-successful response."""


def fetch_prices(
    ticker: str,
    start: date,
    end: date,
    *,
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> List[dict]:
    """Fetch end-of-day prices for ``ticker`` from Tiingo.

    Args:
        ticker: Symbol to query.
        start: Inclusive start date.
        end: Inclusive end date.
        api_key: Optional explicit Tiingo API key. Defaults to ``TIINGO_API_KEY``.
        session: Optional :class:`requests.Session` to reuse connections.
        timeout: Timeout in seconds for the HTTP request.

    Returns:
        List of dictionaries mirroring Tiingo's response payload.
    """

    token = api_key or os.getenv("TIINGO_API_KEY")
    if not token:
        raise RuntimeError("TIINGO_API_KEY must be set before calling fetch_prices().")

    payload = {
        "token": token,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }

    http = session or requests.Session()
    response = http.get(
        f"{API_BASE_URL}/{ticker}/prices",
        params=payload,
        timeout=timeout,
    )

    if not response.ok:
        raise TiingoApiError(
            f"Tiingo request failed for {ticker}: {response.status_code} {response.text}",
        )

    data = response.json()
    if not isinstance(data, list):
        raise TiingoApiError(
            "Unexpected Tiingo response payload. Expected a list of price objects.",
        )

    return data
