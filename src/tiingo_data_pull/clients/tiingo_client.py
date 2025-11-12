"""Client for retrieving market data from Tiingo."""
from __future__ import annotations

from datetime import date
from typing import Iterable, List, Optional

import requests

from ..models import PriceBar


class TiingoClient:
    """Lightweight wrapper around the Tiingo REST API."""

    base_url = "https://api.tiingo.com/tiingo/daily"

    def __init__(
        self,
        api_key: str,
        *,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
    ) -> None:
        """Initialise the Tiingo client.

        Args:
            api_key: The Tiingo API key.
            session: Optional :class:`requests.Session` for connection pooling.
            timeout: Request timeout in seconds.
        """

        self._api_key = api_key
        self._session = session or requests.Session()
        self._timeout = timeout

    def fetch_price_history(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[PriceBar]:
        """Fetch end-of-day price history for a ticker.

        Args:
            ticker: The ticker symbol to fetch.
            start_date: Optional start date (inclusive).
            end_date: Optional end date (inclusive).

        Returns:
            A list of :class:`PriceBar` instances sorted by ascending date.
        """

        params = {"token": self._api_key}
        if start_date is not None:
            params["startDate"] = start_date.isoformat()
        if end_date is not None:
            params["endDate"] = end_date.isoformat()

        response = self._session.get(
            f"{self.base_url}/{ticker}/prices",
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload = response.json()

        prices = [PriceBar.from_tiingo_payload(ticker, item) for item in payload]
        prices.sort(key=lambda item: item.date)
        return prices

    def fetch_price_history_bulk(
        self,
        tickers: Iterable[str],
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> dict[str, List[PriceBar]]:
        """Fetch price history for multiple tickers.

        Args:
            tickers: Iterable of ticker symbols to fetch.
            start_date: Optional start date (inclusive).
            end_date: Optional end date (inclusive).

        Returns:
            Mapping of ticker symbol to list of :class:`PriceBar` objects.
        """

        results: dict[str, List[PriceBar]] = {}
        for ticker in tickers:
            results[ticker] = self.fetch_price_history(
                ticker,
                start_date=start_date,
                end_date=end_date,
            )
        return results
