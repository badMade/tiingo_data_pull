"""Client for retrieving market data from Tiingo."""
from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Iterable, List, Optional

import requests
from requests import Session
import threading

from ..models import PriceBar


class TiingoClient:
    """Lightweight wrapper around the Tiingo REST API."""

    base_url = "https://api.tiingo.com/tiingo/daily"

    def __init__(
        self,
        api_key: str,
        *,
        session: Optional[Session] = None,
        session_factory: Optional[Callable[[], Session]] = None,
        timeout: int = 30,
    ) -> None:
        """Initialise the Tiingo client.

        Args:
            api_key: The Tiingo API key.
            session: Optional :class:`requests.Session` for connection pooling.
            session_factory: Optional callable returning a configured
                :class:`requests.Session`. When provided, it takes precedence
                over ``session`` and is invoked separately for each thread.
            timeout: Request timeout in seconds.
        """

        self._api_key = api_key
        if session_factory is not None:
            self._session_factory: Optional[Callable[[], Session]] = session_factory
        elif session is None:
            self._session_factory = requests.Session
        else:
            self._session_factory = None
        self._session_template = session
        self._thread_local = threading.local()
        self._timeout = timeout

    def _clone_session(self) -> Session:
        """Create a new :class:`requests.Session` using the template, if any."""

        base = self._session_template
        if base is None:
            return requests.Session()

        cloned = requests.Session()
        cloned.headers.update(base.headers)
        cloned.params.update(base.params)
        cloned.auth = base.auth
        cloned.proxies.update(base.proxies)
        cloned.hooks = {k: v[:] for k, v in base.hooks.items()}
        cloned.verify = base.verify
        cloned.cert = base.cert
        cloned.max_redirects = base.max_redirects
        cloned.trust_env = base.trust_env
        cloned.cookies.update(base.cookies)
        for prefix, adapter in base.adapters.items():
            cloned.mount(prefix, adapter)
        return cloned

    def _get_session(self) -> Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            factory = self._session_factory
            if factory is None:
                session = self._clone_session()
            else:
                session = factory()
            if session is None:
                session = requests.Session()
            self._thread_local.session = session
        return session

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

        response = self._get_session().get(
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
        max_workers: int = 10,
    ) -> dict[str, List[PriceBar]]:
        """Fetch price history for multiple tickers concurrently.

        Args:
            tickers: Iterable of ticker symbols to fetch.
            start_date: Optional start date (inclusive).
            end_date: Optional end date (inclusive).
            max_workers: Maximum number of concurrent requests (default: 10).

        Returns:
            Mapping of ticker symbol to list of :class:`PriceBar` objects.
        """

        tickers_list = list(tickers)
        results: dict[str, List[PriceBar]] = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(
                    self.fetch_price_history,
                    ticker,
                    start_date=start_date,
                    end_date=end_date,
                ): ticker
                for ticker in tickers_list
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    results[ticker] = future.result()
                except Exception as exc:
                    # Re-raise the exception to maintain existing error behavior
                    raise exc
        
        return results
