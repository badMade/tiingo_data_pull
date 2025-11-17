"""Compatibility exports for the Notion integration."""
from __future__ import annotations

import logging
from copy import copy
from dataclasses import dataclass
from datetime import date
from threading import local
from typing import Dict, List, Optional, Sequence, Set

import requests

from ..models import PriceBar


@dataclass(frozen=True)
class NotionPropertyConfig:
    """Configuration for mapping price data to Notion database properties."""

    ticker_property: str = "Ticker"
    date_property: str = "Date"
    close_property: str = "Close"
    open_property: str = "Open"
    high_property: str = "High"
    low_property: str = "Low"
    volume_property: str = "Volume"
    adj_close_property: str = "Adj Close"


class NotionClient:
    """HTTP client responsible for writing price data to Notion."""

    notion_version = "2022-06-28"
    base_url = "https://api.notion.com/v1"

    def __init__(
        self,
        api_key: str,
        database_id: str,
        *,
        property_config: Optional[NotionPropertyConfig] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
        page_size: int = 50,
        max_pages: int = 4,
    ) -> None:
        """Initialise the Notion client.

        Args:
            api_key: Notion integration token.
            database_id: Target database identifier.
            property_config: Optional configuration for column names.
            session: Optional HTTP session.
            timeout: Request timeout in seconds.
            page_size: Number of rows to request per query (max 100).
            max_pages: Maximum pagination depth to stay within free tier quotas.
        """

        self._api_key = api_key
        self._database_id = database_id
        self._properties = property_config or NotionPropertyConfig()
        self._session_prototype = session or requests.Session()
        self._thread_local_session = local()
        self._timeout = timeout
        self._page_size = min(max(page_size, 1), 100)
        self._max_pages = max_pages

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Notion-Version": self.notion_version,
        }

    def fetch_existing_dates(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Set[str]:
        """Return all dates already present in Notion for the ticker.

        Args:
            ticker: Symbol to query for.
            start_date: Optional start bound.
            end_date: Optional end bound.

        Returns:
            Set of ISO-formatted date strings already persisted.
        """

        has_more = True
        cursor: Optional[str] = None
        pages_fetched = 0
        seen_dates: Set[str] = set()

        while has_more and pages_fetched < self._max_pages:
            payload: Dict[str, object] = {
                "page_size": self._page_size,
                "filter": self._build_filter(ticker, start_date, end_date),
            }
            if cursor:
                payload["start_cursor"] = cursor

            response = self._get_session().post(
                f"{self.base_url}/databases/{self._database_id}/query",
                headers=self._headers,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
            body = response.json()

            results = body.get("results", [])
            for page in results:
                date_value = self._extract_date_from_page(page)
                if date_value:
                    seen_dates.add(date_value)

            has_more = bool(body.get("has_more"))
            cursor = body.get("next_cursor")
            pages_fetched += 1

        return seen_dates

    def create_price_pages(self, prices: Sequence[PriceBar]) -> List[str]:
        """Persist price bars into Notion as individual pages.

        Args:
            prices: Sequence of price bars to persist.

        Returns:
            List of created page identifiers.
        """

        created_ids: List[str] = []
        session = self._get_session()
        for price in prices:
            payload = self._price_to_page_payload(price)
            response = session.post(
                f"{self.base_url}/pages",
                headers=self._headers,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
            created_ids.append(response.json().get("id", ""))
        return created_ids

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local_session, "session", None)
        if session is None:
            session = self._clone_session(self._session_prototype)
            setattr(self._thread_local_session, "session", session)
        return session

    @staticmethod
    def _clone_session(source: requests.Session) -> requests.Session:
        session = requests.Session()
        session.headers.update(source.headers)
        session.auth = source.auth
        session.cookies = source.cookies.copy()
        session.proxies = source.proxies.copy()
        session.verify = source.verify
        session.cert = source.cert
        session.trust_env = source.trust_env
        session.max_redirects = source.max_redirects
        session.hooks = copy(source.hooks)
        session.params = copy(source.params)
        # Preserve custom adapters (e.g., retry, cache, connection pool configs)
        for prefix, adapter in source.adapters.items():
            session.mount(prefix, NotionClient._clone_adapter(adapter))
        return session

    @staticmethod
    def _clone_adapter(adapter: requests.adapters.BaseAdapter) -> requests.adapters.BaseAdapter:
        try:
            return copy(adapter)
        except Exception as e:
            logging.warning("Failed to copy adapter %r: %s. Retrying with rebuild.", adapter, e)
            adapter_cls = type(adapter)
            adapter_kwargs: Dict[str, object] = {}
            if (max_retries := getattr(adapter, "max_retries", None)) is not None:
                try:
                    adapter_kwargs["max_retries"] = copy(max_retries)
                except Exception:
                    adapter_kwargs["max_retries"] = max_retries
            try:
                return adapter_cls(**adapter_kwargs)
            except Exception as e2:
                logging.warning(
                    "Failed to rebuild adapter of type %s: %s. Falling back to original adapter.",
                    adapter_cls.__name__,
                    e2,
                )
                return adapter

    def _build_filter(
        self,
        ticker: str,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> Dict[str, object]:
        filters: List[Dict[str, object]] = [
            {
                "property": self._properties.ticker_property,
                "title": {"equals": ticker},
            }
        ]
        if start_date:
            filters.append(
                {
                    "property": self._properties.date_property,
                    "date": {"on_or_after": start_date.isoformat()},
                }
            )
        if end_date:
            filters.append(
                {
                    "property": self._properties.date_property,
                    "date": {"on_or_before": end_date.isoformat()},
                }
            )
        if len(filters) == 1:
            return filters[0]
        return {"and": filters}

    def _extract_date_from_page(self, page: Dict[str, object]) -> Optional[str]:
        try:
            date_str = page["properties"][self._properties.date_property]["date"]["start"]
            if isinstance(date_str, str):
                return date_str
            return None
        except (KeyError, TypeError):
            return None

    def _price_to_page_payload(self, price: PriceBar) -> Dict[str, object]:
        properties: Dict[str, object] = {
            self._properties.ticker_property: {
                "title": [
                    {
                        "type": "text",
                        "text": {"content": price.ticker},
                    }
                ]
            },
            self._properties.date_property: {
                "date": {"start": price.date.isoformat()},
            },
            self._properties.close_property: {
                "number": price.close,
            },
            self._properties.open_property: {
                "number": price.open,
            },
            self._properties.high_property: {
                "number": price.high,
            },
            self._properties.low_property: {
                "number": price.low,
            },
            self._properties.volume_property: {
                "number": price.volume,
            },
        }

        if price.adj_close is not None:
            properties[self._properties.adj_close_property] = {"number": price.adj_close}

        return {
            "parent": {"database_id": self._database_id},
            "properties": properties,
        }
