"""Integration helpers for interacting with the Notion API."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Set

from notion_client import AsyncClient
from notion_client.errors import APIResponseError

from ..models import PriceBar
from ..utils.batching import chunked

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotionPropertyMapping:
    """Maps model attributes to Notion database property names."""

    ticker: str = "Ticker"
    date: str = "Date"
    close: str = "Close"
    open: str = "Open"
    high: str = "High"
    low: str = "Low"
    volume: str = "Volume"
    adj_close: str = "Adj Close"


@dataclass(frozen=True)
class NotionDatabaseConfig:
    """Configuration for connecting to a Notion database."""

    api_key: str
    database_id: str
    properties: NotionPropertyMapping


def load_notion_config(
    *,
    config_path: Optional[Path] = None,
    env: Optional[Mapping[str, str]] = None,
    property_overrides: Optional[Mapping[str, str]] = None,
) -> NotionDatabaseConfig:
    """Create a :class:`NotionDatabaseConfig` from the environment or a JSON file.

    Args:
        config_path: Optional path to a JSON config file.
        env: Optional environment mapping used as a fallback when ``config_path``
            omits keys. Defaults to :data:`os.environ`.
        property_overrides: Optional mapping of property names that should take
            precedence over values discovered in the config file or environment.

    Returns:
        A fully populated :class:`NotionDatabaseConfig`.

    Raises:
        RuntimeError: If the API key or database ID cannot be determined.
        ValueError: If the config file exists but does not contain valid JSON.
    """

    env = env or os.environ
    file_data: Dict[str, object] = {}
    if config_path:
        path = Path(config_path)
        if not path.exists():
            raise RuntimeError(f"Notion config file not found: {config_path}")
        with path.open("r", encoding="utf-8") as handle:
            file_data = json.load(handle)

    api_key = _read_config_value("api_key", file_data, env, "NOTION_API_KEY")
    database_id = _read_config_value("database_id", file_data, env, "NOTION_DATABASE_ID")

    if not api_key:
        raise RuntimeError("Notion API key is required via NOTION_API_KEY or config file.")
    if not database_id:
        raise RuntimeError(
            "Notion database ID is required via NOTION_DATABASE_ID or config file."
        )

    properties_section = _extract_dict(file_data.get("properties", {}))
    default_mapping = NotionPropertyMapping()
    mapping_kwargs: MutableMapping[str, str] = {
        field.name: str(
            env.get(f"NOTION_{field.name.upper()}_PROPERTY")
            or properties_section.get(field.name)
            or getattr(default_mapping, field.name)
        )
        for field in fields(NotionPropertyMapping)
    }

    if property_overrides:
        for key, value in property_overrides.items():
            if key in mapping_kwargs and value:
                mapping_kwargs[key] = str(value)

    properties = NotionPropertyMapping(**mapping_kwargs)
    return NotionDatabaseConfig(api_key=api_key, database_id=database_id, properties=properties)


def _read_config_value(
    key: str,
    file_data: Mapping[str, object],
    env: Mapping[str, str],
    env_key: str,
) -> Optional[str]:
    env_value = env.get(env_key)
    if env_value:
        return env_value
    value = file_data.get(key)
    if isinstance(value, str) and value:
        return value
    return None


def _extract_dict(value: object) -> Dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


class NotionClient:
    """Wrapper around the official Notion SDK for price data persistence."""

    def __init__(
        self,
        config: NotionDatabaseConfig,
        *,
        page_size: int = 50,
        batch_size: int = 10,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialise the Notion client."""

        self._config = config
        self._client = AsyncClient(auth=config.api_key)
        self._page_size = max(1, min(page_size, 100))
        self._batch_size = max(1, batch_size)
        self._log = logger or LOGGER

    async def query_existing_dates(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Set[str]:
        """Return ISO date strings already stored for ``ticker`` within the date range."""

        seen_dates: Set[str] = set()
        start_cursor: Optional[str] = None
        has_more = True

        while has_more:
            query_kwargs: Dict[str, object] = {
                "database_id": self._config.database_id,
                "filter": self._build_filter(ticker, start_date, end_date),
                "page_size": self._page_size,
            }
            if start_cursor:
                query_kwargs["start_cursor"] = start_cursor

            try:
                response = await self._client.databases.query(**query_kwargs)
            except APIResponseError as exc:  # pragma: no cover - SDK bubble up
                self._log.error("Failed to query Notion for %s: %s", ticker, exc)
                raise

            for page in response.get("results", []):
                date_value = self._extract_date(page)
                if date_value:
                    seen_dates.add(date_value)

            has_more = bool(response.get("has_more"))
            start_cursor = response.get("next_cursor")

        return seen_dates

    async def create_price_rows(self, prices: Sequence[PriceBar]) -> List[str]:
        """Create Notion pages for the provided prices."""

        created_ids: List[str] = []
        if not prices:
            return created_ids

        for batch in chunked(prices, self._batch_size):
            tasks = [self._create_price_page(price) for price in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for price, result in zip(batch, results):
                if isinstance(result, Exception):
                    self._log.warning(
                        "Failed to create Notion row for %s on %s: %s",
                        price.ticker,
                        price.date.isoformat(),
                        result,
                    )
                    continue
                created_id = result.get("id", "")
                created_ids.append(created_id)
                self._log.debug(
                    "Created Notion row %s for %s on %s",
                    created_id,
                    price.ticker,
                    price.date.isoformat(),
                )

        return created_ids

    async def _create_price_page(self, price: PriceBar) -> Dict[str, object]:
        """Create a single Notion page for the provided price."""
        return await self._client.pages.create(
            parent={"database_id": self._config.database_id},
            properties=self._price_properties(price),
        )

    def _build_filter(
        self,
        ticker: str,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> Dict[str, object]:
        filters = [
            {
                "property": self._config.properties.ticker,
                "title": {"equals": ticker},
            }
        ]
        if start_date:
            filters.append(
                {
                    "property": self._config.properties.date,
                    "date": {"on_or_after": start_date.isoformat()},
                }
            )
        if end_date:
            filters.append(
                {
                    "property": self._config.properties.date,
                    "date": {"on_or_before": end_date.isoformat()},
                }
            )

        if len(filters) == 1:
            return filters[0]
        return {"and": filters}

    def _extract_date(self, page: Mapping[str, object]) -> Optional[str]:
        try:
            properties = page["properties"]
            date_info = properties[self._config.properties.date]
            value = date_info["date"]["start"]
        except (KeyError, TypeError):
            return None
        if isinstance(value, str):
            return value
        return None

    def _price_properties(self, price: PriceBar) -> Dict[str, object]:
        props: Dict[str, object] = {
            self._config.properties.ticker: {
                "title": [
                    {
                        "type": "text",
                        "text": {"content": price.ticker},
                    }
                ]
            },
            self._config.properties.date: {
                "date": {"start": price.date.isoformat()},
            },
            self._config.properties.open: {"number": price.open},
            self._config.properties.close: {"number": price.close},
            self._config.properties.high: {"number": price.high},
            self._config.properties.low: {"number": price.low},
            self._config.properties.volume: {"number": price.volume},
        }
        if price.adj_close is not None:
            props[self._config.properties.adj_close] = {"number": price.adj_close}
        return props
