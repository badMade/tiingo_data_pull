"""Tests for pipeline filtering logic."""
from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import pytest

from tiingo_data_pull.models import PriceBar
from tiingo_data_pull.services.pipeline import PipelineConfig, TiingoToNotionPipeline



class StubTiingoClient:
    def fetch_price_history_bulk(
        self,
        tickers: Iterable[str],
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Mapping[str, List[PriceBar]]:
        raise AssertionError("Not expected to be called in filtering test")


class StubNotionClient:
    def __init__(self, existing_dates: Dict[str, Sequence[str]]) -> None:
        self._existing_dates = existing_dates
        self.created: Dict[str, List[PriceBar]] = {}

    async def query_existing_dates(
        self,
        ticker: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> set[str]:
        return set(self._existing_dates.get(ticker, []))

    async def create_price_rows(self, prices: Sequence[PriceBar]) -> List[str]:
        raise AssertionError("Not expected in filtering test")


@pytest.fixture
def existing_dates() -> Dict[str, Sequence[str]]:
    return {"AAPL": ["2024-01-01"]}


@pytest.fixture
def pipeline(existing_dates: Dict[str, Sequence[str]]) -> TiingoToNotionPipeline:
    return TiingoToNotionPipeline(
        StubTiingoClient(),
        StubNotionClient(existing_dates),
        config=PipelineConfig(batch_size=5, output_directory="/tmp", drive_folder_id="dummy"),
    )


def make_price(ticker: str, day: date) -> PriceBar:
    return PriceBar(
        ticker=ticker,
        date=day,
        open=1.0,
        close=2.0,
        high=3.0,
        low=0.5,
        volume=100,
    )


@pytest.mark.anyio("asyncio")
async def test_filter_new_prices_removes_existing(pipeline: TiingoToNotionPipeline) -> None:
    prices = {
        "AAPL": [
            make_price("AAPL", date(2024, 1, 1)),
            make_price("AAPL", date(2024, 1, 2)),
        ]
    }
    filtered = await pipeline._filter_new_prices(  # noqa: SLF001
        prices,
        start_date=None,
        end_date=None,
    )
    assert [price.date for price in filtered["AAPL"]] == [date(2024, 1, 2)]


@pytest.mark.anyio("asyncio")
async def test_filter_new_prices_handles_empty_existing() -> None:
    pipeline = TiingoToNotionPipeline(
        StubTiingoClient(),
        StubNotionClient({}),
        config=PipelineConfig(batch_size=5, output_directory="/tmp", drive_folder_id="dummy"),
    )
    prices = {
        "MSFT": [
            make_price("MSFT", date(2024, 2, 1)),
        ]
    }
    filtered = await pipeline._filter_new_prices(  # noqa: SLF001
        prices,
        start_date=None,
        end_date=None,
    )
    assert len(filtered["MSFT"]) == 1
