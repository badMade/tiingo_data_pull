"""Pipeline orchestrating Tiingo ingestion, Notion sync, and Drive export."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Optional

from ..clients.drive_client import GoogleDriveClient
from ..clients.notion_client import NotionClient
from ..clients.tiingo_client import TiingoClient
from ..models import PriceBar
from ..utils.batching import chunked
from ..utils.file_io import write_prices_by_ticker


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for pipeline behaviour."""

    batch_size: int = 10
    output_directory: str = "exports"
    json_prefix: str = "tiingo_prices"


class TiingoToNotionPipeline:
    """Coordinates fetching from Tiingo, storing in Notion, and uploading to Drive."""

    def __init__(
        self,
        tiingo_client: TiingoClient,
        notion_client: NotionClient,
        drive_client: GoogleDriveClient,
        *,
        config: Optional[PipelineConfig] = None,
    ) -> None:
        """Initialise the pipeline with its external clients.

        Args:
            tiingo_client: Client for retrieving Tiingo data.
            notion_client: Client for reading/writing Notion pages.
            drive_client: Client for uploading JSON exports to Drive.
            config: Optional runtime configuration values.
        """

        self._tiingo_client = tiingo_client
        self._notion_client = notion_client
        self._drive_client = drive_client
        self._config = config or PipelineConfig()

    def sync(
        self,
        tickers: Iterable[str],
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        dry_run: bool = False,
    ) -> List[Path]:
        """Synchronise Tiingo data to Notion and upload batched JSON exports.

        Args:
            tickers: Iterable of ticker symbols.
            start_date: Optional inclusive start date. ``None`` fetches all available history.
            end_date: Optional inclusive end date. ``None`` fetches through the latest close.
            dry_run: If ``True``, fetch data and write JSON but skip Notion/Drive updates.

        Returns:
            List of :class:`Path` objects for each uploaded JSON file.
        """

        uploaded_files: List[Path] = []
        for ticker_batch in chunked(tickers, self._config.batch_size):
            prices_by_ticker = self._tiingo_client.fetch_price_history_bulk(
                ticker_batch,
                start_date=start_date,
                end_date=end_date,
            )
            filtered = self._filter_new_prices(prices_by_ticker, start_date=start_date, end_date=end_date)
            if not any(filtered.values()):
                continue

            if not dry_run:
                for ticker, prices in filtered.items():
                    if prices:
                        self._notion_client.create_price_pages(prices)

            json_path = write_prices_by_ticker(
                filtered,
                output_dir=self._config.output_directory,
                prefix=self._config.json_prefix,
            )
            if not dry_run:
                self._drive_client.upload_json(str(json_path))
            uploaded_files.append(json_path)
        return uploaded_files

    def _filter_new_prices(
        self,
        prices_by_ticker: Mapping[str, List[PriceBar]],
        *,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> MutableMapping[str, List[PriceBar]]:
        filtered: MutableMapping[str, List[PriceBar]] = {}
        
        def fetch_for_ticker(ticker: str) -> tuple[str, set[str]]:
            existing_dates = self._notion_client.fetch_existing_dates(
                ticker,
                start_date=start_date,
                end_date=end_date,
            )
            return ticker, existing_dates
        
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_for_ticker, ticker): ticker for ticker in prices_by_ticker.keys()}
            
            for future in as_completed(futures):
                ticker, existing_dates = future.result()
                prices = prices_by_ticker[ticker]
                filtered[ticker] = [price for price in prices if price.date.isoformat() not in existing_dates]
        
        return filtered
