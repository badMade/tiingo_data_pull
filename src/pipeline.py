"""Standalone batching pipeline that exports Tiingo prices as JSON files."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
import requests
import time
from typing import Callable, Dict, Iterator, List, Sequence

from config import Settings, load_settings
from tiingo_client import fetch_prices

PriceList = List[dict]
PriceFetcher = Callable[[str, date, date], PriceList]


@dataclass(frozen=True)
class PipelineConfig:
    """Settings controlling batching and retry behaviour."""

    tickers_file: Path
    output_dir: Path
    batch_size: int
    max_retries: int
    backoff_seconds: float

    @classmethod
    def from_settings(cls, settings: Settings) -> "PipelineConfig":
        return cls(
            tickers_file=settings.tickers_file,
            output_dir=settings.output_dir,
            batch_size=settings.batch_size,
            max_retries=settings.max_retries,
            backoff_seconds=settings.backoff_seconds,
        )


class BatchPipeline:
    """Coordinates fetching Tiingo data and writing batch JSON files."""

    def __init__(self, fetcher: PriceFetcher, config: PipelineConfig) -> None:
        self._fetcher = fetcher
        self._config = config

    def run(self, start: date, end: date) -> List[Path]:
        """Fetch all tickers between ``start`` and ``end`` and persist batches."""

        tickers = _load_tickers(self._config.tickers_file)
        batch_files: List[Path] = []
        for index, batch in enumerate(_chunked(tickers, self._config.batch_size), start=1):
            batch_payload = self._fetch_batch(batch, start, end)
            path = self._write_batch(batch_payload, batch_number=index)
            batch_files.append(path)
        return batch_files

    def _fetch_batch(self, batch: Sequence[str], start: date, end: date) -> Dict[str, PriceList]:
        prices: Dict[str, PriceList] = {}
        for ticker in batch:
            prices[ticker] = self._fetch_with_retry(ticker, start, end)
        return prices

    def _fetch_with_retry(self, ticker: str, start: date, end: date) -> PriceList:
        for attempt in range(1, self._config.max_retries + 1):
            try:
                return self._fetcher(ticker, start, end)
            except Exception:  # pragma: no cover - defensive logging
                if attempt == self._config.max_retries:
                    raise
                sleep_for = self._config.backoff_seconds * (2 ** (attempt - 1))
                time.sleep(sleep_for)
        raise RuntimeError("Unreachable")

    def _write_batch(self, batch_payload: Dict[str, PriceList], *, batch_number: int) -> Path:
        output_dir = self._config.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        target = output_dir / f"prices_batch_{batch_number:03d}.json"
        target.write_text(json.dumps(batch_payload, indent=2), encoding="utf-8")
        return target


def _load_tickers(path: Path) -> List[str]:
    content = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(content, list):
        raise ValueError("Ticker file must be a JSON array.")
    tickers = [str(item).strip().upper() for item in content if str(item).strip()]
    if not tickers:
        raise ValueError("Ticker file is empty.")
    return tickers


def _chunked(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    if size <= 0:
        raise ValueError("size must be greater than zero")
    for index in range(0, len(items), size):
        yield items[index : index + size]


def run_from_env(start: date, end: date) -> List[Path]:
    """Entry point that loads configuration from the environment and runs the pipeline."""

    settings = load_settings()
    config = PipelineConfig.from_settings(settings)

    with requests.Session() as session:

        def _fetch(ticker: str, start_date: date, end_date: date) -> PriceList:
            return fetch_prices(
                ticker,
                start_date,
                end_date,
                api_key=settings.api_key,
                session=session,
            )

        pipeline = BatchPipeline(_fetch, config)
        return pipeline.run(start, end)


__all__ = [
    "BatchPipeline",
    "PipelineConfig",
    "run_from_env",
]
