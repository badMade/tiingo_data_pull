"""Tests for the standalone batch pipeline module."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List

import pytest

from pipeline import BatchPipeline, PipelineConfig, _chunked


def test_chunked_splits_sequence() -> None:
    result = list(_chunked(["A", "B", "C", "D"], 2))
    assert result == [["A", "B"], ["C", "D"]]


def test_batch_pipeline_writes_files(tmp_path: Path) -> None:
    tickers = ["AAPL", "MSFT", "GOOG"]
    tickers_file = tmp_path / "tickers.json"
    tickers_file.write_text(json.dumps(tickers), encoding="utf-8")
    output_dir = tmp_path / "out"

    calls: Dict[str, int] = {}

    def fake_fetch(ticker: str, start: date, end: date) -> List[dict]:
        calls[ticker] = calls.get(ticker, 0) + 1
        return [{"ticker": ticker, "start": start.isoformat(), "end": end.isoformat()}]

    pipeline = BatchPipeline(
        fake_fetch,
        PipelineConfig(
            tickers_file=tickers_file,
            output_dir=output_dir,
            batch_size=2,
            max_retries=1,
            backoff_seconds=0,
        ),
    )

    files = pipeline.run(date(2024, 1, 1), date(2024, 1, 5))

    assert len(files) == 2
    assert all(file.exists() for file in files)
    assert set(calls) == {"AAPL", "MSFT", "GOOG"}

    with files[0].open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert isinstance(payload, dict)
    assert "AAPL" in payload or "MSFT" in payload


def test_chunked_rejects_zero_size() -> None:
    with pytest.raises(ValueError):
        list(_chunked(["AAPL"], 0))
