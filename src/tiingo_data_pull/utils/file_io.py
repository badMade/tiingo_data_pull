"""File helpers for exporting price data."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence

from ..models import PriceBar


def write_prices_by_ticker(
    prices_by_ticker: Mapping[str, Sequence[PriceBar]],
    *,
    output_dir: str,
    prefix: str = "tiingo_prices",
) -> Path:
    """Persist grouped prices into a JSON file organised by ticker.

    Args:
        prices_by_ticker: Mapping of ticker to price bar sequences.
        output_dir: Directory where the JSON should be written.
        prefix: Prefix for the generated file name.

    Returns:
        Path to the written JSON file.
    """

    output_directory = Path(output_dir)
    output_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = output_directory / f"{prefix}_{timestamp}.json"

    serialisable: Dict[str, list[dict]] = {
        ticker: [price.to_json_dict() for price in prices]
        for ticker, prices in prices_by_ticker.items()
        if prices
    }

    with path.open("w", encoding="utf-8") as handle:
        json.dump(serialisable, handle, indent=2)

    return path
