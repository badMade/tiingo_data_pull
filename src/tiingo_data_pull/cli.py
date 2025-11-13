"""Command line entry point for running the Tiingo to Notion pipeline."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import List, Optional

from .clients.drive_client import GoogleDriveClient
from .clients.tiingo_client import TiingoClient
from .integrations.notion_client import NotionClient, load_notion_config
from .services.pipeline import PipelineConfig, TiingoToNotionPipeline


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Sync Tiingo data into Notion and Google Drive.")
    parser.add_argument(
        "--tickers",
        type=Path,
        required=True,
        help="Path to a JSON file containing an array of ticker symbols.",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Optional start date (YYYY-MM-DD). Defaults to Tiingo's full history.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="Optional end date (YYYY-MM-DD). Defaults to the most recent close.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_parse_int_env("TIINGO_BATCH_SIZE", 10),
        help="Number of tickers to process per batch (default: 10).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.getenv("TIINGO_EXPORT_DIR", "exports")),
        help="Directory for the generated JSON exports.",
    )
    parser.add_argument(
        "--json-prefix",
        default=os.getenv("TIINGO_JSON_PREFIX", "tiingo_prices"),
        help="Filename prefix for JSON exports.",
    )
    parser.add_argument(
        "--notion-ticker-property",
        default=os.getenv("NOTION_TICKER_PROPERTY", "Ticker"),
        help="Name of the Notion text property containing the ticker symbol.",
    )
    parser.add_argument(
        "--notion-date-property",
        default=os.getenv("NOTION_DATE_PROPERTY", "Date"),
        help="Name of the Notion date property used for the trading day.",
    )
    parser.add_argument(
        "--notion-config",
        type=Path,
        default=None,
        help=(
            "Optional path to a JSON file containing the Notion database ID and property mappings."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If supplied, fetch data and write JSON but skip Notion/Drive updates.",
    )
    return parser.parse_args(argv)


def run(argv: Optional[List[str]] = None) -> None:
    """Execute the pipeline based on command line arguments."""

    args = parse_args(argv)
    tickers = _load_tickers(args.tickers)

    tiingo_api_key = _require_env("TIINGO_API_KEY")
    service_account_file = _require_env("GOOGLE_SERVICE_ACCOUNT_FILE")
    drive_folder_id = _require_env("GOOGLE_DRIVE_FOLDER_ID")

    notion_config = load_notion_config(
        config_path=args.notion_config,
        env=os.environ,
        property_overrides={
            "ticker": args.notion_ticker_property,
            "date": args.notion_date_property,
        },
    )

    tiingo_client = TiingoClient(tiingo_api_key)
    notion_client = NotionClient(notion_config)
    drive_client = GoogleDriveClient(
        service_account_file,
        folder_id=drive_folder_id,
    )
    pipeline = TiingoToNotionPipeline(
        tiingo_client,
        notion_client,
        drive_client,
        config=PipelineConfig(
            batch_size=args.batch_size,
            output_directory=str(args.output_dir),
            json_prefix=args.json_prefix,
        ),
    )

    pipeline.sync(
        tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        dry_run=args.dry_run,
    )


def _load_tickers(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("Ticker file must contain a JSON array of symbols.")
    return [str(item).upper() for item in data]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_int_env(key: str, default: int) -> int:
    """Parse an integer from an environment variable with graceful error handling."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {key} must be a valid integer, got: {value!r}"
        ) from exc


def _require_env(key: str) -> str:
    try:
        value = os.environ[key]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(f"Missing required environment variable: {key}") from exc
    if not value:
        raise RuntimeError(f"Environment variable {key} cannot be empty.")
    return value


if __name__ == "__main__":
    run()
