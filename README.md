# Tiingo Data Pull Pipeline

## Project Overview
This project automates the process of collecting historical market data from the Tiingo API, synchronising the results with a Notion database, and exporting batched JSON snapshots to Google Drive. The pipeline keeps data organised by ticker and avoids inserting duplicate rows for date ranges already present in Notion, helping teams maintain a clean, up-to-date market data journal.

## Features
- Retrieve Tiingo end-of-day price data for any list of tickers.
- Batch requests to respect API free tier rate limits.
- Skip Notion inserts when rows already exist for the selected ticker/date range.
- Persist each batch as JSON grouped by ticker and upload directly to Google Drive.
- Configurable Notion property names to match your database schema.
- Optional dry-run mode for validating configuration without writing to Notion or Drive.

## Installation
1. Create and activate a Python 3.11 virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Make the `src/` directory discoverable (one-time per shell):
   ```bash
   export PYTHONPATH="$(pwd)/src:${PYTHONPATH}"
   ```

## Configuration
Set the following environment variables before running the pipeline:

| Variable | Description |
| --- | --- |
| `TIINGO_API_KEY` | Tiingo API token. |
| `NOTION_API_KEY` | Notion integration secret. |
| `NOTION_DATABASE_ID` | Target Notion database ID. |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | Path to the Google service account JSON credentials. |
| `GOOGLE_DRIVE_FOLDER_ID` | Google Drive folder ID for uploads. |
| `TIINGO_BATCH_SIZE` | *(Optional)* Override batch size for processing tickers. |
| `TIINGO_EXPORT_DIR` | *(Optional)* Directory for generated JSON files. |
| `TIINGO_JSON_PREFIX` | *(Optional)* Prefix for JSON export filenames. |
| `NOTION_TICKER_PROPERTY` | *(Optional)* Override Notion ticker property name. |
| `NOTION_DATE_PROPERTY` | *(Optional)* Override Notion date property name. |

## Usage
1. Prepare a JSON file containing an array of tickers, for example `all_tickers.json`.
2. Execute the CLI:
   ```bash
   python -m tiingo_data_pull.cli \
       --tickers all_tickers.json \
       --start-date 2020-01-01 \
       --end-date 2024-01-01 \
       --batch-size 8 \
       --output-dir exports \
       --json-prefix tiingo_snapshot
   ```
3. To perform a dry run that only validates connectivity and filtering:
   ```bash
   python -m tiingo_data_pull.cli --tickers all_tickers.json --dry-run
   ```

## File Structure
```
.
├── all_tickers.json          # Example ticker list
├── requirements.txt          # Python dependencies
├── src/
│   └── tiingo_data_pull/
│       ├── cli.py             # Command-line interface
│       ├── clients/
│       │   ├── drive_client.py
│       │   ├── notion_client.py
│       │   └── tiingo_client.py
│       ├── models/
│       │   └── price_bar.py
│       ├── services/
│       │   └── pipeline.py
│       └── utils/
│           ├── batching.py
│           └── file_io.py
└── tests/
    ├── test_batching.py
    └── test_pipeline_filter.py
```

## Troubleshooting
- **401 Unauthorized**: Confirm your API keys and service account credentials are valid and accessible to the process.
- **Rate limiting**: Reduce `--batch-size` or increase delays between runs to stay within free tier quotas.
- **Duplicate data**: Ensure the Notion ticker and date property names match your database schema so the filtering step can detect existing rows.

## Contribution Guide
1. Fork the repository and create a feature branch.
2. Install dependencies and run the test suite with `pytest` before submitting changes.
3. Follow the existing code style (type hints, docstrings, and modular functions).
4. Submit a pull request describing your changes and any testing performed.
