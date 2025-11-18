"""CLI integration tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tiingo_data_pull import cli


class _StubPipeline:
    """Captures the pipeline configuration provided by the CLI."""

    def __init__(self, _tiingo_client, _notion_client, *, config):
        self.config = config

    async def sync(self, *args, **kwargs):  # pragma: no cover - invoked via asyncio
        return []


def test_dry_run_skips_drive_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Ensure --dry-run executes without Drive env vars and omits the folder ID."""

    tickers_path = tmp_path / "tickers.json"
    tickers_path.write_text(json.dumps(["AAPL"]), encoding="utf-8")

    monkeypatch.setenv("TIINGO_API_KEY", "token")
    monkeypatch.setenv("NOTION_API_KEY", "notion-token")
    monkeypatch.setenv("NOTION_DATABASE_ID", "notion-db")
    monkeypatch.delenv("GOOGLE_DRIVE_FOLDER_ID", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_CLIENT_SECRETS_FILE", raising=False)

    captured = {}

    def fake_pipeline(_tiingo_client, _notion_client, *, config):
        pipeline = _StubPipeline(_tiingo_client, _notion_client, config=config)
        captured["config"] = pipeline.config
        return pipeline

    monkeypatch.setattr(cli, "TiingoClient", lambda token: object())
    monkeypatch.setattr(cli, "NotionClient", lambda config: object())
    monkeypatch.setattr(cli, "TiingoToNotionPipeline", fake_pipeline)

    cli.run(["--tickers", str(tickers_path), "--dry-run"])

    assert captured["config"].drive_folder_id is None
