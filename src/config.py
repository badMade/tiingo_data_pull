"""Configuration helpers for the standalone Tiingo batch pipeline."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, MutableMapping, Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Runtime configuration resolved from environment variables."""

    api_key: str
    tickers_file: Path = Path("all_tickers.json")
    output_dir: Path = Path("data")
    batch_size: int = 8
    max_retries: int = 3
    backoff_seconds: float = 1.0


_DEFAULTS = Settings(api_key="")


def load_settings(**overrides: Any) -> Settings:
    """Load :class:`Settings` populated from the environment and optional overrides."""

    env_values: MutableMapping[str, Any] = {
        "api_key": os.getenv("TIINGO_API_KEY", ""),
        "tickers_file": Path(os.getenv("TIINGO_TICKERS_FILE", _DEFAULTS.tickers_file)),
        "output_dir": Path(os.getenv("TIINGO_OUTPUT_DIR", _DEFAULTS.output_dir)),
        "batch_size": _parse_int(os.getenv("TIINGO_BATCH_SIZE"), _DEFAULTS.batch_size),
        "max_retries": _parse_int(os.getenv("TIINGO_MAX_RETRIES"), _DEFAULTS.max_retries),
        "backoff_seconds": _parse_float(
            os.getenv("TIINGO_BACKOFF_SECONDS"),
            _DEFAULTS.backoff_seconds,
        ),
    }
    env_values.update(overrides)

    api_key = env_values.get("api_key", "")
    if not api_key:
        raise RuntimeError(
            "Missing TIINGO_API_KEY. Export the environment variable or add it to a .env file.",
        )

    return Settings(
        api_key=str(api_key),
        tickers_file=Path(env_values["tickers_file"]),
        output_dir=Path(env_values["output_dir"]),
        batch_size=int(env_values["batch_size"]),
        max_retries=int(env_values["max_retries"]),
        backoff_seconds=float(env_values["backoff_seconds"]),
    )


def _parse_int(raw_value: Optional[str], default: int) -> int:
    if raw_value is None:
        return default
    return int(raw_value)


def _parse_float(raw_value: Optional[str], default: float) -> float:
    if raw_value is None:
        return default
    return float(raw_value)
