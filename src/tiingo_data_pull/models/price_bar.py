"""Data models for Tiingo price data."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict


@dataclass(frozen=True)
class PriceBar:
    """Represents a single day of price data returned by Tiingo.

    Attributes:
        ticker: The symbol the price bar corresponds to.
        date: The date for the price bar.
        open: The opening price.
        close: The closing price.
        high: The high price.
        low: The low price.
        volume: The traded volume.
        adj_close: Adjusted closing price when provided by Tiingo.
    """

    ticker: str
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: float
    adj_close: float | None = None

    @classmethod
    def from_tiingo_payload(cls, ticker: str, payload: Dict[str, Any]) -> "PriceBar":
        """Create a :class:`PriceBar` from a Tiingo API response payload.

        Args:
            ticker: The ticker symbol the payload represents.
            payload: A JSON dictionary representing the Tiingo response.

        Returns:
            A populated :class:`PriceBar` instance.

        Raises:
            ValueError: If the payload does not contain a valid ``date`` field.
        """

        raw_date = payload.get("date")
        if raw_date is None:
            raise ValueError("Tiingo payload missing 'date'.")

        parsed_date = cls._parse_date(raw_date)
        return cls(
            ticker=ticker,
            date=parsed_date.date(),
            open=float(payload.get("open", 0.0)),
            close=float(payload.get("close", 0.0)),
            high=float(payload.get("high", 0.0)),
            low=float(payload.get("low", 0.0)),
            volume=float(payload.get("volume", 0.0)),
            adj_close=float(adj_close_val) if (adj_close_val := payload.get("adjClose")) is not None else None,
        )

    @staticmethod
    def _parse_date(value: str) -> datetime:
        """Parse the Tiingo date string into a :class:`datetime`.

        Args:
            value: The raw date string returned by Tiingo.

        Returns:
            A :class:`datetime` parsed from the provided value.
        """

        return datetime.fromisoformat(value)

    def to_json_dict(self) -> Dict[str, Any]:
        """Convert the price bar into a JSON serialisable dictionary.

        Returns:
            A dictionary that can be dumped directly to JSON.
        """

        return {
            "ticker": self.ticker,
            "date": self.date.isoformat(),
            "open": self.open,
            "close": self.close,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "adj_close": self.adj_close,
        }
