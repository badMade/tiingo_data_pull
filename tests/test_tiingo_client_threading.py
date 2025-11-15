"""Tests covering the threaded Tiingo client implementation."""
from __future__ import annotations

from collections import defaultdict
import itertools
from pathlib import Path
import sys
import threading

# Add the 'src' directory to the Python path to allow direct imports from the source tree.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tiingo_data_pull.clients.tiingo_client import TiingoClient  # noqa: E402


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:  # pragma: no cover - nothing to raise
        return None

    def json(self):
        return self._payload


class RecordingSession:
    def __init__(self, session_id: int, log, payload, thread_ids_seen, start_barrier):
        self.session_id = session_id
        self._log = log
        self._payload = payload
        self._thread_ids_seen = thread_ids_seen
        self._barrier = start_barrier

    def get(self, url: str, *, params: dict, timeout: int):
        if self._barrier is not None:
            self._barrier.wait(timeout=5)
        thread_id = threading.get_ident()
        self._thread_ids_seen.add(thread_id)
        self._log[self.session_id].add(thread_id)
        return DummyResponse(self._payload)


def test_bulk_fetch_uses_dedicated_session_per_thread():
    payload = [
        {
            "date": "2024-01-01T00:00:00",
            "open": 1,
            "close": 2,
            "high": 3,
            "low": 0.5,
            "volume": 1000,
            "adjClose": 2,
        }
    ]
    session_log = defaultdict(set)
    thread_ids_seen = set()
    counter = itertools.count()
    start_barrier = threading.Barrier(2)

    def session_factory():
        return RecordingSession(
            next(counter),
            session_log,
            payload,
            thread_ids_seen,
            start_barrier,
        )

    client = TiingoClient("demo-token", session_factory=session_factory)
    tickers = ["AAPL", "MSFT"]

    result = client.fetch_price_history_bulk(tickers, max_workers=2)

    assert set(result.keys()) == set(tickers)
    assert len(thread_ids_seen) == 2
    assert len(session_log) == 2
    assert all(len(thread_ids) == 1 for thread_ids in session_log.values())
