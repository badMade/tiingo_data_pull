"""Tests covering the threaded Tiingo client implementation."""
from __future__ import annotations

from collections import defaultdict
import importlib.util
import itertools
from pathlib import Path
import sys
import threading
import types


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "src" / "tiingo_data_pull"


def _ensure_tiingo_package() -> None:
    base_name = "tiingo_data_pull"
    if base_name not in sys.modules:
        base_pkg = types.ModuleType(base_name)
        base_pkg.__path__ = [str(PACKAGE_ROOT)]
        sys.modules[base_name] = base_pkg
    else:
        base_pkg = sys.modules[base_name]

    clients_name = f"{base_name}.clients"
    if clients_name not in sys.modules:
        clients_pkg = types.ModuleType(clients_name)
        clients_pkg.__path__ = [str(PACKAGE_ROOT / "clients")]
        sys.modules[clients_name] = clients_pkg
    else:
        clients_pkg = sys.modules[clients_name]

    models_name = f"{base_name}.models"
    if models_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            models_name,
            str(PACKAGE_ROOT / "models" / "__init__.py"),
            submodule_search_locations=[str(PACKAGE_ROOT / "models")],
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[models_name] = module
        assert spec and spec.loader is not None
        spec.loader.exec_module(module)
    else:
        module = sys.modules[models_name]

    setattr(base_pkg, "clients", clients_pkg)
    setattr(base_pkg, "models", module)


_ensure_tiingo_package()

MODULE_PATH = PACKAGE_ROOT / "clients" / "tiingo_client.py"
spec = importlib.util.spec_from_file_location("tiingo_data_pull.clients.tiingo_client", MODULE_PATH)
tiingo_client_module = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(tiingo_client_module)

TiingoClient = tiingo_client_module.TiingoClient


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
