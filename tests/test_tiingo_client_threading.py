"""Thread-safety tests for :mod:`tiingo_data_pull.clients.tiingo_client`."""
from __future__ import annotations

import threading

import requests

from tiingo_data_pull.clients.tiingo_client import TiingoClient


def test_thread_local_sessions_are_not_shared_across_threads() -> None:
    client = TiingoClient("token", session_factory=lambda: object())
    results: list[tuple[object, object]] = [None, None]  # type: ignore[assignment]

    def worker(index: int) -> None:
        first = client._get_session()
        second = client._get_session()
        results[index] = (first, second)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results[0][0] is results[0][1]
    assert results[1][0] is results[1][1]
    assert results[0][0] is not results[1][0]


def test_thread_local_sessions_clone_provided_template_session() -> None:
    base_session = requests.Session()
    base_session.headers["X-Test"] = "value"

    client = TiingoClient("token", session=base_session)
    thread_sessions: list[requests.Session] = [None, None]  # type: ignore[assignment]

    def worker(index: int) -> None:
        thread_sessions[index] = client._get_session()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert thread_sessions[0] is not thread_sessions[1]
    assert thread_sessions[0] is not base_session
    assert thread_sessions[1] is not base_session
    assert thread_sessions[0].headers["X-Test"] == "value"
    assert thread_sessions[1].headers["X-Test"] == "value"
