"""Tests for batching helpers."""
from __future__ import annotations

import pytest

from tiingo_data_pull.utils.batching import chunked


def test_chunked_yields_chunks_of_requested_size() -> None:
    data = list(range(7))
    result = list(chunked(data, 3))
    assert result == [
        (0, 1, 2),
        (3, 4, 5),
        (6,),
    ]


def test_chunked_rejects_invalid_size() -> None:
    with pytest.raises(ValueError):
        list(chunked([1, 2, 3], 0))
