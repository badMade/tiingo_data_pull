"""Batching helpers to keep within API free tier limits."""
from __future__ import annotations

from typing import Generator, Iterable, Sequence, TypeVar

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Generator[Sequence[T], None, None]:
    """Yield the iterable in fixed-size chunks.

    Args:
        iterable: Items to chunk.
        size: The maximum number of items per chunk.

    Yields:
        Sequences of up to ``size`` elements.

    Raises:
        ValueError: If ``size`` is less than one.
    """

    if size < 1:
        raise ValueError("Chunk size must be at least one.")

    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield tuple(batch)
            batch.clear()
    if batch:
        yield tuple(batch)
