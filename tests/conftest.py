from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def open_data(name: str) -> Iterator[BinaryIO]:
    with (Path(__file__).parent / name).open("rb") as fh:
        yield fh


@pytest.fixture
def sqlite_db() -> Iterator[BinaryIO]:
    yield from open_data("_data/test.sqlite")


@pytest.fixture
def empty_db() -> Iterator[BinaryIO]:
    yield from open_data("_data/empty.sqlite")
