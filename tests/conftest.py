import os
from typing import BinaryIO

import pytest


def open_data(name: str) -> BinaryIO:
    return open(os.path.join(os.path.dirname(__file__), name), "rb")


@pytest.fixture
def sqlite_db() -> BinaryIO:
    return open_data("data/test.sqlite")


@pytest.fixture
def empty_db() -> BinaryIO:
    return open_data("data/empty.sqlite")
