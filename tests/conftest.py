import os

import pytest


def open_data(name):
    return open(os.path.join(os.path.dirname(__file__), name), "rb")


@pytest.fixture
def sqlite_db():
    return open_data("data/test.sqlite")
