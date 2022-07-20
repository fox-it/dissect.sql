from unittest.mock import Mock

from dissect.sql.sqlite3 import Column, Row


def test_row_filled_with_defaults():
    mocked_table = Mock()
    mocked_table.columns = [
        Column("test", "DEFAULT 1"),
        Column("test2", "DEFAULT 2"),
        Column("test3", "TEXT DEFAULT 'hello people'"),
    ]
    mocked_cell = Mock()
    mocked_cell.values = [20]
    result_row = Row(table=mocked_table, cell=mocked_cell)

    assert result_row.get("test") == 20
    assert result_row.get("test2") == 2
    assert result_row.get("test3") == "hello people"


def test_row_more_cells():
    mocked_table = Mock()
    mocked_table.columns = [
        Column("test", "DEFAULT 1"),
    ]
    mocked_table.primary_key = None

    mocked_cell = Mock()
    mocked_cell.values = [20, 22, 33]
    result_row = Row(table=mocked_table, cell=mocked_cell)

    assert result_row.get("test") == 20
    assert result_row._unknowns == [22, 33]
