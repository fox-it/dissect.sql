from dissect.sql import sqlite3
from dissect.sql.c_sqlite3 import SQLITE3_HEADER_MAGIC


def test_sqlite(sqlite_db):
    s = sqlite3.SQLite3(sqlite_db)

    assert s.header.magic == SQLITE3_HEADER_MAGIC

    tables = list(s.tables())
    assert len(tables) == 1

    table = tables[0]
    assert table.name == "test"
    assert table.page == 2
    assert [column.name for column in table.columns] == ["id", "name", "value"]
    assert table.primary_key == "id"
    assert s.table("test").__dict__ == table.__dict__

    rows = list(table.rows())
    assert len(rows) == 4
    assert rows[0].id == 1
    assert rows[0].name == "testing"
    assert rows[0].value == 1337
    assert rows[1].id == 2
    assert rows[1].name == "omg"
    assert rows[1].value == 7331
    assert rows[2].id == 3
    assert rows[2].name == "A" * 4100
    assert rows[2].value == 4100
    assert rows[3].id == 4
    assert rows[3].name == "B" * 4100
    assert rows[3].value == 4100

    assert len(rows) == len(list(table))
    assert table.row(0).__dict__ == rows[0].__dict__
    assert list(rows[0]) == [("id", 1), ("name", "testing"), ("value", 1337)]
