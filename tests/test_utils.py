from __future__ import annotations

import pytest

from dissect.sql.utils import parse_table_columns_constraints

testdata = [
    pytest.param(
        "CREATE TABLE foo (column1, column2 INTEGER NOT NULL PRIMARY KEY)",
        (
            "column2",
            [
                ("column1", ""),
                ("column2", "INTEGER NOT NULL PRIMARY KEY"),
            ],
            [],
        ),
        id="create-table-simple",
    ),
    pytest.param(
        "CREATE TABLE something (\n  column1 INTEGER NOT NULL,\n  column2 INTEGER NOT NULL,\n"
        "value,\n  PRIMARY KEY ( column1, column2)\n);",
        (
            None,
            [
                ("column1", "INTEGER NOT NULL"),
                ("column2", "INTEGER NOT NULL"),
                ("value", ""),
            ],
            [
                "PRIMARY KEY ( column1, column2)",
            ],
        ),
        id="create-table-constraint-pk",
    ),
    pytest.param(
        "GaRbAgE FoOoOoO ( \n\tcolumn1\n\tsome expression (with (nested (braces))), "
        "column2 (()), CONSTRAINT (with( some\n(braces\n)\n))\n)",
        (
            None,
            [
                ("column1", "some expression (with (nested (braces)))"),
                ("column2", "(())"),
            ],
            [
                "CONSTRAINT (with( some\n(braces\n)\n))",
            ],
        ),
        id="create-table-nested-braces",
    ),
    pytest.param(
        "CREATE TABLE bar (column1, column2 INTEGER NOT NULL, PRIMARY KEY ( column2 ))",
        (
            "column2",
            [
                ("column1", ""),
                ("column2", "INTEGER NOT NULL"),
            ],
            [
                "PRIMARY KEY ( column2 )",
            ],
        ),
        id="create-table-empty-column-constraint-pk",
    ),
    pytest.param(
        "CREATE TABLE bla (column1, column2 INTEGER NOT NULL, PRIMARY KEY ( column1, column2 ))",
        (
            None,
            [
                ("column1", ""),
                ("column2", "INTEGER NOT NULL"),
            ],
            [
                "PRIMARY KEY ( column1, column2 )",
            ],
        ),
        id="create-table-empty-column-constraint-multiple-pk",
    ),
    pytest.param(
        "CREATE TABLE bla (userId, test, UNIQUE(userId, test))",
        (
            None,
            [
                ("userId", ""),
                ("test", ""),
            ],
            [
                "UNIQUE(userId, test)",
            ],
        ),
        id="create-table-unique-constraint",
    ),
    pytest.param(
        "CREATE TABLE something (\n  column1, -- comment\n  column2\n)",
        (
            None,
            [
                ("column1", ""),
                ("column2", ""),
            ],
            [],
        ),
        id="create-table-newline-comments",
    ),
    pytest.param(
        'CREATE TABLE foo (\n  "column-1",\n  "column--2", -- comment\n  "column-`-\'-3" -- comment, "column-\\"4"\n)',
        (
            None,
            [
                ("column-1", ""),
                ("column--2", ""),
                ("column-`-'-3", ""),
            ],
            [],
        ),
        id="create-table-newline-comments-quotes-backticks",
    ),
    # Windows Index database
    pytest.param(
        "CREATE TABLE TableName (Id INTEGER PRIMARY KEY, UniqueKey TEXT NOT NULL UNIQUE, Name TEXT NOT NULL)",
        (
            "Id",
            [
                ("Id", "INTEGER PRIMARY KEY"),
                ("UniqueKey", "TEXT NOT NULL UNIQUE"),  # <- this should not end up in the constraints list
                ("Name", "TEXT NOT NULL"),
            ],
            [],
        ),
        id="create-table-unique-in-column-name",
    ),
    # Mozilla Firefox moz_places
    pytest.param(
        "CREATE TABLE moz_places (   id INTEGER PRIMARY KEY, url LONGVARCHAR, title LONGVARCHAR, rev_host LONGVARCHAR, visit_count INTEGER DEFAULT 0, hidden INTEGER DEFAULT 0 NOT NULL, typed INTEGER DEFAULT 0 NOT NULL, frecency INTEGER DEFAULT -1 NOT NULL, last_visit_date INTEGER , guid TEXT, foreign_count INTEGER DEFAULT 0 NOT NULL, url_hash INTEGER DEFAULT 0 NOT NULL , description TEXT, preview_image_url TEXT, origin_id INTEGER REFERENCES moz_origins(id))",  # noqa: E501
        (
            "id",
            [
                ("id", "INTEGER PRIMARY KEY"),
                ("url", "LONGVARCHAR"),
                ("title", "LONGVARCHAR"),
                ("rev_host", "LONGVARCHAR"),
                ("visit_count", "INTEGER DEFAULT 0"),
                ("hidden", "INTEGER DEFAULT 0 NOT NULL"),
                ("typed", "INTEGER DEFAULT 0 NOT NULL"),
                ("frecency", "INTEGER DEFAULT -1 NOT NULL"),
                ("last_visit_date", "INTEGER"),
                ("guid", "TEXT"),
                ("foreign_count", "INTEGER DEFAULT 0 NOT NULL"),  # <- this should not end up in the constraints list
                ("url_hash", "INTEGER DEFAULT 0 NOT NULL"),
                ("description", "TEXT"),
                ("preview_image_url", "TEXT"),
                ("origin_id", "INTEGER REFERENCES moz_origins(id)"),
            ],
            [],
        ),
        id="create-table-firefox-foreign-in-column-name",
    ),
]


@pytest.mark.parametrize(("sql", "result"), testdata)
def test_parse_table_columns_constraints(sql: str, result: tuple) -> None:
    assert parse_table_columns_constraints(sql) == result
