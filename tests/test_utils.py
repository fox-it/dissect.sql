import pytest

from dissect.sql.utils import parse_table_columns_constraints

testdata = [
    (
        "CREATE TABLE foo (column1, column2 INTEGER NOT NULL PRIMARY KEY)",
        (
            "column2",
            [
                ("column1", ""),
                ("column2", "INTEGER NOT NULL PRIMARY KEY"),
            ],
            [],
        ),
    ),
    (
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
    ),
    (
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
    ),
    (
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
    ),
    (
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
    ),
    (
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
    ),
    (
        "CREATE TABLE something (\n  column1, -- comment\n  column2\n)",
        (
            None,
            [
                ("column1", ""),
                ("column2", ""),
            ],
            [],
        ),
    ),
    (
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
    ),
]


@pytest.mark.parametrize("sql, result", testdata)
def test_parse_table_columns_constraints(sql: str, result: tuple) -> None:
    assert parse_table_columns_constraints(sql) == result
