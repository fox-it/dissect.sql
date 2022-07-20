import pytest

from dissect.sql.sqlite3 import Column, Table


def test_column_default():
    column = Column("normal_definition", "")
    assert column.default_value is None


@pytest.mark.parametrize(
    "default_value, expected_value",
    [
        (1, 1),
        (-1, -1),
        (1.1, 1.1),
        (-1.1, -1.1),
        ("TRUE", True),
        ("FALSE", False),
        ("(TRUE)", True),
        ("(FALSE)", False),
        ('("test")', "test"),
        ("(1)", 1),
        ("'1'", "1"),
        ('"this is just a test"', "this is just a test"),
        ('"test" is not NULL', "test"),
        ("(something is not NULL)", None),
        ("(something == (DATA == (BETA not NULL)))", None),
    ],
)
def test_column_default_definitions(default_value, expected_value):
    column = Column("normal_definition", "")
    default = column._parse_default_value_from_description(f"DEFAULT {default_value}")
    assert default == expected_value


def test_parse_table_defaults():
    table_definition = """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            hello_default TEXT DEFAULT "hello" NOT NULL,
            nullable_default TEXT,
            default_integer INTEGER DEFAULT 0,
            new_test DEFAULT (1),
            extra_test DEFAULT ("hello world"),
            all_tests DEFAULT (-1.1),
            test DEFAULT (x == NULL)
        );
    """
    table = Table(sqlite=None, type_=None, name=None, table_name=None, page=None, sql=table_definition)
    assertion_data = [
        ("id", None),
        ("hello_default", "hello"),
        ("nullable_default", None),
        ("default_integer", 0),
        ("new_test", 1),
        ("extra_test", "hello world"),
        ("all_tests", -1.1),
        ("test", None),
    ]
    for key, value in assertion_data:
        assert column_name_for_key(table.columns, key).default_value == value


def column_name_for_key(columns, key):
    for column in columns:
        if column.name == key:
            return column
