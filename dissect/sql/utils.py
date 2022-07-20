import re
from typing import Optional, Tuple

from dissect.sql.exceptions import InvalidSQL


def split_sql_list(sql):
    """Split a string on comma's (`,') while ignoring any comma's contained
    within an arbitrary level of nested braces (`( )')
    """
    level = 0
    line_buf = ""
    for char in sql:
        if char == "(":
            level += 1
            line_buf += char
        elif char == ")":
            level -= 1
            line_buf += char
        elif char == "," and level == 0:
            yield line_buf.strip()
            line_buf = ""
        else:
            line_buf += char

    if level != 0:
        bracket_type = "(" if level < 0 else ")"
        raise InvalidSQL(f"Not a valid SQL list definition: {sql!r} missing {level} {bracket_type}'s")

    if line_buf:
        yield line_buf.strip()


def parse_table_columns_constraints(sql):
    """Parse SQL CREATE TABLE statements and return the primary key, column
    definitions and table constraints.

    The retrun value is a tuple of:

    (primary_key, [column, ...], [table_constraint, ...])
    where column is a tuple of:
    (column_name, column_type_constraint)
    """
    primary_key = None
    columns = []
    table_constraints = []

    # The column definitions and table constraints are a comma separated list
    # of definitions between the outer most `(...)' in the CREATE TABLE
    # statement, e.g.:
    # CREATE TABLE foo (col1, col2, CONSTRAINT)
    #
    # See https://sqlite.org/lang_createtable.html and
    # https://sqlite.org/syntax/create-table-stmt.html
    #
    # re.DOTALL is needed since the SQL statement may be formatted using
    # `\n' and we want the newline to be regarded as any other character.
    column_sql = re.search(r"\((.+)\)", sql, flags=re.DOTALL)
    if not column_sql:
        raise InvalidSQL(
            f"Not a valid CREATE TABLE definition: no column definitions or table constraints found in {sql!r}"
        )

    for column_def in split_sql_list(column_sql.groups()[0]):
        column_name, column_type_constraint = split_column_def(sql, column_def)

        if column_name.upper() == "PRIMARY":
            primary_key = get_primary_key_from_constraint(column_type_constraint, column_def, sql)
        elif "PRIMARY KEY" in column_type_constraint.upper():
            primary_key = column_name

        if column_name.upper().startswith(
            (
                "CONSTRAINT",
                "UNIQUE",
                "CHECK",
                "FOREIGN",
                "PRIMARY",
            )
        ):
            table_constraints.append(column_def)

        else:
            columns.append((column_name, column_type_constraint))

    return primary_key, columns, table_constraints


def split_column_def(sql: str, column_def: str) -> Tuple[str, str]:
    """Splits the column definition to name and constraint."""

    column_parts = column_def.split(maxsplit=1)
    if not column_parts:
        raise InvalidSQL(f"Not a valid CREATE TABLE definition: empty column definition in {sql!r}")

    column_name = column_parts[0]
    column_type_constraint = column_parts[1] if len(column_parts) > 1 else ""

    return column_name, column_type_constraint


def get_primary_key_from_constraint(column_type_constraint: str, column_def: str, sql: str) -> Optional[str]:
    """Finds a primary key from sql string."""
    primary_key = None

    primary_key_sql = re.search(r"\((.+)\)", column_type_constraint, flags=re.DOTALL)
    if not primary_key_sql:
        raise InvalidSQL(
            f"Not a valid CREATE TABLE definition: invalid PRIMARY KEY table constraint {column_def!r} in {sql!r}"
        )
    matched_group = primary_key_sql.groups()[0]
    primary_key_defs = [key_def for key_def in split_sql_list(matched_group)]
    # We only handle single primary keys, no compound keys or
    # expressions, so a single entry in the list consisting of a single
    # part.
    if len(primary_key_defs) == 1:
        primary_key_parts = primary_key_defs[0].split(maxsplit=1)
        if len(primary_key_parts) == 1:
            primary_key = primary_key_parts[0]
    return primary_key
