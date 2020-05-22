import os
import logging
import psycopg2
import psycopg2.extras
from configparser import ConfigParser
from typing import *


DEFINITION_TABLE: Dict[str, str] = {"definition_id": "TEXT PRIMARY KEY",
                                    "definition_type": "TEXT", "definition_name": "TEXT",
                                    "pre_containers": "TEXT []", "post_containers": "TEXT []",
                                    "replaces_container": "TEXT []", "location": "TEXT",
                                    "definition_owner": "TEXT"}

BUILD_TABLE: Dict[str, str] = {"build_id": "TEXT PRIMARY KEY",
                               "definition_id": "TEXT REFERENCES definition(definition_id)",
                               "build_time": "TEXT", "build_version": "INT",
                               "last_built": "TEXT", "container_type": "TEXT",
                               "container_size": "INT", "build_status": "TEXT",
                               "container_owner": "TEXT", "build_location": "TEXT",
                               "container_name": "TEXT"}

build_schema: Dict[str, Union[int, None, str]] = dict(zip(BUILD_TABLE.keys(), [None] * len(BUILD_TABLE)))
definition_schema: Dict[str, Union[int, None, str]] = dict(zip(DEFINITION_TABLE.keys(), [None] * len(DEFINITION_TABLE)))
PROJECT_ROOT: str = os.path.realpath(os.path.dirname(__file__)) + "/"


def config(config_file: str = os.path.join(PROJECT_ROOT, 'database.ini'),
           section: str = 'postgresql') -> Dict[str, str]:
    """Reads PosrgreSQL credentials from a .ini file.

    Parameters:
    config_file (str): Path to file to read credentials from.
    section (str): Section in .ini file where credentials are located.

    Returns:
    credentials (dict (str)): Dictionary with credentials.
    """
    parser = ConfigParser()
    parser.read(config_file)

    credentials = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            credentials[param[0]] = param[1]

    else:
        raise Exception(f"Section {section} not found in the {config_file} file")

    return credentials


def create_connection(config_file: str = os.path.join(PROJECT_ROOT, 'database.ini')):
    """Creates a connection object to a PostgreSQL database.

    Parameters:
    config_file (str): Path to file to read credentials from.

    Returns:
    conn (Connection Obj.): Connection object to database.
    """
    conn = psycopg2.connect(**config(config_file=config_file))
    logging.info("Connection to database succeeded")

    return conn


def table_exists(table_name: str) -> bool:
    """Checks whether a table exists in the database.

    Parameters:
    table_name (str): Name of table to check exists.

    Returns:
    (bool): Whether the table exists.
    """
    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM information_schema.tables WHERE TABLE_NAME=%s", (table_name,))

    return bool(cur.rowcount)


def prep_database():
    """Creates tables containing Container and Build information
    using the conn object.
    """
    conn = create_connection()
    cur = conn.cursor()
    definition_table_columns: List[str] = []
    build_table_columns: List[str] = []

    for column in DEFINITION_TABLE:
        definition_table_columns.append(column + " " + DEFINITION_TABLE[column])

    for column in BUILD_TABLE:
        build_table_columns.append(column + " " + BUILD_TABLE[column])

    definition_command: str = f"""CREATE TABLE definition ({", ".join(definition_table_columns)})"""
    build_command: str = f"""CREATE TABLE build ({", ".join(build_table_columns)})"""

    cur.execute(definition_command)
    cur.execute(build_command)

    cur.close()
    conn.commit()

    logging.info("Succesfully created tables")


def create_table_entry(table_name: str, **columns: Union[int, str]):
    """Creates a new entry in a table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. id="1234a". If no value
    for a column is passed then None is defaulted.
    """
    conn = create_connection()
    entry: List[Union[str, int, None]] = []

    if table_name == "definition":
        table: Dict[str, str] = DEFINITION_TABLE
    elif table_name == "build":
        table: Dict[str, str] = BUILD_TABLE
    else:
        raise ValueError(f"{table_name} not a valid table")

    assert set(list(columns.keys())) <= set(table), "Column does not exist in table"

    statement: str = f"""INSERT INTO {table_name} VALUES {"(" + ", ".join(["%s"] * len(table)) + ")"}"""

    for column in table:
        if column in columns:
            entry.append(columns[column])
        else:
            entry.append(None)

    entry: Tuple[Union[str, int, None]] = tuple(entry)

    cur = conn.cursor()
    cur.execute(statement, entry)
    conn.commit()
    logging.info(f"Successfully created entry to {table_name} table")


def update_table_entry(table_name: str, id: str, **columns: Union[int, str]):
    """Updates an existing table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    id (str): ID of the entry to change.
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. recipe="1234a".
    """
    if table_name == "definition":
        table: Dict[str, str] = DEFINITION_TABLE
    elif table_name == "build":
        table: Dict[str, str] = BUILD_TABLE
    else:
        raise ValueError(f"{table_name} not a valid table")

    values: List[Union[str, int]] = list(columns.values())
    columns: List[str] = list(columns.keys())

    assert set(columns) <= set(table), "Column does not exist in table"

    columns: str = " = %s,".join(columns) + " = %s"
    values.append(id)

    statement: str = f"""UPDATE {table_name}
                      SET {columns}
                      WHERE {table_name}_id = %s"""
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(statement, tuple(values))
    conn.commit()
    logging.info(f"Successfully inserted {values[:-1]} into entry with id {id}.")


def select_all_rows(table_name: str) -> List[Dict[str, Union[int, None, str]]]:
    """Returns all rows from containers table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".

    Returns:
    rows (list (dict)): List of dictionaries containing the
    columns and their values
    """
    if table_name == "definition":
        table: Dict[str, str] = DEFINITION_TABLE
    elif table_name == "build":
        table: Dict[str, str] = BUILD_TABLE
    else:
        raise ValueError(f"{table_name} not a valid table")

    rows: List[Dict[str, Union[int, None, str]]] = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    return rows


def search_array(table_name: str, array: str, value: Union[int, str]) -> List[Dict[str, Union[int, None, str]]]:
    """Searches arrays for a value.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    array (str): Name of array column to search.
    value: Value inside of array to search for.

    Returns:
    rows (list(dict)): List of rows that match the values.
    """
    if table_name == "definition":
        table: Dict[str] = DEFINITION_TABLE
    elif table_name == "build":
        table: Dict[str] = BUILD_TABLE
    else:
        raise ValueError(f"{table_name} not a valid table")

    assert array in table, "Array does not exist"

    rows: List[Dict[str, Union[int, None, str]]] = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} WHERE '{value}'=ANY({array})")

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    logging.info(f"Successfully queried {array} array")

    return rows


def select_by_column(table_name: str,
                     **columns: Union[int, str]) -> List[Dict[str, Union[int, None, str]]]:
    """Searches table by values for columns.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    **columns (str): The value to search passed with the value
    to search for. E.g. recipe="1234a".

    Returns:
    rows (list(dict)): List of rows that match the values.
    """
    if table_name == "definition":
        table: Dict[str, str] = DEFINITION_TABLE
    elif table_name == "build":
        table: Dict[str, str] = BUILD_TABLE
    else:
        raise ValueError(f"{table_name} not a valid table")

    values: List[Union[str, int]] = list(columns.values())
    columns: List[str] = list(columns.keys())

    assert set(columns) <= set(table), "Column does not exist in table"
    assert len(values) == len(columns), "Not enough values to fill columns with"

    rows = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute(f"""SELECT * FROM {table_name} WHERE {"=%s AND ".join(columns) + "=%s"}""",
                values)

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    logging.info("Successfully queried {} columns".format(columns))

    return rows

