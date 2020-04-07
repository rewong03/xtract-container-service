import logging
import psycopg2
import psycopg2.extras
from configparser import ConfigParser


DEFINITION_TABLE = {"definition_id": "TEXT PRIMARY KEY",
                    "definition_type": "TEXT", "definition_name": "TEXT",
                    "pre_containers": "TEXT []", "post_containers": "TEXT []",
                    "replaces_container": "TEXT []", "location": "TEXT",
                    "definition_owner": "TEXT"}

BUILD_TABLE = {"build_id": "TEXT PRIMARY KEY",
               "definition_id": "TEXT REFERENCES definition(definition_id)",
               "build_time": "TIMESTAMP", "build_version": "INT",
               "last_built": "TIMESTAMP", "container_type": "TEXT",
               "container_size": "INT", "build_status": "TEXT",
               "container_owner": "TEXT", "build_location": "TEXT",
               "container_name": "TEXT"}

build_schema = dict(zip(BUILD_TABLE.keys(), [None] * len(BUILD_TABLE)))
definition_schema = dict(zip(DEFINITION_TABLE.keys(), [None] * len(DEFINITION_TABLE)))


def config(config_file='database.ini',
           section='postgresql'):
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
        raise Exception('Section {} not found in the {} file'.format(section,
                                                                     config_file))

    return credentials


def create_connection(config_file='database.ini'):
    """Creates a connection object to a PostgreSQL database.

    Parameters:
    config_file (str): Path to file to read credentials from.

    Returns:
    conn (Connection Obj.): Connection object to database.
    """
    conn = psycopg2.connect(**config(config_file=config_file))
    logging.info("Connection to database succeeded")

    return conn


def table_exists(table_name):
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
    definition_table_columns = []
    build_table_columns = []

    for column in DEFINITION_TABLE:
        definition_table_columns.append(column + " " + DEFINITION_TABLE[column])

    for column in BUILD_TABLE:
        build_table_columns.append(column + " " + BUILD_TABLE[column])

    definition_command = """CREATE TABLE definition ({})""".format(", ".join(definition_table_columns))
    build_command = """CREATE TABLE build ({})""".format(", ".join(build_table_columns))

    cur.execute(definition_command)
    cur.execute(build_command)

    cur.close()
    conn.commit()

    logging.info("Succesfully created tables")


def create_table_entry(table_name, **columns):
    """Creates a new entry in a table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. id="1234a". If no value
    for a column is passed then None is defaulted.
    """
    assert table_name in ["definition", "build"], "Not a valid table"

    conn = create_connection()
    entry = []

    if table_name == "definition":
        table = DEFINITION_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    assert set(list(columns.keys())) <= set(table), "Column does not exist in table"

    statement = """INSERT INTO {} VALUES {}""".format(table_name,
                                                      "(" + ", ".join(["%s"] * len(table)) + ")")

    for column in table:
        if column in columns:
            entry.append(columns[column])
        else:
            entry.append(None)

    entry = tuple(entry)

    cur = conn.cursor()
    cur.execute(statement, entry)
    conn.commit()
    logging.info("Successfully created entry to {} table".format(table_name))


def update_table_entry(table_name, id, **columns):
    """Updates an existing table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    id (str): ID of the entry to change.
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. recipe="1234a".
    """
    assert table_name in ["definition", "build"], "Not a valid table"

    values = list(columns.values())
    columns = list(columns.keys())

    if table_name == "definition":
        table = DEFINITION_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    assert set(columns) <= set(table), "Column does not exist in table"

    columns = " = %s,".join(columns) + " = %s"
    values.append(id)

    statement = """UPDATE {}
                SET {}
                WHERE {}_id = %s""".format(table_name, columns, table_name)
    conn = create_connection()
    cur = conn.cursor()
    cur.execute(statement, tuple(values))
    conn.commit()
    logging.info("Successfully inserted %s into entry with id %s",
                 values[:-1], id)


def select_all_rows(table_name):
    """Returns all rows from containers table.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".

    Returns:
    rows (list (dict)): List of dictionaries containing the
    columns and their values
    """
    assert table_name in ["definition", "build"], "Not a valid table"

    if table_name == "definition":
        table = DEFINITION_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    rows = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM {}".format(table_name))

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    return rows


def search_array(table_name, array, value):
    """Searches arrays for a value.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    array (str): Name of array column to search.
    value: Value inside of array to search for.

    Returns:
    rows (list(dict)): List of rows that match the values.
    """
    assert table_name in ["definition", "build"], "Not a valid table"

    if table_name == "definition":
        table = DEFINITION_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    assert array in table, "Array does not exist"

    rows = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM {} WHERE '{}'=ANY({})".format(table_name, value, array))

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    logging.info("Successfully queried {} array".format(array))

    return rows


def select_by_column(table_name, **columns):
    """Searches table by values for columns.

    Parameters:
    table_name (str): Name of table to create an entry to. Currently
    either "definition" or "build".
    **columns (str): The value to search passed with the value
    to search for. E.g. recipe="1234a".

    Returns:
    rows (list(dict)): List of rows that match the values.
    """
    assert table_name in ["definition", "build"], "Not a valid table"

    if table_name == "definition":
        table = DEFINITION_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    values = list(columns.values())
    columns = list(columns.keys())

    assert set(columns) <= set(table), "Column does not exist in table"
    assert len(values) == len(columns), "Not enough values to fill columns with"

    rows = []

    conn = create_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM {} WHERE {}".format(table_name,
                                                   "=%s AND ".join(columns) + "=%s"),
                values)

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    logging.info("Successfully queried {} columns".format(columns))

    return rows

