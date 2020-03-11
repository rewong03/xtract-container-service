import logging
import uuid
import pickle
import psycopg2
import psycopg2.extras
from configparser import ConfigParser


CONTAINER_TABLE = {"container_id": "UUID PRIMARY KEY",
                   "recipe_type": "TEXT", "container_name": "TEXT",
                   "container_version": "INT", "pre_containers": "TEXT []",
                   "post_containers": "TEXT []", "replaces_container": "TEXT []",
                   "s3_location": "TEXT"}

BUILD_TABLE = {"build_id": "UUID PRIMARY KEY",
               "container_id": "UUID REFERENCES container(container_id)",
               "creation_time": "TIMESTAMP", "last_built": "TIMESTAMP",
               "container_type": "TEXT", "container_size": "INT",
               "build_status": "TEXT", "container_owner": "TEXT",
               "build_location": "TEXT", "container_name": "TEXT"}

build_schema = dict(zip(BUILD_TABLE.keys(), [None] * len(BUILD_TABLE)))
container_schema = dict(zip(CONTAINER_TABLE.keys(), [None] * len(CONTAINER_TABLE)))


def config(config_file='/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/database.ini',
           section='postgresql'):
    """Reads PosrgreSQL credentials from a .ini file.

    Parameters:
    config_file (str): Path to file to read credentials from.
    section (str): Section in .ini file where credentials are located.

    Return:
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


def create_connection(config_file='/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/database.ini'):
    """Creates a connection object to a PostgreSQL database.

    Parameters:
    config_file (str): Path to file to read credentials from.

    Return:
    conn (Connection Obj.): Connection object to database.
    """
    try:
        conn = psycopg2.connect(**config(config_file=config_file))
        logging.info("Connection to database succeeded")

        return conn

    except Exception as e:
        logging.error(e)


def prep_database(conn):
    """Creates tables containing Container and Build information
    using the conn object.

    Parameter:
    conn (Connection Obj.): Connection object to database.
    """
    try:
        cur = conn.cursor()
        container_table_columns = []
        build_table_columns = []

        for column in CONTAINER_TABLE:
            container_table_columns.append(column + " " + CONTAINER_TABLE[column])

        for column in BUILD_TABLE:
            build_table_columns.append(column + " " + BUILD_TABLE[column])

        container_command = """CREATE TABLE container ({})""".format(", ".join(container_table_columns))
        build_command = """CREATE TABLE build ({})""".format(", ".join(build_table_columns))

        cur.execute(container_command)
        cur.execute(build_command)

        cur.close()
        conn.commit()

        logging.info("Succesfully created tables")

    except Exception as e:
        logging.error("Exception", exc_info=True)


def create_table_entry(conn, table_name, **columns):
    """Creates a new entry in a table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_name (str): Name of table to create an entry to. Currently
    either "container" or "build".
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. id="1234a". If no value
    for a column is passed then None is defaulted.
    """
    try:
        entry = []

        assert table_name in ["container", "build"], "Not a valid table"

        if table_name == "container":
            table = CONTAINER_TABLE
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

        psycopg2.extras.register_uuid()
        cur = conn.cursor()
        cur.execute(statement, entry)
        conn.commit()
        logging.info("Successfully created entry to {} table".format(table_name))

    except Exception as e:
        logging.error("Exception", exc_info=True)


def update_table_entry(conn, table_name, id, **columns):
    """Updates an existing table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_name (str): Name of table to create an entry to. Currently
    either "container" or "build".
    id (str): ID of the entry to change.
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. recipe="1234a".
    """
    try:
        assert table_name in ["container", "build"], "Not a valid table"

        values = list(columns.values())
        columns = list(columns.keys())

        if table_name == "container":
            table = CONTAINER_TABLE
        elif table_name == "build":
            table = BUILD_TABLE

        assert set(columns) <= set(table), "Column does not exist in table"

        columns = " = %s,".join(columns) + " = %s"
        values.append(id)

        statement = """UPDATE {}
                    SET {}
                    WHERE {}_id = %s""".format(table_name, columns, table_name)
        psycopg2.extras.register_uuid()
        cur = conn.cursor()
        cur.execute(statement, tuple(values))
        conn.commit()
        logging.info("Successfully inserted %s into entry with id %s",
                     values[:-1], id)
    except Exception as e:
        logging.error("Exception", exc_info=True)


def select_all_rows(conn, table_name):
    """Returns all rows from containers table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_name (str): Name of table to create an entry to. Currently
    either "container" or "build".

    Return:
    rows (list (dict)): List of dictionaries containing the
    columns and their values
    """
    assert table_name in ["container", "build"], "Not a valid table"

    if table_name == "container":
        table = CONTAINER_TABLE
    elif table_name == "build":
        table = BUILD_TABLE

    rows = []

    cur = conn.cursor()
    cur.execute("SELECT * FROM {}".format(table_name))

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    return rows


def search_array(conn, table_name, array, value):
    """Searches arrays for a value.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_name (str): Name of table to create an entry to. Currently
    either "container" or "build".
    array (str): Name of array column to search.
    value: Value inside of array to search for.

    Return:
    rows (list(dict)): List of rows that match the values.
    """
    try:
        assert table_name in ["container", "build"], "Not a valid table"

        if table_name == "container":
            table = CONTAINER_TABLE
        elif table_name == "build":
            table = BUILD_TABLE

        assert array in table, "Array does not exist"

        rows = []

        cur = conn.cursor()
        cur.execute("SELECT * FROM {} WHERE '{}'=ANY({})".format(table_name, value, array))

        results = cur.fetchall()

        for result in results:
            rows.append(dict(zip(table, result)))

        logging.info("Successfully queried {} array".format(array))

        return rows

    except Exception as e:
        logging.error("Exception", exc_info=True)


def select_by_column(conn, table_name, **columns):
    """Searches containers table by values for columns.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_name (str): Name of table to create an entry to. Currently
    either "container" or "build".
    **columns (str): The value to search passed with the value
    to search for. E.g. recipe="1234a".

    Return:
    rows (list(dict)): List of rows that match the values.
    """
    try:
        assert table_name in ["container", "build"], "Not a valid table"

        if table_name == "container":
            table = CONTAINER_TABLE
        elif table_name == "build":
            table = BUILD_TABLE

        values = list(columns.values())
        columns = list(columns.keys())

        assert set(columns) <= set(table), "Column does not exist in table"
        assert len(values) == len(columns), "Not enough values to fill columns with"

        rows = []

        cur = conn.cursor()
        cur.execute("SELECT * FROM {} WHERE {}".format(table_name,
                                                       "=%s AND ".join(columns) + "=%s"),
                    values)

        results = cur.fetchall()

        for result in results:
            rows.append(dict(zip(table, result)))

        logging.info("Successfully queried {} columns".format(columns))

        return rows

    except Exception as e:
        logging.error("Exception", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(filename='/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    conn = create_connection()
    # prep_database(conn)
    #
    id = uuid.uuid4()
    create_table_entry(conn, "container",
                       container_id=id,
                       recipe_type="docker",
                       container_name="good-docker",
                       container_version=1,
                       s3_location=str(id))
    import boto3

    s3 = boto3.client('s3')
    with open("/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/Dockerfile", 'rb') as f:
        s3.upload_fileobj(f, 'xtract-container-service',
                          '{}/Dockerfile'.format(id))


    print("Success!")
