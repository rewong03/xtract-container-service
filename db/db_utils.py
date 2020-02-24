import hashlib
import logging
import sqlite3


def create_connection(db_file):
    """Create a database connection to a SQLite database.

    Parameter:
    db_file (str): Path to database file to connect to.

    Returns:
    conn (Connection Obj.): Connection object to db_file.
    """
    conn = sqlite3.connect(db_file)
    logging.info("Connection to %s succeeded",
                 db_file)

    return conn


def create_table(conn, table_statement):
    """Creates a new table in a SQLite database.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    table_statement (str): SQLite table statement.
    """
    c = conn.cursor()
    c.execute(table_statement)
    logging.info("Successfully created table in database")


def create_table_entry(conn, **columns):
    """Creates a new entry to the containers table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. id="1234a". If no value
    for a column is passed then None is defaulted.
    """
    try:
        all_columns = ["id", "singularity_def_upload_time",
                       "singularity_def_hash", "singularity_def_location",
                       "singularity_build_time", "singularity_container_hash",
                       "singularity_container_location", "dockerfile_upload_time",
                       "dockerfile_hash", "dockerfile_location",
                       "dockerfile_build_time"]
        entry = []

        for column in all_columns:
            if column in columns:
                entry.append(columns[column])
            else:
                entry.append(None)

        entry = tuple(entry)

        sql = """INSERT INTO containers
                  VALUES(?,?,?,?,?,?,?,?,?,?,?) """

        cur = conn.cursor()
        cur.execute(sql, entry)
        conn.commit()
        logging.info("Successfully created entry to database")
    except Exception as e:
        logging.error("Exception", exc_info=True)


def update_table_entry(conn, id, **columns):
    """Updates an entry within the containers table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    **columns (str): The value to write passed with the name
    of the column to write to. E.g. id="1234a".
    id (str): ID of the entry to change.
    """
    try:
        all_columns = {"singularity_def_upload_time",
                       "singularity_def_hash", "singularity_def_location",
                       "singularity_build_time", "singularity_container_hash",
                       "singularity_container_location", "dockerfile_upload_time",
                       "dockerfile_hash", "dockerfile_location",
                       "dockerfile_build_time"}

        values = list(columns.values())
        columns = list(columns.keys())

        assert set(columns) <= all_columns, "Column does not exist in table"

        columns = " = ?,".join(columns) + " = ?"
        values.append(id)


        sql = """UPDATE containers
                 SET {}
                 WHERE id = ?""".format(columns)

        cur = conn.cursor()
        cur.execute(sql, tuple(values))
        conn.commit()
        logging.info("Successfully inserted %s into entry with id %s",
                     values[:-1], id)
    except Exception as e:
        logging.error("Exception", exc_info=True)


def select_all_rows(conn):
    """Returns all rows from containers table.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.

    Return:
    rows (list (dict)): List of dictionaries containing the
    columns and their values
    """
    all_columns = ["id", "singularity_def_upload_time",
                   "singularity_def_hash", "singularity_def_location",
                   "singularity_build_time", "singularity_container_hash",
                   "singularity_container_location", "dockerfile_upload_time",
                   "dockerfile_hash", "dockerfile_location",
                   "dockerfile_build_time"]
    rows = []

    cur = conn.cursor()
    cur.execute("SELECT * FROM containers")

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(all_columns, result)))

    return rows


def select_by_column(conn, **columns):
    """Searches containers table by values for columns.

    Parameters:
    conn (Connection Obj.): Connection object to db_file.
    columns (list (str)): Names of columns to search.
    values (list (str)): Values to search for in columns.

    Return:
    rows (list(dict)): List of rows that match the values.
    """
    try:
        all_columns = ["id", "singularity_def_upload_time",
                       "singularity_def_hash", "singularity_def_location",
                       "singularity_build_time", "singularity_container_hash",
                       "singularity_container_location", "dockerfile_upload_time",
                       "dockerfile_hash", "dockerfile_location",
                       "dockerfile_build_time"]

        values = list(columns.values())
        columns = list(columns.keys())

        assert set(columns) <= set(all_columns), "Column does not exist in table"
        assert len(values) == len(columns), "Not enough values to fill columns with"

        rows = []

        cur = conn.cursor()
        cur.execute("SELECT * FROM containers WHERE {}".format("=? AND ".join(columns) + "=?"), values)

        results = cur.fetchall()

        for result in results:
            rows.append(dict(zip(all_columns, result)))

        logging.info("Successfully queried {} columns".format(columns))

        return rows

    except Exception as e:
        logging.error("Exception", exc_info=True)


def file_hasher(file_path):
    """Hashes a file.

    Parameter:
    file_path (str): Path of file to hash.

    Returns:
    """
    block_size = 65536
    hasher = hashlib.sha256()

    with open(file_path, 'rb') as f:
        file_bytes = f.read(block_size)

        while len(file_bytes) > 0:
            hasher.update(file_bytes)
            file_bytes = f.read(block_size)

    return hasher.hexdigest()





