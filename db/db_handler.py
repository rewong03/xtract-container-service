import logging
import db_utils


def prep_database(db_file):
    """Establishes a connection to a SQLite database and creates a
    table within it. The table contains information about the history
    of a Singularity definition file or Dockerfile and the actions performed
    on those files.

    Parameter:
    db_file (str): Path to SQLite database.
    """
    try:
        conn = db_utils.create_connection(db_file)

        container_table = """CREATE TABLE IF NOT EXISTS containers (
                                        id text PRIMARY KEY,
                                        singularity_def_upload_time text,
                                        singularity_def_hash text,
                                        singularity_def_location text,
                                        singularity_build_time text,
                                        singularity_container_hash text,
                                        singularity_container_location text,
                                        dockerfile_upload_time text,
                                        dockerfile_hash text,
                                        dockerfile_location text,
                                        dockerfile_build_time text
                                    ); """
        db_utils.create_table(conn, container_table)

        logging.info("Succesfully prepped the database")

    except Exception as e:
        logging.error("Exception", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')

    prep_database("test.db")
    conn = db_utils.create_connection("test.db")
    db_utils.create_table_entry(conn, id="5")





