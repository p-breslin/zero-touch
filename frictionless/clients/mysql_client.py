import logging
import os
from contextlib import contextmanager

import mysql.connector
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)


def get_connection():
    """Establishes a new connection to the MySQL database using environment variables.

    Environment Variables:
        MYSQL_HOST (str): Hostname or IP of the MySQL server.
        MYSQL_PORT (str or int): Port number.
        MYSQL_USR (str): Username for authentication.
        MYSQL_PWD (str): Password for authentication.
        MYSQL_DB (str): Target database name.

    Returns:
        mysql.connector.MySQLConnection: An active MySQL connection with autocommit disabled.
    """
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USR"),
        password=os.getenv("MYSQL_PWD"),
        database=os.getenv("MYSQL_DB"),
        autocommit=False,
    )


@contextmanager
def mysql_cursor():
    """Context manager that yields a MySQL cursor with automatic commit or rollback.

    Ensures the connection is properly closed after execution. Rolls back on error.

    Usage:
        with mysql_cursor() as cursor:
            cursor.execute(...)

    Yields:
        mysql.connector.cursor.MySQLCursor: A database cursor ready for queries.
    """
    conn = get_connection()
    log.debug("MySQL connection established.")
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        log.debug("MySQL connection closed.")
