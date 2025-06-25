import os
import logging
import mysql.connector
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()
log = logging.getLogger(__name__)


def get_connection():
    """Returns a new MySQL connection."""
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
    """
    Context manager that yields a MySQL cursor and commits or rollbacks transactions.

    Usage:
        with mysql_cursor() as cursor:
            cursor.execute(...)
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
