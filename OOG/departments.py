import logging
from dotenv import load_dotenv
from utils.helpers import get_arango_db
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DEPARTMENT_KEY = "engineering"
DEPARTMENT_NAME = "Engineering"


def import_departments():
    """
    Inserts a single 'Engineering' document into the Departments collection,unless it already exists.
    """
    db = get_arango_db()

    if not db.has_collection("Departments"):
        log.error("Collection 'Departments' does not exist.")
        return

    dept_col = db.collection("Departments")

    # Check if the 'engineering' document already exists
    if dept_col.has(DEPARTMENT_KEY):
        log.info(f"Department '{DEPARTMENT_KEY}' already exists; skipping insert.")
    else:
        try:
            dept_col.insert({"_key": DEPARTMENT_KEY, "name": DEPARTMENT_NAME})
            log.info(f"Inserted Department '{DEPARTMENT_KEY}'.")
        except Exception as e:
            log.error(f"Failed to insert Department '{DEPARTMENT_KEY}': {e}")
            raise


def main():
    import_departments()
    log.info("Department(s) imported.")


if __name__ == "__main__":
    main()
